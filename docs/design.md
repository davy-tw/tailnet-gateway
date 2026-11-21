# tailnet-gateway design

## Goal

Expose in-cluster workloads to a Tailscale tailnet (not to the public
internet) from one Kubernetes controller, through three front doors:

1. Gateway API `Gateway`s of a GatewayClass served by this controller
2. `Ingress`es of a dedicated IngressClass
3. `Service`s of `type: LoadBalancer` with a dedicated `loadBalancerClass`

Ingresses and LoadBalancer Services are translated into Gateway API objects,
so all three share one data plane driven by Gateway API. The data plane is
the stock `tailscale/tailscale` container running `tailscale serve`, which
publishes each route hostname as a Tailscale Service (`svc:<name>`).

The design is built only on public interfaces: the Kubernetes Gateway API
specification, the Kubernetes core APIs, and Tailscale's documentation for
OAuth clients, the Tailscale API, auth keys and tags, Tailscale Services, the
`tailscale serve` config file and CLI, and the container image's environment
variables.

## Names

Everything this project owns lives under one domain,
`tailnet-gateway.davy.tw`:

| Name | Value |
| --- | --- |
| GatewayClass `controllerName` | `tailnet-gateway.davy.tw/gateway-controller` |
| IngressClass `controller` | `tailnet-gateway.davy.tw/ingress-controller` |
| Finalizer | `tailnet-gateway.davy.tw/finalizer` |
| Annotation: pick a GatewayClass (Ingress, Service) | `tailnet-gateway.davy.tw/gateway-class` |
| Annotation: Tailscale Service name and HTTP handling (LoadBalancer Service) | `tailnet-gateway.davy.tw/hostname` |
| Annotation: Tailscale Service name (TCPRoute, TLSRoute) | `tailnet-gateway.davy.tw/tailscale-service` |
| Annotation on generated objects | `tailnet-gateway.davy.tw/source: ingress` or `service` |

The install bundle ships a GatewayClass and an IngressClass named `tailnet`.

## Controller configuration

The controller runs as one Deployment with leader election (a Lease), so
more than one replica is safe. Settings come from environment variables,
loaded from an optional Secret in the controller's namespace.

| Setting | Default | Meaning |
| --- | --- | --- |
| OAuth client ID and secret | unset | Used to mint auth keys and delete devices. Without them the controller runs in manual key mode |
| Device tags (per class) | unset | Comma-separated tags requested on minted keys. Required with an OAuth client, as a default or per class |
| Proxy replicas (per class) | `2` | Proxy pods per Gateway, from 1 to 64 |
| Proxy image (per class) | a pinned `tailscale/tailscale` tag | At least v1.86.0, the first release that can host Tailscale Services |
| Drain grace period (per class) | `30s` | Pod termination grace period, covering the drain |
| GatewayClass controller name | see [Names](#names) | Every GatewayClass with this `controllerName` is served |
| Default GatewayClass | `tailnet` | Class of Gateways generated from Ingresses and Services |
| IngressClass name | `tailnet` | `spec.ingressClassName` served, also when that class is the cluster default |
| LoadBalancer class | `tailnet` | `spec.loadBalancerClass` served |
| Auth key description | `tailnet-gateway proxy` | Description on minted keys |
| Tailnet domain | learned | MagicDNS suffix such as `tail1234.ts.net` (see [Tailnet domain](#tailnet-domain)) |
| Cluster domain | `cluster.local` | Used for backend FQDNs |
| Metrics and probe addresses | `:8080`, `:8081` | Prometheus metrics, `/healthz` and `/readyz` |

### Manual key mode

Without OAuth credentials the controller logs a warning and keeps running.
The user supplies an auth key per Gateway by creating a Secret named after
the Gateway with an `authkey` entry. The controller reads it from the API
server when it needs a key; it does not watch or cache users' Secrets, and
picks up a new one on its next retry. Gateways without one stay
`Programmed=False` with reason `Pending` and a message saying that an auth
key, or OAuth, is needed. Device cleanup on deletion is skipped with a
warning; devices from ephemeral keys still expire on their own. Because
every replica uses the same key, it must be reusable. The docs recommend a
reusable, ephemeral, pre-authorized, tagged key.

### Tags

Tags decide what a proxy may do on the tailnet, so they belong to whoever
owns the GatewayClass (a cluster-scoped object, normally the platform
admin), not to whoever creates a Gateway. Every minted key requests the
class's tags, and they must be tags the OAuth client may assign. The
Tailscale API mints no untagged keys for an OAuth client, so with an OAuth
client a class that ends up with no tags (no default and none in its
parameters) is `Accepted=False` with reason `InvalidParameters`, and the
controller logs a warning at start-up when the default is empty. Manual key
mode has no such check: the user's key carries its own tags. When
the Tailscale API rejects key creation, its error is shown on the Gateway's
`Programmed` condition.

### Per-class parameters

A GatewayClass may set `spec.parametersRef` to a ConfigMap (group `""`, kind
`ConfigMap`, with a namespace). No custom resource is needed. Recognized
keys, each optional and falling back to the controller default: `tags`,
`replicas`, `image` and `drainGracePeriod`.

- Unknown keys are ignored with a warning.
- Invalid values or a missing ConfigMap make the class `Accepted=False` with
  reason `InvalidParameters`, and its Gateways are not programmed: their
  proxy StatefulSets are removed until the class is valid again. The same
  goes for a Gateway that is not accepted. State Secrets are kept, so the
  replicas come back as the same devices.
- A valid class is `Accepted=True`. The controller watches the ConfigMap and
  re-reconciles the class's Gateways when it changes.
- An empty value counts as unset and keeps the default, so a class cannot
  clear the default tags.
- Changing `tags` only affects keys minted afterwards. Existing devices keep
  their tags until their state Secret is deleted.
- Gateways cannot override class parameters. Per-Gateway overrides through
  `spec.infrastructure.parametersRef` are left for a later version.

Several classes can coexist, for example `tailnet-prod` (`tags: tag:prod`,
`replicas: 3`) and `tailnet-dev` (`tags: tag:dev`, `replicas: 1`), as long as
the OAuth client owns both tags. Ingresses and Services can pick a class with
the `gateway-class` annotation; a class that is not served by this controller
is skipped with a warning event.

## Gateway API versions

The minimum is Gateway API **v1.6.0, Standard channel**. Every resource used
is served at `v1` there, so the controller uses `v1` only, with no fallback
versions.

| Resource | Role |
| --- | --- |
| GatewayClass, Gateway, HTTPRoute | Required |
| GRPCRoute, TLSRoute, TCPRoute | Optional route kinds |
| UDPRoute | Optional, recognized only to report it as unsupported |
| ReferenceGrant | Authorizes cross-namespace backend references |
| BackendTLSPolicy | Upgrades the proxy-to-backend hop to TLS |
| ListenerSet | Optional: adds listeners to a Gateway (see [ListenerSets](#listenersets)) |
| Ingress (`networking.k8s.io/v1`), Service (`v1`) | Translated into Gateways and routes |

At startup the controller checks which of these are served. If a required
kind is missing, it logs that Gateway API v1.6.0 or newer is needed and keeps
retrying instead of crash-looping. A missing optional kind disables that
feature with one log line, and listeners report the kind as unsupported; it
never fails a Gateway. Without the ListenerSet CRD, ListenerSets are simply
not watched.

Gateways are selected by resolving their GatewayClass and comparing its
`controllerName`, and every served class gets an `Accepted` condition.

## Data plane

For a Gateway `N` in namespace `NS`, the controller server-side applies
these objects, owned by the Gateway so that they are garbage-collected with
it:

| Object | Name | Purpose |
| --- | --- | --- |
| Secret, optional | `N` | User-supplied auth key in manual key mode. Never written by the controller |
| Secret per replica | `N-<i>` | tailscaled state of replica `i`, seeded with its auth key |
| ConfigMap | `N` | The generated `tailscale serve` config |
| ServiceAccount | `N` | Identity of the proxy pods |
| Role and RoleBinding | `N` | Access to the replicas' own state Secrets only |
| StatefulSet | `N` | The proxy replicas |
| PodDisruptionBudget | `N` | At most one replica voluntarily evicted at a time |

### Why a StatefulSet

- The number of tailnet devices does not grow with the number of nodes.
- StatefulSet pod names are stable, so a restarted or rescheduled replica
  finds the same state Secret and comes back as the same device, with the
  same tailnet IP and the same Tailscale Service host approval.
- `kubectl drain` evicts and drains these pods, whereas it skips DaemonSet
  pods by default.
- Tailscale Services can have several hosts, and traffic goes to the hosts
  that are available, so high availability comes from replicas.

### Auth keys and state

Every replica is its own device and owns the state Secret `N-<i>`, which the
container uses as its Kubernetes state Secret (it writes the device ID and
FQDN there). A replica's auth key is only seeded while its state Secret
holds no device identity yet:

- **OAuth mode:** the controller mints one key per replica: single use,
  ephemeral, pre-authorized, with the configured description and tags.
- **Manual key mode:** the controller copies the user's key from Secret `N`.
- A user-supplied key is never overwritten.

When replicas are scaled down, the removed ordinals' state Secrets are
deleted once their pods are gone, and in OAuth mode their devices are
deleted through the API. In OAuth mode, a replica whose device has
disappeared from the tailnet (an ephemeral device that stayed offline too
long) is reset and re-registered.

### Proxy pods

- One container with the configured image, running tailscaled in userspace
  networking mode as a non-root user, with no Linux capabilities and the
  `RuntimeDefault` seccomp profile, so the pods meet the `restricted` Pod
  Security Standard.
- Tailnet hostname `<NS>-<N>-<i>`.
- The serve config ConfigMap is mounted and passed through the image's
  serve-config environment variable, so the container applies it on start
  and on change.
- Pod name, UID and IP come from the downward API, as the image expects.
- Advertisement follows the mounted config, not only the one a replica
  starts with. Once a replica runs the mounted config, the readiness probe
  runs `tailscale serve advertise svc:<name>` for every Tailscale Service in
  it that is not advertised yet, and `tailscale serve drain svc:<name>` for
  every advertised one no longer in it, so adding, renaming or removing a
  listener hostname takes effect without restarting the replicas. Until the
  config runs, the probe only drains advertised names found in neither the
  mounted nor the running config (stale ones, which a replica killed
  without its stop hook keeps in its state Secret). The image's own
  auto-advertisement is turned off: it only follows the config the
  container starts with, and could advertise again during a drain. The
  advertised list is read from `tailscale debug prefs`; if its output has
  no `AdvertiseServices` field, the replica stays not ready rather than
  advertising blindly, so the parsing must be rechecked when the pinned
  image is upgraded. The probe has a 10 s timeout, since a run that adds
  Services starts one CLI process per Service.
- Before stop, a hook marks the replica as draining, runs
  `tailscale serve drain svc:<name>` for each Service in the config or
  advertised (ignoring failures), waits out any probe run already in
  progress (the probe timeout plus one second), drains again, and waits for
  the rest of the drain window, so `terminationGracePeriodSeconds` covers
  it. The second drain covers a probe run that passed the marker check
  before the hook created it, or whose write undid the first drain (the CLI
  rewrites the whole advertised list). A draining replica is not ready, and
  the probe checks the marker again before each advertise.
- A replica is ready once tailscaled is up, runs the mounted serve config
  and advertises exactly its Services. `tailscale serve status --json` must
  hold the same strings (keys, hostnames, paths and targets) as the mounted
  file, so any change of backend, path, port or mode counts, and the
  `AdvertiseServices` prefs must list the config's Services. Having a
  tailnet IP alone is not enough, since the Gateway counts ready replicas
  and the PDB lets another replica go.
- Soft pod anti-affinity on `kubernetes.io/hostname` and topology spread
  across zones keep one node or zone from taking out every replica.
- Pod management policy `Parallel`: replicas are independent.
- The selector uses a fixed app label and an instance label equal to the
  Gateway name. It is immutable, so other labels never go into it.
- Pods get the labels of the Gateway's `spec.infrastructure.labels`, not
  the Gateway's own labels, which could make them match the selector of an
  unrelated Service.
- Gateway names are limited to 52 characters: the StatefulSet's pods carry
  a `<name>-<hash>` revision label, and label values are limited to 63.
  Longer names leave the Gateway `Programmed=False`.
- An existing object with a data-plane name that the Gateway does not
  control is never taken over (it would be garbage-collected with the
  Gateway); the Gateway stays `Programmed=False` and says why.

### RBAC

Each Gateway's Role allows get, update and patch only on its own state
Secrets (`resourceNames` `N-0` to `N-<replicas-1>`), plus the event access
the image needs. No proxy gets cluster-wide Secret access.

The controller's ClusterRole covers the Gateway API kinds (ListenerSets
included) and their status,
Ingresses and their status, Services and their status, Secrets, ConfigMaps,
ServiceAccounts, Roles, RoleBindings, StatefulSets, PodDisruptionBudgets,
Leases and Events.

### Disruptions

The PodDisruptionBudget (`maxUnavailable: 1`) makes voluntary evictions such
as `kubectl drain`, autoscaler scale-down and node upgrades take at most one
replica at a time. The evicted replica drains its Tailscale Services,
finishes existing connections and exits while the others keep serving. It
is recreated elsewhere with the same name and state Secret. State lives in a
Secret, not a volume, so pods are not pinned to a zone. With one replica a
PDB cannot prevent an outage and could only block drains, so none is
created.

## Route attachment

- A route attaches to a Gateway when one of its `parentRefs` names the
  Gateway (the namespace defaults to the route's namespace), and a listener
  allows it: `sectionName`, `port`, `allowedRoutes` namespaces and kinds, and
  hostname intersection are all evaluated.
- Each route gets a `RouteStatus.parents[]` entry with `Accepted` and
  `ResolvedRefs`, and each listener reports `attachedRoutes`.
- The controller watches routes, ListenerSets, ReferenceGrants, BackendTLSPolicies,
  Services, Namespaces and the owned objects, and enqueues the affected
  Gateways. Its cache holds only the Secrets it labels as its own (the
  state Secrets), never the rest of the cluster's Secrets.

## ListenerSets

A ListenerSet adds listeners to the Gateway its `parentRef` names, so that
for example a team can expose its own Tailscale Service names on a shared
Gateway without editing it. Its listeners are served by that Gateway's
proxy: there is no data plane per ListenerSet.

- A Gateway allows no ListenerSet unless its `spec.allowedListeners`
  says so: `from: Same` (the Gateway's namespace), `All`, or `Selector`
  (namespace labels). Others get `Accepted=False` and `Programmed=False`
  with reason `NotAllowed`, and their listeners are not used.
- Listeners are merged in the Gateway API precedence order: the Gateway's
  own, then ListenerSets oldest first, then by namespace/name. A ListenerSet
  listener that conflicts with an earlier one (another protocol on the same
  port, or the same protocol, port and hostname) gets `Accepted=False`,
  `Conflicted=True` and `Programmed=False` with reason `ProtocolConflict` or
  `HostnameConflict`, and the earlier listener keeps serving. Conflicts
  among the Gateway's own listeners still leave all of them out.
- A ListenerSet is `Accepted=True` when at least one of its listeners is
  valid (reason `ListenersNotValid` when some are not), and
  `Accepted=False` with `ListenersNotValid` when none is, or with
  `ParentNotAccepted` when the Gateway is not accepted.
  `Programmed` follows the Gateway's (`ParentNotProgrammed` until then).
  The Gateway's `status.attachedListenerSets` counts the accepted ones;
  its `status.listeners` lists only its own listeners, and each
  ListenerSet lists its own in `status.listeners`.
- Routes attach to ListenerSet listeners only through a parentRef of kind
  `ListenerSet` (optionally with `sectionName` or `port`), and to the
  Gateway's own listeners only through a parentRef to the Gateway. The
  route's status entry names the parent it used. `allowedRoutes` with
  `from: Same` on a ListenerSet listener means the ListenerSet's namespace.
- Tailscale Service names work as for the Gateway's own listeners; a route
  without hostnames on a listener without a hostname still uses the
  Gateway's name.
- When the Gateway is deleted, or its GatewayClass disappears, its
  ListenerSets get `Accepted=False` with reason `ParentNotAccepted`.
- Listener `certificateRefs` are ignored on ListenerSets too, so no
  ReferenceGrant is checked for them.

## Translation to the serve config

The ConfigMap holds a `tailscale serve` config in its Services form: a map
from Tailscale Service name to that service's TCP ports and web handlers.
Every replica of a Gateway advertises every Tailscale Service in it.

### What Tailscale Services can express

A Tailscale Service can expose, per port, `http`, `https` (TLS terminated by
tailscaled with the tailnet's automatic certificate), a raw `tcp` forward,
or `tls-terminated-tcp`. HTTP(S) ports support several path mount points
with prefix matching. Backends can be `http`, `https` or `https+insecure`.
Transport is TCP only. Each Tailscale Service has its own virtual IP and
MagicDNS name and can listen on several ports.

| Route kind | How it maps | Not expressible, reported on the route |
| --- | --- | --- |
| HTTPRoute | Each hostname is a Tailscale Service. HTTP and HTTPS listener ports become `http` and `https` ports. `PathPrefix` matches become mount points | `Exact` and `RegularExpression` paths, header, query and method matches, traffic splitting, filters, timeouts, retries |
| GRPCRoute | As HTTPRoute, on HTTPS listeners only. Service and service/method matches become the path prefixes `/<service>/` and `/<service>/<method>` | Header matches, regular expression matches, gRPC on HTTP listeners |
| TCPRoute | Listener port becomes a `tcp` forward to the single backend | Several or weighted backends |
| TLSRoute, passthrough | Each hostname becomes a Tailscale Service with a `tcp` forward, so the service name does the SNI selection | SNI routing inside one Tailscale Service |
| TLSRoute, `Terminate` | `tls-terminated-tcp` on the listener port | Listener `certificateRefs`; certificates always come from Tailscale |
| UDPRoute, UDP listeners | Not supported: Tailscale Services carry TCP only | Everything |

What cannot be expressed is reported (`Accepted`, `ResolvedRefs`, or
`PartiallyInvalid` when the rest of the route still works) instead of being
dropped silently. One route never fails the whole Gateway.

### Tailscale Service names

- HTTPRoute and GRPCRoute: one Tailscale Service per hostname, `svc:<label>`.
  A route without hostnames uses the Gateway's name.
- TLSRoute: one Tailscale Service per hostname. TCPRoute: one per route,
  named after the route or the `tailscale-service` annotation.
- Tailscale Service names are single DNS labels. A hostname must be a single
  label (`demo`) or a name under the tailnet (`demo.tail1234.ts.net`); both
  become `svc:demo`, reachable at `demo.<tailnet>.ts.net`. Anything else,
  including a name under another tailnet, is reported on the route.
- When routes of one Gateway compete for the same Tailscale Service,
  compatible routes are merged. Where they claim the same path or port, the
  oldest route wins and newer routes get `Accepted=False` with reason
  `Conflicted`.
- Tailscale Service names are tailnet-wide, so a name belongs to one
  Gateway: if two Gateways advertised it, its traffic would be spread over
  both Gateways' backends. The oldest Gateway holding a name keeps it, and
  routes of newer Gateways that map to it get `Conflicted`. A Gateway holds
  the names it serves and the ones waiting for the tailnet domain; each
  Gateway lists them in an annotation of its serve config ConfigMap. Only a
  ConfigMap named after its controlling Gateway, whose class this controller
  serves, and that this controller applied counts, so a hand-made copy
  cannot reserve names.
- Tailscale Services must be defined, and their hosts approved, in the
  tailnet. The controller does not manage them. Hosts must be tagged
  devices, and new replicas wait for approval unless the tailnet policy has
  an `autoApprovers` entry for `svc:<name>` and the proxies' tag. The setup
  docs show the policy snippets.

### Listeners and rules

- HTTP listeners serve plain HTTP on their port; HTTPS listeners serve HTTPS
  with TLS terminated by Tailscale. `tls.certificateRefs` are ignored.
- Each (hostname, listener port) pair gets its own handler set.
- A rule without matches maps to `/`. Each `PathPrefix` match is a mount
  point forwarding to the backend, keeping the request path.
- One backend per rule. Extra backends with weight 0 are allowed, since they
  receive no traffic.
- Backend targets are always fully qualified:
  `<name>.<namespace>.svc.<cluster-domain>:<port>`, with the namespace
  defaulting to the route's namespace.
- A backend that cannot be used (not found, not permitted, or with a
  BackendTLSPolicy that cannot be honored) keeps its mount point, pointed at
  an address where nothing listens, so its requests fail instead of falling
  through to a shorter path prefix that belongs to another backend.
- TCPRoute and TLSRoute take the port of the listener they attach to.

### BackendTLSPolicy

A BackendTLSPolicy in the backend's namespace that targets the backend
Service (honoring `sectionName`) switches that backend to `https`. The serve
config can only verify against the system roots and the backend's own name,
so a policy is honored when it uses `wellKnownCACertificates: System`, a
`hostname` equal to the backend's cluster FQDN and no `subjectAltNames`.
Any other policy is not downgraded to TLS without verification: it gets
`Accepted=False` with reason `Invalid`, the routes using that backend get
`ResolvedRefs=False`, and the backend is not used.

### ReferenceGrant

A backend in another namespace is allowed only if a ReferenceGrant in that
namespace lets the route's kind and namespace refer to that Service.
Otherwise only that route fails, with `ResolvedRefs=False` and reason
`RefNotPermitted`.

### Tailnet domain

A Tailscale Service's web handlers are keyed by
`<label>.<tailnet>.ts.net:<port>`, so the controller needs the tailnet's
MagicDNS suffix. It learns it from the device FQDN in the first replica's
state Secret, or takes it from the tailnet domain setting. Until it is
known, web handlers are left out and the Gateway stays `Programmed=False`.

## Gateway status

- `Accepted=True` once the Gateway's class is served and valid, or
  `Accepted=False` with the reason.
- `Programmed=False` with reason `Pending` while a step fails (minting keys,
  applying objects) or no replica is ready, and rechecked every few seconds.
  `Programmed=True` once at least one replica is ready. A step that fails
  while replicas are ready (a key that could not be minted while scaling
  up, an API rate limit) keeps `Programmed=True` and the addresses, since
  those replicas still serve, and the error goes into the message. Both
  messages carry the ready and desired counts, so a degraded Gateway is
  visible.
- Listener status: kinds supported per protocol (HTTP and HTTPS: HTTPRoute
  and GRPCRoute; TLS: TLSRoute; TCP: TCPRoute), `attachedRoutes`, and
  `Accepted=False` with `UnsupportedProtocol` for UDP.
- Addresses: one `Hostname` address `<label>.<tailnet>.ts.net` for every
  Tailscale Service of the Gateway.
- Every condition carries `observedGeneration`.

## Ingress translation

Applies to Ingresses whose `spec.ingressClassName` is the configured
IngressClass, or that have none while that class is the cluster default,
and only while that IngressClass names this controller in
`spec.controller`. An Ingress without `spec.ingressClassName` that sets the
deprecated `kubernetes.io/ingress.class` annotation belongs to the class it
names: it is served only when that is the configured class, never as a
default-class Ingress.
For Ingress `I`, the controller applies a Gateway `I` and HTTPRoutes owned by
the Ingress and annotated `source: ingress`:

- The Gateway has an `https` listener on 443 when the Ingress has any `tls`
  entry, and an `http` listener on 80 otherwise.
- One HTTPRoute per Ingress host, with one rule per path. `Prefix`,
  `ImplementationSpecific` and unset path types become `PathPrefix`. `Exact`
  stays `Exact`, so the HTTPRoute reports it as unsupported, and a warning
  event is recorded on the Ingress.
- Named ports are resolved through the Service. A host with a backend that
  cannot be used (not a Service, or an unknown named port) is not served
  until it resolves, with a warning event: leaving out only that path would
  send its requests to a shorter prefix of another backend.
- Rules without a host go to the route without hostnames, as with other
  Ingress controllers, where a host's own rules take precedence.
- `defaultBackend` takes the requests no rule matches: it is added as a
  `/` rule to every host that has none, and to a route without hostnames
  for hosts without rules.
- The Gateway's addresses are written to `status.loadBalancer.ingress[]`.
- When the Ingress is deleted or stops being served, the generated objects
  are deleted.

## LoadBalancer Service translation

Applies to Services of `type: LoadBalancer` whose `loadBalancerClass` is the
configured LoadBalancer class. The controller adds its finalizer to the
Service.

- The `hostname` annotation names the Tailscale Service (default: the
  Service's name). It is a single DNS label or a name under the tailnet,
  reduced to its first label; anything else is reported in a warning event
  and the Service is not exposed. It also switches on HTTP handling for one
  port: the first TCP
  port whose `appProtocol` is `http` or `kubernetes.io/ws`, or else the
  first TCP port whose `appProtocol` is not `kubernetes.io/h2c`, `https` or
  `kubernetes.io/wss`, gets an HTTP listener and an HTTPRoute. h2c needs
  HTTP/2 without TLS, which a Tailscale Service HTTP port does not accept,
  and TLS backends expect the client's TLS, so those ports stay raw.
- Every other TCP port becomes a TCPRoute on the same Tailscale Service.
  UDP and SCTP ports are skipped with a warning event.
- Generated objects are owned by the Service and annotated
  `source: service`: a Gateway named after the Service with one listener per
  handled port (`http-80`, `tcp-5432`, ...), HTTPRoute `<svc>-http-<port>`
  and TCPRoute `<svc>-tcp-<port>`. The Service is
  the backend of its own Gateway, so traffic goes through its cluster IP.
- Routes for ports removed from the Service are pruned.
- The Gateway's addresses are written to `status.loadBalancer.ingress[]`.
- On deletion, or when the Service stops being served, the generated objects
  are deleted and the finalizer is removed. When it can no longer be exposed
  (its GatewayClass is not served, or no port is left), the generated
  objects are deleted and its load balancer status is cleared.

## Gateway deletion

The controller adds its finalizer to each Gateway, and records the device
ID of every replica in the Gateway's `tailnet-gateway.davy.tw/devices`
annotation as soon as a state Secret holds one. On deletion it:

1. deletes the proxy StatefulSet and waits until its pods are gone, so the
   replicas drain while their devices still serve;
2. in OAuth mode, deletes every device listed in the state Secrets or the
   annotation through the Tailscale API (in manual key mode it skips this
   with a warning). The annotation is what keeps this working under
   foreground cascading deletion, where garbage collection removes the
   state Secrets before the finalizer runs;
3. removes the finalizer and lets garbage collection remove the owned
   objects.

A missing state Secret means there is nothing to clean up and never blocks
deletion. Ephemeral devices that go offline are also removed by Tailscale
after a while. A Gateway that moves to another controller's class is
cleaned up the same way, and its generated objects and state Secrets are
deleted too.

Devices are only deleted with the Gateway. While a GatewayClass has
Gateways, the controller keeps the standard
`gateway-exists-finalizer.gateway.networking.k8s.io` finalizer on it. If the
class disappears anyway, its Gateways are treated like Gateways of an
invalid class: `Accepted=False` with reason `Pending`, the StatefulSets
removed, and the state Secrets and devices kept, so the replicas come back
as the same devices when the class is recreated.

## Operations

- Metrics and liveness and readiness probes on the configured addresses.
- Install bundle: namespace, Deployment, ServiceAccount, RBAC, metrics
  Service, and the `tailnet` GatewayClass and IngressClass. The same objects
  are also packaged as a Helm chart, published with the image to GHCR on
  each release tag. A demo Gateway
  and HTTPRoute split `/api` and `/` across two backends for smoke tests.
- Integration tests run against a real API server (envtest). End-to-end
  tests run against a real cluster behind a build tag. CI runs them on kind
  and on k3s, installed with the Helm chart, without a tailnet: manual key
  mode with a fake key checks admission under the restricted Pod Security
  Standard, translation, and deletion with real garbage collection.

## Out of scope

- The Gateway API conformance suite.
- Listener `tls.certificateRefs`.
- Mirroring, redirects, rewrites, header modifiers, timeouts, retries,
  session persistence.
- Managing tailnet policy, Tailscale Service definitions or approvals.
- More than one tailnet per controller.

## Risks checked before implementation

Two assumptions were prototyped first; see [spike-findings.md](spike-findings.md):

1. Whether a serve config can proxy straight to a cluster DNS name, or needs
   a local forwarding sidecar.
2. Whether gRPC works end to end through a Tailscale Service.
