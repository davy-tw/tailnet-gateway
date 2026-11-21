# tailnet-gateway

A Kubernetes controller that exposes in-cluster workloads to your
[Tailscale](https://tailscale.com) tailnet (not the public internet) as
**Tailscale Services**, through three front doors:

1. Gateway API `Gateway`s of a managed GatewayClass (HTTPRoute, GRPCRoute,
   TLSRoute, TCPRoute)
2. `Ingress`es of the `tailnet` IngressClass
3. `Service`s of `type: LoadBalancer` with `loadBalancerClass: tailnet`

Ingresses and LoadBalancer Services are translated into Gateways and routes,
so all three share one data plane: a StatefulSet per Gateway running the
stock `tailscale/tailscale` image with a generated `tailscale serve` config.
Every replica is its own tailnet device and hosts every Tailscale Service of
the Gateway, so replicas give high availability.

```
tailnet client ──> svc:demo (demo.<tailnet>.ts.net)
                     ├─ proxy replica 0 ─┐
                     └─ proxy replica 1 ─┴─> api.demo.svc.cluster.local:8080 (/api)
                                            web.demo.svc.cluster.local:80   (/)
```

## Requirements

- Kubernetes with the Gateway API **v1.6.0 or newer, Standard channel** CRDs
  (only `v1` APIs are used; tested with v1.6 and v1.7)
- Tailscale image **v1.86.0 or newer** for the proxies (default
  `tailscale/tailscale:v1.104.7`)
- A Tailscale OAuth client, or one auth key per Gateway ([setup](docs/setup.md))

## Install

```sh
kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.6.3/standard-install.yaml
kubectl apply -f dist/install.yaml
kubectl -n tailnet-gateway-system create secret generic tailnet-gateway \
  --from-literal=TAILSCALE_OAUTH_CLIENT_ID=... --from-literal=TAILSCALE_OAUTH_CLIENT_SECRET=...
# The settings are read at start-up: restart after creating or changing them.
kubectl -n tailnet-gateway-system rollout restart deployment/tailnet-gateway
kubectl apply -f examples/demo.yaml
```

### With Helm

Released charts are published to GHCR as OCI artifacts:

```sh
kubectl apply --server-side -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.6.3/standard-install.yaml
kubectl create namespace tailnet-gateway-system
kubectl -n tailnet-gateway-system create secret generic tailnet-gateway \
  --from-literal=TAILSCALE_OAUTH_CLIENT_ID=... --from-literal=TAILSCALE_OAUTH_CLIENT_SECRET=...
helm install tailnet-gateway oci://ghcr.io/davy-tw/charts/tailnet-gateway \
  --version <version> --namespace tailnet-gateway-system \
  --set oauth.existingSecret=tailnet-gateway --set proxy.tags=tag:k8s
```

Every setting in [Configuration](#configuration) has a value in
[`charts/tailnet-gateway/values.yaml`](charts/tailnet-gateway/values.yaml). The
chart also installs the `tailnet` GatewayClass and IngressClass
(`gatewayClass.create`, `ingressClass.create`, `ingressClass.default`). It
does not install the Gateway API CRDs.

Then define `svc:demo` in your tailnet and let it auto-approve the proxies'
tag; see [docs/setup.md](docs/setup.md). Once a replica is up, the Gateway
reports `Programmed=True` and its address `demo.<tailnet>.ts.net`.

## Configuration

The controller reads environment variables (the install bundle loads them
from the optional Secret `tailnet-gateway`). Settings marked "per class" are
defaults that a GatewayClass can override.

| Variable | Default | Meaning |
| --- | --- | --- |
| `TAILSCALE_OAUTH_CLIENT_ID`, `TAILSCALE_OAUTH_CLIENT_SECRET` | unset | OAuth client for minting auth keys and deleting devices. Without them the controller runs in manual key mode |
| `PROXY_TAGS` (per class) | unset | Comma-separated tags requested on minted keys. Unset: the OAuth client's tags apply |
| `PROXY_REPLICAS` (per class) | `2` | Proxy pods per Gateway (1 to 64) |
| `PROXY_IMAGE` (per class) | `tailscale/tailscale:v1.104.7` | Proxy image |
| `PROXY_DRAIN_GRACE_PERIOD` (per class) | `30s` | Pod termination grace period, covering the drain |
| `GATEWAY_CONTROLLER_NAME` | `tailnet-gateway.davy.tw/gateway-controller` | Every GatewayClass with this `controllerName` is served |
| `DEFAULT_GATEWAY_CLASS` | `tailnet` | Class for Gateways generated from Ingresses and Services |
| `INGRESS_CLASS` | `tailnet` | IngressClass served (also when it is the cluster default) |
| `LOAD_BALANCER_CLASS` | `tailnet` | `loadBalancerClass` served |
| `AUTH_KEY_DESCRIPTION` | `tailnet-gateway proxy` | Description on minted keys |
| `TAILNET_DOMAIN` | learned | MagicDNS suffix (`tail1234.ts.net`). Learned from the first replica that logs in when unset |
| `CLUSTER_DOMAIN` | `cluster.local` | Used for backend FQDNs |
| `METRICS_BIND_ADDRESS`, `HEALTH_PROBE_BIND_ADDRESS` | `:8080`, `:8081` | Metrics and `/healthz`, `/readyz` |

### Per-class parameters

A GatewayClass may point `spec.parametersRef` at a ConfigMap (group `""`,
kind `ConfigMap`, with a namespace) with any of `tags`, `replicas`, `image`
and `drainGracePeriod`. Invalid values set `Accepted=False` with reason
`InvalidParameters` on the class, and its Gateways stop serving until the
class is valid again. See
`examples/classes.yaml`. Tags are under the class owner's control: Gateways
cannot override them.

### Annotations

| Annotation | On | Meaning |
| --- | --- | --- |
| `tailnet-gateway.davy.tw/gateway-class` | Ingress, Service | Use another managed GatewayClass |
| `tailnet-gateway.davy.tw/hostname` | LoadBalancer Service | Tailscale Service name; also serves one port over HTTP: the first with `appProtocol` `http` or `kubernetes.io/ws`, or else the first that is not `kubernetes.io/h2c`, `https` or `kubernetes.io/wss`. Other ports are forwarded as raw TCP |
| `tailnet-gateway.davy.tw/tailscale-service` | TCPRoute, TLSRoute | Tailscale Service name instead of the route name |

## What is supported

Tailscale Services carry TCP only, and `tailscale serve` maps paths to
backends by prefix. The controller reports anything it cannot express on
the route (`Accepted`, `ResolvedRefs`, `PartiallyInvalid`) instead of
silently dropping it.

| Feature | Support |
| --- | --- |
| HTTP and HTTPS listeners | Yes. HTTPS terminates with the tailnet's automatic certificate; `certificateRefs` are ignored |
| HTTPRoute `PathPrefix` matches, rules without matches | Yes |
| `Exact`/`RegularExpression` paths, header/query/method matches, filters, timeouts, retries | No: reported as `PartiallyInvalid` |
| One backend per rule (others with weight 0 are fine) | Yes. Traffic splitting is not supported |
| GRPCRoute | Yes, on **HTTPS listeners only** (Tailscale Services negotiate HTTP/2 only over TLS). Exact service and service/method matches are supported |
| TCPRoute | Yes, raw TCP forward |
| TLSRoute | Passthrough (raw forward) and Terminate (`tls-terminated-tcp`), one Tailscale Service per hostname |
| UDP listeners and UDPRoute | No: Tailscale Services are TCP-only |
| Cross-namespace backends | With a ReferenceGrant |
| `allowedRoutes` (namespaces and kinds), `sectionName`, `port`, hostname intersection | Yes |
| BackendTLSPolicy | Upgrades the backend hop to verified TLS. Only `wellKnownCACertificates: System` with a `hostname` equal to the backend's cluster FQDN can be honored; other policies are `Accepted=False` and their backend is not used |
| Route conflicts | Oldest route wins a path or port; newer routes report `Conflicted` |
| Tailscale Service names across Gateways | A name belongs to the oldest Gateway serving it; routes of newer Gateways report `Conflicted` |

Hostnames must be single DNS labels (`demo`) or names under the tailnet
(`demo.tail1234.ts.net`): each becomes the Tailscale Service `svc:demo`,
reachable at `demo.<tailnet>.ts.net`.

## Data plane

For a Gateway `N` in namespace `NS`, the controller applies (owned by the
Gateway): ServiceAccount, Role and RoleBinding `N` (the pods can only read and
update their own state Secrets), ConfigMap `N` (the serve config), StatefulSet
`N`, a PodDisruptionBudget `N` (`maxUnavailable: 1`, from two replicas up)
and one state Secret `N-<i>` per replica.

- Replicas run tailscaled in userspace mode as a non-root user with no
  capabilities (the `restricted` Pod Security Standard), as devices named
  `<NS>-<N>-<i>`. A replica is ready once it runs the current serve config.
  Gateway names are limited to 52 characters. A rescheduled replica finds its state Secret
  and comes back as the same device.
- After start, each replica advertises its Tailscale Services; before stop it
  runs `tailscale serve drain` for each and waits for the grace period, so
  `kubectl drain` moves replicas one at a time without dropping traffic.
- Replicas are spread across nodes (soft anti-affinity) and zones.
- Scaling down deletes the removed replicas' state Secrets and devices once
  their pods are gone. Deleting the Gateway stops its pods first and then
  deletes all its devices (OAuth mode), also under foreground deletion.
- Devices are only deleted with their Gateway. A GatewayClass in use keeps
  the standard `gateway-exists-finalizer`; if it disappears anyway, its
  Gateways stop serving and keep their devices until it is back.
- In OAuth mode, a replica whose device has disappeared from the tailnet (an
  ephemeral device that stayed offline too long) is reset and re-registered.

## Development

```sh
make test          # unit tests
make test-envtest  # unit + integration tests against kube-apiserver/etcd
make e2e           # against the current kubeconfig context (build tag e2e)
make e2e-kind      # on a throwaway kind cluster, no tailnet needed (as CI does)
make e2e-k3d       # on a throwaway k3s cluster through k3d (as CI does)
make manifests     # regenerate dist/install.yaml
make chart-lint chart-render  # lint and render the Helm chart to bin/chart.yaml
```

With `CHART_MANIFESTS=$PWD/bin/chart.yaml`, `make test-envtest` also checks
that the rendered chart validates against the API server and grants the same
RBAC as `dist/install.yaml`.

### Releases

[`.github/workflows/release.yaml`](.github/workflows/release.yaml) first runs
the ci and e2e workflows on the commit, and only if both pass publishes to
GHCR:

- every push to `main`: image `ghcr.io/davy-tw/tailnet-gateway/controller:main` and
  `:sha-<commit>`, and chart `oci://ghcr.io/davy-tw/charts/tailnet-gateway`
  version `0.0.0-main.<commit>` using that image (a prerelease: Helm only
  picks it when asked for that version);
- every tag `vX.Y.Z`: image `:X.Y.Z`, `:X.Y` and `:latest` (amd64 and arm64),
  chart `oci://ghcr.io/davy-tw/charts/tailnet-gateway` version `X.Y.Z`, and a
  GitHub release with the chart and an `install.yaml` pinned to that image.

It can then deploy the release with `helm upgrade --install`. To turn that
on, set the repository variable `DEPLOY_ENABLED=true` and add a `production`
environment with a `KUBECONFIG` secret. The OAuth credentials stay in the
cluster: create the Secret `tailnet-gateway` (or the one named by the
variable `OAUTH_SECRET`) in the target namespace first. Optional variables:
`DEPLOY_NAMESPACE` (default `tailnet-gateway-system`) and `HELM_VALUES` (a
values file's contents). Running the workflow by hand with a version
redeploys that version.

[`docs/design.md`](docs/design.md) describes the design and its decisions.
[`docs/spike-findings.md`](docs/spike-findings.md) records the prototype that validated direct proxying
to cluster DNS names and gRPC through Tailscale Services.

## License

Licensed under either of [MIT-0](LICENSE-MIT-0) or [Apache-2.0](LICENSE-APACHE), at your option
(`SPDX-License-Identifier: MIT-0 OR Apache-2.0`).
