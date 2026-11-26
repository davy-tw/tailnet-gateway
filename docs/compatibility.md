# Compatibility

tailnet-gateway follows [Semantic Versioning](https://semver.org) from
1.0.0. The controller image, the Helm chart and the install bundle of a
release share one version.

tailnet-gateway defines no CRDs of its own: its interface is the Gateway API
and Kubernetes objects it reads, plus the names, settings and annotations
listed here. Within 1.x, a minor or patch release does not remove, rename or
change the meaning of anything under [Stable](#stable). Such a change waits
for 2.0, and is listed in [`CHANGELOG.md`](../CHANGELOG.md) with what to do
on upgrade.

## Stable

**Settings**

- The controller's environment variables in the
  [Configuration](../README.md#configuration) table, their defaults (except
  `PROXY_IMAGE`, see below) and accepted values.
- The GatewayClass `parametersRef` ConfigMap keys `tags`, `replicas`,
  `image` and `drainGracePeriod`.
- The Helm chart's values, other than `proxy.certificateSharing` and
  `proxy.certificateReload` (experimental).
- The key names in the controller's Secret (`oauth.existingSecret`, or
  `tailnet-gateway` in the install bundle), which are loaded as environment
  variables: `TAILSCALE_OAUTH_CLIENT_ID` and `TAILSCALE_OAUTH_CLIENT_SECRET`.
- Manual key mode: for a Gateway `N`, the Secret `N` you create in its
  namespace, with the auth key under `authkey`
  ([example](../examples/manual-authkey.yaml)).

**Class names**

- The GatewayClass `controllerName` `tailnet-gateway.davy.tw/gateway-controller`
  and the IngressClass controller `tailnet-gateway.davy.tw/ingress-controller`.
- The default class names `tailnet` (GatewayClass, IngressClass,
  `loadBalancerClass`).

**Annotations you set**

- `tailnet-gateway.davy.tw/gateway-class` on Ingresses and Services.
- `tailnet-gateway.davy.tw/hostname` on LoadBalancer Services.
- `tailnet-gateway.davy.tw/tailscale-service` on TCPRoutes and TLSRoutes.
- The listener TLS option `tailnet-gateway.davy.tw/certificates: tailscale`
  stays accepted (and ignored), so listeners that carry it keep passing the
  Gateway API validation rule.

**Names on the tailnet and in the cluster**

Renaming any of these would re-register devices or move Tailscale Services,
so they are kept:

- Tailscale Service names, as in the
  [design](design.md#tailscale-service-names): a hostname `demo` or
  `demo.<tailnet>.ts.net` becomes `svc:demo`; an HTTPRoute or GRPCRoute
  without hostnames uses the Gateway's name; a TCPRoute uses its own name
  or its `tailscale-service` annotation; a LoadBalancer Service its own name
  or its `hostname` annotation.
- Devices are named `<namespace>-<gateway>-<i>`.
- For a Gateway `N`: ServiceAccount, Role, RoleBinding, ConfigMap,
  StatefulSet and PodDisruptionBudget `N`, state Secrets `N-<i>`.
- Gateways and routes generated from an Ingress or Service are named after
  it.

**Behavior**

- What the [support table](../README.md#what-is-supported) lists as
  supported keeps working. Something listed as unsupported may become
  supported in a minor release.
- Status uses the Gateway API's conditions and reasons. New reasons and
  events may be added in a minor release.

## Not covered

- **Experimental features**, marked as such in the README: certificate
  sharing (`certificateSharing`, `certificateReload`, their environment
  variables, the chart values `proxy.certificateSharing` and
  `proxy.certificateReload`, and the certificate Secrets named after
  hostnames). They may change or go away in a minor release.
- **The default proxy image** (`PROXY_IMAGE`) is updated to newer Tailscale
  releases in minor and patch releases. Pin it per class to control when
  proxies change.
- **Minimum versions** (Kubernetes, Gateway API, Tailscale) are raised only
  in minor releases, and the CHANGELOG says so.
- **Internal bookkeeping**: the finalizer, and the annotations and labels the
  controller writes for itself (`source`, `source-kind`, `source-name`,
  `tailscale-services`, `devices`, `listener-sets`, `certificate-*`,
  `restart-readers-by`, `readers-certificate-not-after`, `unused-since`).
  Do not set or edit them; the controller migrates them on upgrade.
- Metric names, log messages and the Go packages (all under `internal/`).
