# Changelog

All notable changes are listed here. The project follows
[Semantic Versioning](https://semver.org); [docs/compatibility.md](docs/compatibility.md)
says what that covers.

## 1.0.0 (unreleased)

First release. tailnet-gateway exposes in-cluster workloads to a tailnet as
Tailscale Services.

### Features

- **Gateway API** (v1.6 or newer, Standard channel, `v1` APIs only):
  Gateways of a managed GatewayClass with HTTP, HTTPS, TLS and TCP
  listeners; HTTPRoute (`PathPrefix`), GRPCRoute (on HTTPS listeners),
  TLSRoute (passthrough and terminate) and TCPRoute; ReferenceGrant for
  cross-namespace backends; BackendTLSPolicy with system CAs; ListenerSets.
- **Ingress** of the `tailnet` IngressClass and **LoadBalancer Services**
  with `loadBalancerClass: tailnet`, translated into Gateways and routes.
- **Data plane**: a StatefulSet per Gateway running the stock
  `tailscale/tailscale` image (default `v1.104.7`, at least v1.86.0
  required) in userspace mode under the `restricted` Pod Security Standard,
  two replicas by default, with a PodDisruptionBudget, drain on stop, and a
  stable tailnet device per replica.
- **Auth**: an OAuth client that mints auth keys and deletes devices, or one
  auth key per Gateway (manual key mode).
- **Per-class parameters** through a GatewayClass `parametersRef` ConfigMap:
  tags, replicas, image, drain grace period.
- HTTPS with the tailnet's automatic Let's Encrypt certificates.
- **Experimental**: certificate sharing between replicas
  (`certificateSharing`, `certificateReload`), using Tailscale's
  experimental cert share mode.
- Install with Helm (`oci://ghcr.io/davy-tw/charts/tailnet-gateway`) or the
  release's `install.yaml`; images for amd64 and arm64.

### Known limitations

- Tailscale Services carry TCP only: no UDP listeners or UDPRoute.
- Only path prefixes: `Exact` and regular expression paths, header, query
  and method matches, filters, timeouts, retries and traffic splitting are
  reported as `PartiallyInvalid`.
- Listener `certificateRefs` are ignored; certificates always come from
  Tailscale.
- Hostnames must be single labels or names under the tailnet.
- The controller does not define Tailscale Services or approve their hosts;
  see [docs/setup.md](docs/setup.md).
- The Gateway API conformance suite is not run.
