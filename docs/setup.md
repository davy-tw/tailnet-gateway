# Tailscale setup

tailnet-gateway does not create Tailscale Service definitions, approvals or
access rules. Do these once per tailnet, then once per Tailscale Service.

## 1. Tags and the OAuth client

Proxy devices must be tagged devices: Tailscale Services can only be hosted
by tagged devices. Pick a tag (`tag:k8s-gateway` below), and let the OAuth
client assign it. In the tailnet policy file:

```jsonc
{
  "tagOwners": {
    "tag:k8s-gateway": ["autogroup:admin"]
  }
}
```

Create an OAuth client (admin console, Settings, OAuth clients) with:

- **Auth Keys: write**, with the tag `tag:k8s-gateway`, so the controller can
  mint one single-use, ephemeral, pre-authorized key per proxy replica;
- **Devices Core: write**, so the controller can delete devices when a Gateway
  is deleted or scaled down.

Store the credentials in the controller's namespace. Every key of this Secret
becomes an environment variable of the controller:

```sh
kubectl -n tailnet-gateway-system create secret generic tailnet-gateway \
  --from-literal=TAILSCALE_OAUTH_CLIENT_ID=... \
  --from-literal=TAILSCALE_OAUTH_CLIENT_SECRET=... \
  --from-literal=PROXY_TAGS=tag:k8s-gateway
kubectl -n tailnet-gateway-system rollout restart deployment/tailnet-gateway
```

The controller reads these settings at start-up, so restart it whenever the
Secret is created or changed.

`PROXY_TAGS` is required: the Tailscale API only mints tagged keys for an
OAuth client. It may be left out only if every GatewayClass sets `tags` in
its parameters (see the README); a class left with no tags is not accepted.

### Without an OAuth client (manual key mode)

Leave the OAuth variables out. For each Gateway, create a Secret with the
same name and namespace holding a **reusable, ephemeral, pre-authorized,
tagged** auth key under `authkey` (see `examples/manual-authkey.yaml`). The
key is reusable because every replica uses it. In this mode, devices are not
deleted by the controller; ephemeral devices are removed by Tailscale after
they go offline.

## 2. HTTPS

HTTPS listeners (and gRPC, which needs them) use Tailscale's automatic
certificates. Enable MagicDNS and HTTPS certificates for the tailnet (admin
console, DNS).

## 3. Approve proxies automatically

Every replica of a Gateway advertises every Tailscale Service of that
Gateway, and each host must be approved. Let the policy file approve the
proxies' tag for the Services they host, or every new replica waits for a
manual approval:

```jsonc
{
  "autoApprovers": {
    "services": {
      "svc:demo": ["tag:k8s-gateway"],
      "svc:db":   ["tag:k8s-gateway"]
    }
  }
}
```

## 4. Define each Tailscale Service

Define the Service in the admin console (Services) or through the API, with
the ports its listeners use (for example `tcp:80` and `tcp:443` for
`svc:demo`). The Service name comes from the route:

| Source | Tailscale Service |
| --- | --- |
| HTTPRoute / GRPCRoute | `svc:<hostname>` for each entry of `spec.hostnames` (a single label like `demo`, or `demo.<tailnet>.ts.net`); `svc:<gateway name>` when the route has no hostname |
| TLSRoute | `svc:<hostname>` per hostname, else as TCPRoute |
| TCPRoute | the `tailnet-gateway.davy.tw/tailscale-service` annotation, else `svc:<route name>` |
| Ingress | as HTTPRoute, with the Ingress rule hosts |
| LoadBalancer Service | `svc:<hostname annotation>`, else `svc:<service name>`, for all of its ports |

## 5. Grant access

Tailscale Services are reached through grants like any other destination:

```jsonc
{
  "grants": [
    {"src": ["autogroup:member"], "dst": ["svc:demo"], "ip": ["tcp:80", "tcp:443"]}
  ]
}
```

## Changing tags

Changing a class's `tags` only affects keys minted afterwards. Existing
replicas keep their device and tags until their state Secret
(`<gateway>-<ordinal>`) is deleted; delete it and the pod to re-register.
