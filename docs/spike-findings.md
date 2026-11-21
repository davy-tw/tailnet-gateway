# Spike: serve proxy targets and gRPC through Tailscale Services

Date: 2026-11-20. Tailscale source: `tailscale/tailscale` at `5c7e705` (v1.105.0
development tree). Method: drive tailscaled's real serve code path
(`ipn/ipnlocal`) in-process with a Tailscale Service (`svc:demo`) serve
config, with the backend on a **non-loopback** address reached through a
**cluster-style DNS name** (`echo.demo.svc.cluster.local` in `/etc/hosts`).
The test file is in [`hack/spike/serve_spike_test.go.txt`](../hack/spike/serve_spike_test.go.txt);
drop it into `ipn/ipnlocal/` of a Tailscale checkout and run
`SPIKE_BACKEND_IP=<non-loopback ip> go test ./ipn/ipnlocal -run TestSpike -v`.

Not covered: real WireGuard transport and a real control plane (needs a
tailnet auth key). The data path after tailscaled accepts a connection on a
Service VIP is the code exercised here.

## Results

| Case | Result |
| --- | --- |
| HTTP/1.1 web handler, target `http://<dns-name>:<port>` | Works |
| Raw TCP forward (`TCPForward: <dns-name>:<port>`) carrying gRPC | Works (unary, client/server streaming, bidi, metadata, status) |
| gRPC client to an **HTTPS** Service port, target `http://<dns-name>:<port>` (plaintext gRPC backend) | Works, full interop set |
| gRPC client to an HTTPS Service port, target `https+insecure://<dns-name>:<port>` (TLS gRPC backend) | Works, full interop set |
| gRPC client to a **plain HTTP** Service port | Fails: tailscaled's plain HTTP server does not accept h2c |
| Mounts `/` and `/api` with the mount path appended to the proxy URL | Paths reach the backend unchanged (`/api`, `/api/`, `/api/v1/x`; `/apix` goes to `/`), which is Gateway API `PathPrefix` semantics |

## Conclusions for the implementation

1. **Proxy targets can point directly at `<svc>.<ns>.svc.<cluster-domain>`.**
   tailscaled dials backends with the system dialer and resolver; the
   "127.0.0.1 only" note is outdated. The CLI only requires a scheme for
   non-localhost targets. No local forwarding sidecar is needed.
2. **gRPC works end to end, but only on HTTPS listeners.** tailscaled
   negotiates `h2` via ALPN on HTTPS ports, then uses an h2c transport to an
   `http://` backend when the request is HTTP/2 with an `application/grpc`
   content type (or HTTP/2 over TLS to an `https://` backend). Plain HTTP
   Service ports are HTTP/1.1 only. So GRPCRoutes attached to a `HTTP`
   listener get a route condition saying gRPC needs an `HTTPS` listener.
3. **New finding: the Service web-handler key needs the tailnet DNS suffix.**
   In the serve config, a Tailscale Service's web handlers are keyed by
   `<service>.<tailnet>.ts.net:<port>`. The image's serve-config file is a
   plain `ipn.ServeConfig` and only substitutes `${TS_CERT_DOMAIN}` (the
   device's own FQDN), which does not help. The controller therefore learns
   the tailnet suffix from the first replica's state Secret (device FQDN, the
   same data the Gateway addresses are built from), or from an optional
   setting, and re-renders the ConfigMap. Until then, web handlers are left
   out and the Gateway stays `Programmed=False`.
4. The proxy forwards the client's `Host` header unchanged
   (`<service>.<tailnet>.ts.net`) and adds `X-Forwarded-*` and Tailscale
   identity headers.
5. The image auto-advertises every Service in the serve config by default
   (`TS_EXPERIMENTAL_SERVICE_AUTO_ADVERTISEMENT` defaults to true); the
   postStart `advertise` hook is kept anyway as the documented behaviour.
