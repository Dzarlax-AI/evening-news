# Native browser CONNECT gateway

Build from `go/` with `go build ./cmd/extract-egress`. The command accepts only
HTTP/1 CONNECT to an explicit `hostname:443` or public literal IP on port 443.
It checks every DNS answer, rejects mixed public/private answers, and dials the
selected numeric IP. It never resolves the hostname again at connection time.
Each new CONNECT repeats admission; an established tunnel remains pinned.

The proxy requires `Proxy-Authorization: Basic <base64(extractor:TOKEN)>` and
responds with a Basic `Proxy-Authenticate` challenge when credentials are absent.
The browser controller must answer proxy authentication challenges only for this
configured proxy, never send the proxy token as origin credentials. Proxy
authentication itself is HTTP on the isolated private network; origin TLS stays
encrypted end to end. The gateway does not install a CA or terminate TLS.

| Environment variable | Default / requirement |
| --- | --- |
| `EGRESS_TOKEN` | Required, 16–256 bytes; never passed on the command line |
| `EGRESS_LISTEN_ADDR` | `127.0.0.1:8081`; explicit loopback or private IP |
| `EGRESS_ALLOW_WILDCARD_BIND` | `false`; explicit `true` permits `0.0.0.0` / `::` in a dedicated container network |
| `EGRESS_MAX_CONCURRENT` | `32`, range 1–128; includes in-progress DNS/dial admission |
| `EGRESS_MAX_TUNNEL_BYTES` | `33554432`, range 1–134217728; combined encrypted bytes in both directions |
| `EGRESS_MAX_LIFETIME_SECONDS` | `35`, range 1–60; includes DNS/dial and tunnel lifetime |
| `EGRESS_TEST_MODE` | `false`; isolated fixture stand only |
| `EGRESS_TEST_ALLOWLIST` | Unset; test-only JSON object mapping exact hostname to exact IP, maximum 16 entries |

The fixture allowlist requires explicit test mode. For example,
`{"fixture.example.com":"172.30.0.2"}` pins only that hostname to that IP without
DNS. The synthetic TLS origin still listens on port 443. No request/header can
activate this exception. Production configuration must omit both test variables.

DNS and dial each have a three-second bound inside the lifetime budget. The CLI
also caps accepted HTTP sockets and applies short header/idle deadlines. New
tunnels receive 503 when busy or draining. DNS unavailability produces 503;
destination-policy refusal produces 403. SIGTERM stops admission, allows five
seconds of drain, then closes remaining owned tunnels. The standard HTTP server's
shutdown alone would not close hijacked CONNECT sockets.

`extract-egress -healthcheck` checks the local `/health/ready` route. This proves
gateway readiness only, not browser readiness or origin reachability. No
destination, credential or tunneled body is logged. `Gateway.Snapshot()` provides
aggregate active/tunnel/denied/wire-byte counters to an embedding process.

An encrypted tunnel cannot enforce decoded-body size, origin HTTP methods,
WebSocket upgrades, download policy or the hostname of requests coalesced onto an
existing TLS connection. The native worker owns its CDP policies; the isolated
network must block direct browser Internet/LAN/DNS/UDP and control-plane access.
These component tests do not prove that network isolation or actual Cloak JS
execution is configured. Full stand tests remain required.

Run component checks from `go/`:

```text
go test -count=1 ./egress ./cmd/extract-egress
go vet ./egress ./cmd/extract-egress
```

The TLS fixture test verifies the origin certificate, client SNI, encrypted body
and cookie continuity through the gateway. Other tests cover DNS rebinding,
mixed/private/special addresses, authentication, test-only policy, byte caps,
deadline, client disconnect, admission capacity and drain cleanup.
