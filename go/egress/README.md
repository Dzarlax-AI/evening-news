# Native browser CONNECT gateway

Build from `go/` with `go build ./cmd/extract-egress`. The proxy accepts only
HTTP/1 CONNECT to an explicit `hostname:443` or public literal IP on port 443.
It checks every DNS answer, rejects mixed public/private answers, and dials the
selected numeric IP. It never resolves the hostname again at connection time.
Each new CONNECT repeats admission; an established tunnel remains pinned.

The controller first allocates a render budget through the authenticated control
API on the same private listener. Each render gets a random, short-lived proxy
token; all its tunnels share one byte allowance. `EGRESS_TOKEN` authenticates only
the control API and is never supplied to Chrome or accepted for CONNECT.

The proxy requires `Proxy-Authorization: Basic <base64(extractor:SCOPED_TOKEN)>` and
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

Control requests use `Authorization: Bearer <EGRESS_TOKEN>`, HTTP/1, no query or
escaped path, and JSON bodies/replies of at most 1024 bytes. Version 1 requires
every declared field exactly once; unknown fields, aliases, duplicates, nulls,
trailing data and incompatible receipts are rejected by the native controller.

| Endpoint | Request / response |
| --- | --- |
| `POST /v1/render-budgets` | Required `contract_version: "1"`, `wire_limit_bytes` (1–5242880), `ttl_ms` (1–35000). HTTP 201 returns those fields, `budget_id` and random `proxy_token`; TTL is also bounded by gateway lifetime. |
| `GET /v1/render-budgets/{budget_id}` | HTTP 200 reports version, ID, limit, `wire_bytes`, `exhausted`, `expired`, `revoked`, `active_tunnels`. The controller polls while rendering. |
| `DELETE /v1/render-budgets/{budget_id}` | Revokes only this budget, closes and waits for its tunnels, then returns a frozen receipt with `active_tunnels: 0`. Repeated DELETE returns the same receipt during retention. |

The per-render limit counts encrypted bytes forwarded in **both directions**,
including TLS overhead, across all connections using its scoped credential.
Reservations are shared before writes and never refunded; short writes tighten
the bound. The existing per-tunnel limit remains an additional guard. Exhaustion
or expiry closes only that render's tunnels. Revoked/expired tokens cannot open
new tunnels; scoped tokens cannot access the control API. Registry size is
bounded by `max(128, EGRESS_MAX_CONCURRENT * 8)` and old receipts become eligible
for removal five seconds after their original expiry.

The native worker allocates before browser-context creation, supplies the scoped
credential only to verified proxy challenges, disposes its context and confirms
target absence before final revocation. An exhausted budget returns an empty
`resource_limit` result. Unknown cleanup or receipt outcomes fence the worker.
Decoded document replay and DOM output retain their separate shared bounds.
HTML replay preserves decoded publisher bytes and existing headers/policies,
normalizes compression/framing headers, and adds CSP `worker-src 'none';
connect-src https:` to prevent worker startup and prohibited WebSocket handshakes.

Native worker and egress must be released together. Older gateways lacking the
control API fail readiness before navigation; there is no legacy-token fallback.
Environment variable names and network isolation requirements are unchanged.

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
