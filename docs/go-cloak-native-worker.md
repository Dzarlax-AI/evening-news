# Experimental native Go/Cloak worker

The user approved the [implementation plan](ai-plans/2026-10-16-go-cloak-native-worker.html).
This backend is experimental and disabled by default. No production configuration,
profile, secret, container, database or delivery path has been changed.

## Native transport and ownership

`go/transport/native_browser.go` controls the configured Cloak over CDP. Origin
requests, TLS, cookies, JavaScript and response bodies stay in the browser. It
does not issue Go HTTP origin fetches, fulfill responses, or override browser UA,
client hints or viewport. The gateway in `go/egress/` authenticates CONNECT,
validates every destination and pins the selected public IP, then relays opaque
TLS without a MITM certificate. Its exact test-only allowlist is not a wire field.

Each job owns a disposable context and target. Cleanup stops loading and disposes
the owned context atomically, then verifies that context and its targets are
absent. It does not close foreign targets or the global browser. Cleanup has its
own bounded context after caller cancellation. Unknown ownership taints readiness
and prevents further jobs. Discovery failure invalidates readiness; browser
generation replacement requires explicit recovery rather than silent adoption.

Main-document status is correlated with the main frame, rather than any iframe
Document event. Proxy credentials are supplied only for the configured proxy
challenge; origin authentication is canceled. DOM transfer, concurrent admission,
total requests/auth challenges and cleanup are bounded. Unsupported child
targets/downloads/WebSockets produce typed failures. CDP filters are defense in
depth, not proof that a network handshake cannot occur.

## Isolated runtime configuration

The existing release unit and `/v1/extract` contract remain. A native request uses
`use_browser=true`, article/web source type and a supported browser/parser
strategy. It bypasses the Go HTTP method graph. Requests without browser use
remain HTTP-only even when the native backend is configured.

| Server setting | Experimental meaning |
|---|---|
| `EXTRACTOR_HTTP_ONLY=false` | Explicitly leave the safe HTTP-only default |
| `EXTRACTOR_BROWSER_BACKEND=native` | Select this backend; no implicit legacy fulfill fallback |
| `CHROME_ENDPOINT` | Dedicated trusted Cloak discovery endpoint |
| `EXTRACTOR_NATIVE_PROXY_URL` | Trusted private CONNECT gateway, without URL credentials |
| `EXTRACTOR_NATIVE_PROXY_TOKEN` | Gateway token; server configuration only |
| `EXTRACTOR_NATIVE_NETWORK_ISOLATED=true` | Explicit attestation after independently establishing the network boundary; this flag does not create firewall rules |
| `EXTRACTOR_BROWSER_CONCURRENT=1` | Current supported native concurrency |
| `EXTRACTOR_NATIVE_READY_SELECTOR` | Optional bounded server readiness condition; fixed to the fixture marker in matched benchmark runs |

Startup runs an owned-resource capability probe. Health checks additionally
verify discovery/generation with a maximum 500-ms budget and allocate no targets.
Health exposes a hash of the configured readiness selector so a benchmark can
verify its exact profile without logging the selector or source content.

Native errors use distinct codes that cannot trigger Python's legacy browser
fallback. The outer API deadline preserves `native_browser_timeout`; upstream
429 and bounded Retry-After survive durable retry handling. Native policy,
unsupported input, challenge and deterministic content failures are terminal;
resource failures retain their resource classification. Learned-selector
observations assess that exact candidate and preserve its supplied identity;
generic parser success does not count as selector success.

## Reproduced Cloak cleanup failure

Read-only verification on 2026-10-16 confirmed production's pinned
`cloakhq/cloakbrowser:0.5.2@sha256:2c0d257802f54c4f86c9ccae336864baf63e118612732127cb96df35756a57a4`,
patched Chromium 146, 768-MiB memory limit and 1-GiB shm configuration.
All subsequent checks used separate containers with that digest.

The initial native Go capability repetition reproduced the earlier failure:
one success, then EOF/ownership loss, followed by discovery failures. A raw
Python/websockets diagnostic removed Go from the reproduction:

| Minimal sequence, fresh dedicated Cloak per row | Result |
|---|---|
| Discovery/connect only | 6/6 passed |
| Create/dispose empty context | 6/6 passed |
| Create/attach target, close target, dispose context | Failed on cycle 2 |
| Same sequence with per-context proxy | Failed on cycle 6 |
| Same sequence with dispose-on-detach disabled | Failed on cycle 2 |
| Stop loading, dispose context without explicit target close | 12/12 passed |

After adopting atomic context disposal, the real Go capability test passed
12/12 repetitions in 6.93 seconds. Native HTTPS fixture loading through the
authenticated gateway passed 6/6 repetitions in 10.69 seconds, including JavaScript
and a cookie-confirming same-origin fetch. The fixture used an isolated,
short-lived certificate trusted by SPKI only in its test Cloak. This supports the
cleanup change, not a complete explanation of the Chromium crash internals or a
production reliability claim.

## Benchmark and remaining gates

`scripts/benchmark_native_cloak.py` provides fixed-corpus A/B/C/D controller
runs and `tests/fixtures/native-cloak/origin.py` serves controlled HTTPS pages.
Real cgroup-v2 counters are required: absent data fails instead of becoming zero.
Distinct, non-overlapping cgroups prevent duplicate sums. One-second synchronized
samples yield the maximum observed sum; `memory.peak` is a separate container
lifetime peak, not a sum claimed as a simultaneous system peak. Reports exclude
URL/body/credentials and distinguish short smoke from independent fresh runs.

A retains the existing Python browser helper and its production policy. B uses
nodriver with a matched native profile. C uses the Go service with that profile.
D currently proves controller/client plumbing only; it is not full application
processing evidence. A/C includes policy changes; B/C is the intended controller
comparison. Three independent repeats require fresh external containers, not a
loop labeled as independent inside one process.

Fresh verification: **487 Python tests passed, 5 explicit real-browser/soak tests
skipped**, using dedicated local PostgreSQL; Go test/vet/module verification
passed, including Linux race across all eight packages. The new pinned-Cloak
integration evidence above is separate from those skipped Python checks.

Three short fresh-container series completed: A/B/C (90 jobs), matched-readiness
B/C (60 jobs), and matched-readiness/equal-384-MiB-controller-limit B/C (60 jobs).
All 210 measured jobs met fixture expectations with no OOM events. Warmups are
separate from these counts. Each series is one invocation per variant; this is
not three independent repetitions of the same comparison.

Latest equal-limit B/C results, one-second cgroup samples:

| Metric | B: Python + native Cloak | C: Go + native Cloak |
|---|---:|---:|
| Jobs / mismatches | 30 / 0 | 30 / 0 |
| Controller sampled maximum | 100.0 MiB | 8.1 MiB |
| Cloak sampled maximum | 343.4 MiB | 299.5 MiB |
| Gateway sampled maximum | 6.4 MiB | 4.7 MiB |
| Maximum synchronized sum | 445.7 MiB | 310.3 MiB |
| Job latency p50 | 1.186 s | 1.174 s |
| Job latency p95 | 1.527 s | 1.480 s |

The independent component maxima do not necessarily occur at the same instant.
Browser variation between runs is material. The Python application remains after
extraction is moved, and C excludes the Python load generator: these numbers are
not proof of a whole-application saving. No kernel peak is available on this host.

The reviewed `scripts/soak_native_cloak.py` requires a new empty output directory
and one uninterrupted run; old reports cannot reset or contribute elapsed time.
It checks readiness and cgroup OOM deltas, fails without retry/restart, and bounds
reports to 2048 batches / 256 MiB. The 180-second live driver smoke completed
33 one-case batches and four final-idle samples with no OOM or failures. The
HTTP-only real API smoke of the current binary also passed.

A fresh C-only controlled soak started **2026-10-16 10:28:54 UTC**, requested
86400 seconds (earliest planned completion 2026-10-17 10:28:54 UTC). Initial
state was `running`; no 24-hour pass is claimed. It cycles all five cases six
times per batch, with 5/30/60-second idle gates after each cycle, concurrency 1,
no healthcheck side jobs, bounded CPU/RAM/PIDs/logs, and no external network.
An outer 90000-second timeout and cleanup trap remove only its dedicated
`news-native-driver-soak-*` containers on completion/failure. Reports remain at
`/tmp/news-native-cloak.vh6lLSy2/out/driver-soak/` on the authorized VPS. No
notification automation was created.

Whole-system RAM savings, three identical independent matched repeats, completed
24-hour soak, concurrency 2, all-source live parity and production sizing remain
open. The fixture stand has no external network and is suitable only for
controlled content; a public-origin release still requires the complete network
boundary/UDP/private-redirect/subrequest gate. TLS tunneling caps are per tunnel,
not exact decoded response or aggregate per-job caps. The CDP dependency allows
messages up to 256 MiB before application-level guards; cgroup limits remain
necessary. Final native image CI, production rollout and dedicated-browser
supervisor recovery remain separate release gates.

Evidence is under ignored `artifacts/native-cloak/`: raw lifecycle diagnostics,
failed and fixed probes, real HTTPS fixture test, Python suite and Linux race
logs. Temporary stand containers and test profiles never attach to production.

## Duration adjustment approved on 2026-10-16

The user requested shortening the initial observation from 24 hours to several
hours. The first observation now targets **four hours from the existing start**:
2026-10-16 10:28:54–14:28:54 UTC (12:28:54–16:28:54 Europe/Belgrade).
The same worker/browser run and all existing measurements are retained.

The already running driver has an immutable 86400-second argument. A bounded,
run-identity-checked supervisor sends SIGTERM only to its exact driver at the
four-hour cutoff; the existing cleanup trap then removes only this stand.
`out/soak-shortening.json` records the changed target and final original summary.
The driver will honestly record `interrupted`, not a passed 24-hour run. A final
partial batch may be canceled and is not counted as a successful completed batch.
The supervisor does not restart the driver or recover failed jobs.

At review, assess completed batch expectations, native readiness, memory/PID
trend, idle behavior, OOM counters and restart counts over the actual interval.
Early failure remains failure. A four-hour interval is an initial stability
observation, not evidence excluding slower leaks or establishing public-origin
production readiness. A 24-hour run is deferred; it is not the current wait gate.

## Live URL batch on 2026-10-16

The isolated [live shadow report](go-cloak-live-shadow-2026-10-16.md) records
50 Python attempts and eight Go attempts. Go stopped on case-0008 with
`native_browser_unavailable`; its remaining 42 cases were not attempted.
Production remained unchanged and temporary live stands were cleaned up.
The final native cleanup budget is five seconds; stopLoading is best effort,
while disposal and both absence receipts remain mandatory. Controlled gates
passed, but native availability and successful live quality remain release
blockers. Offline parsing of three saved Python DOMs produced two Go successes
and one no_content; those results do not replace live browser validation.

A separately approved one-case diagnostic replay reproduced the failure in
12998 ms. The 256-message CDP event queue overflowed after successful navigation;
the reader then stopped consuming command replies, preventing cleanup receipts
and tainting readiness. The dedicated stand was removed and production baseline
was unchanged. The next release blocker is bounded event handling that preserves
command replies and ownership cleanup under overload; the original failure's
exact phase remains unknown. See the linked live report for evidence and scope.

The subsequent approved local filter retains only the 16 event methods consumed
by both browser handlers before enqueueing; command replies are unaffected.
The 256-event cap and fail-closed overload remain. A 1024-unused-event WebSocket
regression fails before filtering and passes after it; required-event overload
still fails closed. Windows tests/vet and offline Linux race/vet/module checks
passed. No post-filter native Cloak/public-origin run has been performed.

## Four-hour cutoff observation

Read-only inspection at 2026-10-16 14:29:47 UTC found the separate synthetic
driver in `interrupted` state with 23 completed batches / 690 jobs: 552 expected
successes, 138 expected rejections, zero expectation mismatches. No soak
containers remained. This follows the approved four-hour cutoff and preserves
the original driver's interruption; it is not a passed 24-hour observation.
The soak used the earlier binary/configuration and does not validate the later
event filter or public-origin compatibility.
