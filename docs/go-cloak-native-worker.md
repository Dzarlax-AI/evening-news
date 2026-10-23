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

An owned `Inspector.targetCrashed` fails that job with empty output and a
non-retryable `native_browser_target_crashed` result. The worker may serve a
later independent job only after pending work is canceled and joined, a
same-connection `Browser.getVersion` ordering barrier completes within 500 ms of
the existing five-second cleanup deadline, the owned context is disposed and
both context/target absence receipts pass, the original CDP connection remains
live, and a bounded health check confirms the unchanged browser generation.
The extractor does not replay the crashed invocation. The application may schedule
a later attempt through its existing bounded durable queue (see the approved
deferred-retry follow-up below). Unknown ownership, detach, disconnect,
overflow, generation change, or any cleanup/health-check failure remains
terminal and taints readiness.

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
| `EXTRACTOR_BROWSER_CONCURRENT=1` | Default native render limit; supported values are 1 and 2. Two renders require isolated acceptance before deployment. |
| `EXTRACTOR_NATIVE_READY_SELECTOR` | Optional bounded server readiness condition; fixed to the fixture marker in matched benchmark runs |

Native render admission is shared by article extraction, Telegram browser fallback,
and Page Monitor. It starts only when a render is needed; native SourceManager
jobs do not hold a browser lane during HTTP work. Up to eight renders can wait
for a slot, bounded by their original caller contexts. The navigation timeout
starts after admission, while the source/RPC deadline continues to bound the
whole operation. A slot remains occupied through owned-resource cleanup.
Excess waiters receive `native_browser_busy`; normal slot contention waits.

Each active operation has its own CDP connection, disposable context, target,
and cancellation. Shutdown cancels all owned operations; generation/ownership
loss taints the worker and wakes waiters. The worker never globally closes or
restarts the externally managed shared browser. Legacy backend source scheduling
is unchanged. Production configuration remains at one render until a separate
rollout decision.

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

## Bounded overload cleanup — Mac continuation, 2026-10-16

The user approved the [overload cleanup plan](ai-plans/2026-10-16-go-cloak-overload-cleanup.html).
Luna (`gpt-6-luna`) performed the main Go implementation and tests; the root
agent independently reviewed the diff, requested corrections, and ran integration
checks. This remains local development on `codex/go-cloak-mac-handoff`, based on
`c9441de4a6e0314a3c7073158ac960de11b9b4d3`, with uncommitted changes.

The shared CDP transport now separates terminal event overload from physical
reader termination. The needed-event queue remains 256, and the same 16-method
filter remains in effect. After required-event overload, extraction fails,
ordinary commands are rejected, and subsequent notifications are discarded only
for this irrevocably failed job. The reader stays available for command replies
needed to clean up owned resources. Allocation commands already sent can still
receive their IDs; unsent allocations cannot start after overload.

A single writer per connection uses a one-entry request queue and one-entry
result channels. Caller cancellation can return without closing a healthy
connection while waiting for a reply. A stalled write is released by the cleanup
context's connection-close guard. There are no per-event or per-call writer
goroutines. The explicit cleanup path allows only stop/dispose, owned target
fallback and absence queries, plus the existing detach/stream-close operations.
Late or duplicate replies cannot block the reader. Closing joins both loops.

Native overload taints the generation even when disposal and both absence
receipts succeed. Readiness stays false; automatic recovery/restart is not
implemented. All shared adapters clear any completed HTML if deferred cleanup
finds failure. Overload remains in the error chain, and HTTP/API mappings and
Python fallback behavior are unchanged.

The native cleanup command budget is still five seconds. The CDP dependency's
existing graceful socket-close envelope is additional; an exact five-second
wall-clock bound including transport teardown is not claimed. This work also
does not introduce a byte-level CDP message cap or replace container limits.

Fresh verification:

- A deterministic regression failed on the old reader because overflow also
  prevented the disposal reply; the corrected test passes.
- Root `go test -race -count=1 ./...` on Go 1.25.6/darwin-arm64: all eight
  packages passed. Vet and module verification passed; module files unchanged.
- Overload/filter/allocation/late-reply and native cancellation/cleanup tests
  passed 100 repetitions, including a fresh root run with `GOMAXPROCS=1`.
  Blocked-write and ordinary cancellation tests also passed ten focused race
  repetitions after fixing the test handler's explicit shutdown barrier.
- The focused Python suite passed **318 tests**, with seven warnings. No app,
  scheduler, AI or delivery run was started.
- All eight Linux/amd64 test binaries passed in an offline capped Docker
  container. They were cross-compiled without CGO and run under Apple Silicon
  emulation: this is not native Linux race or performance evidence.
- Two fresh pinned-Cloak stands, each `--network none`, passed a total of
  **12/12 capability probes and 12/12 HTTPS fixture fetches**. Six fetches were
  the JS/cookie fixture; six were fan-out pages awaiting 96 subrequests each.
  Origin receipts recorded all 576 fan-out subrequests. Browser binary SHA-256
  matched `715722e8605ae3ce81523c1218aba1ec89425786ab33ceaf99f8a6cb5e70e6e8`.
- A disposable negative control disabled only the cumulative request guard:
  the paced budget test failed with a nil extraction error, as expected.

The Cloak stands had no published ports or external network, used synthetic
credentials and fixture-only SPKI trust, and shared only their own container's
loopback namespace. These were positive lifecycle/HTTPS/fan-out probes; required
overload and its cleanup receipts were exercised through deterministic CDP
fixtures, not a flood injected into actual Cloak. No public-origin replay,
private-egress gate, RAM saving, long soak or production effectiveness is claimed.

Evidence is in ignored `artifacts/cdp-overload-2026-10-16/`, including the
52-file final Go source manifest, RED/green/race logs, Linux suite, fixture
receipts and both stand summaries. No VPS access, production mutation, commit,
push, PR, image build/publication or deployment was performed. Native Linux/VPS
validation remains a separate gate with a fresh production/resource baseline,
explicit stand budget and independent authorization.

## Native Linux harness gate status

The bounded synthetic VPS gate on 2026-10-16 stopped in the offline Linux race
phase: seven packages passed, while the replay boundary test needed a 64-MiB
fixture and exhausted the harness's 32-MiB `/tmp`. No browser fixture or relay
overload was attempted on native Linux. The local correction routes test temp
files to the existing `/go-tmp` stage mount via `TMPDIR`, preserving all caps.
It passes the real boundary test locally with race detection; local harness
checks pass 72 tests without warnings. The user subsequently approved one repeat with the same caps.

Production fingerprints, OOM counters, health and public home/feed checks were
unchanged or successful after cleanup. All resources and the new synthetic
stage were removed; prior private live evidence was untouched. Detailed scope,
checksums and evidence are in the
[Linux gate result](ai-plans/2026-10-16-go-cloak-linux-vps-synthetic-gate.html).

The approved repeat stopped after 125.3 seconds in the race phase with
`memory_psi_pressure` (full avg10 guard threshold >0.5%). The exact PSI trigger
value was not recorded. Native Cloak/relay tests did not start, so native Linux
validation remains incomplete. Production fingerprints/OOM counters and
home/feed postchecks passed; all owned resources and the fresh stage were
removed. No further run or limit change was performed. See `repeat-2/` evidence
under the Linux gate artifacts and the plan's actual-result section.

Local follow-up isolated the replay CLI's boundary workload: race-enabled
execution peaked at 1150.69 MiB cgroup memory, compared with 382.33 MiB for
race compilation, 61.38 MiB for the other replay tests and 478.49 MiB for the
same boundary test without race instrumentation. Pprof attributed 99.93% of
sampled cumulative allocation to JSON decoder refill during trailing-data
validation. No source change was made. These emulated Linux/amd64 results
identify a local allocation hotspot; they do not establish the earlier VPS
PSI trigger or measure the native extractor/browser service. Details are in
`artifacts/race-memory-profile-2026-10-16/profile-report.md`.


The subsequently approved local fix replaces whitespace-buffering JSON checks
in both replay CLI and HTTP service with `internal/jsonsingle`. It preserves
64-MiB/8-MiB inclusive bounds, strict single-value/unknown-field validation,
reader-error rejection, oversized HTTP `413` precedence and admission before
body reads. Independent macOS ARM64 helper/API race checks passed. Under the
same local Linux/amd64 emulation caps, the unchanged three replay boundary
fixtures dropped from 1150.69 to 125.16 MiB cgroup peak; five concurrent lazy
near-8-MiB API requests peaked at 63.00 MiB and a sixth was rejected before
reading its body. Both measured runs passed with zero memory-event/full-PSI
deltas. This excludes Cloak and production workloads; native VPS validation
remains incomplete. See the handoff's JSON whitespace section and
`artifacts/json-whitespace-fix-after-2026-10-16/`.

The final full Linux/amd64-emulated Go 1.25.6 race suite passed all nine
packages; whole-container peak including compilation was 480.50 MiB, with
zero memory-event/full-PSI deltas. Independent focused source review found no
remaining blocker in the JSON decoding fix.

Final `go vet ./...` completed with confirmed exit status 0 and no diagnostics;
`go mod verify` passed. Root verified source hashes and exact-label container
cleanup. Reproduction commands and setup-error limitations are in the after-fix
evidence README; no commit, push, PR, image publication or deployment occurred.

### Post-JSON-fix admission check on 2026-10-17

The user approved one fresh isolated VPS gate with unchanged caps. Luna passed
119 targeted local harness tests (one existing Pydantic warning), prepared a
fresh 67-source payload including both untracked JSON helper files, and ran
read-only admission checks. Root independently checked ten runner tests and
verified all archive/source hashes against the dirty checkout.

Admission stopped before any stage or test container was created: MemAvailable
was 2928.6 MiB, below the required 3200 MiB Go-phase minimum. Memory PSI full
avg10 was 0.00%; pinned Go/APP/Cloak images matched and disk space was sufficient.
No threshold reduction, production resource cleanup, remote test phase or
automatic repeat occurred. Production fingerprints/restart/OOM counters were
unchanged and health/home/feed checks passed. Root independently rechecked
fingerprints, OOM counters and absence of native containers afterwards.

The runner now records the actual PSI/MemAvailable sample and phase in its
summary, without changing guards or caps. Native Linux race/build/Cloak/relay
validation remains incomplete. Evidence is in
`artifacts/linux-vps-gate-2026-10-17/`; the plan records archive/manifest hashes
and the preflight result. A further attempt needs fresh resource admission or
an independently approved separate Linux/amd64 runner. No commit, push, PR,
image publication or deployment was performed.

### Local cold-cache validation of a smaller Go gate budget

The user approved a local-only 768-MiB/no-swap experiment. Luna ran pinned
Go 1.25.6 Linux/amd64 under Docker Desktop ARM64 emulation, network disabled,
one CPU, 128 PIDs, with a fresh empty race GOCACHE. Module verification and
all nine race packages passed in 185.2 seconds; observed cgroup peak was
556.56 MiB. A separate empty-cache static build of extract-service and
extract-egress passed in 96.6 seconds at 347.00 MiB; both ELF files had no
INTERP program header. Both containers exited 0 with OOMKilled=false.

Race sampled-window max/OOM/PSI values were zero; no explicit final race
cgroup snapshot was retained. The build final snapshot had zero max/OOM/PSI.
Root independently checked logs, caps, ELF headers and the 64 captured hashes
(54 Go/module files). The same pinned index resolved to a different local
config ID than previously recorded, so these are standalone cap-validation
results, not a controlled before/after image comparison or native VPS proof.

A proposed Go cap of 768 MiB gives admission 2432 MiB (768 + 128 supervisor
+ unchanged 1536 production reserve). The earlier spoken 2496-MiB sum was
incorrect. Browser admission stays 2960 MiB. The VPS runner's existing
1536-MiB cap and 3200-MiB admission have not been changed, and no VPS attempt
was made. The plan records this candidate for the next separately authorized
remote gate. Evidence: `artifacts/go-cold-cache-768m-2026-10-17/`.

### Runtime admission correction and one native attempt

The runner now uses the locally validated Go cap 768 MiB and a 2432-MiB
admission threshold (768 phase +128 supervisor +1536 production reserve).
Browser runtime uses a monitored 768-MiB aggregate working-set budget over
exact-label disjoint test cgroups, rather than adding every individual cap.
Individual hard caps and production PSI/reserve/health/OOM/deadline guards
remain. The aggregate budget is a sampled stop threshold, not a kernel cap.
The new --runtime-only mode verifies pinned prebuilt binaries and all 54
Go/module inputs, then skips remote Go toolchain/compilation/race.

One fresh native runtime attempt passed admission, then stopped after 26.2 s
at memory PSI full avg10=0.99 (limit0.5). MemAvailable was 2857.09 MiB and
five test containers used 200.32 MiB in that sample. Extraction had not
started; no successful native runtime or memory-saving result is claimed.
No retry or guard relaxation occurred. Shared production Chrome was excluded
from test accounting and was not modified.

Cleanup exposed three invalid docker rm -f -t 1 calls. Exact-label fallback
removed all five containers and the internal network; root independently
verified stage/helper/resource absence, unchanged production fingerprints/OOM
and healthy home/feed. All three calls are now docker rm -f with ownership
checks and bounded timeouts, confirmed-absent handling and sanitized errors.
The 25 focused tests and a real local 16-MiB Docker cleanup smoke passed.
That cleanup fix has not been rerun on the VPS. Evidence and final regression
logs: artifacts/runtime-budget-fix-2026-10-17/. Earlier dirty work is preserved;
no commit, push, PR, image publication or deployment occurred.

### Authorized short news_web maintenance experiment

One runtime-only native attempt ran with only news_web intentionally stopped.
An independent 10-minute systemd exact-container-ID return timer was armed and
verified before stop. Shared infra-chrome remained running throughout. The
maintenance runner preserves host PSI/reserve/OOM, ownership, shared Chrome
fingerprint and sampled runtime budget guards; only unavailable web probes are
replaced by strict stopped-container identity/exit checks. Its active/hard
watchdog deadlines are 6/8 minutes; normal mode remains 28/30.

The runner stopped after 1.2 seconds at its initial guard: memory PSI full
avg10=0.90 exceeded 0.50, with 3296.42 MiB available. No test container or
extraction had started. The source of the host PSI spike remains unknown;
this supplies no Go/Cloak memory measurement or successful native gate result.
No retry or guard relaxation occurred.

news_web was immediately restarted using the captured exact ID. The upper
bound from stop command through Docker healthy and health/home/feed checks
was 12.5 seconds. ID/image/restart/OOM matched; StartedAt changed as expected.
Root independently confirmed shared Chrome fingerprint/OOM unchanged and
healthy production. All exact-label resources and the fresh stage/helper are
absent. Both backup units are independently confirmed garbage-collected and
inactive. The wrapper initially reported cancellation unconfirmed because
systemd had collected the units; a local named-property fix and regression
test now handle that state, without a second VPS run.

Local regressions: 141 passed plus 7 subtests; controller fake-executor tests: 10
passed. Evidence: artifacts/news-maintenance-gate-2026-10-17/result.json and
native-evidence.json. Native fixture/overload acceptance is still incomplete.

### Embeddings stop experiment and short runtime window

The user authorized temporarily stopping only memory-embeddings for the
synthetic gate. Luna added --short-window (runtime-only, 6-minute active /
8-minute hard deadlines) independently from the stopped-News profile; all
normal News/Chrome guards remain. A server-side exact-ID controller verifies
an independent10-minute return timer before stop and restores the same TEI
container/model with synthetic384D acceptance.

The first preflight-only stop aborted at PSI0.90 before any test child, then
restored embeddings in11.2s. The observer captured swap-in/major faults and
reclaim around model warmup/stop/start, with PSI3.42 during restoration.
Preflight was corrected to wait bounded45s after stop for three stable
admission samples, without lowering runtime PSI/reserve guards.

One actual native gate ran37.2s and stopped at runtime_working_set_above_budget
while waiting for Chrome: seven test cgroups totaled781.125 MiB versus768,
including test Cloak peak754.68 MiB. This includes charged cache/kernel memory.
Host PSI0.00, MemAvailable3213.64 MiB; no production reserve breach. Extraction
and fixture/overload checks did not start. The sampled aggregate budget is
too small for the observed native stand startup; native acceptance remains
incomplete, and no actual gate retry occurred.

Embeddings recovered in51.3s for the gate window (62.5s summed across both
windows), same ID/image/E5 revision, restart/OOM unchanged. Root verified TEI
health/info and synthetic384D vector, MCP health/get_stats, unchanged
News/Chrome/Memory-MCP fingerprints and News home/feed. All scoped resources,
backup units and both fresh stages/helpers are absent. Local checks:142
regressions+7 subtests;15 controller tests. Evidence:
artifacts/embeddings-maintenance-gate-2026-10-17/. No publication/deployment.


## Native cleanup verification — 2026-10-20

A user-authorized native Linux runtime-only repeat with supervisor-owned cleanup
finished `diagnostic_completed` with process exit 0 after 92.3 seconds. All 14
network deny probes, nine controlled browser cases and the 257-event overload
check passed. Relay receipts confirmed context/target absence; the subsequent
503 request allocated no new browser resources. The final supervisor guard
sampled all nine live containers before teardown, and the Docker race did not
recur. Exact-owned resources, helper, stage processes and private stage are absent.

Sampled aggregate cgroup charge peaked at 372.1 MiB. Host PSI full avg10 peaked
at 1.33, with seven threshold exceedances recorded in observation-only mode.
Aggregate memory never exceeded its threshold; other guards and hard caps stayed
active. This is diagnostic functional/lifecycle evidence, not strict resource-gate
acceptance, publisher parity or a long-run production capacity estimate. Native
Go race/build suites were not rerun. Independent production fingerprints,
max/OOM counters, health and real home/feed checks passed unchanged. No production
change or publication. Evidence: `artifacts/native-cleanup-verify-2026-10-20/`.


## Quality and stability attempt — 2026-10-20

The approved live comparison remains incomplete. The first C attempt consumed
case-0001 but produced no article result: the host supervisor process-tree RSS
guard interrupted it. B and the remaining C cases were not attempted. No live
retry was made. Read-only corpus validation used about 20 MiB RSS, disproving
the large-corpus hypothesis. The controller now uses detached observer execution
with an atomic completion receipt instead of a persistent host Docker exec CLI;
the 128 MiB host RSS limit is unchanged.

The subsequent single synthetic stability attempt completed four batches
(20 expected case outcomes, zero mismatches/OOM in completed batches), then
failed readiness with `native_capability_unavailable` after 99.71 driver seconds.
The total gate took 153.3 seconds; neither the requested 600-second observation
nor its final overload test completed. The underlying readiness failure was not
captured, so browser crash, taint and transport failure are not distinguished.
Sampled aggregate test-stack cgroup charge peaked at 407.2 MiB; it did not exceed
the observed budget. This is partial evidence, not stability acceptance.

Exact-owned test resources and stage were removed. Independent production
fingerprints/health, memory max/OOM deltas, and home/feed checks passed unchanged.
No production change, publication or repeat publisher request. Evidence:
`artifacts/quality-stability-2026-10-20/live-2/` and `stability/` in the same directory.

A localized diagnostic follow-up now preserves finite readiness failure reasons
(including HTTP status) in future soak summaries and failed batch receipts,
without saving response bodies or changing retries/limits. Fresh focused tests:
53 passed; Python compilation and diff checks passed. Not rerun on the VPS.


## Readiness recovery and synthetic stability — 2026-10-21

A native Linux diagnostic reproduced `CheckReady` failing its 500 ms discovery
budget and permanently clearing readiness while the containers remained alive
with zero OOM events. Idle readiness can now recover only after discovery of the
same previously verified browser generation. Initial bootstrap, replaced browser
generations, ownership/cleanup taint and closed workers cannot recover through
this path. Healthy active jobs retain their existing readiness snapshot; browser
capability reporting no longer repeats the discovery callback. The timeout and
resource caps are unchanged. The request that encounters a timeout still fails.

A separate intermittent test-relay upstream TCP connect timeout was captured at
two seconds. No capacity rejection, listener queue overflow, or OOM accompanied
it. Output SYN/SYNACK drop counters did not change from the control-probe baseline.
The exact TCP timeout cause remains unproved; diagnostic-only input counters,
relay loop-lag sampling and CPU throttling receipts were added without changing
network verdicts. Do not describe those instrumentation changes as a TCP fix.

The frozen diagnostic package completed one 600.03-second observation with
18 passing batches / 90 main case outcomes and 18 successful warmups, followed by
the 257-event overload and no-allocation post-taint 503 checks. The observation
includes about 376 seconds of batches with pauses and 224 seconds of final idle
readiness/cgroup checks; it is not 600 seconds of continuous extraction stress.
Worker/browser generations stayed unchanged until the intentional overload.
Sampled aggregate test-stack cgroup charge peaked at 416485376 bytes (397.2 MiB).
No strict production capacity or publisher parity acceptance is claimed.

Evidence: `artifacts/quality-stability-2026-10-21/stability-7/`; exact-owned resources
and stage removed, production fingerprints/health/max/OOM counters and home/feed
verified unchanged. The identical immutable package also passed on fresh stand `stability-8/`:
600.04 seconds, another 18 passing batches / 90 main outcomes and 18 successful
warmups, overload/disposal/post-taint checks passed. Peak sampled charge was
416325632 bytes. Production and exact-owned stage cleanup verified unchanged.
Both receipt sets passed the final strict warmup validator offline. See the
combined result at `artifacts/quality-stability-2026-10-21/result.json`.

Final local suites passed: 253 Python tests and 16 subtests, plus Go race suites
for transport, service and extract-service. A subsequent strict final warmup
acceptance assertion passed 14 focused tests and revalidated all 18 collected
passed batch receipts offline. No production changes, publisher retries, commit,
push, PR or image publication were performed.


## 2026-10-21 live comparison: incomplete

A fresh approved batch passed its synthetic pre-gate. C case-0001 returned HTTP403 (native TLS/HTTP proved; no article extraction acceptance). Python B failed its first case with browser_transport_unavailable. Ledger contains exactly B1/C1; remaining eight attempts were not executed. B relay recorded one protocol error and no connect timeout/OOM. Production fingerprints, health, memory max/OOM counters and home/feed unchanged; all owned resources and stages removed. Five private capture/report files remain root-only on VPS for seven days; no article text or URLs were exported. Evidence: `artifacts/quality-live-rss-2026-10-21/`.

An earlier bootstrap-only attempt stopped on the unchanged128MiB supervisor tree-RSS limit before creating any captures/ledger. Cleanup overwrote its triggering sample; the runner now preserves the first RSS failure sample separately, with a regression test. The later run did not trip that limit; the earlier cause remains unknown. Evidence: `artifacts/quality-live-2026-10-21/`. Next investigation targets Python WebSocket compression negotiation in the diagnostic relay, using local/synthetic fixtures only.


### Synthetic relay compression compatibility fix

The local real WebSocket client/server regression reproduces the Python default permessage-deflate mismatch: relay forwarded extension negotiation but its frame parser rejects RSV1. In synthetic inspection mode only, relay now strips Sec-WebSocket-Extensions from the upstream handshake; parser/caps/network/ownership checks are unchanged. Identity-patched old behavior fails, corrected default-client200KB response passes.237related Python tests+16subtests passed (one existing Pydantic warning).

Native Linux/Cloak C synthetic regression passed93.3seconds:14network probes,9browser scenarios,257event overload, owned disposal and post-taint rejection. Production and exactowned cleanup independently verified. Evidence:`artifacts/relay-compression-synthetic-2026-10-21/`. No publisher requests followed the fix; native Python live acceptance and five-article quality parity remain unverified.


## 2026-10-21 supervisor memory diagnosis

The approved post-relay-fix live batch passed pre-gate and C smoke (HTTP403), then aborted before B navigation on133636KiB summed host-treeRSS vs unchanged131072KiB cap. B0/C1; no quality parity. Only3anchor cgroups existed; the snapshot contained6Python processes plus a transient DockerCLI. The raw-network gate deliberately runs3namespace trap helpers concurrently. MemAvailable~4GiB, PSI0; this does not show browser/application exhaustion or a leak. Triggering rows survive in `artifacts/quality-live-compression-2026-10-21/native-evidence.json`. Private results retained separately7days; production and cleanup verified.

Provenance hashing now uses fixed1MiB reads rather than whole-file reads. A no-network VPS comparison on identical frozen binaries reduced hash-process retainedRSS by~10MiB and peak by~12MiB; exact hashes matched. No guard cap/accounting changes.238tests+16subtests passed. Fresh nativeC synthetic gate passed101.3s with noRSS trip; network/browser/overload/cleanup and independent production checks passed. Evidence:`artifacts/stream-hash-synthetic-2026-10-21/`. This single synthetic pass does not prove full live-tree stability; nativePython live and five-article extraction parity remain unverified. No publisher requests followed this memory change.


## 2026-10-21 latest live evidence: B complete, C incomplete

After bounded hashing, the fresh approved comparison passed synthetic pre-gate, C smoke, and the full B phase with final guards; noRSS trip. B attempted5 cases:0001 HTTP403,0008 resource_limit afterHTTP200,0034/0041 ~30s timeouts,0050 extracted5132chars. C attempted2:0001 HTTP403,0008 native_browser_unavailable; the latter is fatal so remaining3C cases were not attempted. B5/C2 ledger is valid withoutduplicates; C report incomplete. Quality parity is not accepted. Evidence:`artifacts/quality-live-stream-hash-2026-10-21/`.

C failure relay recorded9connections/9connected, no protocol or connect errors. Underlying Go cause is unproven: the current public code does not distinguish eventoverflow/transport/ownership. Existing finite synthetic stage/reason diagnostics were disabled for live. A next diagnostic should capture finite terminalstage/reason before teardown without URLs/IDs/body; do not assert overflow from the public error. Production fingerprint/health/max/OOM/home/feed independently unchanged; ownedresources/helpers/stage removed.10privatefiles64519bytes retained root-only7days onVPS.


## 2026-10-21 confirmed CDP overflow

Opt-in finite live diagnostics captured dom/event_overflow and cleanup/event_overflow for Ccase0008. Evidence:`artifacts/quality-live-diagnostics-2026-10-21/`; B5/C2, remaining3C unattempted. This proves the failure class, not which liveRPC blocked. Local characterization holds Fetch.continueRequest reply while injecting300relevant events, reproducing overflow with existing256queue and correct ownedcleanup/taint. No behaviorfix yet. Production and exactowned cleanup verified; privatecaptures retained7days. Diagnostic-only candidate passed247Python+16subtests, MacARM64Go race suites and pinnedLinuxamd64 static build.

Proposed concurrency change and acceptance/rollback gate:`docs/ai-plans/2026-10-21-go-cloak-event-consumer.html`. Implement only after explicit approval required by project HTML Planning Gate.


## 2026-10-21 final scoped acceptance: owned page crash isolation

The final frozen binary manifest `139d1404476b8d7b68fe979cb1443d38e6620ecd49102744a49368ded0a3fc55`
passed the controlled Linux native gate in 131.3 seconds. A trusted test helper
selected only the exact owned synthetic fixture target and requested `Page.crash`.
The service returned empty `failed/native_browser_target_crashed`, recovered
readiness on the same browser generation, and extracted the next independent
article successfully. The crashed article was not retried. The wire-limit case
retained `resource_limit` and also allowed the next article to succeed. True
257-event overflow, verified owned disposal and post-taint 503 rejection passed.
Evidence: `artifacts/owned-page-crash-controlled-native-2026-10-21/result.json`.

The preceding final-native attempt is retained separately: its stress page
returned `resource_limit`, so it did not exercise a crash. The deterministic
helper replaces that unreliable crash trigger; service runtime was unchanged.
Fresh affected Python checks: 413 tests and 16 subtests passed. Full Go race
checks and the Linux static build are recorded in the verified-build evidence.
Production fingerprints, health, max/OOM counters and home/feed were independently
verified unchanged; all owned test containers, networks, helper and stage were
removed. No further test remains running.

This closes the approved crash-isolation scope. The earlier five-case live run
and 600-second / 95-outcome synthetic observation used the earlier `f5c1ab...`
candidate; final guards have local race and the short native proof above. They
are not all tests of an identical binary. Extraction quality parity remains
unaccepted (including the case-0041 no-content difference). The last native run
was diagnostic: memory PSI thresholds were observed with 31 bypass samples,
so it is not strict resource-capacity acceptance. No production change, new
publisher request, commit, push, PR or image publication was performed.


### Approved deferred retry follow-up (2026-10-21)

`native_browser_target_crashed` is no longer a terminal article-processing error
in the Python client. Its wire `retryable=false` and the extractor's immediate
fallback allowlist are unchanged: one invocation still fails without replay or
Python fallback. The application persists `retry_wait` and schedules a later
processing cycle using the existing configured attempt budget and delays
(defaults: three total attempts, 30 minutes after the first failure, six hours
after the second). Repeated failures become `exhausted`, retain their error,
and remain available for explicit manual retry. Existing rejected historical
rows are not automatically reset. Existing queue eligibility/date filters remain.

Local checks: 208 tests passed across extractor service client, processing
batches and processing outcomes. A persisted lifecycle regression uses the real
AIProcessor with isolated SQLite: crash schedules a delayed attempt, a sibling
completes, a cycle before the due time does no replay, then a later attempt can
succeed or exhaust its budget. Restoring the old terminal classification makes
that test fail. A loopback API regression proves exactly one service request
and no Python fallback even when fallback is configured. No Go changes, new
publisher requests, server tests, production changes or external Git actions
were needed for this follow-up. This follow-up is implemented locally only.


### 2026-10-21 case-0041 baseline correction

The 1,433-character Python result was a membership plan chooser, not evidence
of a successfully extracted article. Its retained text matches the complete
existing Go signature: membership near the start, multiple annual prices and
billing labels, cancellation text and the terminal free-tier CTA. Offline Go
replay of the retained B rendered HTML (20,990 bytes) also returns no content.
C rendered HTML was not retained, so the exact DOM seen by that live invocation
cannot be asserted to match B.

Python's shared `ExtractionUtils.is_good_content` now rejects the same narrow
complete chooser signature, after the existing text-size bound. Ordinary
editorial discussion of memberships, prices and cancellation remains accepted.
Synthetic tests cover the shared validator and selector/heuristic paths; the
three regressions fail with the guard disabled. The exact new predicate also
rejects the retained B result without exporting its text. 154 affected Python
tests passed. Evidence: `artifacts/case0041-quality-2026-10-21/result.json`.

This corrects a false baseline success; it does not recover a paywalled article
or establish full extraction parity. No Go source changed, no publisher request
was made, and no production service/container was changed. The offline parser
helper and its owned temporary VPS directory were removed; original retained
captures keep their existing retention policy.


## 2026-10-21 final candidate control run: B5/C5 complete

The explicitly approved isolated comparison completed in 592.9 seconds on the
final Go manifest `139d1404476b8d7b68fe979cb1443d38e6620ecd49102744a49368ded0a3fc55`.
A narrow stand correction mounts current `extraction_utils.py` read-only beside
`service_client.py`, so Python uses the corrected membership guard. The source
package contains 70 inputs with manifest
`b50bee65d74d792df3662d27daf392faf1cfc17ad3821f6eb69e768692f1933a`.
136 focused local checks passed before dispatch; Go source/binaries were unchanged.

Both ledgers contain exactly five unique attempts, no fatal series error or
duplicate navigation. Outcomes:

| Case | Python B | Go C |
|---|---|---|
| 0001 | HTTP403 | HTTP403 |
| 0008 | resource_limit | resource_limit |
| 0034 | timeout | no_content |
| 0041 | timeout | owned target crash |
| 0050 | success, 5,132 characters | success, 5,144 characters |

Go continued after the live case-0041 crash and extracted case-0050. The
synthetic pre-gate also passed controlled crash recovery, wire-limit recovery,
true 257-event overflow and post-taint rejection. The last successful article's
text shingle Jaccard was 0.991892 and smaller-text containment 0.997283. This
supports continuation and close text agreement for that one article; only one
of five extracted successfully in each implementation. Full quality parity
and production readiness are not accepted. Case-0041 timed out in Python, so
this run does not itself prove the membership guard fired; its saved-result
and synthetic proof remains separate.

No OOM deltas were reported in the B/C resource receipts. Diagnostic PSI bypass
count was 114, with no working-set-budget bypass, so strict VPS capacity is not
accepted. Relay receipts include one connect timeout and two transport errors
across phases; their causes remain unproved. No further run or investigation
was started automatically.

Independent postchecks verified production fingerprints/health/max/OOM unchanged
and home/feed HTTP200. All owned containers, networks, helpers, processes and
stage were removed. Thirteen private capture/report files (75,226 bytes) remain
root-only under their existing seven-day retention metadata, expiring
2026-10-28T15:54:02Z; no article text or URLs were exported. No production
configuration, commit, push, PR or image publication was changed.

Evidence: `artifacts/final-control-live-2026-10-21/result.json`,
`comparison.json`, `native-evidence.json`, `root-postflight-verification.json`.


## Native readiness revision 2 — 2026-10-22

The approved fatal-worker lifecycle is implemented. Permanent taint clears
readiness and emits a single fatal signal; the service performs bounded drain
and owned cleanup, then exits 1 for its supervisor. Transient discovery failure,
normal shutdown and a successfully isolated owned-page crash do not terminate
the worker. No shared browser restart or Browser.close was introduced.

Fresh Mac race checks passed for transport, service and extract-service; relevant
Python queue/extraction suites passed (251 tests). Extractor/pipeline/replay
checks and go vet passed. Real Linux amd64 race executables passed all three
packages, including fatal exit and a fresh process becoming ready on the same
fake CDP endpoint. These process tests do not prove real Cloak recovery.

Build provenance: base c9441de4a6e0314a3c7073158ac960de11b9b4d3, 54 Go inputs;
binary manifest SHA256
`2b3cada429b8a8fbd351691b774ad5b8829d30f3728c4f55aeae24bf66e64a20`.
Evidence: `artifacts/native-prod-readiness-2026-10-22/`.

The explicit synthetic recovery gate preserves all parent guards and only
allows the exact marker-named, owned worker to exit 1 temporarily. It tests
stopped-endpoint refusal, absence of new allocation, explicit worker start,
fresh readiness, a following successful article and unchanged browser identity.
Affected harness suites passed independently: 203 tests and 22 subtests.

One strict native VPS run was stopped after 23.2 seconds by host
`memory_psi_pressure` (full avg10 0.51), before recovery was exercised. Sampled
fixture working-set peak was 76,881,920 bytes (about 73 MiB); this partial startup
sample is not service sizing or proof of pressure attribution. Recovery and
strict capacity acceptance remain open. No bypass or automatic retry occurred.
Production fingerprints/health and home/feed HTTP200 were independently
verified; all owned containers, networks, processes and the stage were removed.
Evidence: `artifacts/native-worker-recovery-2026-10-22/result.json`.

The shared production browser has no host CDP port, but its actual egress
boundary remains unverified. Native deployment drafts depend on the pending
choice between isolating the one shared Cloak and adding a dedicated instance.
No production switch, topology mutation, publication or new publisher request
was performed. No new evidence justifies changing no-content retry semantics.


### Shared browser profile preparation — 2026-10-22

The user selected one shared Cloak. Luna prepared the worker-only
`deploy/news-extractor/compose.native-standby.yml`; root independently verified
Compose merge with dummy inputs, four missing-variable rejections, concurrency
one, inherited security/resource settings, 55-second stop grace, no new browser
and unchanged Python routing. Evidence:
`artifacts/shared-cloak-profile-2026-10-22/validation.json`.

Read-only known-endpoint inventory found `news_web` using healthy `infra-chrome`;
it does not exhaustively enumerate all CDP clients. No services were started,
restarted or changed. Real native recovery/capacity acceptance remains open.

`deploy/news-extractor/shared-browser-isolation.md` records the concrete boundary
and rollback requirements. Isolation cannot be activated before adapting the
remaining Python browser callers to an owned proxy context; the production pool
currently opens direct tabs. That broader workflow delta is recorded in the
existing HTML plan for separate approval. The overlay alone creates no firewall
or gateway. The existing extractor image packages only the service binary, so a
gateway release artifact also remains required. No production readiness or
completed network-isolation claim is made.


### Opt-in Python proxy contexts — 2026-10-22

The user approved the Python compatibility delta. Luna implemented validated
`BROWSER_PROXY_ENABLED=false` defaults, URL and SecretStr token settings, an
owned disposable context adapter and pool integration for the existing article,
Telegram and Page Monitor callers. Handlers precede navigation; only matching
Proxy challenges receive extractor/token credentials. Public HTTPS443 is
required. Errors and unknown ownership are explicit, cleanup uses the exact live
connection/generation, and proxy failures do not request shared-browser restart.

Root reviewed the code and ran the final affected local suites: 137 passed,
10 warnings. Seven protocol cases use real nodriver against loopback CDP,
including actual Page Monitor connection results and a fresh following context.
Compile, diff whitespace and dummy Compose validation passed. No browser engine,
publisher or VPS run occurred. File hashes/results:
`artifacts/python-proxy-context-2026-10-22/checks.json`.

The optional `deploy/news-extractor/compose.python-proxy.yml` is a local draft,
not activated. Linux isolation, real shared-Cloak acceptance, fatal-worker
recovery and strict resource acceptance remain open. No production configuration,
commit, push, PR or image publication was changed.


### Shared native Linux gate — 2026-10-22

The approved opt-in worker-recovery profile now mounts the current Python
config/pool/proxy files read-only and checks production browser_tab navigation,
proxy/publisher auth separation, exact context-baseline cleanup and real browser
discovery generation before running the Go cases. Local harness verification:
207 passed, 22 subtests. The versioned payload includes 73 source files and the
same previously verified Linux binaries; no new image was pulled or published.

The single strict VPS run passed 14 raw namespace-denial probes with zero trap
connections, then stopped during Cloak startup after 26.2 seconds on host memory
PSI full avg10=0.55. Neither the Python shared-browser checks nor worker recovery
was reached. Last MemAvailable was 4,370,767,872 bytes; sampled partial runtime
peak was 183,533,568 bytes. These numbers are not a production sizing result or
proof of pressure attribution. There was no guard bypass or automatic retry.

Independent production fingerprints/health, memory max/OOM deltas0 and home/feed
HTTP200 passed. All exact-owned test resources and the stage were removed.
Read-only post-run container pressure/events were collected; historical counters
cannot establish the cause of this run. Linux production isolation, actual shared
Python/Go compatibility and fatal recovery remain open.
Evidence: `artifacts/shared-native-acceptance-2026-10-22/result.json`,
`pressure-after.json`, `root-postflight-verification.json`.


### PSI startup localization — 2026-10-22

Added best-effort owned-cgroup PSI/events/limits/swap, host PSI/vmstat interval
deltas and an eight-sample diagnostic ring; thresholds and caps are unchanged.
Root verified 211 harness tests and 22 subtests. One instrumented strict run
stopped after 23.2 seconds during test Cloak startup. Its cgroup accumulated
180,057 microseconds full memory stall, with 61,726,720 bytes current charge,
768-MiB max, zero high/max/OOM events and zero swap. Host available memory was
4,398,768,128 bytes; retained startup direct-reclaim/swap counter deltas were
zero. The kernel did not expose memory.peak or the selected allocstall key.
A bounded read-only neighbor sampler recorded 24 snapshots with no observed
neighbor PSI/event interval increments. Stalls were observed in the test Cloak
startup; their internal mechanism remains unproved.

Production fingerprints/health/max/OOM deltas and home/feed200 passed; all owned
resources and stage were removed. Python shared-browser and fatal-worker
recovery were not reached. No retry, guard bypass, production mutation or cap
increase occurred. A profile-specific sustained-PSI proposal is recorded in the
existing HTML plan and awaits separate approval; it is not implemented.
Evidence: `artifacts/psi-startup-diagnostic-2026-10-22/result.json`.


### Go-only worker recovery acceptance — 2026-10-22

The explicit synthetic `--worker-recovery-gate --go-worker-only` profile passed on the pinned Linux/Cloak runtime. Owned-page crash recovery and terminal CDP overflow/worker exit/restart/fresh readiness/next article were verified with the same browser container and generation. Fourteen raw network probes were denied with zero trap accepts. Evidence: `artifacts/go-worker-only-2026-10-22/result.json` and `native-evidence.json`. Production postflight and exact-owned cleanup passed.

This accepts Go worker recovery with memory caps only; Python shared-browser compatibility was explicitly not tested, and strict PSI capacity/production sizing are not accepted. Python compatibility remains a separate shared-browser release requirement: Telegram and Page Monitor still use the Python pool. The application article client also still sends `use_browser=False` from the HTTP-only pilot; native article routing has not been switched. No deployment, publication, commit, or PR was performed.


### Native article routing opt-in implemented locally — 2026-10-22

`EXTRACTION_SERVICE_NATIVE_BROWSER` now defaults to false. Explicit true requires
Go mode and Python fallback disabled; the article client then sends
`use_browser=true`, clamps bytes to the smaller HTTP/browser cap, and never
replays a native failure through Python extraction. Existing metadata, learning
and durable processing remain in Python. Shadow and default pilot stay HTTP-only.
`compose.native-canary.yml` is the separate activation draft; final rollback
explicitly resets the native flag. No production activation was performed.

Independent local checks: 169 tests passed including real loopback HTTP routing,
config and durable queue/AI regressions; dummy Compose activation/rollback renders
passed. Evidence: `artifacts/native-article-routing-2026-10-22/checks.json`.
The new client and real native worker have separate acceptance evidence; one
combined client-to-Go-to-Cloak campaign is still open. Telegram and Page Monitor
remain on Python browser paths; their migration is deferred as requested.
