# Go/Cloak development handoff to Mac — 2026-10-16

This branch preserves the entire in-progress extractor prototype from the
Windows working tree. It is a development checkpoint, not a production release.

## Checkout and environment

Repository: `https://github.com/Dzarlax-AI/evening-news.git`.
Branch: `codex/go-cloak-mac-handoff`.
Base: `72d39a6514fb86b57eadc98b2e6510855f946088` (merged memory fixes, PR #8).

On the existing Mac checkout, inspect its status, branch and remotes first. Do
not reset, clean or overwrite local changes. Fetch origin and switch to the
remote handoff branch only when local changes are preserved. Do not cherry-pick
just the event filter: the complete Go module and Python adapter are in this
checkpoint. The workstation is Apple Silicon M4 Pro; Linux production is a
separate validation environment. Use Go from `go/go.mod` (1.25.6), Python 3.12
and `requirements.txt`. Do not transfer the Windows venv or private SSH keys.

The handoff commit uses `[skip ci]`: the existing Docker workflow publishes
images on arbitrary branch pushes. This transfer must not publish an image.
There is no PR or deployment associated with this checkpoint.

## Read first

- [Live shadow results and diagnostic](go-cloak-live-shadow-2026-10-16.md).
- [Native worker behavior and synthetic soak](go-cloak-native-worker.md).
- [Approved native worker plan](ai-plans/2026-10-16-go-cloak-native-worker.html).
- [Approved isolated live batch plan](ai-plans/2026-10-16-go-cloak-live-shadow.html).
- [Backend migration roadmap](ai-plans/2026-10-16-go-backend-migration-roadmap.html):
  a long-term proposal, not approval to rewrite the entire backend.
- [Extraction method inventory](extraction-methods.md) and
  [parity contract](go-parity-contract.md).
- [Deployment restrictions](../deploy/news-extractor/README.md).

## Current implementation

The new Go module includes extraction, an independent HTTP service, public
egress gateway and a native Cloak CDP path. Python remains the production owner;
service routing defaults to `python`. The HTTP-only adapter does not silently
enable the native browser path. Source extraction, metadata, media and learning
semantics must be considered together. Telegram authenticated/media paths remain
explicitly separate where the Go contract lacks equivalent support.

Native browser execution uses actual Cloak navigation, not supplied responses.
Public-origin test stands use the pinned production Cloak binary, fresh contexts,
an authenticated public-HTTPS gateway, namespace isolation and a restricted CDP
relay. Their launcher lifecycle differs from production cloakserve; do not claim
complete production equivalence. Keep TLS, admission and ownership guards.

## What has actually been observed

The live batch completed 50 Python attempts: 3 nonempty results, 10 HTTP 403,
25 resource-limit rejections, 10 timeouts and 2 site errors. Go attempted 8:
7 HTTP 403 and a fatal `native_browser_unavailable`; the other 42 were not
attempted. These results do not establish quality parity or RAM savings.

One separately approved diagnostic repeat of case-0008 reproduced the failure
after 12998 ms. Navigation succeeded, then the CDP event queue overflowed.
The reader exited, command replies stopped, cleanup receipts failed and the
worker lost readiness. The original uninstrumented failure's precise phase
remains unproven. The diagnostic stand was fully removed; production image,
start times and restart counters were unchanged at 13:33:15 UTC.

Offline parsing of three successful Python DOM captures produced two Go text
results and one no_content. Media differences remain. Those results do not
replace native browser validation.

The latest local fix filters events **before enqueueing** in
`go/transport/browser.go`. Only the 16 event methods consumed by the native and
supplied-HTML handlers remain. All replies bypass the filter. The queue remains
256 messages; overflow of required events still fails closed and still requires
separate recovery work. Do not claim overload handling is fully fixed.

`TestBrowserCDPEventFiltering` reproduces 1024 unused events before a command
reply with no event consumer. It failed before filtering and passes afterwards;
required-event overload remains bounded and fails closed. This fix has not been
tested against a real Cloak page yet. No new public-origin request is authorized
by this handoff; the previously approved diagnostic exception was already used.

## Validation at checkpoint

- Windows `go test ./...`: passed; browser-dependent tests without an endpoint
  skip. Linux `go test -race ./...`, `go vet ./...`, `go mod verify` passed for
  the filter in a capped, network-disabled container on a disposable source copy.
- A fresh Windows run of the service-client, service-comparison, extraction-replay,
  native benchmark/soak/shadow/corpus/gate/stand/relay suites: **318 passed**.
  This is a focused suite, not a fresh full PostgreSQL application integration run.
- Windows race was unavailable with CGO disabled; do not report it as passed.
- Earlier controlled Cloak gates passed. Injected-resolver mixed/private DNS and
  rebinding unit tests are not external public-DNS rebinding integration evidence.

For a new local setup, start with `cd go && go test -race ./... && go vet ./...`.
Install Python dependencies into a new Mac venv, disable the scheduler, and run
the focused suites above. Native Linux/amd64 behavior must be checked separately
from an ARM64 Mac run. Do not enable real-browser tests against shared production.

## VPS evidence and separate soak

Raw URLs, DOMs, source content, private logs, credentials and Windows artifacts
are intentionally absent from Git. The authorized VPS stage is
`/tmp/news-native-cloak.vh6lLSy2`; raw corpus/captures are root-only under
`shadow-private/`, with manual retention expiry 2026-10-23 12:01:31 UTC.
Do not download raw evidence into the public repository. Redacted findings are
preserved in the linked reports. One-off diagnostic tools remain in the VPS
stage; ignored Windows `artifacts/` is not required to build the checked-in code.

A separate synthetic soak started 10:28:54 UTC with an approved four-hour
cutoff at 14:28:54 UTC (16:28:54 Europe/Belgrade). It uses an earlier uninterrupted
binary/configuration, not the new filter. At 14:28:15 UTC it was still running:
23 completed batches / 690 jobs, 552 expected successes / 138 expected rejections,
zero expectation mismatches. A later read-only observation at 14:29:47 UTC found
`summary_state=interrupted`, the same 23 completed batches / 690 jobs with zero
expectation mismatches, and no remaining soak containers. This is consistent
with the approved cutoff; it is not a 24-hour pass or validation of the new filter.
The original 86400-second driver's interruption is preserved honestly. Its
supervisor and cleanup already ran; do not restart or duplicate this soak.
Detailed summary and policy remain under `out/driver-soak/summary.json` and
`out/soak-shortening.json` for read-only inspection.

## Next work and boundaries

1. Verify the Mac checkout and rerun local checks without real credentials.
2. Review the separate soak's final evidence; do not mix it with public-origin
   results or validation of the filter.
3. Exercise the filter on synthetic Linux Cloak fixtures first. Preserve policy,
   request/byte limits, readiness criteria and both ownership absence receipts.
4. Before any additional public case replay, agree on the bounded scope: previous
   per-URL limits and the single diagnostic exception have already been consumed.
5. If required events still overflow, design bounded event dispatch that keeps
   command replies and cleanup alive. Do not enlarge the queue without bounds or
   drop security/accounting events. Use an HTML approval plan for architecture,
   deployment, API or broader behavior changes.

Production stays unchanged. No merge, PR, image publication, production rollout,
browser restart, DB migration, AI/learning/delivery run or full backend rewrite
is authorized by the workstation transfer. All existing local changes on the
Mac must be preserved. Continue the conversation in Russian; code comments and
Git/outgoing communication stay in English.

## Mac continuation: local validation on 2026-10-16

Checked out `codex/go-cloak-mac-handoff` at exactly
`c9441de4a6e0314a3c7073158ac960de11b9b4d3`. The previous checkout was `main`
at `48d1d49a0ee745a8ce0d17a745cb81cde2c601f9`, with no tracked modifications.
The untracked `AGENTS.md` remains unchanged. The other untracked file,
`docs/ai-plans/2026-08-19-infra-chrome-cdp-connection-leak.html`, was identical
to the tracked handoff version (SHA-256
`d993ab6e2411ec5ce22e690ac29331fb53f0a010599d6a9a59f617d72b071c5f`).
Both originals were copied to `/tmp/evening-news-mac-handoff-backup` before
switching; no reset, clean, commit, push or PR was performed.

Environment: darwin/arm64, Python 3.12.14 in a fresh `.venv`, dependencies from
`requirements.txt`, and Go 1.25.6 selected explicitly through `GOTOOLCHAIN`.
The system Go is 1.26.4; changing `go.mod` to that version was unnecessary.
`pip check` passed. Test commands set `SCHEDULER_ENABLED=false`, empty Gemini
and Telegram credentials, and an unused loopback database endpoint. The app,
scheduler, AI, DB processing and delivery were not started. The existing runtime
configuration was not overwritten.

Fresh checks:

- Mac `go test -race -count=1 ./...`: all eight packages passed, including a
  fresh complete run after the test-fixture correction below. Browser tests
  without explicit endpoints skip in this suite.
- Mac `go vet ./...` and `go mod verify`: passed.
- `TestBrowserCDPEventFiltering`, race enabled, `-count=100`: passed. Both
  unused-event filtering and required-event fail-closed overload remain tested.
- Focused Python service-client/comparison/replay, native benchmark/soak/shadow,
  corpus/gate/stand/relay suite: **318 passed**, seven warnings. An initial
  sandboxed invocation failed on forbidden loopback socket binds; the permitted
  invocation passed. This is not a full PostgreSQL application integration run.
- All eight cross-compiled Linux/amd64 Go test binaries passed in a capped,
  `--network none` container after the fixture correction. This was emulated
  Linux execution on Apple Silicon, with CGO disabled, **not Linux race** or
  native-amd64 performance evidence. Four external-browser tests skipped there.

The first Linux suite exposed a scheduling-dependent request-budget fixture:
`Page.navigate` emitted 129 request/finish pairs plus the document event before
its reply (259 required events for a 256-entry queue). Depending on scheduling,
queue overload could win before the intended cumulative request-limit error.
A single-processor reproduction confirmed intermittent overflow. Only
`go/transport/native_browser_test.go` changed: each synthetic request now waits
for its Fetch continuation before completing and admitting the next request.
The assertion additionally requires exactly 128 continuations, owned-resource
cleanup and retained readiness. Thirty Linux/amd64 repetitions with
`GOMAXPROCS=1` passed afterwards. Disabling only the cumulative guard in a
disposable source copy made the corrected test fail with a nil error, confirming
that active-request accounting alone cannot satisfy the test. Runtime event
dispatch, queue capacity, policy and accounting limits are unchanged.

### Post-filter pinned-Cloak synthetic smoke

Two fresh disposable containers used the exact pinned Linux/amd64 Cloak image
above under emulation. Recorded browser SHA-256 matched
`715722e8605ae3ce81523c1218aba1ec89425786ab33ceaf99f8a6cb5e70e6e8`.
Each container had `--network none`, no published ports, two CPUs, 1536 MiB
combined memory, 256 PIDs, 1-GiB shm, no added capabilities and no restart policy.
The browser, Go gateway and synthetic HTTPS origin shared only that container's
loopback namespace. An exact short-lived fixture SPKI was trusted only by the
test browser; no global TLS bypass was introduced. Tokens were synthetic.

- First stand: **6/6 capability probes and 6/6 HTTPS JS/cookie fetches passed**.
  Each fetch required the rendered cookie-confirmation anchor and retained
  readiness after native cleanup.
- Second stand: **6/6 capability probes and 6/6 fan-out pages passed**. Each
  page awaited 96 same-origin fixture subrequests before setting its readiness
  marker. Origin evidence recorded all **576 subrequests**, six main documents
  and one fixture 404. All recorded origin requests used TLS 1.3. The ordinary
  JS/cookie stand also recorded one fixture 404; no external site was requested.

These probes used the checkpoint transport binary before the fixture-only edit;
runtime sources remain identical. There was no before/after Cloak comparison,
unused-event count instrumentation, RAM comparison, new soak, public-origin
replay or public-network isolation gate. Sharing a loopback namespace is a
deliberately narrower smoke than the Linux nftables/UID-restricted relay stand;
it does not prove private-egress isolation. The host `nsenter`/`/proc` stand
cannot run directly on macOS. Required-event overload recovery and production
release gates remain open. No VPS or production access was made.

Local ignored evidence, disposable runners and cross-compiled binaries are in
`artifacts/mac-handoff-2026-10-16/` (including `burst/`). Go caches are also there;
reuse `GOTOOLCHAIN=go1.25.6` and that directory's `gopath`/`go-cache` for checks.
No test containers or labeled networks remained after completion. No Docker
image was built or published; the pinned upstream image was only pulled.

## Bounded native Linux gate follow-up

The separately approved synthetic VPS gate ran beside production once, using
Go 1.25.6 on Linux/amd64 with CGO and race detection, one CPU, 1536 MiB, no swap
and no network. Module verification passed. Seven of eight packages passed;
`cmd/extract-replay` failed because its 64-MiB manifest boundary fixture wrote
to the harness's 32-MiB `/tmp` tmpfs. The gate stopped after 159.3 seconds.
Static builds, Cloak fixtures and relay overload were not attempted on the VPS.

Luna implemented the bounded supervisor, stand profile and opt-in frame-aware
relay. The relay correlates owned context/target/session receipts, emits exactly
257 notifications before a real Fetch reply, and verifies context disposal plus
both later absence replies. Local loopback and harness checks passed: 72 tests,
zero warnings. The Go runtime source was unchanged during this harness work.

The local harness correction sets both `GOTMPDIR` and `TMPDIR` to `/go-tmp`, an
already allocated writable stage mount; resource caps are unchanged. All three
boundary cases pass locally with the race detector. This correction has not
been rerun on native Linux. The first approval allowed one VPS gate only, so
the second run received separate approval and used a fresh stage/preflight.

All owned test containers and networks were removed. Production fingerprints
and OOM counters were unchanged; health, home page and feed/no-mock checks
passed afterwards. The new synthetic stage was removed after evidence capture;
the old private live-shadow stage was untouched. See the
[Linux gate plan and actual results](ai-plans/2026-10-16-go-cloak-linux-vps-synthetic-gate.html)
and `artifacts/linux-vps-gate-2026-10-16/`. No commit, push, PR or image publication
was performed.

### Approved repeat stopped on memory pressure

The user approved one repeat with the TMPDIR fix and unchanged caps. Luna ran
it from a fresh root-only stage using the verified 65-file payload. It stopped
after 125.3 seconds during Linux race testing with `memory_psi_pressure`
(full avg10 guard threshold greater than 0.5%). The trigger value was not
recorded; a later zero sample is not a peak measurement. Module verification
and the egress command package passed before termination. The full race suite,
static builds and native Cloak/relay gate remain unverified on the VPS.

Production fingerprints and OOM counters stayed unchanged; home/feed checks
passed. Exact-label containers/networks and the new stage/helper were removed.
The already removed Go test container's own OOM state is unavailable. There was
no further attempt, cap increase or code change. Evidence is in
`artifacts/linux-vps-gate-2026-10-16/repeat-2/`. Investigate race workload memory
locally or use a separate Linux runner before proposing another shared-host run.

### Local memory diagnosis after the PSI stop

Luna profiled the preserved dirty working tree locally with Go 1.25.6,
Linux/amd64 emulation inside Docker Desktop's ARM VM. Separate networkless
containers used the original 1536-MiB/no-swap, one-CPU, 128-PID limits and
staged disk temp files. The full eight-package race suite was not rerun.

| Measured phase | Cgroup memory.peak | Wall time |
|---|---:|---:|
| Cold module verification and race test compilation | 382.33 MiB | 123.06 s |
| Replay 64-MiB boundary test, race enabled | 1150.69 MiB | 21.20 s |
| Other five replay tests combined, race enabled | 61.38 MiB | 1.14 s |
| Same boundary test, race disabled | 478.49 MiB | 1.43 s |

All measured workloads passed, without cgroup max/OOM events or full-PSI
stall deltas. The cgroup peaks include the sampler and file cache; anonymous
memory at the sampled race-boundary peak was 1072.98 MiB. Pprof attributed
99.93% of sampled cumulative allocation to `encoding/json.(*Decoder).refill`.
This allocation volume is separate from peak/live memory. The second
`Decode(new(any))` checks trailing data and retains the fixture's whitespace
in a growing decoder buffer, providing a concrete local optimization target.

These are local/emulated diagnostic results, not the missing VPS PSI sample
or a native production-service RAM benchmark. All 52 staged Go/module hashes
still match the checkout; source was not changed. Exact-label containers were
removed and temp fixtures cleared. Evidence, commands and setup-diagnostic
limitations are in `artifacts/race-memory-profile-2026-10-16/profile-report.md`.
The next candidate change is bounded streaming validation of the trailing
whitespace while preserving the inclusive 64-MiB limit, sentinel-byte overflow
check, strict unknown-field handling and rejection of trailing non-whitespace.

### Local JSON whitespace allocation fix

The user approved fixing both the replay CLI and Go HTTP service. Luna added
`internal/jsonsingle`, a bounded single-value decoder that consumes leading
JSON whitespace before decoding and scans the buffered/unread tail in fixed
chunks. The service no longer copies the entire body with `io.ReadAll`.
Unknown fields, multiple values, non-JSON whitespace and non-EOF reader errors
remain rejected. Temporary empty reads do not terminate tail validation.

The CLI keeps its inclusive 64-MiB bound plus one sentinel byte. The API keeps
the inclusive 8-MiB bound and `413` precedence for oversized bodies, including
malformed/unknown-field prefixes; authentication, readiness and admission still
precede body reads. Five admitted near-limit requests complete, while a sixth
returns `429` without reading its body. Original three real 64-MiB boundary
fixtures were retained. Root independently reran the new helper and affected
API race tests on macOS ARM64 with pinned Go 1.25.6; they passed.

Separate compiled race-test binaries were measured locally in the same pinned
Linux/amd64 image under emulation, with 1536 MiB/no swap, one CPU, 128 PIDs and
network disabled, using the preserved cgroup sampler.

| Measured workload | Before peak | After peak | After wall time |
|---|---:|---:|---:|
| Replay three 64-MiB boundary cases | 1150.69 MiB | 125.16 MiB | 3.05 s |
| Five concurrent near-8-MiB API bodies | Not measured | 63.00 MiB | 1.72 s |

Both successful measured runs passed without cgroup memory-event changes or
full-PSI stall deltas. These figures include sampler/file-cache overhead and
exclude browser workload; they do not measure production memory or prove the
previous VPS PSI trigger resolved. No VPS retry or real-site request was made
for this fix. Evidence and setup diagnostics are in
`artifacts/json-whitespace-fix-after-2026-10-16/`; the pre-fix dirty diff and
source copies remain in `artifacts/json-whitespace-fix-2026-10-16/`.

The final pinned-Go 1.25.6 Linux/amd64-emulated full race suite passed all
nine packages (`go test -race -count=1 ./...`). Its whole-container peak,
including compilation, was 480.50 MiB over 67.94 seconds, with zero memory-event
and full-PSI deltas. This is distinct from the isolated runtime measurements
above. Focused independent source review found no remaining blocker.

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

### Read-only PSI diagnosis after the maintenance attempt

A single 60-second collector invocation captured 12 snapshots spanning 54.752 s
on the VPS, with no test workload or production mutation. Host and all 37
observed Docker cgroups had zero memory PSI increments. Swap-in/out, major
faults, page scanning/reclaim and allocation-stall counters also did not grow.
Available memory remained above 3058.8 MiB. news_web used 166.59 MiB with no
swap; shared infra-chrome used 472.84 MiB plus 314.54 MiB swap, counted
separately. All container identities remained unchanged; News and Chrome
were healthy and health/home checks returned 200 afterwards.

The prior spike remains unattributed. There is no sysstat history; a bounded
kernel-log query around the event found no OOM/allocation/storage errors.
Existing PostgreSQL/Chrome memory-cap event totals did not grow during the
sample, so their historical counts do not identify the prior spike.
The local collector has three passing fake-proc/collection tests; evidence
and the analysis are in artifacts/news-psi-diagnosis-2026-10-17/.
For a separately authorized next gate, align these counters with the gate
phases, including the pre-stop interval. The existing PSI guard is unchanged;
no repeat gate, service restart, configuration change, or deployment occurred.

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

### Testing resumed on 2026-10-20

Preserved the dirty checkout on the same handoff branch/base. Fresh Mac race tests
passed all nine packages; Python synthetic/stand/CDP checks passed155 tests plus7
subtests. Additional focused memory-diagnostic checks passed35 plus7 subtests.
Luna added selected per-role memory.stat fields to guard evidence without changing
charged-total thresholds, host reserve, PSI or ownership checks.

Read-only VPS preflight found News/shared Chrome healthy, health/home HTTP200,
4.34 GiB available and memory PSI full avg10=0.00. No production mutation or VPS
test workload occurred. Historical embedding-maintenance scripts were not reused.

A local cached pinned Cloak ran with network=none,768MiB,no swap,0.5CPU under
Docker Desktop Linux/amd64 emulation. Startup reached CDP without OOM; that alone
is not extraction acceptance. Real Go BrowserLive and native-probe tests failed
on deadlines. A separate about:blank diagnostic narrowed the first failure to
Network.enable after successful context/target allocation and flattened attach;
context disposal succeeded. This is not yet attributed to emulation or a transport
bug. The first diagnostic log incorrectly reports PASS despite recording the
command deadline; the test now marks command errors as failures. Original evidence
is retained with this limitation explicitly recorded. All local containers were
removed. Native fixture/overload acceptance remains incomplete.

Evidence: artifacts/testing-resume-2026-10-20/README.md and its JSON/log files.
Next useful control: independent minimal CDP client on the same isolated Cloak;
keep existing timeouts and resource budgets until evidence identifies the cause.

### Independent CDP control and local test topology

An independent standard-library Python client reproduced the initial Network.enable
stall on the same isolated pinned Cloak as Go (run31ba68323e). A scalar renderer
probe then showed Runtime.evaluate also timing out before Network setup at the
local0.5CPU quota. With local CPU1 and the same RAM/timeouts, independent Python
and both Go configuration profiles passed; native pinned-Cloak probe also passed.
This is a local CPU-sensitivity result under Linux/amd64 emulation, not production
sizing evidence or proof that CPU was the only problem.

A full local page integration passed11/13 BrowserLive cases but recorded browser
OOM; it is not acceptance. Root corrected the ad-hoc harness topology by moving Go
into an independently bounded128MiB client cgroup sharing only loopback with Cloak.
The browser's768MiB cap was retained. In run1a05928fd3 the client stayed OOM-free,
but both selected failed page cases still timed out, with browser OOM and temporary
filesystem exhaustion. A single tmpfs32-to64MiB control (d2d29c5a22) removed the
No-space signal while preserving RAM/CPU/timeouts; the two page cases still failed
alongside browser oom_kill=3. Native capability probe passed again.

The local artifact runner now defaults to CPU1/tmpfs64, keeps Go in its own cgroup,
and fails on test/OOM/unavailable required memory evidence. Formal VPS guards,
resource limits, native stand and production were not changed. All local owned
containers were removed. No real publisher/VPS requests, image pulls, commits,
publication or deployment occurred in this continuation. Native Linux fixture and
overload acceptance remains incomplete; do not infer VPS RAM demand from this Mac
emulation. Evidence and reproduction commands:
artifacts/testing-resume-2026-10-20/README.md.


### Native Linux retry — 2026-10-20

One fresh authorized runtime-only short-window attempt ran with production up.
Fresh locally built static binaries and current 54 Go/module inputs were verified;
65-source payload and exact binary/source manifests passed validation. The runner
pin is now 00c5cac0f5fdaed99507da91bb3c3e30d87ec27095966dab5cf23e114d0b96ec.
Only provenance was refreshed; all formal thresholds and native resource caps stayed
unchanged. Focused preflight regressions passed 87 tests plus 7 subtests.

All 14 raw namespace-denial probes passed; three traps accepted zero connections.
The parent guard then stopped the gate after 32.2 seconds on system memory PSI
full avg10=0.55 (limit 0.50). MemAvailable was 4,564,140,032 bytes, and five test
cgroups totaled 446,640,128 bytes, below the existing aggregate budget. Cloak's
sample was file-cache dominated (file 372,871,168 bytes, anon 61,702,144 bytes).
These are startup cgroup charges, not completed extraction or process-RSS figures.
Launch info contained the pinned browser SHA, but local CDP readiness had not yet
been observed; relay/observer/extractor/origin and fixture/overload acceptance did
not start. Per-cgroup pressure was not captured, so the host PSI source is unknown.

No automatic retry or threshold relaxation followed. Root independently verified
unchanged production fingerprints and OOM/max counters, healthy News/shared
Chrome, and successful home/feed. Exact-label resources, stage/helpers and active
stage watchdog processes were absent after cleanup. No publisher, production
processing, service stop/restart, image pull/publication, commit/push/PR or deploy.
Native acceptance remains incomplete; a future attempt needs per-owned-cgroup
pressure/event evidence, not a conclusion that the VPS ran out of RAM.

Evidence: artifacts/native-resume-2026-10-20/README.md and result.json.

## Native resource-observation diagnostic — 2026-10-20

One explicitly authorized native Linux attempt made only host PSI and aggregate
owned-cgroup memory stop thresholds observation-only. Hard container limits,
reserve, deadlines, provenance, network isolation and production guards remained.
The inner stand completed all 14 raw-network deny probes, nine controlled browser
cases and the required-event overload check. Relay receipts confirm 257 injected
events, disposal of the owned context and target, and no new allocation on the
subsequent 503 response. This is synthetic functional evidence with the pinned
Cloak binary; it does not establish publisher parity or production capacity.

The outer supervisor still reported `command_failed_docker` after 93.3 seconds.
The child reported `controlled_gate_passed` and its manifest was already cleaned.
The final memory sample was invalid; a Docker list/inspect race with normal child
teardown is suspected, but the sanitized error cannot identify the exact subcall.
Sampled aggregate peak was 473.3 MiB, including charged cache/kernel memory.
PSI full avg10 peaked at 0.69, with two observation-only threshold exceedances.
Production fingerprints, max/OOM counters, health and home/feed checks were
unchanged/passing. All exact-owned test resources and the private stage were
removed. No retry, publisher request, production change or publication occurred.
Evidence: `artifacts/native-observe-2026-10-20/`.


### Local teardown race fix

The bounded gate now explicitly delegates Docker teardown to its outer supervisor
with `--supervisor-cleanup`. Standalone child cleanup is unchanged. The outer
runner performs one final guard on the live owned containers after successful
child exit and before exact-label teardown. Late OOM/status failures remain
terminal and still trigger cleanup. On supervisor death, the detached watchdog
first stops the recorded new-session command group (validated against `/proc`),
then removes owned resources, preventing an orphaned stand from recreating them.

Fresh local validation: 94 tests and 16 subtests passed across the Linux gate,
native stand and CDP relay suites; Python compilation passed. Resource constants
are unchanged. The fix has not been rerun on the VPS. Evidence:
`artifacts/cleanup-race-fix-2026-10-20/result.json`.


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


## 2026-10-21 event-consumer follow-up

The bounded asynchronous event-RPC coordinator is implemented locally with a
32-command in-flight limit. Policy and request/resource accounting remain in
the serial event consumer; pending commands are joined before disposal. The
short native gate previously passed in 99.3 seconds, including the real
257-event cleanup-overflow and post-taint checks. Full Go race suites for
transport, service and extract-service, plus 250 Python tests and 16 subtests,
passed. The pinned Linux build was still running at this update.

The 600-second synthetic soak attempt before the latest relay counter additions
failed after six passing batches on one roughly two-second upstream TCP connect
timeout. The cause remains unknown. New accepted loopback SYN/SYN-ACK counters
are diagnostic only and do not alter firewall verdicts or limits. No soak
acceptance is claimed from that failed attempt.

The latest live The Verge run now advances past the expected `resource_limit`
for B and continues to case-0034. C case-0034 then failed with
`native_browser_ownership_lost`. Local code now classifies
`Target.detachedFromTarget` using its `params.sessionId`, distinguishing child
detaches from loss of the owned session. This detach change has not yet been
validated on the native stand. No further publisher requests were made for
this diagnosis.

During the live period, the production `infra-chrome` max counter increased by
470. Health, restart, OOM, and home/feed checks were unchanged. Two idle samples
at 12:17 and 12:20 reported max 13915 with no change; the counter increase's
cause is unknown. These observations do not establish a causal link to the
Go/Cloak run.


## 2026-10-21 detach fix: native stability validation

Correction to the preceding live-run note: both B and C advanced past the
expected `resource_limit` on The Verge. C then failed at case-0034 with
`native_browser_ownership_lost`. The detach-event fix now classifies
`Target.detachedFromTarget` by `params.sessionId`, distinguishing child-session
detaches from loss of the owned session.

A fresh native synthetic run with that fix completed the 600-second observation:
18 batches, 90 main outcomes, zero mismatches, and one successful warmup per
batch. Browser and container generations stayed unchanged; OOM deltas were zero.
The full stage lasted 675 seconds, including a deliberate 230-second final idle
reserve before the next batch. Sampled role peaks were worker 10,326,016 bytes,
Cloak 292,536,320 bytes, and observer 99,799,040 bytes; aggregate sampled stack
peak was 428,023,808 bytes.

This diagnostic run bypassed the PSI threshold 49 times and did not exceed the
runtime working-set budget. It supports stability for the sequential synthetic
fixtures and ownership scenario, but is not strict resource-cap acceptance or
public-site parity. The earlier TCP connect timeout did not recur in this run;
its cause remains unknown.

The pinned Linux static build completed in 108.8 seconds, sampled peak
398,569,472 bytes, with no OOM. Binary manifest SHA-256:
`adaf5bd2a974a5a8d69a821a8bc053166ea4a139decdcf488287cff05a0eb199`.
Production fingerprints, health, max/OOM counters and home/feed checks were
unchanged, and exact-owned stage cleanup was confirmed. A fresh five-article
live comparison is running; no acceptance claim is made until its final report
and postflight checks are available.


### 2026-10-21: native renderer crash classified

The diagnostic-only candidate (binary manifest `0c9d8f94cc6ed009df3e0c9e0373f2ce7532add90dbe69f7d7bcaf653de92e61`) distinguishes four finite event causes. The live run in `artifacts/ownership-diagnostic-live-2026-10-21` confirmed `events/target_crashed` on C case-0034 after The Verge returned resource_limit. B completed five attempts; C completed three. B case-0034 timed out in this run, so these observations do not establish a Go-only cause. The renderer crash's underlying cause remains unresolved. C relay connected 14/14 times without transport/protocol/connect errors, and no OOM was recorded. Independent production max/OOM/fingerprint/health/home/feed checks passed; all owned resources and the stage were removed. Private captures are retained root-only for seven days.

The Go diagnostic changes passed ARM64 race tests; pinned offline Linux build took 103.4 seconds, peaked at 380284928 bytes, had no OOM, and its owned builder was removed. The prior detach-fix candidate's 600-second soak passed 18 batches/90 main outcomes; this does not prove renderer-crash recovery.

A new bounded synthetic discriminator tests an oversized fixed response followed by a normal page on the same browser. The first unpaced attempt failed with native_browser_unavailable rather than resource_limit; its evidence and production/cleanup verification remain in `artifacts/resource-recovery-synthetic-2026-10-21`. That gate did not emit native diagnostics, so its cause is unclassified. The paced attempt enables the existing finite synthetic diagnostic mode and keeps resource_limit, healthy readiness and next-page success mandatory. The paced attempt also failed, now with a finite `events/target_crashed` diagnostic. Evidence and independent production/cleanup verification are in `artifacts/resource-recovery-synthetic-2-2026-10-21`. This reproduces the same event class without publisher traffic; it does not establish that the underlying crash cause matches the live page.

Any change that lets a worker continue after an owned renderer crash requires the separate proposed runtime/API contract in `docs/ai-plans/2026-10-21-go-cloak-owned-page-crash.html`. That recovery is not implemented or approved. No production changes, commits, pushes, PRs or image publication occurred.


### Final checkpoint for this implementation scope

A one-variable experiment removed `background:true` to match Python target creation. The same paced local 6 MiB fixture still produced `target_crashed` (`artifacts/foreground-target-synthetic-2026-10-21`). The hypothesis was unsupported, and only that experimental change was reverted. Both Go transport source/test files match the prior diagnostic snapshot byte-for-byte. The runtime binary manifest pin is restored to `0c9d8f94cc6ed009df3e0c9e0373f2ce7532add90dbe69f7d7bcaf653de92e61`. All experiment containers/networks/processes/stages were removed; independent production checks passed. No test remains running.

Final local checks: Go race transport/service/extract-service passed, Python 257 tests plus 16 subtests passed; the existing Pydantic deprecation warning remains. These local checks do not override the native crash regression failure. The new resource-limit-recovery synthetic gate is intentionally strict and remains red on the pinned Linux Cloak. No live parity, renderer-crash recovery, production-readiness or full completion is claimed. The earlier 600-second stability pass and both later live failures are retained separately.

The proposed next behavior is job-level isolation of a confirmed owned renderer crash after successful disposal/absence receipts and a healthy unchanged browser generation, without retrying the failed article. Unknown ownership, cleanup failure, overflow, generation change and disconnect remain terminal. This changes runtime/API behavior and requires approval of the separate HTML plan before implementation.


## 2026-10-21 owned page crash recovery: implementation and acceptance status

The user-approved contract in `docs/ai-plans/2026-10-21-go-cloak-owned-page-crash.html`
is now implemented. An owned renderer crash returns an empty, failed,
non-retryable `native_browser_target_crashed` result. Only the next independent
job may proceed, and only after pending RPCs are joined, a same-connection
`Browser.getVersion` ordering barrier completes within 500 ms of the existing
five-second cleanup deadline, disposal and both absence receipts succeed, the
old CDP connection stays live, and a bounded health check confirms the same
browser generation. No failed invocation is immediately retried by the extractor.
The later deferred-retry follow-up below governs application queue scheduling. Unknown ownership, cleanup
failure, disconnect, detach, overflow, generation change or health-check failure
still taints the worker. The service reports a pure crash as failed; ownership
loss retains the existing unavailable result.

The first live candidate (`f5c1ab...`, evidence in
`artifacts/owned-page-crash-live-2026-10-21/`) completed five unique B and five
unique C attempts with no duplicates. C case-0034 was classified as an owned
target crash; its output was empty, readiness recovered on the unchanged
generation, and the following case-0050 succeeded with 5,144 characters. The
failed article was not retried. B case-0050 returned 5,132 characters. Other
case outcomes differ: for example B case-0041 succeeded while C returned
`native_browser_no_content`, and C case-0008 was reported as cancelled before
that classification was corrected locally. This run establishes crash
isolation behavior for that candidate, not B/C quality parity.

The corresponding 600-second synthetic observation completed 19 batches / 95
main outcomes and 19 successful warmups, with zero mismatches and zero OOM
deltas. Browser/container generations stayed unchanged. Sampled peaks were
9,474,048 bytes for the worker, 287,072,256 bytes for Cloak, and 419,008,512
bytes for the aggregate test stack. The soak is diagnostic synthetic stability,
not public-site parity or strict resource-cap acceptance. Evidence:
`artifacts/owned-page-crash-stability-2026-10-21/`.

Full Go race suites passed after a final local fix for a race between the crash
event and a following owned-detach event; the same-connection ordering barrier
is covered by 30 repeated race-test runs. The final current-source static build
used binary manifest `139d1404476b8d7b68fe979cb1443d38e6620ecd49102744a49368ded0a3fc55`,
service binary SHA-256 `3f9b83dc67dd28cbb85623f7b9d8c38d43ed558fa9ab7fc9bafa2c79c7b50a07`,
and source manifest `2e34e9598d56ddbdc954d34f84ee32621dada609f1ff6aa5fa4bef608fc400f9`.
It took 104 seconds, sampled 416,423,936 bytes peak, and had no OOM. These artifacts are not the same
binary as the earlier live and soak candidates.

The first final-source native attempt did not induce a crash: its stress page
returned resource_limit. Its evidence is retained in
`artifacts/owned-page-crash-final-native-2026-10-21/`. The deterministic final
acceptance and independent production/cleanup checks are recorded below.


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
