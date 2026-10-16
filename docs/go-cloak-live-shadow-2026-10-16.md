# Go/Cloak live shadow — 2026-10-16

The isolated live batch ran beside production. It did not pass the native Go
release gate: Go stopped on case-0008 with `native_browser_unavailable`.
The independent Python control completed all 50 jobs. Production remained on
its existing configuration, and all temporary live/diagnostic stands were removed.

This is an out-of-band batch using real public URLs from production records,
not continuous traffic mirroring or a production canary.

## Scope and provenance

- Approved plan: [live shadow](ai-plans/2026-10-16-go-cloak-live-shadow.html).
- Working branch: `prototype/go-extraction-parity`, base commit
  `72d39a6514fb86b57eadc98b2e6510855f946088`, with uncommitted prototype work.
- A bounded read-only transaction selected 50 public HTTPS URLs from 260
  candidate records. Hostname caps excluded 206 candidates; the total cap
  excluded another four. Detailed content was fetched only for the selected IDs.
- Corpus size: 150389 bytes. Raw corpus/captures remain root-only on the VPS,
  under `/tmp/news-native-cloak.vh6lLSy2/shadow-private/`.
- Raw expiry is 2026-10-23 12:01:31 UTC. Cleanup is manual, not scheduled.
- The completed batch has 50 B attempts and eight C attempts, with unique IDs
  within each variant. The failed C attempt may have failed before navigation;
  58 attempted jobs is an upper bound on main navigations, not a verified count
  of successful browser navigations. No failed URL was retried.
- Final collection verified 62 staged source SHA-256 values against the local
  working tree, with no mismatches. This is a final-source manifest; orchestration
  gate corrections made during the run are described below.

Pinned images:

```text
Python:
ghcr.io/dzarlax-ai/rss-summarizer@sha256:b6abbb428543e846d0310b86ff41236a6df028985a00ebfe4714357d22e67060
Cloak:
cloakhq/cloakbrowser:0.5.2@sha256:2c0d257802f54c4f86c9ccae336864baf63e118612732127cb96df35756a57a4
```

All public stands recorded the same patched Chromium 146 binary SHA-256:
`715722e8605ae3ce81523c1218aba1ec89425786ab33ceaf99f8a6cb5e70e6e8`.
Their recorded arguments had no certificate bypass or host-resolver override.
The browser binary was launched directly using the pinned image's browser
arguments, with a restricted CDP relay. This changes the wrapper lifecycle from
production `cloakserve`; it does not establish complete production equivalence.

## Observed results

`complete` means the test loop finished, not that every article succeeded.

| Result | B: Python native control | C: Go native service |
|---|---:|---:|
| Selected corpus | 50 | 50 |
| Attempted jobs | 50 | 8 |
| Nonempty extraction reported as success | 3 | 0 |
| Origin HTTP 403 | 10 | 7 |
| Resource-limit rejection | 25 | 0 observed |
| Job timeout | 10 | 0 observed |
| Other site error | 2 | 0 observed |
| Fatal native availability error | 0 | 1 |
| Not attempted | 0 | 42 |
| Variant status | complete | incomplete |

The first five URLs returned 403 in both variants. This confirmed normal native
TLS and preserved site refusals; it did not validate article extraction quality.
The initial orchestration helper mistakenly required extraction success for this
TLS gate. It was corrected to accept typed origin HTTP responses, while still
rejecting proxy 407, transport, policy and deadline failures.

On case-0008, Go failed after 3033 ms without an upstream status. Python later
received HTTP 200 and rejected the page with `resource_limit`. The snapshots
were taken at different times. This difference cannot by itself establish a
source change or the exact Go failure phase.

No OOM delta or Chrome crash marker was observed in the stopped Go stand.
Several internal errors currently collapse to the same public availability
error. A three-second context-allocation deadline and event-stream failure are
possible explanations, not confirmed causes. The failed Go variant was not
restarted. Only the already healthy, independent Python control continued on a
fresh stand; its existing attempt ledger prevented repeated B requests.

The three B success responses contained 212, 1433 and 5132 characters. They are
nonempty extractor results, not three accepted quality cases. For case-0041,
only a small portion of the extracted tokens occurs in the source `<article>`
region, although the extracted tokens occur in the body; this needs quality
review. No successful Go text exists for live B/C quality parity. Historical
archived content was not treated as an oracle.

## Offline parsing of the same saved DOM

The exact Go `nativeArticleResponse` parser was compiled with a fake renderer
returning each successful B capture. Both analysis containers had networking
disabled; no site was fetched again. Live C reports and counts are unchanged.

| Case | B characters | Offline Go characters | Go outcome | Token-set Jaccard |
|---|---:|---:|---|---:|
| case-0034 | 212 | 218 | success | 0.9714 |
| case-0041 | 1433 | 0 | native_browser_no_content | unavailable |
| case-0050 | 5132 | 5144 | success | 0.9973 |

None matched normalized text exactly. In both Go successes all extracted tokens
occurred in the source article region. Available title, description, image URL
and publication-date fields matched across all three cases. Media counts were
B/Go 1/1, 1/0 and 1/0 respectively; this remains a contract difference.
Case-0041 needs source review: rejection may be an improvement or a missed
article. Token presence alone does not adjudicate quality. These offline parser
observations do not establish native Go browser availability. Redacted evidence:
`offline-redacted-summary.json`.

## Memory observations

These are one-second sampled cgroup `memory.current` values, not kernel peaks.
Independent component maxima must not be added as if they were synchronized.
The sum column is the maximum of actual synchronized samples and includes all
named controller/observer, relay, gateway and namespace-anchor overhead.

| Run | Jobs in this run | Maximum synchronized sum, MiB |
|---|---:|---:|
| C first-origin check | 1 | 386.7 |
| C remainder of five-case smoke | 4 | 424.4 |
| B five-case smoke | 5 | 374.1 |
| C expansion, stopped at case-0008 | 3 | 414.8 |
| B independent expansion | 45 | 486.4 |

In B's 45-job expansion, sampled component maxima were Cloak 378.6 MiB,
Python controller 93.3 MiB, relay 15.5 MiB and gateway 11.3 MiB. In C's stopped
three-job expansion, Go reached 4.2 MiB, Cloak 304.9 MiB and the separate Python
load-generator observer 87.1 MiB. That Go sample did not include a successful
article parse. The production Python application was not removed.

Different coverage, cold imports, browser variation and mostly rejected jobs
prevent a valid Go-versus-Python saving estimate from this batch. Per-job
latencies include controller imports and cleanup. Ten B timeout observations
were 30.014–30.029 seconds around the configured 30-second deadline.

At the separate production snapshot, 2026-10-16 12:37:24 UTC:

| Production container | memory.current, MiB | memory.swap.current, MiB |
|---|---:|---:|
| `news_web` | 151.2 | 6.5 |
| `infra-chrome` | 444.7 | 304.7 |

This single operational snapshot is not a capacity estimate or a before/after
comparison. It does show why replacing the controller alone cannot remove the
browser's memory cost.

## Changes and verification

Implemented bounded exporter, native B/C runner, owned-resource stand manager,
default-deny namespace rules, controlled origin fixtures and a UID-restricted
CDP relay. No source HTML or origin responses are fulfilled by the Go controller.

Controlled tests exposed and corrected:

- X11 TCP fallback on `::1:6099`; fixed clients to use the same Unix display.
- Two-second shared cleanup budget; increased to five seconds after measuring
  a context receipt taking about two seconds after disposal.
- False ownership taint from `Page.stopLoading` rejection after a denied redirect.
  Disposal and both absence receipts remain mandatory.
- Ignored `Page.navigate.isDownload`; downloads now produce typed unsupported
  results even when no download event arrives.
- Standalone runner import collision with the application image's `scripts` package.

Final source checks: 145 focused Python shadow/benchmark tests passed;
Linux `go test -race ./...`, `go vet ./...` and `go mod verify` passed.
The normal controlled gate `news-native-shadow-f65d6c3bd3` passed the positive
JS/cookie case and all eight negative cases. Private trap connections and origin
authorization leaks were zero. Public stands repeated their raw network gates.

Mixed/private DNS answers and rebinding were exercised with injected-resolver
Go unit tests, alongside real namespace private-egress traps. No externally
controlled public-DNS rebinding integration test was performed; that part of
the planned gate does not have end-to-end evidence.

The client deadline remains 30 seconds. C client/outer timeout records the
attempt and stops C rather than dispatching during uncertain server cleanup.
The five-second cleanup envelope is separate and remains bounded.

## Prepared diagnostic and separately approved replay

A disposable instrumented copy passed the full Linux race suite, vet and module
verification. Its synthetic gate `news-native-shadow-d265994441` passed, and
286 diagnostic records passed the URL/canary-token log check. The instrumenter
was independently reviewed and tested against symlink redirects.

It records allowlisted CDP methods, error classes/numeric codes, durations,
discovery/dial/event-stream phases, static readiness callsites, navigation error
enums and cleanup receipt flags. It does not log URLs, protocol IDs, arguments,
tokens or source text. It changes no execution budgets or request policy.
Synchronous diagnostic logging makes its timings unsuitable for performance
comparison.

Prepared binary SHA-256:
`c0f2ffd2553b57340e031fc4e3234158b4b370538001fc9ab30e28c342d459d7`.
The user subsequently explicitly approved one diagnostic repeat of case-0008,
with a 30-second request deadline and a separate five-minute stand budget.
No additional retry was performed. The original failed report and ledger remain
unchanged; this replay is separate evidence, not another completed live batch.

Run `news-native-shadow-30eb166075` reproduced `native_browser_unavailable`
after 12998 ms, returned no content, and reported readiness false afterwards.
Discovery, WebSocket dial, context creation, attachment, configuration and
`Page.navigate` all succeeded. The event reader then logged
`event_queue outcome=overflow`; subsequent CDP calls returned `event_overflow`.
Cleanup could not obtain disposal or either absence receipt and correctly
tainted the worker. The dedicated stand was then removed entirely.

The immediate cause of this replay is the bounded 256-message CDP event queue
overflow in `transport/browser.go`. Its reader exits on overflow, so command
replies needed for cleanup can no longer be consumed. The native event handler
also awaits Fetch command replies synchronously, which can delay event draining;
the trace does not quantify event types or prove a single cause of the burst.
It does not prove that the earlier uninstrumented failure had this exact cause.

The next fix should preserve bounded memory and fail closed on event overload,
while keeping command replies and ownership cleanup operational. Simply making
the queue unlimited or dropping security/accounting events is not acceptable.
Use a synthetic event flood to reproduce the failure, test bounded cancellation
and both cleanup receipts, and only then consider another public-origin stage.
No production source fix or further public navigation was made in this replay.

At 2026-10-16 13:33:15 UTC, both production containers remained running with
the same images, start times and restart counts as the baseline. Their OOM
counters were zero. No `native-shadow-run` containers or networks remained.
The diagnostic stand completed its idle OOM checks and cleanup. Evidence:
`one-case-diagnostic.json` and `state-after-diagnostic.json` in the redacted
artifact directory.

## Subsequent event-filter change

The user approved restricting unnecessary events before the queue. The shared
CDP reader now enqueues only the 16 event methods consumed by the supplied-HTML
and native browser handlers. Command replies bypass this filter. Required Fetch
policy/auth, Network accounting/status, frame ownership, download, popup,
WebSocket and crash/detachment events retain their ordering and session IDs.
Unused extra-info, lifecycle, console and other unhandled notifications are
discarded before enqueueing. This also means ignored notifications no longer
refresh the native activity timestamp; actual handled network/frame events and
the existing DOM/readiness checks remain in use.

The queue remains bounded at 256. Overload by required events still fails closed
and can still prevent cleanup receipts. This change reduces avoidable queue
pressure; it does not implement the separate overload-recovery work above.

A real WebSocket regression test sends 1024 unused events before a command
reply while no consumer drains the queue, then verifies all 16 required event
types survive in order. It failed with `prototype_browser_event_overflow` before
the change and passed afterwards. A second branch floods required events and
verifies bounded, fail-closed behavior is preserved.

Validation: Windows `go test ./...` and `go vet ./...` passed. Windows race was
unavailable with CGO disabled; Linux `go test -race ./...`, `go vet ./...` and
`go mod verify` passed in an offline, capped container using a disposable source
copy. Both changed-file hashes matched the local tree. Browser-dependent live
tests without a configured endpoint were skipped; no new Cloak or public-origin
run was performed, so production effectiveness and memory benefit remain
unmeasured. Original staged binaries, batch reports and soak were unchanged.

## Operational state and evidence

Final collection found no remaining `native-shadow-run` containers or networks.
Production image, start time and restart counters matched the initial baseline;
the historical `infra-chrome` restart count of 94 did not increase. Both production
containers were running. The temporary export credential file was removed.
Host MemAvailable was about 3.0 GiB at final collection.

The independent synthetic soak was still running at its separate observation:
12 completed batches / 360 jobs, zero expectation mismatches, OOMs or restarts.
It uses the earlier uninterrupted controlled configuration; those jobs are not
live URLs or validation of the diagnostic build. Its approved cutoff remains
2026-10-16 14:28:54 UTC (16:28:54 Europe/Belgrade). No completed four-hour pass
is claimed here.

Redacted evidence is in ignored
`artifacts/native-cloak/live-shadow-2026-10-16/`: `summary.json`, `report-B.json`,
`report-C.json`, `corpus-manifest.json`, `state.json`, and the separate soak status.
Only redacted reports were retrieved locally. No commit, push, PR, image
publication or production deployment was performed.

Release remains blocked on the native availability failure and insufficient
successful live extraction/quality evidence. This batch does not justify
switching production to the Go native worker.
