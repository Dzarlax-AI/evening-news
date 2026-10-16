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
