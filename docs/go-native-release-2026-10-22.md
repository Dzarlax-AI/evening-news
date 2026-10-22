# Native Go article extraction: release verification, 2026-10-22

Status: source verification passed; production activation blocked by the persistent shared-browser isolation design. No production configuration or image changed.

## Current behavior

The opt-in `EXTRACTION_SERVICE_NATIVE_BROWSER` setting requires Go mode and Python fallback disabled. HTTPS articles on port 443 enter the native Go browser route; HTTP articles retain the Go HTTP route. Existing public-network port policy still rejects unsupported ports. Fragment identifiers are removed on the wire while the original article identity is retained for learning and persistence. The default and shadow profiles remain HTTP-only. Native rendering does not first perform the HTTP extraction graph.

The worker owns only its contexts and targets. Controlled page crashes preserve the shared browser; permanent CDP taint drains the worker and exits so supervision can recover it. Failed articles retain the existing durable processing/retry semantics. The image now packages both `/extract-service` and `/extract-egress`; gateway deployments must override the inherited service healthcheck.

## Fresh verification

- All Go packages: `go test -race -count=1 ./...`, `go vet ./...`, `go mod verify` passed.
- Full local Python suite with scheduler/browser disabled: 867 passed, 8 skipped, 22 subtests passed. PostgreSQL-dependent checks and browser integration skips must be distinguished from the separate native campaign and PR CI.
- Dummy Compose merge accepted standby, native activation, explicit HTTP canary and final routing rollback. No services were started by Compose validation.
- Independent cumulative source review identified fragment and HTTP-routing regressions. Both were corrected and re-reviewed; the client test file independently passed 118 tests.

## Actual CoreExtractor to Go to native Cloak campaign

The current production CoreExtractor and aiohttp service client ran against the real Linux/amd64 native worker and pinned Cloak 0.5.2 on isolated synthetic fixtures. Learning used explicit in-memory stubs; no application DB, AI, Telegram or Python browser path was invoked.

- JS/cookie article succeeded with 3,434 extracted characters and metadata.
- Publisher authentication was rejected with `upstream_auth_required`; zero Python fallback calls.
- Controlled owned-page crash recovered; the next article succeeded.
- Terminal CDP overflow caused exit1; owned cleanup and no allocation before restart were verified; fresh readiness and the next article succeeded after worker restart.
- Browser generation and container identity remained unchanged. Fourteen raw namespace probes were denied and trap accepts remained zero.
- Campaign completed in 149.4 seconds. Sampled runtime working-set peak was 395,681,792 bytes across the fixture stack; this is not production sizing or a retained-memory baseline. PSI was diagnostic-only under the recovery profile, while memory/OOM/reserve/ownership guards remained active.
- Exact-owned cleanup and independent production fingerprint, health and public user-path checks passed.

Local private evidence: `artifacts/core-native-e2e-2026-10-22/native-evidence.json`, `run.log`, `postflight.json`, `cleanup-confirmation.json`; final source tests: `artifacts/native-release-2026-10-22/python-tests-final.log`. The evidence directory is intentionally not published with source.

## Remaining deployment gate

The test stand creates ephemeral namespace policy. The current production Cloak runs as UID0 on the shared infra network and its existing root healthcheck/restart marker is incompatible with blindly applying the tested loopback CDP policy. Compose isolation assertions alone are insufficient.

The proposed permanent lifecycle and its approval gate are in [the production isolation plan](ai-plans/2026-10-22-native-production-isolation.html). Python Telegram and Page Monitor still use this shared browser; their proxy compatibility remains a release requirement even though parser migration is deferred. PR/CI success does not accept that topology or authorize claiming native production activation.


## PR review corrections

PR #9 at initial revision `29233dd` received Codex review. The native runbook
was corrected to describe direct browser routing, and the Python proxy tab now
waits for DOMContentLoaded from the exact navigation loader under the existing
caller deadline. It no longer reattaches after Fetch guards are enabled: a
session-aware delayed-resource regression reproduced that real defect and now
verifies continuations stay on the original session. Cancellation cannot
re-cancel and interrupt an owned-context teardown already in progress.

CI also exposed test nondeterminism: harness unit tests read the actual CI
process RSS, and a disconnect assertion omitted a legitimate event-reader
transport diagnostic. The unit fixtures now supply deterministic RSS; the Go
assertion accepts the existing valid diagnostic pair while retaining readiness,
empty output and no-replay checks. Production limits were not changed.

Fresh local verification after all corrections: 871 Python tests passed,
8 skipped, 22 subtests passed; all Go race tests, vet and module verification
passed. The disconnect test independently passed 20 race repetitions, and
an independent browser review passed 76 tests. New PR CI and final automated
review are still required; this does not close the production-isolation gate.


### Final response-size review correction

The second Codex pass found that unused captured `study_html` could expand a
near-limit page beyond the client's JSON response cap and hide the intended
selector-study outcome. The production response converter now omits that
optional capture; internal pipeline recovery, study status and observations
remain unchanged. A near-5-MiB escaped-capture actual HTTP regression and three
Python loopback fallback-policy cases passed independently. No limits increased.
Fresh full local Python suite: 874 passed, 8 skipped, 22 subtests. Affected Go
service/pipeline/lifecycle packages passed race tests and vet. Prior revision
`34b7b06` completed all Linux CI checks; the updated revision requires new CI
and automated review. Production remains unchanged pending isolation approval.
