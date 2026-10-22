# Native Go article extraction: release verification, 2026-10-22

Status: Go source verification passed; native activation is deferred. Persistent shared-browser lifecycle assets are an inert review draft. News and Cloak production configuration/images have not changed. Personal Assistant was separately stopped at the user's explicit request, with rollback metadata retained.

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
and automated review. News/Cloak production remains unchanged. Isolation implementation was subsequently approved; activation remains deferred pending acceptance.

### Media and metadata response bounds

The third Codex review identified JSON amplification from a large number of
relative media URLs. Media collection now stops at 256 unique entries or a
256-KiB serialized-array budget, skips whole URLs over 8 KiB, and bounds alt text
to 1 KiB. Duplicate embedded URLs do not consume the unique-entry limit.
Article metadata is bounded on every return path, including failed extraction
and resource-limit results; title/author are 1 KiB each, description 4 KiB,
publication date 128 bytes, and oversize image URLs are omitted whole. Trace
markers expose changed output. Article identities are preserved.

The same amplification pattern existed in alternative-page discovery: all links
were collected before retaining two. Collection now retains at most two from
the outset and skips raw/resolved URLs over 8 KiB whole. Existing ordering and
self-canonical behavior remain covered by pipeline tests.

Offline Python/Go replay on 432 pairs passed with zero assertion failures,
zero golden failures and no new unreviewed differences. Fresh full Go race tests,
vet and module verification passed after these corrections. Revision `22e36fe`
completed all Linux CI jobs successfully; its review finding prompted this
additional correction. Fresh CI and Codex review of this correction are still
required. The earlier native Linux campaign predates these response-bound
changes and does not constitute exact-revision production acceptance.


## Persistent runtime preparation and client inventory

The user approved implementing the persistent isolation plan, then deferred the
shared cutover while Personal Assistant compatibility was investigated. Read-only
inspection confirmed that the deployed Personal Assistant had enabled plain-CDP
fallback to the shared Cloak. Readeck and its toolkit configs had no CDP endpoint;
no active CDP socket was present at the inspection snapshot. This is not a proof
that every historical or hidden browser client has been enumerated.

At the user's later explicit request, only Personal Assistant was stopped,
exited with code 0 and received Docker restart policy `no`; its image/container
and original restart settings remain in a root-only rollback receipt. No
Assistant code, database, News, Cloak or Readeck deployment was changed. The
runtime draft excludes it from active controllers and verifies its recorded
disabled state. No later instruction has explicitly resumed the shared cutover.

The inert `deploy/news-extractor/runtime/` assets provide a root supervisor,
role namespace policies, a systemd unit and an intentionally incomplete config
example. Exact controller identities, bounded startup, owned cleanup/recovery,
explicit DNS, worker HTTP return traffic and Docker lifecycle declarations were
independently reviewed. Target Linux `systemd-analyze verify` returned 0 on a
temporary unit; it was not installed. Actual systemd restart/reboot, production
role-network policy and CI-image namespace acceptance remain unperformed.
The workflow retains its default-branch manual publication gate; a new temporary
CI candidate image archive supports isolated acceptance before publication.

The shared Python fixture campaign exposed a test-order defect: Python
navigation ran before the synthetic origin listened. Finite counters showed a
paused/continued request and successful proxy-auth continuation followed by
`ERR_TUNNEL_CONNECTION_FAILED`. The fixture now starts and passes its listener
check before Python compatibility, preserving the browser-generation check and
subsequent Go checks. Source tests alone do not accept that real-browser path;
the regenerated campaign result is recorded separately below.


### Shared Python and Go native acceptance

The regenerated 79-source payload with the freshly built Linux binaries passed
on pinned Cloak 0.5.2 in 186.4 seconds. The Python receipt confirms initial and
deferred navigation, JS/cookie reads, HTTP rejection before allocation, publisher
auth cancellation, proxy authentication, production Telegram browser parsing,
production Page Monitor parsing, six context-baseline checks and bounded HTML
reads. No DB, AI or delivery code was imported by those parser fixtures.

On the same unchanged browser generation/container, actual CoreExtractor and
service client extraction passed with metadata, cookie content and zero Python
fallback. Controlled crash and terminal overflow recovery passed, including
worker exit1, verified owned cleanup, no allocation before restart, fresh
readiness and success of the next article. Fourteen raw namespace-denial probes
passed; trap accepts remained zero. Memory/OOM/reserve guards remained active;
PSI was diagnostic-only. This is synthetic integration evidence, not production
capacity or acceptance of the new persistent supervisor policy/lifecycle.

Independent postflight confirmed unchanged healthy News/Cloak fingerprints,
zero new memory max/OOM events, home/feed success and exact-owned cleanup with no
remaining containers, networks or stage. Private evidence is under
`artifacts/shared-native-final-2026-10-22/`: `native-evidence.json`,
`root-postflight-verification.json`, `cleanup-confirmation.json`, and
`payload-combined-4/payload-metadata.json` (source-head lineage `2a86e33`, exact
per-file hashes). The Go source set still matches that revision; the Python
and harness changes are recorded by the payload's per-file manifest.

Fresh full local Python checks after the ordering correction: 893 passed,
8 skipped, 22 subtests passed. Subsequent test-only image-ID identity handling
was independently covered by the 43-test runtime and relay suite. Targeted
independent proxy/harness review also passed 177 tests. Final PR CI/review must
cover the complete checkpoint revision. The systemd unit has not been installed;
News/Cloak activation remains deferred.


### Persistent-supervisor graceful-stop review correction

Codex review of `fd871bb` found a 12-second worker stop grace conflicting with
the Go drain/cleanup envelope. Both browser-generation restart and owned shutdown
now give the worker 55 seconds, with a 70-second Docker CLI deadline. Other role
stop limits remain unchanged. The calculated cleanup CLI wait budget is 137
seconds within the unit's 150-second stop timeout. Independent runtime/relay
regressions passed 46 tests. This correction was not installed on production.


### Release resumed after explicit authorization

The user resumed the approved News/Cloak release with “Погнали”. Personal
Assistant remains stopped with restart policy `no`; its code is not migrated.
The current `458150a` checks all succeeded. The persistent supervisor still
requires isolated Linux kernel/lifecycle acceptance before production cutover.
A fresh read-only snapshot covers all 36 existing containers. Current enabled
sources are three HTTPS Page Monitors, four HTTPS RSS feeds, and one HTTPS
Telegram source; there are no enabled HTTP-only browser sources. Source checks
used aggregate-only read-only database queries. No production cutover has
occurred at this checkpoint.

The production application's current database schema already contains all four
article-processing columns and all five scheduler-outcome columns required by
this branch. This was checked read-only in `current_schema()`; no schema or
article state was changed. The first persistent acceptance attempt stopped
before creating resources because the harness incorrectly expected a
`memory.events` file at the cgroup-v2 mount root. The host uses per-scope memory
event files; guard discovery is being corrected before the next attempt. This
is a harness preflight failure, not a browser or production runtime failure.


### User-directed Compose simplification

The user rejected the host-supervisor complexity and explicitly requested
completion of the existing Compose scheme, preserving Readeck and PR review.
The old host runtime/anchors/systemd files and new acceptance runners were
archived privately and removed from the source tree. The replacement consists
of a container-local browser policy/bootstrap, Cloak plus the existing relay
inside one browser container, and normal Compose worker/proxy services. Docker
handles restart; no host Docker API reconciler remains. Worker/proxy retain Go
URL/DNS/dial guards on dedicated networks, without a kernel-isolation claim.

Fresh inventory covers 36 existing containers. Readeck/bot/MCP have no observed
CDP env, mounted-config reference or established 9222 connection; toolkit source
has no matching browser client code. A real read-only Readeck baseline returned
HTTP 200 for a bookmark list and successfully read an existing article; no
bookmarks, messages or delivery state were changed. Full Python baseline after
simplification: 892 passed, 8 skipped, 22 subtests. Subsequent cold-start race
and portable nft transaction fixes have focused regressions; final CI and
isolated Compose/capability acceptance remain required before deployment.

Final integrated local Python check after startup/Compose corrections: 894
passed, 8 skipped, 22 subtests. Independent focused browser/Compose/relay review
passed 27 tests with no remaining code blocker. Linux image acceptance and
production cutover are still pending.
