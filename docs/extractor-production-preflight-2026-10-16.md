# Production preflight — 2026-10-16

**Production activation is not approved or ready.** Preparation found a real
pinned-CloakBrowser compatibility failure. The previously successful ordinary
Chrome tests do not establish CloakBrowser compatibility.

## Confirmed environment

- Fresh `git fetch origin`: HEAD and origin/main have zero commits of divergence;
  feature work remains uncommitted on `prototype/go-extraction-parity`.
- `news_web` runs the pinned image recorded in the readiness plan, on `infra` and
  `traefik`. `infra-chrome` is on `infra`; no new public extraction route exists.
- Production browser is `cloakhq/cloakbrowser:0.5.2` pinned to digest
  `sha256:2c0d257802f54c4f86c9ccae336864baf63e118612732127cb96df35756a57a4`,
  with `cloakserve --port=9222` and auto-update disabled. Its version endpoint
  reports patched Chromium `Chrome/146.0.7680.177` and a standard browser target
  path. Wrapper version and Chromium version are separate identities.

## Completed checks and fixes

- The three previously skipped PostgreSQL tests now pass on a new isolated local
  cluster at `127.0.0.1:59421`. Tests removed their own schemas; the server was
  stopped. Production PostgreSQL was not used.
- Latest deterministic Python suite: **359 passed, 8 skipped**, 369 warnings,
  26.21 seconds. These skips are three DB checks (verified separately above) and
  five explicitly disabled live-browser checks. Earlier live ordinary-Chrome
  results remain historical, not substituted for Cloak checks.
- Final local Go tests passed in all six packages; vet and module verification
  passed. Initial sandbox socket denials were rerun with authorized loopback
  access. No failure was waived in the pinned-Cloak runs.
- CDP discovery resolves the configured Docker hostname, pins the discovery
  socket, sends the numeric Host required by Chrome and preserves configured TLS
  authority. Readiness validates bounded discovery JSON through the same probe;
  arbitrary HTTP 200 no longer claims browser readiness.
- Privacy-safe comparison facts are visible through the actual production logging
  formatter. Text, URLs, tokens and digests are not logged. Inline shadow remains
  diagnostic and shares its caller's deadline: it must not be enabled in prod.
- Browser size checks now precede `innerText` layout. This is a boundedness
  improvement; it did not resolve the failing Cloak stress case.
- AI secret's captured short page explicitly asks for account access instead of
  exposing the article body. Rejecting that body is not evidence of missing
  Readability content. No access protection was bypassed.

## Exact pinned-image tests

The user explicitly authorized sending Go source and synthetic tests to their
VPS after automatic approval review initially rejected source-code egress. Only
that authorized source/test material was transferred. Tests used a separate
container of the exact production digest, `--network none`, one CPU, 768-MiB
memory limit, 1-GiB shm and no production profile, secrets or volumes. Dependencies
were prepared separately; controlled pages generated no publisher traffic.

| Test | Observed result |
|---|---|
| Initial full controlled transport suite | Failed: 6-MiB JS DOM deadline, later 502/disconnected CDP and failed cleanup assertions |
| Fresh isolated huge-DOM reproduction, before/after early size guards | Both failed the five-second size-rejection expectation with a deadline; OOM counters zero |
| Public adapter JS/egress/cancel/foreign-target test on a fresh browser | One standalone run passed in 2.54 seconds |
| Loopback success/UTF-8/cancel/repeated cleanup subset | Initial cases passed; repeated cleanup lost the observer connection with EOF |
| Public adapter repeated six times | One passed, five failed with close-frame/EOF/discovery errors; suite exit nonzero |

Wrapper FD counts before and after the repeated tests were **7 → 7** and cgroup
OOM/kill counters remained zero. That short observation does not prove absence of
leaks. Wrapper logs contain Chromium crashpad diagnostics; they do not yet establish
the native crash's cause. Do not infer OOM or blame the multiplexer without further
evidence. No production browser connection was closed or restarted by these tests.

Local evidence lives under ignored `artifacts/extractor-release/`: PostgreSQL and
Python logs, `cloak-preflight.log`, `cloak-diagnostic.log`, `cloak-guard-fixed.log`,
`cloak-public-diagnostic.log`, `cloak-recovery.log`, and `cloak-public-repeat.log`.
Temporary test containers were stopped and removed by their cleanup traps.

## Deployment decision

Current Go `BrowserFetchPublic` fetches responses through Go and fulfills them
over CDP. The destination therefore sees Go's network behavior, not native
CloakBrowser TLS/HTTP. Neither a successful CDP fixture nor CSP/dead proxy proves
native fingerprint parity or complete OS-level UDP/WebRTC isolation.

Approved implementation: **Go HTTP-only with native Python/Cloak fallback**,
outside inline shadow, as recorded in the
[concrete readiness plan](ai-plans/2026-10-16-extractor-production-readiness.html).
Implementation approval does not activate production.
In particular, upstream browser-needed failures must reach the allowed fallback
without weakening terminal URL/resource/cancel safeguards or Retry-After.

A full Go browser cutover needs a separate native-browser egress/lifecycle design
and successful same-image recovery tests. Whole-system memory/CPU comparison,
24-hour isolated soak, CI image publication and production token/network setup
remain open. No memory saving, completed all-source live gate, production rollout,
commit, push or PR is claimed.
