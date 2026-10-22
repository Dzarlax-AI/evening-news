# HTTP-only extractor pilot verification — 2026-10-16

The approved implementation is Go HTTP extraction with the existing native
Python/CloakBrowser fallback. Changes are in the working tree on
`prototype/go-extraction-parity`; no image publication or production activation
has occurred. This report supplements the earlier
[Cloak preflight](extractor-production-preflight-2026-10-16.md).

## Implemented behavior

- Go defaults to `EXTRACTOR_HTTP_ONLY=true`, rejects a configured Chrome endpoint
  in that mode, and disables every Go browser path. Python requests explicitly
  disable Go browser use, including learned-browser strategies.
- Browser-needed and selected recoverable extraction failures can use native
  Python fallback within the remaining total budget. Auth/configuration,
  admission, resource, cancellation and rate-limit failures cannot trigger it.
- Upstream HTTP status failures stop the Go method graph. Reddit linked-article
  failures are checked before accepting a fallback post. Status and bounded
  Retry-After survive the service response and Python durable retry handling.
- Public DNS is checked before browser handoff. Private/mixed address answers
  are terminal; DNS unavailability is retryable `admission_unavailable`, without
  native fallback. Go pins its own HTTP connections. Python/Cloak redirects and
  subrequests retain their existing policy; this is not cross-runtime pinning.
- The authoritative successful attempt owns learning. Native fallback does not
  record the Go failure as another learning observation. RSS, Telegram, custom
  collection, AI, database writes and delivery remain Python responsibilities.
- The independent captured-input comparator runs outside article processing.
  It rejects strategies or inputs that can fetch pages, including empty and
  challenge HTML; it validates both implementations against explicit fixture
  expectations and emits bounded comparisons rather than article text or URLs.
  State-only negative expectations prove state parity, not error equivalence.
- CI verifies before publication. Publication requires a manual default-branch
  run with `publish=true`. Proposed image: `ghcr.io/dzarlax-ai/rss-extractor`.
  Standby/canary/rollback overlays are review drafts in
  [`deploy/news-extractor/`](../deploy/news-extractor/README.md).

## Fresh checks

| Check | Result |
|---|---|
| Full Python suite with dedicated PostgreSQL on loopback port 59421 | 429 passed, 5 skipped, 377 warnings; 26.91 seconds |
| Skips | Four explicitly gated real-browser/network tests and one browser memory soak; no database tests skipped |
| Windows Go `test -count=1 ./...`, `vet ./...`, `mod verify`, service build | Passed |
| Linux/amd64 Go `test -race -count=1 ./...`, `vet ./...`, `mod verify` in disposable network-disabled container | Passed on final source snapshot |
| Full offline extraction corpus: 72 cases × six cycles | 432 pairs; zero assertion/golden failures, zero unreviewed differences, 18 previously reviewed differences |
| Real Python client → final Go binary, authenticated API/version/readiness/captured Readability/private URL smoke | Passed |
| Independent comparator, captured source-content fixture, six cycles | Six matches, zero mismatches |
| Strict comparator, captured Readability fixture, six cycles | Six visible content differences; expected anchors/state/metadata/media pass; exits nonzero |
| Workflow YAML and verify/publish dependency/permission checks | Passed locally; remote Actions has not run |
| Compose overlays merged with canonical `personal_ai_stack/deploy/news/docker-compose.yml` using dummy digest refs and `--no-env-resolution` | Standby Python, canary Go, rollback Python; HTTP-only/no Go published ports/env file/Chrome; infra network only |

The Readability difference is Python retaining a navigation prefix: 463 vs 444
characters. The historical exact-case review records that removal as an
improvement in `extraction-review-decisions.json`. The new API comparator uses a
curated input and deliberately does not inherit that hash-bound waiver. It
therefore fails instead of silently approving the changed input. The positive
source-content smoke proves comparator/API plumbing, not all-method parity.

Independent Astra review found and prompted fixes for comparator offline escapes,
shared-failure false passes, ignored fixture expectations, unsafe report IDs,
Reddit status propagation, DNS admission before handoff and transient DNS
classification. Focused regressions cover these cases. Its final rereview found
no remaining implementation blocker for this approved pilot preparation; it
does not approve production activation or assert full native-browser parity.

Local evidence is under ignored `artifacts/extractor-release/`, including
`pilot-pytest-final.log`, `http-pilot-positive-comparison-final.json`,
`http-pilot-comparison.json`, `pilot-replay-comparison.json` and
`http-pilot-linux-race-final.log`. The first Linux run passed race tests but its
final dependency verification lacked an unused module in the offline cache.
After downloading public Go modules into the isolated cache, the complete final
race/vet/module command passed with network disabled. The local test database and Go
process were stopped after checks. VPS tests receive only explicitly allowed Go
sources and synthetic fixtures, without production credentials or mounts.

## Remaining activation gates

The local Docker daemon is unavailable. Final service-image build/run and the new
publication workflow therefore require remote CI; earlier image tests do not
prove the final image. No production image is built on the VPS.

Whole-system Python + Go + dedicated pinned CloakBrowser resource comparison,
resource sizing, a 24-hour isolated soak, source-specific live QA and rollout
thresholds remain open. No measured RAM saving or full production readiness is
claimed. Go native-browser cutover remains blocked by the earlier pinned-Cloak
preflight and requires its own network/lifecycle design. Production activation,
secret changes and release actions are separate from implementation approval.
