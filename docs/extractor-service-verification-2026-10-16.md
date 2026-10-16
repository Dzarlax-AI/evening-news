# Extraction service implementation — 2026-10-16

**Subsequent production preflight found pinned-CloakBrowser failures.** The
[preflight report](extractor-production-preflight-2026-10-16.md) supersedes any
inference of browser production readiness from this earlier ordinary-Chrome run.
The three PostgreSQL checks below were subsequently executed and passed separately.

The approved private-service boundary is implemented. Production routing still
defaults to Python. No commit, push, PR, migration or production deployment was
performed for this change.

## Implemented

- Standalone Go entrypoint, strict authenticated `/v1/extract`, health/version and
  bounded-cardinality operational stats; admission, request cancellation, browser
  concurrency and shutdown drain.
- Public HTTP with destination-pinned DNS validation, redirect admission,
  independent wire/decoded limits and stable error classes. Existing loopback
  adapters remain restricted.
- External configured Chrome with disposable owned resources, a fail-closed
  browser proxy, Go-mediated public requests, bounded asynchronous settling and
  blocked private destinations/workers/WebSockets/frames/popups.
- Runtime article graph using the existing extraction core, learned observations,
  alternative pages and Reddit; redirect provenance and relative media are
  preserved. Incomplete matching tables can request one bounded render.
- Python client and opt-in `python`/`shadow`/`go` routing. Sequential fallback uses
  the remaining total budget. Outages retain durable retry; deterministic resource
  errors remain terminal. Metadata/media reach existing processing callers.
- Learned failures apply to the supplied identity only after real selector
  evaluation. Transport outages do not degrade selectors; shadow does not persist
  observations, and Python fallback owns its own learning.
- Independent non-root image, isolated HTTP Compose configuration, build/test CI,
  and a real Python-to-Go smoke script. Source collection, C04 AI structure study,
  snapshots, database transactions and delivery stay in Python.

## Verification

| Check | Observed result |
|---|---|
| Full Python suite, real-site tests and dedicated browser enabled | **358 passed, 3 skipped**, 370 warnings, 39.30 seconds |
| Go full suite with dedicated Chrome 154 required | All six packages passed |
| Go vet and module verification | Passed |
| Final native Go build and actual Python client/server smoke | Passed: build identity, golden source anchors, auth, replay-field exclusion and private URL rejection |
| Synthetic replay, 72 cases × 6 cycles | 432 pairs; zero assertions, golden failures or unreviewed differences; 18 pinned reviewed field differences |
| Linux Go 1.25.6 race / vet / module verification | All six packages passed in an isolated one-CPU/1536-MiB container; browser tests ran separately on Windows |
| Independent image update and rollback | Two scratch images built; A → B → A passed API version, health and golden content checks in network-disabled non-root containers |
| Isolated Compose configuration and diff whitespace | Passed |

The three skipped tests require a dedicated PostgreSQL URL; existing production
database configuration was not used. The Python suite has existing warnings; this
is not a warning-free claim. A prior full run failed because its external test
Chrome process had exited. The final run owned Chrome in the same invocation and
passed, followed by Go CDP tests; this did not require a production-code workaround.

Live browser evidence uses actual Chrome with a controlled public-response backend:
async fetch changes a nonempty shell before capture, private egress attempts never
reach the private fixture server, foreign targets survive, and owned targets are
released after success and cancellation. This is separate from live publisher
compatibility.

An additional live HTTP-only check of the configured Satya source's
`models-as-insider-risks` article returned success through `enhanced_selectors`
with 5144 characters. The AI secret `coreweaves-lenders-want-out` page returned
`needs_selector_study`: its currently fetched article/main contains 315 visible
characters and fails the quality gate. That page is an open live-compatibility
case, not a successful full-source check or a reviewed new parity waiver. An
initial smoke expected success there and failed; diagnosis retained the bounded
markup only in ignored artifacts. No protection bypass was attempted.

## Independent review

Astra found and re-reviewed fixes for terminal retry classification, shadow-mode
legacy retry behavior, failed learned observations, transport-outage learning,
HTTP 400 for unsupported input, and redirect provenance across main/browser/
alternative/Reddit-linked paths. No blocking issue remained in the reviewed code
slice. This review does not approve production activation or all-source parity.

The original plan assumed an active article AI-selector round trip. Code inspection
confirmed that only C04 custom collection currently calls AI structure study;
article extraction does not. That boundary remains unchanged. Reserved study
responses fail closed unless explicit bounded Python fallback is enabled.

## Before production activation

- Verify actual public-source/browser compatibility for all applicable contracts,
  including the currently rejected AI secret page and source-specific delayed JS.
  Finite settling cannot prove arbitrary delayed applications ready.
- Run matched whole-system Python versus Python + Go + Chrome measurements and
  the planned sustained isolated soak. No memory-saving percentage is established.
- Resolve production private networking/egress, token provisioning, resource sizing
  and endpoint switching. No public CDP/extractor port is intended.
- Retain prior v1 release artifacts for bidirectional compatibility tests once the
  first version exists. Switching build labels from the same code tests image
  independence and rollback mechanics, not compatibility with a historical binary.
- Complete dedicated-PostgreSQL checks before claiming persistent integration
  coverage. CI has deterministic tests; live CDP evidence was obtained locally.

See [operation and rollback instructions](extractor-service.md) and the approved
[HTML plan](ai-plans/2026-10-15-go-prototype-and-migration.html#extractor-service).
Local logs and temporary test scripts are in ignored `artifacts/extractor-service/`.

The Linux source/fixture archive SHA-256 is recorded in
`artifacts/extractor-service/source-sha256.txt`. Both image builds used that same
snapshot. Temporary test containers used `--rm` and were stopped after each smoke;
images and the source archive remain available for inspection. This checked
version switching without any Python deployment. It does not establish historical
API compatibility or production endpoint-switch behavior.
