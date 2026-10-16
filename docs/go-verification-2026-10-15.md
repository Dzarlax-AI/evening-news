# Prototype verification — 2026-10-15

Branch: `prototype/go-extraction-parity`, uncommitted working tree based on `72d39a6514fb86b57eadc98b2e6510855f946088`.

## Executed checks

| Check | Fresh result |
|---|---|
| Full Python suite with public-site integrations and dedicated Chrome markup/memory test enabled | **307 passed, 3 skipped**, 38.42 seconds. Skips require an explicitly configured isolated PostgreSQL database. |
| All Go tests with `GO_BROWSER_TEST_REQUIRED=1` | Passed, including actual Chrome 154 tests; no silent live-browser skip. |
| Linux Go 1.25.6 `go test -race -count=1 ./...` | All three packages passed. Linux run has no Chrome; live CDP was tested separately on Windows. |
| Linux `go vet ./...`, `go mod verify` | Passed; container exit code 0. |
| Shared synthetic corpus, six cycles | 72 cases, **432 records per implementation**, zero assertion failures; Go used two workers. |
| Captured source parser corpus | Both implementations pass all **16 cases across eight configured source indexes**. |
| Strengthened real-article quality corpus | Go passes all **28 method cases**; Python baseline has **five failing cases**. |
| Source snapshot integrity | Packaged Go/fixture SHA-256 values match the local source at verification. |
| Whitespace check | `git diff --check` passed. |

Python warnings were reported by pytest; the passing count is not a warning-free claim. Three PostgreSQL checks were not run: one extraction-cache persistence case and two processing/migration cases.

## Comparison gate remains open

Comparators intentionally return nonzero for these remaining differences:

- Synthetic corpus: **18 differences** — two navigation-cleanup cases and one displayed-date case, repeated six times.
- Source corpus: **six differences**, all rich RSS HTML representation/sanitization. Telegram fields match the reviewed two-message capture.
- Article corpus: **41 field differences** and five Python-baseline quality assertion failures. These counts include intentional Go policy changes and genuine unresolved quality/metadata differences; they are not automatically waived.

The five baseline failures cover membership UI accepted as article content, long transcript rejection/metadata-only fallback, and a short commercial-page Readability result. Source-backed transcript assertions require both introductory and substantive body text. Passing Go assertions does not establish complete editorial equivalence.

## Test cleanup correction

The first four public-site browser integrations passed assertions but emitted Windows `BaseSubprocessTransport` shutdown errors afterward. Their synchronous `browser.stop()` terminated Chrome without awaiting the subprocess pipes/reaping. `tests/test_integration_real.py` now awaits `communicate()` on each test-owned process before pytest closes its event loop, with a bounded timeout/kill fallback.

All four integrations then passed without those post-exit errors, and the complete live-enabled suite also passed. Astra found no blocker for normal completion of these test-owned sessions. This helper is not claimed to be cancellation-safe and does not change production browser handling.

## Isolation and artifacts

Linux verification used a temporary official `golang:1.25.6-bookworm` container limited to one CPU and 1536 MiB. Only source/fixture files were mounted read-only plus an output directory. No production data, credentials, database, scheduler or sender was mounted or invoked. The container removed itself after completion. The dedicated local Chrome instance was stopped after tests.

Evidence is under ignored `artifacts/verification-final/`: full Python and live Go logs, Linux race/static-check log and exit code, per-run JSONL outputs, non-waived comparison reports, and source hashes. This was a direct Linux test run, not a GitHub Actions execution.

Production was not deployed or switched. Full migration parity, actual N+1 article access (403 in the captures), AI regeneration, persistent processing/snapshots and sustained whole-system resource measurements remain open. No RAM-saving claim is established.
