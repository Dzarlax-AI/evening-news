# Go extraction prototype: test evidence

Date: 2026-10-15. Branch: `prototype/go-extraction-parity`, based on `72d39a6514fb86b57eadc98b2e6510855f946088`.

**Isolated prototype; A–C and the migration decision remain open.** Production application code, data and running services were not changed. No prototype commit, push or deployment was performed.

## Extraction and parity

[Extraction inventory](extraction-methods.md) records 32 active contracts. The corpus contains 61 synthetic cases, including automatic selector-to-Readability fallback. Both implementations produced 366 records over six cycles with zero fixture assertion failures. This is component evidence, not proof that every branch of all 32 contracts works on real sources.

Full-field comparison includes content, original URL, title, dates, author, description, image and complete media metadata. It rejects empty or mismatched case/cycle sets. Nine Python harness/comparison regression tests pass.

Comparison currently exits nonzero: **12 unreviewed differences**, two Readability cases repeated over six cycles (`article.readability_after_selector_miss`, `article.auto_readability_after_selector_miss`). Python retains the navigation prefix “Sections and menus”; Go removes it. Astra found the remaining article text identical for these fixtures. This has not been waived automatically or established for real sources.

Testing corrected custom container text and 500-character descriptions, browser-versus-HTTP capture selection, original URL identity, RSS description/enclosure semantics, and article attachment type/source/alt metadata. Regression tests cover these boundaries. Go uses Readeck `go-readability/v2 v2.1.3`.

## HTTP and browser transport

New independent adapters in `go/transport` operate on controlled loopback inputs. HTTP tests cover normal/Safari headers, statuses, redirects/admission, charset, identity/gzip/zlib/Brotli decoding, malformed streams, chunking, wire/output limits, deadlines, cancellation and body closure. Alternate headers alone do not prove the production retry orchestration.

The required live suite passed serially against dedicated local Chrome 154: script-rendered markup, metadata, blocked resources, response/DOM limits, failure/cancellation cleanup and repeated target ownership checks. A concurrent reviewer initially interfered with exact global target assertions; after both reviewers stopped, the serial full suite passed.

Independent Astra probes found worker/WebSocket requests bypassing main-target interception. Bounded streamed response fulfillment now installs CSP before parsing; repeated probes produced zero worker HTTP requests and WebSocket handshakes. Workers, frames, popups and WebSockets are intentionally disabled in this controlled adapter. Existing CSP and gzip rendering have tests. Decoded source also uses the wire-byte ceiling conservatively. This is not a production browser integration or proof of general browser sandboxing.

## Verification and limits

- Final Windows Go 1.25.6: required live `go test -count=1 ./...`, `go vet ./...`, `go mod verify` passed.
- Python 3.12.11: nine harness tests passed; 61 cases × six cycles passed assertions in both implementations.
- An isolated resource-limited Linux container passed `go test -race -count=1 ./...` on the earlier source snapshot. It excludes the final CSP changes and has no live Chrome; final-source Linux race/CI remains open. No production data or credentials were mounted.
- No Go memory-saving percentage is established. Windows Go heap is not process RSS. Component timings and synthetic workloads do not establish production latency. Chrome must be included in whole-system measurements.

## Remaining A–C work

1. Review the Readability quality differences on representative captures from all eight configured sources. Current fixtures are synthetic.
2. Integrate and test complete retry/source access ordering, Telegram scroll/host fallback and short-link expansion.
3. Preserve learned-selector ordering and updates, AI regeneration, relative dates, snapshots and change detection.
4. Implement typed AI contracts and durable processing outcomes against an isolated copy of the actual PostgreSQL schema.
5. Run final-source Linux race/CI, comparable whole-process and Chrome resource trials, and sustained soak.

Production integration, auth/API/site/delivery changes and cutover require the later approval gate. Current evidence supports continued prototype work, not replacing the running service.

## Subsequent real-source review

Public captures from all eight configured source indexes and fourteen linked pages are now available locally. N+1 article requests returned 403; AI Secret includes a membership-gated capture. Separate raw-library and pipeline comparison revealed missing SmartFilter behavior, a date parsing defect, lost introductions and a legitimate transcript rejected by a vocabulary threshold. See [the real-source review](go-real-source-readability-review.md) for scope, evidence and the prioritized fixes. The earlier synthetic-only evidence above is not the latest source-coverage result; the overall gate remains open.

The subsequent fix iteration added date/source/media and structural quality regressions, aligned forced Python candidate checks/finalization, and expanded the shared corpus to 72 cases (432 records per runner across six cycles, zero assertion failures). Separate sixteen-case source replay covers all eight configured indexes; the Go article-quality replay passes 28 method cases while the unchanged Python baseline exposes five failures. Nineteen Python harness tests pass. Remaining full-field differences and all-method limitations are recorded in the real-source review and `coverage.json`; no total-system resource comparison has been made.

Final authorized verification now also covers the current source snapshot on Linux: race tests, vet and module integrity pass. Full live-enabled Python suite: 307 passed, three isolated-PostgreSQL checks skipped. Required dedicated-Chrome Go tests pass. See [fresh verification results and remaining comparison failures](go-verification-2026-10-15.md). These supersede earlier notes that final-source Linux race/live checks had not run; GitHub CI and the migration gate remain open.

The subsequent source adjudication fixed additional introduction/heading, lazy-media, timestamp-provenance, UI and rendered-table regressions. The captured comparison now has zero unexplained differences and zero source-golden failures: 44 unique reviewed field decisions plus five pinned baseline defects. These are source-backed approvals, not blanket normalization. See [the final captured quality gate](go-extraction-adjudication-2026-10-15.md); full migration and resource-measurement gates remain open.
