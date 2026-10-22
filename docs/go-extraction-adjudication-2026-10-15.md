# Captured extraction quality gate — 2026-10-15

**The captured component comparison passes.** There are no unexplained differences or remaining regressions in this reviewed slice. This is not approval to migrate production or a claim that all A–C work is complete.

## Results

| Corpus | Result pairs | Reviewed field differences | Unreviewed differences | Source-golden failures |
|---|---:|---:|---:|---:|
| 72 synthetic cases × six cycles | 432 | 18 | 0 | 0 |
| Two entries from each of eight configured source indexes | 16 | 6 | 0 | 0 |
| Fourteen captured linked pages × two extraction modes | 28 | 35 | 0 | 0 |

The article set also has **five explicitly reviewed Python-baseline defects**. Their failing expectations remain visible in reports. They can be recognized only when the exact baseline error list matches and Go passes its source-backed outcome. No Go assertion failure is waived.

Across corpora, 44 unique field decisions are recorded: 35 article fields, six RSS HTML fields and three synthetic fields repeated over six cycles. Astra independently classified the final article fields as 27 improvements and eight equivalent representations, with zero unresolved cases or regressions.

## Fixed regressions

- Recovered bounded, source-adjacent introductions/subtitles and meaningful repository headings in their original order.
- Preserved article images behind lazy-loading placeholders; retained canonical SVG type for proxied badges.
- Stopped treating unrelated embedded repository-creation timestamps as publication dates.
- Removed author biographies, navigation, sharing controls, bookmark attribution and recommendation UI while retaining authored prose, bookmark titles/excerpts and legitimate discussion of subscriptions/privacy.
- Excluded inert template content from article text.
- Preserved a JavaScript-populated price table by preferring a supplied rendered capture when a matching static table is empty. The match requires corresponding table count/order and ID or class identity. A real browser capture confirmed fifteen rows and thirty visible cells; source price pairs are asserted. This does not implement production browser acquisition/orchestration.

The earlier improvements also remain: valid dates, source-card identity, Telegram video/gallery semantics and long transcript acceptance. Subscription chooser UI is rejected instead of being presented as article content.

## How accepted differences are constrained

[extraction-review-decisions.json](extraction-review-decisions.json) contains exact case and value hashes, source evidence and reasons. Changes to the input, either compared value, a baseline error list, or Go's outcome invalidate the corresponding decision. There are no wildcard case or field permissions.

[extraction-golden-cases.json](extraction-golden-cases.json) supplies independent outcomes for all 116 case IDs. These run even when both implementations agree. They require source anchors, metadata and selected media; specific guards cover restored podcast subtitles, repository publication dates remaining null, and recovered inline photographs. Forced Readability checks its restored source subtitle; the automatic podcast case additionally requires its show-note sections.

The comparator requires every supplied corpus ID and all requested cycles. Matching incomplete replay files cannot pass. Reviewed decisions require the global golden policy at the CLI. New unreviewed differences, missing source content/media, changed source pins and any Go assertion failure produce a nonzero result.

Raw comparison without decisions continues to report the differences. The approved comparison recognizes reviewed improvements; it does not redefine Python output as the source of truth or normalize away arbitrary HTML/media differences.

## Reproduction

From the repository root, after generating the replay files:

```powershell
.\.venv\Scripts\python.exe scripts/compare_extraction_replays.py --python artifacts/adjudication/final/python-synthetic.jsonl --go artifacts/adjudication/final/go-synthetic.jsonl --corpus tests/fixtures/go-parity/manifest.json --cycles 6 --decisions docs/extraction-review-decisions.json --golden docs/extraction-golden-cases.json --output artifacts/adjudication/final/reviewed-synthetic.json
```

For captured source/article runs use their exact local manifests and `--cycles 1`. Publisher HTML remains in ignored local artifacts. Capture evidence, independent reviews, source hashes, live-test logs, Linux race output and final reports are under `artifacts/adjudication/`.

The prepared GitHub workflow now runs the reviewed synthetic comparison and retains its report; it has not been pushed or executed in GitHub merely because its YAML changed.

## Final verification

- Complete Python suite with live public-site and dedicated-Chrome checks: **317 passed, three isolated-PostgreSQL checks skipped**, 38.59 seconds. The harness/comparison guard subset contains 29 passing tests. Pytest emitted existing warnings; this is not a warning-free claim.
- All Go tests with required live Chrome 154 passed serially after Python released the browser.
- Linux Go 1.25.6 `go test -race -count=1 ./...`, `go vet ./...` and `go mod verify` passed in a temporary one-CPU/1536-MiB container. The source/fixture hashes still match the tested snapshot. Linux race did not include Chrome; live CDP was verified separately.
- All three reviewed CLI comparisons returned exit code zero with complete corpus/cycle checks, the global golden policy, zero golden failures and zero unreviewed differences. The five Python baseline defects remain separately enumerated.
- `git diff --check` passed. No commit, push or deployment was performed.

## Remaining scope

N+1 article captures returned 403, though its source feed entries were tested. Complete runtime source access/retries, learned-selector state, AI regeneration/contracts, durable outcomes, snapshots, matched whole-system resource measurements and sustained soak remain open. No RAM-saving percentage or production cutover is established by this gate. Production was not changed.
