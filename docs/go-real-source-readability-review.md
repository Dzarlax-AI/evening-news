# Readability review on configured public sources

Date: 2026-10-15. Isolated `prototype/go-extraction-parity` working tree. No production application changes, database writes, commits, push or deployment.

## Result

Go Readability is usable as an extraction component, but it is not a drop-in replacement for the Python pipeline. Real inputs expose missing pipeline quality checks, metadata differences and some library-level loss of article introductions or headings. Keep the migration gate open.

## Sample and method

The eight enabled source names/types/URLs were read directly from the production database using a read-only SELECT. Public pages were captured once and then replayed offline, without DB, AI, sender or network access in either parser.

- Eight source-index captures succeeded. Two linked pages per source were attempted.
- Fourteen linked-page captures succeeded, representing seven sources. Both N+1 article requests returned HTTP 403; its feed was accessible. No bypass or authenticated capture was attempted.
- The sample includes a commercial service page linked from Telegram and two project repository pages linked from AI news. These are real source inputs, not all editorial news articles.
- AI secret candidates were corrected after the initial discovery pass selected category/membership navigation. The reviewed pass uses actual article-container links. Initial results under `artifacts/live-readability/` are superseded by `artifacts/live-readability-v2/`.
- Each captured linked page was tested with forced Readability and automatic strategy selection: 28 case pairs, one cycle. Assertions currently check extraction state only; they do not establish editorial completeness or correctness.
- Raw library output was compared separately: Python `Document.summary()` and Readeck `FromReader`/`RenderHTML`, both converted to plain text with the same BeautifulSoup whitespace normalization. This excludes application quality filters and the 8000-character presentation limit.

Raw publisher HTML/text stays in ignored local artifacts; it was not added to committed fixtures. Capture timestamps, resolved URLs, response statuses and SHA-256 hashes are recorded in `captures.json`. The capture tool bounds wire and decoded bodies to 5 MiB.

## Findings

| Area | Evidence | Implication |
|---|---|---|
| Readability library | All fourteen raw Go library calls returned content without an error. Three normalized raw outputs were exactly equal to Python; other outputs need quality review rather than automatic acceptance. | The library works on these inputs; output equivalence is not established. |
| Quality rejection | Forced Go extraction rejected AI secret's first article and Karpathy's Sequoia talk. Raw Go produced 1422 and 36186 characters respectively. | These are application quality rejections, not failure to parse HTML. The forced Python helper skips that quality gate, so its passing state is not a fair pipeline comparison. |
| Access barrier | AI secret's first capture is membership-gated. Both libraries select subscription UI rather than the unavailable article body. | Its unconditional `expected.state=extracted` assertion is an inadequate oracle. Do not weaken Go's quality gate to make it pass. Access/content classification must precede acceptance. |
| Selector acceptance | On both Verge pages Go accepts broad enhanced-selector text, while Python proceeds to Readability. Go's accepted text includes unrelated page UI/ad/footer material. Similar strategy divergence occurs on Lenny and Satya pages. | Port or redesign the missing SmartFilter acceptance contract before accepting broad selectors. Changing language or Readability library alone will not fix it. |
| Existing Python filter | Astra traced Python's rejection to SmartFilter; its unanchored month/date pattern can classify ordinary byline dates as metadata. | Do not blindly copy this regular expression into Go. Test actual contamination and legitimate bylines separately. |
| Article introductions | Raw Go output drops a standfirst from the linked Serbian article and introductory text from one Lenny article. | Some differences are substantive; a blanket waiver for Readability differences would hide text loss. |
| Extra context | Raw Go retains author biographies/follow prompts on Verge. It also retains different headings/captions in repository pages. | Add source-backed quality assertions for introductions, headings and exclusion of page UI. |
| Date metadata | Go misses Karpathy's valid ISO `<time datetime>` values and its displayed `30 Apr, 2026` style. | Fix date admission/normalization with targeted regression cases; this is independent of Readability selection. |
| Truncation | Some forced Python outputs exceed 8000 characters; Go finalizes them to the presentation cap. | Do not count this apparent size difference as library content loss. Compare raw extraction separately from final output. |
| Short fallback | Both automatic pipelines return only 437 characters for the long Sequoia page. | Current Python behavior is also a poor quality target here. A migration should preserve the main article, not mechanically reproduce this fallback result. |

Astra independently traced the two rejections to the vocabulary-diversity threshold: cleaned CoreWeave output falls just below 0.3; the legitimate long Sequoia transcript also falls below it. The latter preserves substantive raw text apart from the title. A global vocabulary threshold cannot reliably distinguish a subscription modal from a long repetitive transcript.

The final full-pipeline comparison records **two assertion failures and forty unreviewed field differences across 28 pairs**. Those forty differences include content, media, metadata and harness-stage effects; they are not forty independently confirmed Go bugs. Comparison exits nonzero deliberately.

## Next fixes, in order

1. Align the comparison stages: raw library extraction, strategy acceptance, then common finalization. Preserve failures instead of weakening assertions.
2. Add regression fixtures for contaminated broad selectors, legitimate byline dates, preserved introductions and ISO/displayed publication dates. Derive small fixtures from the observed structure without committing whole publisher pages.
3. Correct date parsing and the missing quality-acceptance contract. Recheck every selector/fallback path affected by that contract.
4. Decide explicit source policies for subscription previews, repository READMEs and commercial Telegram links. Readability cannot determine whether these should enter the digest.
5. Obtain accessible N+1 article captures through the already-supported source transport, then repeat the reviewed corpus. No claim of complete eight-source article coverage yet.
6. Only after quality stabilizes, run comparable Linux memory/latency trials including Chrome. These captures establish no RAM-saving figure.

## Evidence and reproduction

- Capture tool: `scripts/capture_readability_sources.py`.
- Final local evidence: `artifacts/live-readability-v2/{captures.json,manifest.json,python.jsonl,go.jsonl,differences.json,raw-go.jsonl,raw-comparison.json,raw-texts.json}`.
- Raw review helpers: `artifacts/raw-readability.go`, `artifacts/review-raw.py`; local diagnostic artifacts, not production executables.
- Capture script syntax check and `git diff --check` passed. Both offline runners completed; the Go runner and comparator intentionally returned failure for the findings above.

```powershell
.\.venv\Scripts\python.exe scripts/capture_readability_sources.py --sources artifacts/live-sources.json --output artifacts/live-readability-v2
.\.venv\Scripts\python.exe scripts/benchmark_python_pipeline.py --corpus artifacts/live-readability-v2/manifest.json --cycles 1 --output artifacts/live-readability-v2/python.jsonl
```

From `go/`:

```powershell
go run ./cmd/extract-replay -corpus ../artifacts/live-readability-v2/manifest.json -cycles 1 -workers 1 -output ../artifacts/live-readability-v2/go.jsonl
```

Run comparison from the repository root with `scripts/compare_extraction_replays.py --python artifacts/live-readability-v2/python.jsonl --go artifacts/live-readability-v2/go.jsonl --output artifacts/live-readability-v2/differences.json`.

Production compatibility, all-method coverage and migration approval remain open.

## Subsequent fixes and checks across methods

The next iteration extended beyond Readability. Date metadata now admits valid minute-only ISO timestamps and displayed comma dates while rejecting malformed calendar dates. Source adapters preserve raw RSS authors, trim feed titles, read custom-card dates embedded in text, resolve self/ancestor card links, and select Telegram publication time instead of video duration. Telegram media extraction prioritizes actual video sources and excludes video-preview images from gallery content.

Article quality uses structural cleanup on a clone: subscription dialogs, footer/ad containers and identified recommendation widgets are removed without discarding surrounding article paragraphs. Editorial discussions of privacy rules and subscription prices remain valid. Vocabulary is checked in bounded windows so legitimate long transcripts survive; an additional guard detects aligned repeated 100-word chunks, not arbitrary pagination duplication. Complete membership chooser signatures are rejected across article extraction methods. Raw source retention remains a separate contract.

The Python harness now applies candidate acceptance and finalization to forced article components, bounds alternative captures, and distinguishes Telegram media-only posts from rejected article bodies with attachments. These are test-adapter changes; production Python was not modified.

The shared corpus now has **72 cases**, including learned/automatic fallback to JSON-LD, automatic browser/metadata fallback, successful Telegram short-link expansion, source retention and alternative-byte admission. Both runners pass **432 result records over six cycles**. Nineteen Python harness regression tests pass. Full-field synthetic comparison still reports eighteen differences: two navigation-cleanup cases and one corrected displayed-date case, each repeated six times. None is silently waived.

Separate real-source replay covers **sixteen entries from all eight configured indexes** using RSS/Atom, Telegram and configured custom CSS. Both implementations pass source-backed title/link/content assertions. The real article corpus has **28 method cases** with strengthened quality expectations: membership content must fail, and the Sequoia article must retain actual transcript text. Go passes all 28; Python baseline fails five cases (membership, long transcript and one short commercial-page Readability result). Full-field comparison remains nonzero; assertion success does not establish complete text/media equivalence.

Astra independently reviewed the frozen date, source, structural-cleanup and harness fixes and found no remaining blocker in that slice. Publisher standfirst/subtitle recovery, rich RSS HTML sanitization/serialization policy, complete source orchestration, AI regeneration, durable outcomes, final-source Linux race/CI and sustained resource trials remain open. No RAM-saving claim or production migration follows from these tests.

Final full-field comparison: six source-parser differences (RSS rich HTML), eighteen synthetic differences and 41 article-method differences remain visible. Final Go test/vet/module checks passed. Live Chrome and Linux race were verified in earlier prototype iterations; they were not rerun on the final source revision in this fix iteration.
