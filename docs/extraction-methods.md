# Extraction compatibility ledger

Baseline: `72d39a6514fb86b57eadc98b2e6510855f946088`. Read-only inventory by Astra, 2026-10-15. This is an implementation checklist, not a claim that the Go prototype supports these paths. Every active row needs positive, failure, and fallback evidence before the all-methods gate passes. Synthetic method fixtures and actual-source captures are separate evidence.

## Active methods

Paths below are relative to `news_aggregator/`.

| ID | Contract | Authoritative source |
|---|---|---|
| E01 | HTTP(S) URL admission, media/document exclusion, invisible/control removal and NFKC URL normalization | `extraction/core_extractor.py:44`, `extraction/extraction_utils.py:26` |
| E02 | Primary HTTP headers/status/charset; raw and decompressed bounds | `extraction/extraction_strategies.py:587`, `core/resource_limits.py` |
| E03 | Alternate Safari-header HTTP fetch only when initial HTML unavailable | `extraction/extraction_strategies.py:618` |
| E04 | Reddit JSON before HTML; self/link/image posts, filtered comments, author/date/preview | `extraction/extraction_strategies.py:106` |
| E05 | Reddit linked-page enhanced selectors; replacement preserves Reddit metadata; failed link retains post | `extraction/extraction_strategies.py:241` |
| E06 | Learned CSS selector, method-prefix normalization, quality, failure degradation and success recording | `extraction/extraction_strategies.py:285`, `services/extraction_memory.py` |
| E07 | Learned browser strategy; generic rendered extraction then longer learned-selector candidate | `extraction/extraction_strategies.py:289` |
| E08 | Ordered enhanced selectors including N+1 `.mb-14`, schema attributes, semantic tags and class/ID/role; copied-element cleanup | `extraction/html_processor.py:17`, `:213` |
| E09 | Readability on the shared HTTP response, clean text and quality | `extraction/extraction_strategies.py:385` |
| E10 | Browser/mobile UA, blocked image/media/font requests, readiness polling and deadlines; bounded rendered markup, selectors then heuristics | `extraction/extraction_strategies.py:487` |
| E11 | JSON-LD body object/list/graph and article types; articleBody then text then description | `extraction/metadata_extractor.py:17` |
| E12 | Open Graph description then standard meta description | `extraction/metadata_extractor.py:57` |
| E13 | DOM scores, top-five retention, top-three combination when needed | `extraction/html_processor.py:64`, `:228`, `:278` |
| E14 | After retry exhaustion: canonical then AMP then read-more/full-article links, resolved URLs, first two only | `extraction/core_extractor.py:259`, `extraction/metadata_extractor.py:74` |
| E15 | Cleaning, minimum length/sentences/vocabulary/whitespace/navigation/repetition/error quality, Smart Filter; final sentence/word truncation at 8000 characters | `extraction/extraction_utils.py:51`, `:153`, `extraction/html_processor.py:184` |
| M01 | Title OG/Twitter/title/h1; description OG/Twitter/meta; author microdata/rel/classes/meta | `extraction/metadata_extractor.py:109`, `:133`, `:230` |
| M02 | Date JSON-LD fields/types then ordered CSS/meta/time attributes then text patterns | `extraction/date_extractor.py:18`, `:105` |
| M03 | Image OG/Twitter/image_src/JSON-LD/main-content image, with tiny/avatar/logo/tracker exclusion | `extraction/metadata_extractor.py:152` |
| M04 | Article image/video/source and embedded video/document links, relative resolution, HTTP(S), URL dedup | `processing/media_extractor.py:33`, `processing/ai_processor.py:701` |
| R01 | RSS/Atom parsing: content versus description/summary, title/link/guid/author/tags, missing-link skip and malformed-feed behavior | `sources/rss_source.py:20`, `:51` |
| R02 | RSS enclosure/media:content/media:thumbnail/inline image priority; published/updated/created/parsed-time/current-time fallback | `sources/rss_source.py:98`, `:154` |
| T01 | Telegram browser then HTTP; t.me/s then telegram.me/s; selector readiness, three scrolls, alternate HTTP headers | `telegram/telegram_source.py:109`, `:158`, `:187` |
| T02 | Message container fallbacks; read metadata/media before removing UI; text selector/text/meta fallback; media-only posts | `telegram/telegram_source.py:244`, `telegram/message_parser.py:36`, `:158` |
| T03 | Message permalink/date/ID/forwarding; external-link priority/dedup/normalization, original URL and Telegram provenance, hashtags/title | `telegram/message_parser.py:203` through `:468` |
| T04 | Short Telegram external-link expansion through independent HTTP/readability path, accepted only if over twice original length | `telegram/message_parser.py:111` |
| T05 | Telegram image/video/round-video/audio/voice/sticker/GIF/document/poll/location/contact, CSS backgrounds, media metadata and exclusions | `telegram/media_extractor.py:106` through `:655` |
| C01 | Custom browser default, explicit HTTP mode, JS wait and bounded markup; no implicit browser-to-HTTP fallback | `sources/page_monitor_adapter.py:18`, `sources/page_monitor_source.py:241` |
| C02 | Custom learned containers, top-three learned selectors above 0.7, configured selectors; learned/configured title/link/date and ancestor/self links | `sources/page_monitor_source.py:337`, `:443`, `:479`, `:514` |
| C03 | Custom date families, relative dates, future/old rejection, missing-date fallback | `sources/page_monitor_source.py:598`, `:667` |
| C04 | List-page detection, AI structure study, extraction rerun with new selectors; persistent learned structure, failure-triggered heuristics | `sources/page_monitor_source.py:375`, `:798`, `:820`, `:858`, `:918` |
| C05 | First/unchanged/changed snapshots, title/link/description hash, case-insensitive title dedup, max count and description limit | `sources/page_monitor_source.py:194`, `:708`, `:733`, `:758`, `:849` |
| X01 | Identity/gzip/deflate/br; dishonest length/chunking, wire/output caps, unsupported/truncated/trailing encodings; browser char/UTF-8 bounds | `core/resource_limits.py` |

## Ordering and compatibility traps

```text
URL admission -> Reddit JSON -> optional linked-page enhanced selector -> result
             -> shared HTTP -> learned browser OR learned CSS
                -> enhanced CSS -> Readability -> browser CSS/heuristics
                -> shared HTML or alternate HTTP:
                   JSON-LD OR OG/meta OR scored heuristics
             -> bounded retry -> first two canonical/AMP/read-more links -> failed
```

- A nonempty poor JSON-LD candidate currently short-circuits OG/heuristics in that attempt. Changing this is an intentional improvement, not identical parity.
- Learned browser currently still incurs the initial HTTP request. Removing that request needs explicit trace/call-count comparison.
- The authoritative processor is `process_article_outcome`, not the older combined processor. Ordinary extractable URLs currently extract first, then fall back to source content. Source-first Go is a separate experiment and must not be mixed into language-only benchmarking.
- Forced analysis uses fresh extracted text even if storage retains a longer archive. Existing successful stages, existing media, original labels and category confidences are preserved.
- The 100000-character admission limit is distinct from the 8000-character final presentation truncation. Report original length, final length, truncation policy and method; do not silently conflate them.
- T04 currently uses an unbounded `resp.read()`. Preserve the feature in Go with the approved bounds; do not reproduce the resource bug. No production Python change is authorized by this ledger.
- Poll/location/contact synthetic identifiers currently use Python randomized hashes. Compare semantic payloads, or explicitly adopt a documented stable identifier; process-specific bytes are not a valid parity target.

## Dormant and later API capabilities

Keep these visible without claiming they are active production fallbacks:

- `AIClient.extract_publication_date`, `extract_full_article_link`, domain `AIExtractionOptimizer` generation, and `AIPageAnalyzer.analyze_content_changes`: no current application callers found.
- Core learned-date helpers are not called by core metadata extraction; custom learned dates are active.
- `HTMLProcessor.remove_unwanted_elements` and `is_likely_content_element` are unused; copied-element cleanup is active.
- Telegram channel Open Graph image fallback is intentionally dormant because it returns a channel logo.
- `sources/telegram_source.py` is a facade; metadata wrapper methods are not separate algorithms.
- `api/articles_router.py:77` has authenticated media re-extraction with direct HTTP and container-to-body fallbacks. Its behavior belongs to the later API migration; the shared media parser belongs to A–C.

## Gate

Each active ID needs a test or a documented open gap. A method forced in isolation is not proof of the fallback graph or its transport lifecycle. Rendered-markup replay is not a real-browser test; feed parsing is not source fetching; successful canned AI output is not correct prompt construction. All eight actual sources need separate captured/live evidence before the source-coverage gate passes. Unsupported/failed cases remain visible and cause replay validation failure. No translation to Go is considered complete while active ledger rows remain unverified.
