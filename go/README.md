# Captured extraction replay

This Go 1.25.6 module contains captured extraction replay and a separately invoked private extraction service. The replay CLI does not connect to production databases, fetch pages, start a browser, call AI services, or send messages. Loopback prototype adapters remain separate from the public HTTP/browser transport used by the service. See [service contracts and operations](../docs/extractor-service.md).

From `go/`:

```powershell
go test ./...
go vet ./...
go run ./cmd/extract-replay -corpus ../tests/fixtures/go-parity/manifest.json -cycles 3 -workers 2 -output replay.jsonl
go build -o extract-replay.exe ./cmd/extract-replay
```

The CLI reads the version 1 manifest used by the Python replay harness. It emits one JSONL record per case per cycle. Unknown input fields, unsupported strategies, missing expectations, and failed expectations do not silently pass. Object fields in `expected.fields` are recursive projections; arrays retain strict length and order. This lets semantic poll/contact/location checks omit process-dependent identifiers. Cases run concurrently, so output order is unspecified; join by `case_id` and `cycle`.

Exit codes: 0 means all supplied assertions passed; 1 means one or more case assertions failed; 2 means invalid arguments, manifest, or output errors. Passing a synthetic corpus is not an all-methods compatibility result.

## Implemented replay behavior

- Ordered learned CSS/browser, enhanced DOM selectors, Readeck readability, captured browser selectors/heuristics, JSON-LD, OG/meta and scored DOM fallback.
- Reddit JSON with self/link/image distinctions, comment filtering, metadata and captured linked articles.
- RSS/Atom first-entry (or `entry_index`) content/summary, metadata and image priorities.
- Telegram message metadata before UI cleanup, text/meta fallback, captured external expansion, media families and avatar/emoji/reply exclusions.
- Configured/learned/AI-supplied custom listing selectors, captured browser versus explicit HTTP mode, relative links and absolute dates.
- URL validation/normalization, content quality, bounded bytes and Unicode code points, explicit 8000-character sentence/word finalization metadata, and captured alternative URL recovery.
- `DecodePage` independently tests bounded identity/gzip/zlib/Brotli response decoding. Brotli is pinned to v1.2.0 because v1.1.1 accepted a tested truncated stream as EOF.

Result `state` is `extracted`, `failed`, `resource_limit`, or `unsupported`. Missing metadata serializes as JSON null. `original_chars`, `truncated`, and `truncation_policy` expose presentation truncation. `cancel_at=browser_html_capture` injects a replay cancellation; it does not assert that a real browser tab was closed. Unrecognized cancellation boundaries return `unsupported`.

Alternative-page recovery retains the original article `url`; `extracted_from` records the captured canonical/AMP/read-more page supplying its text. This keeps source identity separate from the extraction location.

Optional `input.now` supplies an RFC3339 clock value for deterministic missing-date fallbacks in RSS/Atom, Telegram, and custom sources. Without it, one current UTC value is captured at extraction entry. Source fallback dates are naive UTC ISO strings; ordinary article metadata remains null when no date is present. Invalid injected clocks fail explicitly.

Byte and text caps default to 5 MiB per captured body and 100000 Unicode code points. `input.max_bytes` / `max_chars` or an `input.limits` object override these. The manifest is bounded to 64 MiB. Concurrency is 1вЂ“8 workers (default 2). Extraction context cancellation is checked at entry and strategy boundaries; DOM parsing is synchronous and cannot be forcibly interrupted midway.

## Measurement

Each record reports extraction `duration_ns` separately from `memory`. Memory is sampled after each case: Go heap allocation/in-use/total allocation, object/GC/goroutine counts, and Linux resident bytes from `/proc/self/statm`. On other platforms resident bytes are null and `rss_source` is `unavailable`. Concurrent samples describe the whole process; they are not per-case allocation attribution. Use the same workload, worker count, cycle count and host for comparisons. These metrics alone establish no memory-saving claim.

## Remaining compatibility work

The authoritative inventory is [the extraction ledger](../docs/extraction-methods.md). Replay does not implement or prove live HTTP headers/status/charset, browser launch/readiness/deadline/tab cleanup, actual retry timing, browser-to-HTTP source access, persistent learned selector degradation/success recording, Smart Filter service behavior, custom relative-date clocks, learned selector confidence ranking, AI structure regeneration, custom snapshot/change detection, production orchestration, or durable outcome semantics. The alternative URL replay tests captured first-two recovery but does not reproduce live retries. Captured source content fallback is tried after extraction strategies for ordinary URLs; it is not a source-first optimization.

The Go readability library differs from Python readability. Body/metadata equivalence must be assessed against actual source captures, not inferred from similar library names. Stable SHA-256-based poll/location/contact references intentionally differ from Python randomized hashes. Date parsing preserves naive date strings and converts aware values to UTC; historical Python host-local timezone behavior needs an explicit compatibility decision.

The service entrypoint is `go run ./cmd/extract-service`; it requires a dedicated
`EXTRACTOR_SERVICE_TOKEN` and defaults to port 8080. It has no sender or database.
Python integration is disabled by default. Building the service does not activate
it in production; production rollout remains separately gated.

## Controlled live transport checks

From `go/`, with a dedicated test Chrome already running:

```powershell
$env:GO_BROWSER_TEST_ENDPOINT='http://127.0.0.1:<debugging-port>'
$env:GO_BROWSER_TEST_REQUIRED='1'
go test -count=1 ./...
```

Run serially against that browser: concurrent reviewers change its global target set and invalidate ownership assertions. Without the endpoint, live tests skip. The loopback adapter disables workers, frames, popups and WebSockets through additional CSP installed before parsing. It preserves existing CSP and bounds both wire responses and decoded source conservatively. These tests do not implement production orchestration. See [current evidence and limitations](../docs/go-benchmark-report.md).
