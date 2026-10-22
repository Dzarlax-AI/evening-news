# Go Page Monitor

`PAGE_MONITOR_SOURCE_MODE=go` routes custom sources through the existing private
extractor service. The default is `python`. Go mode requires
`EXTRACTION_SERVICE_URL` and `EXTRACTION_SERVICE_TOKEN` and does not fall back to
Python HTTP or CDP. It preserves each source's `use_browser` and `wait_for_js`.

The authenticated `POST /v1/page-monitor/extract` endpoint has two operations:

- `fetch` captures a page once and extracts an ordered publication list.
- `parse` processes supplied HTML and a UTC snapshot timestamp without network
  access. Python uses this after optional selector learning on the same capture.

Both use the service's existing admission slots, cancellation, drain and stats.
HTML is bounded at 5 MiB, JSON at 8 MiB, each operation at 30 seconds and each
list at 100 items (source default: 20). Browser sources retain a two-second
minimum settling period when JavaScript waiting is enabled. Other extraction
routes keep their existing settling behavior.

Python retains selector memory, AI calls, article hashes, acknowledged snapshots
and normal SourceManager persistence. Only delivered items are acknowledged;
caller limits do not discard the remainder of a captured list. Optional learning
and parsing share a source deadline with time reserved for returning captured
items. Resource limits do not trigger learning. Publication dates become naive
UTC before reaching the existing ORM.

The shared oracle in `tests/fixtures/page-monitor/list-parity.json` checks the
actual Python and Go parsers with a frozen clock. Unsupported CSS produces a
finite error. Invalid nested-anchor HTML can differ because the HTML5 Go parser
and Python's `html.parser` repair malformed markup differently; browser DOM
captures have already undergone HTML5 normalization.

Release scope, approval, measured results and rollback are recorded in
`docs/ai-plans/2026-10-22-go-page-monitor.html`. Reverting a release uses its saved
app/worker digests and `PAGE_MONITOR_SOURCE_MODE=python`; do not recreate the
shared browser or egress proxy for this mode change.
