# Go Page Monitor

The Go service owns Page Monitor capture and parsing. The News app requires its
authenticated service URL and token at startup and does not fall back to Python
HTTP or CDP. Browser-enabled sources require the separately configured native
Go browser profile; the minimal local Compose stack is HTTP-only.

The authenticated `POST /v1/page-monitor/extract` endpoint has two operations:

- `fetch` captures a page once and extracts an ordered publication list.
- `parse` processes supplied HTML and a UTC snapshot timestamp without network
  access. The News app uses this after optional selector learning on the same capture.

Both use the service's existing admission slots, cancellation, drain and stats.
HTML is bounded at 5 MiB, JSON at 8 MiB, each operation at 30 seconds and each
list at 100 items (source default: 20). Browser sources retain a two-second
minimum settling period when JavaScript waiting is enabled. Other extraction
routes keep their existing settling behavior.

An optional zero-based `offset` continues a resource-limited prefix within the
same configured first-N unique-title window. `limit` is the total window size,
not a page size. A partial response can return `next_offset`; the client advances
only after all received items were delivered or previously acknowledged. The
next scan starts at zero. One item too large for a response advances one position
for that scan without acknowledging the missing item; later scans retry it.
Continuation refetches the page; it does not pin a historical DOM, so a changing
list can shift positions between calls. State stores only bounded position/hash
maps and the current snapshot, with no server cache or stored HTML.

Go ingestion owns collection progress, bounded cursor/hash metadata, and article
persistence. The Python executor retains the selector-learning callback for a
captured snapshot and stores learned selectors in the existing memory service.
Only articles accepted by the fenced Go ingestion run are persisted. The source
probe remains observational and does not update cursor or selector state.

The Go SourceManager runs source jobs concurrently; only actual browser
renders wait for the native gate shared with article extraction. Admission
waiting respects the caller's overall deadline and keeps retryable busy
separate from an active-render timeout. See the
[source collection guide](go-source-manager.md) for batch scheduling.

The shared oracle in `tests/fixtures/page-monitor/list-parity.json` checks the
Go parser with a frozen clock. Unsupported configured CSS produces a finite error. Malformed learned selectors are skipped, preserving
their priority and the top-three general-selector attempts, so persisted AI
output cannot disable the source. Non-web, invalid, credentialed or oversized
item links fall back to the source URL before ID generation. This normalization
intentionally tightens the legacy parser's unrestricted href handling to the
HTTP(S) wire contract. Invalid nested-anchor HTML can differ because the HTML5 Go parser
and Python's `html.parser` repair malformed markup differently; browser DOM
captures have already undergone HTML5 normalization.

Roll back by restoring previously approved app/worker and Go service images that
share the retained v1 contract. There is no Python Page Monitor runtime to
select. Browser and egress infrastructure remain governed by the native Go
deployment profile.
