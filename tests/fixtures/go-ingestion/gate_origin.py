"""Run the reusable synthetic origins with a page that requests selector study."""

import json
import sys
import threading
from urllib.parse import urlsplit

sys.path.insert(0, "/origin-fixtures")
import fixture_origin  # noqa: E402

fixture_origin.PAGE_MONITOR_HTML = b"""<!doctype html><html><body>
<main>
  <section class="zx-entry">
    <span class="zx-title">Learned Selector Gate Story</span>
    <time class="zx-time" datetime="2026-10-22T11:00:00Z">22 Oct</time>
    <p>A synthetic Page Monitor story with enough fixture text to exercise normal processing.</p>
  </section>
</main>
</body></html>"""

_original_do_get = fixture_origin.Handler.do_GET
_request_lock = threading.Lock()
_page_fetches = 0


def _counted_do_get(self):
    global _page_fetches
    path = urlsplit(self.path).path
    if path == "/__gate/counters":
        with _request_lock:
            payload = json.dumps({"page_fetches": _page_fetches}).encode()
        self._reply(200, payload, "application/json")
        return
    if path == "/news":
        with _request_lock:
            _page_fetches += 1
    _original_do_get(self)


fixture_origin.Handler.do_GET = _counted_do_get

fixture_origin.main()
