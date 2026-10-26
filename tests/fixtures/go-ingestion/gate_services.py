"""Synthetic Gemini and Telegram endpoints for the isolated ingestion gate."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
from urllib.parse import urlsplit

LOCK = threading.Lock()
COUNTERS = {"gemini": 0, "selector_study": 0, "telegram": 0, "page_monitor_original_title": 0}


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *_args):
        return

    def _json(self, status, value):
        data = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        if self.path == "/__gate/counters":
            with LOCK:
                counts = dict(COUNTERS)
            self._json(200, counts)
        else:
            self._json(404, {"error": "not_found"})

    def do_POST(self):
        path = urlsplit(self.path).path
        length = int(self.headers.get("Content-Length", "0"))
        if length > 2 * 1024 * 1024:
            self._json(413, {"error": "too_large"})
            return
        body = self.rfile.read(length)
        if path.startswith("/v1/models/") and path.endswith(":generateContent"):
            with LOCK:
                COUNTERS["gemini"] += 1
            try:
                prompt = json.loads(body).get("contents", [{}])[0].get("parts", [{}])[0].get("text", "")
            except (ValueError, TypeError, IndexError):
                self._json(400, {"error": "invalid_request"})
                return
            if "CSS selectors" in prompt or "Identify CSS selectors" in prompt:
                with LOCK:
                    COUNTERS["selector_study"] += 1
                result = {
                    "content_selectors": [".zx-entry"],
                    "title_selectors": [".zx-title"],
                    "date_selectors": [".zx-time"],
                    "confidence_scores": {".zx-entry": 0.99},
                    "page_type": "news",
                    "reasoning": "Synthetic gate page has one article element per story.",
                    "suggested_config": {"max_articles_per_check": 2},
                }
            else:
                if "Learned Selector Gate Story" in prompt:
                    with LOCK:
                        COUNTERS["page_monitor_original_title"] += 1
                result = {
                    "optimized_title": "Synthetic ingestion fixture story",
                    "summary": "A synthetic article was collected and processed inside the isolated fixture.",
                    "categories": ["Tech"], "category_confidences": [0.99],
                    "category": "Tech", "category_confidence": 0.99,
                    "summary_confidence": 0.99, "original_categories": ["fixture"],
                    "is_advertisement": False, "ad_type": "news_article",
                    "ad_confidence": 0.0, "ad_reasoning": "Synthetic fixture content.",
                    "publication_date": None, "content_quality": 0.99, "confidence": 0.99,
                }
            self._json(200, {"candidates": [{"content": {"parts": [{"text": json.dumps(result)}]}}]})
            return
        if path.startswith("/telegram/") and path.endswith(("/sendMessage", "/sendRichMessage")):
            with LOCK:
                COUNTERS["telegram"] += 1
                message_id = COUNTERS["telegram"]
            self._json(200, {"ok": True, "result": {"message_id": message_id, "date": 1}})
            return
        self._json(404, {"error": "not_found"})


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
