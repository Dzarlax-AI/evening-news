"""Private fake provider endpoints for the scheduler fixture network."""

import hashlib
import json
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit
from uuid import uuid5, NAMESPACE_URL


LOCK = threading.RLock()
COUNTERS = {"source_collection": 0, "extraction": 0, "gemini": 0, "telegram": 0}
CONSUMER = {"mode": "normal", "requests": 0, "by_occurrence": {}, "operations": {}}
GEMINI_RELEASE = threading.Event()
GEMINI_RELEASE.set()
HOLD = threading.Event()
HOLD.set()
TEXT = " ".join([
    "Synthetic fixture article reports a significant technology development in a regional research center.",
    "Engineers introduced a low power sensor that measures air quality in busy public spaces.",
    "The prototype combines a replaceable filter with a small processor and a rechargeable battery.",
    "Researchers compared its readings with laboratory equipment during a six week field trial.",
    "Their report describes stable measurements across several temperature and humidity conditions.",
    "This synthetic report contains no real people, companies, places, or external source material.",
])


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, _fmt, *_args):
        return

    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > 64 * 1024:
            raise ValueError("request_too_large")
        return self.rfile.read(length) if length else b""

    def _json(self, status, value):
        raw = json.dumps(value, separators=(",", ":")).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        try:
            self.wfile.write(raw)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        if self.path == "/__gate/counters":
            with LOCK:
                result = {"providers": dict(COUNTERS), "consumer": {
                    "requests": CONSUMER["requests"],
                    "occurrences": len(CONSUMER["by_occurrence"]),
                    "operations": dict(CONSUMER["operations"]),
                }}
            self._json(200, result)
        else:
            self._json(404, {"error": "not_found"})

    def do_POST(self):
        path = urlsplit(self.path).path
        try:
            payload = json.loads(self._body() or b"{}")
        except (ValueError, UnicodeDecodeError):
            self._json(400, {"error": "invalid_json"})
            return

        if path == "/__gate/control":
            mode = payload.get("consumer_mode")
            if mode in {"normal", "503", "malformed", "accept_then_hold"}:
                with LOCK:
                    CONSUMER["mode"] = mode
                (HOLD.clear if mode == "accept_then_hold" else HOLD.set)()
            gemini_mode = payload.get("gemini_mode")
            if gemini_mode in {"block", "release"}:
                (GEMINI_RELEASE.clear if gemini_mode == "block" else GEMINI_RELEASE.set)()
            self._json(200, {"status": "updated"})
            return

        if path == "/v1/sources/collect":
            if self.headers.get("Authorization") != "Bearer fixture-only-token":
                self._json(401, {"error": "unauthorized"})
                return
            with LOCK:
                COUNTERS["source_collection"] += 1
            results = []
            for source in payload.get("sources", []):
                results.append({
                    "source_id": source["source_id"],
                    "type": "rss",
                    "outcome": "success",
                    "rss_articles": [{
                        "title": "Fixture technology article",
                        "url": "https://fixture.invalid/articles/one",
                        "content": TEXT,
                        "summary": None,
                        "image_url": None,
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "source_type": "rss",
                        "source_name": source["source_name"],
                        "raw_data": {"guid": "go-scheduler-fixture-article", "author": "Fixture", "tags": []},
                    }],
                })
            self._json(200, {
                "contract_version": "1",
                "build_version": "fixture-only",
                "request_id": payload.get("request_id"),
                "results": results,
            })
            return

        if path == "/v1/extract":
            with LOCK:
                COUNTERS["extraction"] += 1
            self._json(200, {
                "contract_version": "1",
                "request_id": payload.get("request_id"),
                "original_url": payload.get("url"),
                "outcome": "success",
                "result": {"content": TEXT, "title": "Fixture technology article", "media": []},
                "error": None,
                "observations": [],
            })
            return

        if path.endswith(":generateContent") and "/models/" in path:
            with LOCK:
                COUNTERS["gemini"] += 1
            GEMINI_RELEASE.wait(timeout=180)
            result = {
                "optimized_title": "Fixture technology article",
                "summary": "A synthetic technology report with verified fixture details.",
                "categories": ["Tech"], "category_confidences": [0.98],
                "category": "Tech", "category_confidence": 0.98,
                "summary_confidence": 0.98, "original_categories": ["technology"],
                "is_advertisement": False, "ad_type": "news_article",
                "ad_confidence": 0.0, "ad_reasoning": "Synthetic fixture is editorial content.",
                "publication_date": None, "content_quality": 0.95, "confidence": 0.98,
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


class ConsumerHandler(Handler):
    """Second generic consumer with duplicate receipts and controllable failures."""

    def do_POST(self):
        if self.path != "/commands":
            self._json(404, {"error": "not_found"})
            return
        with LOCK:
            CONSUMER["requests"] += 1
            mode = CONSUMER["mode"]
        if self.headers.get("Authorization") != "Bearer fixture-generic-token":
            self._json(401, {"error": "unauthorized"})
            return
        try:
            raw = self._body()
            envelope = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            self._json(400, {"error": "invalid_json"})
            return
        occurrence = envelope.get("occurrence_id")
        if not occurrence or self.headers.get("Idempotency-Key") != occurrence:
            self._json(422, {"error": "invalid_command"})
            return
        if mode == "503":
            self._json(503, {"error": "fixture_unavailable"})
            return

        digest = hashlib.sha256(raw).hexdigest()
        with LOCK:
            prior = CONSUMER["by_occurrence"].get(occurrence)
            if prior is not None and prior["digest"] != digest:
                self._json(409, {"error": "idempotency_conflict"})
                return
            if prior is None:
                prior = {
                    "digest": digest,
                    "receipt": {
                        "version": 1,
                        "occurrence_id": occurrence,
                        "job_id": str(uuid5(NAMESPACE_URL, "fixture:" + occurrence)),
                        "status": "accepted",
                    },
                }
                CONSUMER["by_occurrence"][occurrence] = prior
                operation = envelope.get("operation", "unknown")
                CONSUMER["operations"][operation] = CONSUMER["operations"].get(operation, 0) + 1
            receipt = prior["receipt"]

        if mode == "accept_then_hold":
            HOLD.wait(timeout=180)
        if mode == "malformed":
            self._json(202, {"status": "accepted", "occurrence_id": occurrence})
        else:
            self._json(202, receipt)


def serve(server_class, port):
    server_class(("0.0.0.0", port), Handler if port == 8090 else ConsumerHandler).serve_forever()


if __name__ == "__main__":
    threading.Thread(target=serve, args=(ThreadingHTTPServer, 8090), daemon=True).start()
    serve(ThreadingHTTPServer, 8091)
