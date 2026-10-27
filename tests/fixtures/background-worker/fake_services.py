"""Internal-network fake endpoints for source collection, extraction, AI, and Telegram."""

import hashlib
import json
import threading
import uuid
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit


LOCK = threading.Lock()
GATES = {"ai": threading.Event(), "telegram": threading.Event()}
GATES["ai"].set()
GATES["telegram"].set()
COUNTERS = {"source_probe": 0, "ingestion_stub_runs": 0, "extraction": 0,
            "gemini": 0, "editorial_gemini": 0, "telegram": 0}
INGESTION_RUNS = {}
INGESTION_REQUESTS = {}
TEXT = " ".join([
    "Synthetic fixture article reports a significant technology development in a regional research center.",
    "Engineers introduced a low power sensor that measures air quality in busy public spaces.",
    "The prototype combines a replaceable filter with a small processor and a rechargeable battery.",
    "Researchers compared its readings with laboratory equipment during a six week field trial.",
    "Their report describes stable measurements across several temperature and humidity conditions.",
    "The team plans to publish the full methods and anonymized results after an independent review.",
    "Local officials said the device could help identify streets that need additional monitoring.",
    "The project received funding from a public university research program and two civic grants.",
    "Investigators have not yet completed long term reliability tests or evaluated maintenance costs.",
    "A follow up study will compare the prototype with existing equipment at additional locations.",
    "This synthetic report contains no real people, companies, places, or external source material.",
    "It exists only inside the isolated fixture network and exercises the ordinary summarization path.",
])


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        return

    def _body(self):
        length = int(self.headers.get("Content-Length", "0"))
        if length > 9_000_000:
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
                counters = dict(COUNTERS)
            self._json(200, counters)
            return
        prefix = "/internal/v1/ingestion/runs/"
        if self.path.startswith(prefix):
            if self.headers.get("Authorization") != "Bearer fixture-only-article-ingestion-token":
                self._json(401, {"error": "unauthorized"})
                return
            run_id = self.path[len(prefix):]
            with LOCK:
                run = INGESTION_RUNS.get(run_id)
            if run is None:
                self._json(404, {"error": "not_found"})
            else:
                self._json(200, run)
            return
        self._json(404, {"error": "not_found"})

    def do_POST(self):
        path = urlsplit(self.path).path
        try:
            payload = json.loads(self._body() or b"{}")
        except (ValueError, UnicodeDecodeError):
            self._json(400, {"error": "invalid_json"})
            return

        if self.path == "/__gate/control":
            for name in ("ai", "telegram"):
                if name in payload:
                    (GATES[name].clear if payload[name] == "block" else GATES[name].set)()
            self._json(200, {"status": "updated"})
            return

        if path == "/internal/v1/ingestion/runs":
            if self.headers.get("Authorization") != "Bearer fixture-only-article-ingestion-token":
                self._json(401, {"error": "unauthorized"})
                return
            if (payload.get("version") != 1 or not isinstance(payload.get("job_id"), str)
                    or type(payload.get("attempt_generation")) is not int
                    or not isinstance(payload.get("owner_id"), str)
                    or type(payload.get("generation")) is not int
                    or type(payload.get("backend_pid")) is not int
                    or not isinstance(payload.get("executor_instance_id"), str)
                    or not isinstance(payload.get("deadline_at"), str)):
                self._json(422, {"error": "invalid_request"})
                return
            raw = bytearray(hashlib.sha256(
                f"evening-news-ingestion-v1\0{payload['job_id']}\0{payload['attempt_generation']}".encode("ascii")
            ).digest()[:16])
            raw[6] = (raw[6] & 0x0F) | 0x50
            raw[8] = (raw[8] & 0x3F) | 0x80
            run_id = str(uuid.UUID(bytes=bytes(raw)))
            request_hash = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
            run = {
                "run_id": run_id,
                "job_id": payload["job_id"],
                "attempt_generation": payload["attempt_generation"],
                "state": "succeeded",
                "result": {"status": "success", "sources_completed": 0,
                           "articles_fetched": 0, "errors_count": 0, "source_outcomes": {}},
            }
            with LOCK:
                previous = INGESTION_RUNS.get(run_id)
                if previous is not None and INGESTION_REQUESTS[run_id] != request_hash:
                    self._json(409, {"error": "idempotency_conflict"})
                    return
                if previous is None:
                    INGESTION_RUNS[run_id] = run
                    INGESTION_REQUESTS[run_id] = request_hash
                    COUNTERS["ingestion_stub_runs"] += 1
                else:
                    run = previous
            self._json(200, run)
            return

        if path.startswith("/internal/v1/ingestion/runs/") and path.endswith("/cancel"):
            if self.headers.get("Authorization") != "Bearer fixture-only-article-ingestion-token":
                self._json(401, {"error": "unauthorized"})
                return
            run_id = path[len("/internal/v1/ingestion/runs/"):-len("/cancel")].rstrip("/")
            with LOCK:
                run = INGESTION_RUNS.get(run_id)
            if run is None:
                self._json(404, {"error": "not_found"})
            else:
                self._json(200, run)
            return

        if self.path == "/v1/sources/collect":
            if self.headers.get("Authorization") != "Bearer fixture-only-token":
                self._json(401, {"error": "unauthorized"})
                return
            with LOCK:
                COUNTERS["source_probe"] += 1
            request_id = payload.get("request_id")
            results = []
            for source in payload.get("sources", []):
                source_id = source["source_id"]
                name = source["source_name"]
                results.append({
                    "source_id": source_id,
                    "type": "rss",
                    "outcome": "success",
                    "rss_articles": [{
                        "title": "Fixture technology article",
                        "url": "https://fixture.example.test/articles/one",
                        "content": TEXT,
                        "summary": None,
                        "image_url": None,
                        "published_at": datetime.now(timezone.utc).isoformat(),
                        "source_type": "rss",
                        "source_name": name,
                        "raw_data": {"guid": "fixture-article-one", "author": "Fixture", "tags": []},
                    }],
                })
            self._json(200, {
                "contract_version": "1",
                "build_version": "fixture-only",
                "request_id": request_id,
                "results": results,
            })
            return

        if self.path == "/v1/extract":
            if self.headers.get("Authorization") != "Bearer fixture-only-token":
                self._json(401, {"error": "unauthorized"})
                return
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
            GATES["ai"].wait(timeout=180)
            schema = payload.get("generationConfig", {}).get("responseSchema", {})
            if "article_stories" in schema.get("properties", {}):
                with LOCK:
                    COUNTERS["editorial_gemini"] += 1
                prompt = payload.get("contents", [{}])[0].get("parts", [{}])[0].get("text", "")
                marker = "СНИМОК (недоверенные данные):\n"
                try:
                    snapshot = json.loads(prompt.split(marker, 1)[1])
                    articles = snapshot["articles"]
                except (IndexError, KeyError, TypeError, ValueError):
                    self._json(400, {"error": "invalid_editorial_fixture_request"})
                    return
                main_stories = []
                article_stories = {}
                for index, article in enumerate(articles):
                    summary = "Синтетический материал сообщает проверенный факт."
                    article_stories[article["id"]] = {
                        "title": article["title"], "summary": summary,
                        "span_id": article["evidence_spans"][0]["id"],
                    }
                    if index < 5:
                        main_stories.append({
                            "title": article["title"],
                            "summary": summary,
                            "primary_article_id": article["id"],
                            "article_ids": [article["id"]],
                        })
                editorial_text = json.dumps({"main_stories": main_stories, "article_stories": article_stories}, ensure_ascii=False)
                self._json(200, {
                    "candidates": [{"finishReason": "STOP",
                                    "content": {"parts": [{"text": editorial_text}]}}],
                    "usageMetadata": {"promptTokenCount": 100, "candidatesTokenCount": 100,
                                       "totalTokenCount": 200},
                })
                return
            result = {
                "optimized_title": "Fixture technology article",
                "summary": "A synthetic technology report with verified fixture details.",
                "categories": ["Tech"],
                "category_confidences": [0.98],
                "category": "Tech",
                "category_confidence": 0.98,
                "summary_confidence": 0.98,
                "original_categories": ["technology"],
                "is_advertisement": False,
                "ad_type": "news_article",
                "ad_confidence": 0.0,
                "ad_reasoning": "Synthetic fixture is editorial content.",
                "publication_date": None,
                "content_quality": 0.95,
                "confidence": 0.98,
            }
            self._json(200, {"candidates": [{"content": {"parts": [{"text": json.dumps(result)}]}}]})
            return

        if path.startswith("/telegram/") and path.endswith(("/sendMessage", "/sendRichMessage")):
            with LOCK:
                COUNTERS["telegram"] += 1
                message_id = COUNTERS["telegram"]
            # Count before blocking to model acceptance by an external receiver.
            GATES["telegram"].wait(timeout=180)
            self._json(200, {"ok": True, "result": {"message_id": message_id, "date": 1}})
            return

        self._json(404, {"error": "not_found"})


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
