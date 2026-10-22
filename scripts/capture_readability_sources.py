"""Capture public configured source pages for offline Readability review.

No production imports, database writes, authentication, or browser automation.
Raw publisher content stays in ignored artifacts rather than committed fixtures.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import hashlib
import gzip
import io
import json
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen
from urllib.error import HTTPError

import feedparser
from bs4 import BeautifulSoup


LIMIT = 5 * 1024 * 1024


def capture(url):
    record = {"requested_url": url, "captured_at": datetime.now(timezone.utc).isoformat()}
    try:
        request = Request(url, headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36", "Accept-Encoding": "identity"})
        with urlopen(request, timeout=20) as response:
            record.update(status=response.status, url=response.url,
                          content_type=response.headers.get("Content-Type", ""))
            body = response.read(LIMIT + 1)
            if len(body) > LIMIT:
                raise ValueError("wire capture exceeds 5 MiB")
            if response.headers.get("Content-Encoding", "").lower() == "gzip":
                with gzip.GzipFile(fileobj=io.BytesIO(body)) as stream:
                    body = stream.read(LIMIT + 1)
            elif response.headers.get("Content-Encoding", "identity").lower() not in {"", "identity"}:
                raise ValueError("unsupported capture encoding")
            if len(body) > LIMIT:
                raise ValueError("decoded capture exceeds 5 MiB")
            record.update(bytes=len(body), sha256=hashlib.sha256(body).hexdigest(),
                          html=bytes(body).decode(response.headers.get_content_charset() or "utf-8", errors="replace"))
    except HTTPError as exc:
        record.update(status=exc.code, error=str(exc))
    except Exception as exc:
        record["error"] = str(exc)
    return record


def candidates(source, document):
    url = document.get("url", source["url"])
    html = document.get("html", "")
    if source["source_type"] == "rss":
        return [(entry.link, entry.get("title", "")) for entry in feedparser.parse(html.encode("utf-8")).entries[:2] if entry.get("link")]
    soup = BeautifulSoup(html, "html.parser")
    if source["source_type"] == "telegram":
        items = soup.select(".tgme_widget_message_text a[href]")
        return [(a["href"], a.get_text(" ", strip=True)) for a in items
                if urlparse(a["href"]).scheme in {"http", "https"}
                and urlparse(a["href"]).hostname not in {"t.me", "telegram.me"}][:2]
    links = []
    preferred = soup.select("article a[href]") or soup.select("h2 a[href], h3 a[href]")
    if not preferred:
        preferred = [a for a in soup.select("a[href]") if urlparse(a["href"]).hostname in {"github.com", "huggingface.co"} and a.get("target") == "_blank"]
    for a in preferred:
        target = urljoin(url, a["href"])
        parsed = urlparse(target)
        path = parsed.path.rstrip("/")
        if (parsed.scheme not in {"http", "https"} or parsed.hostname not in {urlparse(url).hostname, "github.com", "huggingface.co"}
                or not path or path in {"/about", "/privacy", "/terms", "/subscribe", "/archive", "/login", "/signin"}
                or any(part in path for part in ("/tag/", "/category/", "/author/", "/cdn-cgi/", "/page/"))
                or parsed.fragment or not a.get_text(" ", strip=True)):
            continue
        if target not in {item[0] for item in links}:
            links.append((target, a.get_text(" ", strip=True)))
    return links[:2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sources", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    sources = json.loads(Path(args.sources).read_text(encoding="utf-8-sig"))
    output = Path(args.output)
    output.mkdir(parents=True, exist_ok=True)
    manifest = {"version": 1, "cases": []}
    records = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        urls = [source["url"].replace("https://t.me/", "https://t.me/s/") if source["source_type"] == "telegram" else source["url"] for source in sources]
        documents = list(pool.map(capture, urls))
        tasks = []
        for source, document in zip(sources, documents):
            document.update(source_id=source["id"], source_name=source["name"], role="source_index")
            records.append(document)
            for index, (url, title) in enumerate(candidates(source, document)):
                tasks.append((source, index, title, url))
        for (source, index, title, url), document in zip(tasks, pool.map(capture, [t[3] for t in tasks])):
            document.update(source_id=source["id"], source_name=source["name"], role="article", linked_title=title)
            records.append(document)
            if document.get("error") or "html" not in document.get("content_type", "").lower():
                continue
            for strategy in ("readability", "auto"):
                manifest["cases"].append({
                    "id": f"live.s{source['id']}.article{index + 1}.{strategy}",
                    "source_type": "article", "source_name": source["name"], "methods": ["E09", "E15", "M01", "M02", "M03", "M04"],
                    "provenance": {"kind": "public_http_capture", "derived_from": [url]},
                    "input": {"url": document["url"], "html": document["html"], "strategy": strategy},
                    "expected": {"state": "extracted"}})
    (output / "captures.json").write_text(json.dumps(records, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    (output / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"sources": len(sources), "captures": len(records), "article_captures": sum(r["role"] == "article" and not r.get("error") for r in records),
                      "cases": len(manifest["cases"]), "errors": [{k: r.get(k) for k in ("source_id", "requested_url", "status", "error")} for r in records if r.get("error")]}, ensure_ascii=False))


if __name__ == "__main__":
    main()
