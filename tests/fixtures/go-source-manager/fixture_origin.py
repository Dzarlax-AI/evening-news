#!/usr/bin/env python3
"""Local-only synthetic RSS, Telegram, and Page Monitor origin."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit
import argparse
import ssl


def rss_document(edition: str) -> bytes:
    entries = []
    for number in (1, 2):
        title = (f"Новости: выпуск два — история {number}" if edition == "two"
                 else f"Fixture RSS {edition} story {number}")
        entries.append(
            f"""<item>
<title>{title}</title>
<link>http://rss.fixture.test/story/{edition}/{number}</link>
<guid>fixture-{edition}-{number}</guid>
<pubDate>Thu, 22 Oct 2026 10:0{number}:00 GMT</pubDate>
<description>Independent fixture report {number} for edition {edition}.</description>
<content:encoded><![CDATA[Fixture article body for edition {edition}, story {number}.]]></content:encoded>
</item>"""
        )
    encoding = "windows-1251" if edition == "two" else "UTF-8"
    document = (
        f"<?xml version='1.0' encoding='{encoding}'?>"
        "<rss version='2.0' xmlns:content='http://purl.org/rss/1.0/modules/content/'>"
        "<channel><title>Fixture feed</title><link>http://rss.fixture.test/</link>"
        "<description>Local acceptance fixture</description>"
        + "".join(entries)
        + "</channel></rss>"
    )
    return document.encode("cp1251" if edition == "two" else "utf-8")


PAGE_MONITOR_HTML = b"""<!doctype html><html><body>
<article><h2><a href="/story/pm-1">Fixture monitor story one</a></h2><time datetime="2026-10-22T11:00:00Z">22 Oct</time></article>
<article><h2><a href="/story/pm-2">Fixture monitor story two</a></h2><time datetime="2026-10-22T11:05:00Z">22 Oct</time></article>
</body></html>"""

TELEGRAM_HTML = b"""<!doctype html><html><body>
<div class="tgme_widget_message" data-post="fixture_channel/1">
  <div class="tgme_widget_message_text">A synthetic Telegram post with a long enough body to exercise collection metadata and avoid linked article enrichment. It contains only fixture text served inside the isolated test network.</div>
  <div class="tgme_widget_message_footer"><a class="tgme_widget_message_date" href="https://t.me/fixture_channel/1"><time datetime="2026-10-22T12:00:00Z">12:00</time></a></div>
</div>
</body></html>"""


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def do_GET(self):  # noqa: N802 - BaseHTTPRequestHandler API
        parsed = urlsplit(self.path)
        if parsed.path == "/healthz":
            self._reply(200, b"ok", "text/plain; charset=utf-8")
        elif parsed.path == "/rss.xml":
            query = parse_qs(parsed.query)
            edition = query.get("edition", ["one"])[0]
            if edition not in {"one", "two"}:
                self._reply(400, b"bad request", "text/plain; charset=utf-8")
                return
            content_type = "application/rss+xml" if edition == "two" else "application/rss+xml; charset=utf-8"
            self._reply(200, rss_document(edition), content_type)
        elif parsed.path == "/news":
            self._reply(200, PAGE_MONITOR_HTML, "text/html; charset=utf-8")
        elif parsed.path == "/s/fixture_channel":
            self._reply(200, TELEGRAM_HTML, "text/html; charset=utf-8")
        else:
            self._reply(404, b"not found", "text/plain; charset=utf-8")

    def _reply(self, status, body, content_type):
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Connection", "close")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        return


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cert", required=True)
    parser.add_argument("--key", required=True)
    args = parser.parse_args()

    httpd = ThreadingHTTPServer(("0.0.0.0", 80), Handler)
    httpsd = ThreadingHTTPServer(("0.0.0.0", 443), Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(args.cert, args.key)
    httpsd.socket = context.wrap_socket(httpsd.socket, server_side=True)

    from threading import Thread
    Thread(target=httpd.serve_forever, daemon=True).start()
    httpsd.serve_forever()


if __name__ == "__main__":
    main()
