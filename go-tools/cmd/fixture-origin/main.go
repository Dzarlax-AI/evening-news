// fixture-origin serves synthetic sources on an isolated test network.
package main

import (
	"flag"
	"fmt"
	"golang.org/x/text/encoding/charmap"
	"log"
	"net/http"
	"regexp"
	"strings"
	"sync/atomic"
)

const page = `<!doctype html><html><body><main><section class="zx-entry"><span class="zx-title">Learned Selector Gate Story</span><time class="zx-time" datetime="2026-10-22T11:00:00Z">22 Oct</time><p>A synthetic Page Monitor story with enough fixture text to exercise normal processing.</p></section></main></body></html>`
const telegram = `<!doctype html><html><body><div class="tgme_widget_message" data-post="fixture_channel/1"><div class="tgme_widget_message_text">A synthetic Telegram post with a long enough body to exercise collection metadata and avoid linked article enrichment. It contains only fixture text served inside the isolated test network.</div><div class="tgme_widget_message_footer"><a class="tgme_widget_message_date" href="https://t.me/fixture_channel/1"><time datetime="2026-10-22T12:00:00Z">12:00</time></a></div></div></body></html>`

var markerPattern = regexp.MustCompile(`^/active-sigterm/([0-9a-f]{12})$`)

type origin struct{ pageFetches atomic.Int64 }

func (o *origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var data []byte
	ctype := "text/html; charset=utf-8"
	switch r.URL.Path {
	case "/healthz":
		ctype = "text/plain; charset=utf-8"
		data = []byte("ok")
	case "/__gate/counters":
		ctype = "application/json"
		data = []byte(fmt.Sprintf(`{"page_fetches":%d}`, o.pageFetches.Load()))
	case "/news":
		o.pageFetches.Add(1)
		data = []byte(page)
	case "/s/fixture_channel":
		data = []byte(telegram)
	case "/rss.xml":
		edition := r.URL.Query().Get("edition")
		if edition == "" {
			edition = "one"
		}
		if edition != "one" && edition != "two" {
			http.Error(w, "bad request", 400)
			return
		}
		encoding := "UTF-8"
		if edition == "two" {
			encoding = "windows-1251"
		}
		var items strings.Builder
		for n := 1; n <= 2; n++ {
			title := fmt.Sprintf("Fixture RSS %s story %d", edition, n)
			if edition == "two" {
				title = fmt.Sprintf("Новости: выпуск два — история %d", n)
			}
			fmt.Fprintf(&items, `<item><title>%s</title><link>http://rss.fixture.test/story/%s/%d</link><guid>fixture-%s-%d</guid><pubDate>Thu, 22 Oct 2026 10:0%d:00 GMT</pubDate><description>Independent fixture report %d for edition %s.</description><content:encoded><![CDATA[Fixture article body for edition %s, story %d.]]></content:encoded></item>`, title, edition, n, edition, n, n, n, edition, edition, n)
		}
		doc := fmt.Sprintf(`<?xml version='1.0' encoding='%s'?><rss version='2.0' xmlns:content='http://purl.org/rss/1.0/modules/content/'><channel><title>Fixture feed</title><link>http://rss.fixture.test/</link><description>Local acceptance fixture</description>%s</channel></rss>`, encoding, items.String())
		data = []byte(doc)
		ctype = "application/rss+xml; charset=utf-8"
		if edition == "two" {
			var e error
			data, e = charmap.Windows1251.NewEncoder().Bytes(data)
			if e != nil {
				http.Error(w, "fixture error", 500)
				return
			}
			ctype = "application/rss+xml"
		}
	default:
		m := markerPattern.FindStringSubmatch(r.URL.Path)
		if m == nil {
			http.NotFound(w, r)
			return
		}
		title := "Synthetic active SIGTERM fixture " + m[1]
		paragraph := fmt.Sprintf("<p>This is synthetic fixture text served only inside the disposable acceptance network. It describes a fictional systems research result with no real people or organizations. The unique fixture marker is %s.</p>", m[1])
		data = []byte("<!doctype html><html><head><title>" + title + "</title><meta property=\"og:title\" content=\"" + title + "\"></head><body><main><article><h1>" + title + "</h1>" + strings.Repeat(paragraph, 8) + "</article></main></body></html>")
	}
	w.Header().Set("Content-Type", ctype)
	w.Header().Set("Connection", "close")
	_, _ = w.Write(data)
}
func main() {
	cert := flag.String("cert", "", "")
	key := flag.String("key", "", "")
	flag.Parse()
	if *cert == "" || *key == "" {
		log.Fatal("certificate and key required")
	}
	o := &origin{}
	go func() { log.Fatal(http.ListenAndServe(":80", o)) }()
	log.Fatal(http.ListenAndServeTLS(":443", *cert, *key, o))
}
