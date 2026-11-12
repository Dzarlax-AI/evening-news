package sourceprobe

import (
	"context"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
)

func TestProbeAcceptsRSSExtractorImages(t *testing.T) {
	source := Source{ID: 1, Name: "RSS", Type: "rss", URL: "https://example.test/feed"}
	for _, body := range []string{"", `<description><![CDATA[<img src="/one.gif"><img src="/two.png">]]></description>`} {
		feed := `<rss version="2.0"><channel><item><title>Fixture</title><link>https://example.test/story</link>` + body + `</item></channel></rss>`
		articles, err := extractor.ParseRSSSource(context.Background(), feed, source.URL, source.Name, 0, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if outcome, _, ok := validateResponse(wrap(t, source, "rss_articles", articles), "req", source, nil); !ok || outcome != "success" {
			t.Fatal("source probe rejected actual RSS extractor output")
		}
	}
}

func TestProbeRejectsMalformedRSSImages(t *testing.T) {
	source := Source{ID: 1, Name: "RSS", Type: "rss", URL: "https://example.test/feed"}
	for _, media := range []any{nil, "bad", []any{"bad"}, []any{map[string]any{"type": "image", "source": "rss_feed", "url": "javascript:alert(1)"}}, []any{map[string]any{"type": "image", "source": "content_html", "url": "https://example.test/page.jpg"}}} {
		article := map[string]any{"title": "Fixture", "url": "https://example.test/story", "content": nil, "summary": nil, "image_url": nil, "published_at": "2026-11-12T12:00:00Z", "source_type": "rss", "source_name": "RSS", "raw_data": map[string]any{"guid": "1", "author": nil, "tags": []any{}}, "media_files": media}
		if _, _, ok := validateResponse(wrap(t, source, "rss_articles", []any{article}), "req", source, nil); ok {
			t.Fatal("malformed RSS media accepted")
		}
	}
}
