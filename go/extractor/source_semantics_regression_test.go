package extractor

import (
	"context"
	"testing"
)

func TestSourceMetadataRegression(t *testing.T) {
	tests := []struct {
		name                string
		input               Input
		title, author, date string
	}{
		{"rss raw author and trimmed title", Input{URL: "https://fixture.test/feed", Strategy: "rss", SourceType: "rss", RSSXML: `<rss version="2.0"><channel><title>Feed</title><item><title>Research report </title><link>https://fixture.test/report</link><author>hidden (researcher)</author><description>Useful report text.</description></item></channel></rss>`}, "Research report", "hidden (researcher)", ""},
		{"custom embedded date", Input{URL: "https://fixture.test/", Strategy: "custom_selectors", SourceType: "custom", HTML: `<article><h2>Research report title</h2><p>October 14, 2026 · 6 min read</p><p>Useful report text.</p></article>`, Selectors: map[string][]string{"article": {"article"}}}, "Research report title", "", "2026-10-14T00:00:00"},
		{"telegram duration before publication", Input{URL: "https://t.me/s/fixture", Strategy: "telegram_http", SourceType: "telegram", TelegramHTML: `<div class="tgme_widget_message" data-post="fixture/1"><time>0:08</time><p class="tgme_widget_message_text">Independent report from regional observations.</p><a class="tgme_widget_message_date" href="https://t.me/fixture/1"><time datetime="2026-10-15T10:31:47Z">10:31</time></a></div>`}, "", "", "2026-10-15T10:31:47Z"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			tc.input.Now = "2026-10-15T12:00:00Z"
			r := Extract(context.Background(), tc.input)
			if r.State != "extracted" {
				t.Fatalf("unexpected state: %+v", r)
			}
			if tc.title != "" && r.Title != tc.title {
				t.Errorf("title=%q want%q", r.Title, tc.title)
			}
			if tc.author != "" && r.Author != tc.author {
				t.Errorf("author=%q want%q", r.Author, tc.author)
			}
			if tc.date != "" && r.PublicationDate != tc.date {
				t.Errorf("date=%q want%q", r.PublicationDate, tc.date)
			}
		})
	}
}
