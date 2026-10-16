package extractor

import (
	"context"
	"strings"
	"testing"
)

func TestCustomDescriptionUsesNormalizedContainerTextAndUnicodeCap(t *testing.T) {
	body := strings.Repeat("界", 510)
	html := `<article><h2>Container parity heading</h2><time datetime="2026-10-14">Published October 14 2026</time><p>Alpha <strong>beta</strong> ` + body + `</p></article>`
	r := Extract(context.Background(), Input{
		URL:        "https://custom.example.test/story",
		HTML:       html,
		Strategy:   "custom_selectors",
		SourceType: "custom",
		Now:        "2026-10-15T12:00:00Z",
		Selectors: map[string][]string{
			"article": {"article"},
			"title":   {"h2"},
			"date":    {"time"},
			// Python page_monitor_source intentionally has no content-selector
			// setting: descriptions always come from the whole container.
			"content": {"p"},
		},
	})

	containerText := "Container parity heading Published October 14 2026 Alpha beta " + body
	wantContent := string([]rune(containerText)[:500]) + "..."
	if r.State != "extracted" {
		t.Fatalf("expected extracted result, got %+v", r)
	}
	if r.Title != "Container parity heading" {
		t.Errorf("title = %q, want heading", r.Title)
	}
	if r.PublicationDate != "2026-10-14T00:00:00" {
		t.Errorf("publication date = %q, want normalized date", r.PublicationDate)
	}
	if r.Content != wantContent {
		t.Errorf("content = %q, want full normalized container text capped to 500 Unicode characters plus ellipsis", r.Content)
	}
}
