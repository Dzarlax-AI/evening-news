package extractor

import (
	"strings"
	"testing"

	"github.com/PuerkitoBio/goquery"
)

func TestMetadataPublicationDateRegression(t *testing.T) {
	tests := []struct {
		name string
		html string
		want string
	}{
		{
			name: "ISO datetime without seconds and with UTC timezone",
			html: `<time datetime="2026-04-30T16:00Z">Published</time>`,
			want: "2026-04-30T16:00:00Z",
		},
		{
			name: "ISO datetime with seconds and offset timezone",
			html: `<time datetime="2026-04-30T16:00:12+02:00">Published</time>`,
			want: "2026-04-30T14:00:12Z",
		},
		{
			name: "ISO datetime without timezone",
			html: `<time datetime="2026-04-30T16:00">Published</time>`,
			want: "2026-04-30T16:00:00",
		},
		{
			name: "displayed abbreviated month with comma",
			html: `<time>30 Apr, 2026</time>`,
			want: "2026-04-30T00:00:00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc, err := goquery.NewDocumentFromReader(strings.NewReader(`<html><body>` + tt.html + `</body></html>`))
			if err != nil {
				t.Fatal(err)
			}
			result := &Result{}
			metadata(doc, result)
			if result.PublicationDate != tt.want {
				t.Fatalf("PublicationDate = %q, want %q", result.PublicationDate, tt.want)
			}
		})
	}
}

func TestMalformedPublicationDateDoesNotInventDate(t *testing.T) {
	for _, html := range []string{
		`<time datetime="not-a-date">sometime soon</time>`,
		`<time datetime="2026-99-99T16:00Z">Published</time>`,
		`<p>Archive date: 2026-99-99T16:00Z</p>`,
	} {
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(`<html><body>` + html + `</body></html>`))
		if err != nil {
			t.Fatal(err)
		}
		result := &Result{}
		metadata(doc, result)
		if result.PublicationDate != "" {
			t.Errorf("html %q: PublicationDate = %q, want empty for malformed date", html, result.PublicationDate)
		}
	}
}

func TestMetadataIgnoresIncidentalScriptTimestamp(t *testing.T) {
	doc := parse(`<html><head><title>Repository README</title></head><body><script type="application/json">{"repo":{"createdAt":"2026-10-11T19:10:25Z"}}</script><article><p>Repository usage instructions.</p></article></body></html>`)
	result := Result{}
	metadata(doc, &result)
	if result.PublicationDate != "" {
		t.Fatalf("repository creation timestamp reported as publication: %q", result.PublicationDate)
	}
}
