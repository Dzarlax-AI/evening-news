package extractor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/mmcdole/gofeed"
)

func readRSSFixture(t *testing.T, name string) string {
	t.Helper()
	body, err := os.ReadFile("../../tests/fixtures/go-source-manager/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return string(body)
}

func TestParseRSSSourceMatchesPythonRSSArticleSemantics(t *testing.T) {
	now := time.Date(2026, 10, 23, 11, 12, 13, 0, time.UTC)
	got, err := ParseRSSSource(context.Background(), readRSSFixture(t, "rss-semantics.xml"), "https://publisher.example/feed.xml", "Fixture RSS", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 4 {
		t.Fatalf("got %d articles, want 4 (the entry without a link is skipped)", len(got))
	}
	first := got[0]
	if first.Title != "Новости: café" || first.URL != "https://publisher.example/story/1" || first.SourceType != "rss" || first.SourceName != "Fixture RSS" {
		t.Fatalf("unexpected first article identity: %+v", first)
	}
	if first.Content == nil || *first.Content != "Полный <em>текст</em> &amp; details" || first.Summary != nil {
		t.Fatalf("content and summary should remain separate: content=%v summary=%v", rssTestString(first.Content), rssTestString(first.Summary))
	}
	if first.ImageURL == nil || *first.ImageURL != "https://publisher.example/cover.jpg" {
		t.Fatalf("enclosure should win image priority, got %v", first.ImageURL)
	}
	if first.PublishedAt != "2002-10-02T11:00:00Z" {
		t.Fatalf("published date = %q", first.PublishedAt)
	}
	if first.RawData["guid"] != "urn:story:1" || first.RawData["author"] != "Иван Иванов" {
		t.Fatalf("raw metadata not preserved: %#v", first.RawData)
	}
	tags, ok := first.RawData["tags"].([]string)
	if !ok || strings.Join(tags, ",") != "World,Serbia" {
		t.Fatalf("tags = %#v", first.RawData["tags"])
	}
	second := got[1]
	if second.PublishedAt != "2021-04-05T06:07:08Z" || second.ImageURL == nil || *second.ImageURL != "https://publisher.example/content.webp" {
		t.Fatalf("updated date/media content fallback mismatch: %+v", second)
	}
	third := got[2]
	if third.Summary == nil || !strings.Contains(*third.Summary, "Summary") || third.Content != nil || third.ImageURL == nil || *third.ImageURL != "https://publisher.example/inline.png?size=large" {
		t.Fatalf("description and HTML image fallback mismatch: %+v", third)
	}
	if got[3].PublishedAt != "2017-09-07T10:00:00Z" {
		t.Fatalf("created date fallback = %q", got[3].PublishedAt)
	}
}

func TestParseRSSSourceMatchesPythonAtomArticleSemantics(t *testing.T) {
	now := time.Date(2026, 10, 23, 0, 0, 0, 0, time.UTC)
	got, err := ParseRSSSource(context.Background(), readRSSFixture(t, "atom-semantics.xml"), "https://publisher.example/atom.xml", "Fixture Atom", 0, now)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d articles, want 2", len(got))
	}
	first := got[0]
	if first.URL != "https://publisher.example/atom/1" || first.Title != "Атомная статья" {
		t.Fatalf("Atom alternate link/title mismatch: %+v", first)
	}
	if first.Content == nil || *first.Content != "Полный <strong>текст</strong>" || first.Summary != nil {
		t.Fatalf("Python Article content priority mismatch: content=%v summary=%v", rssTestString(first.Content), rssTestString(first.Summary))
	}
	if first.PublishedAt != "2022-02-02T00:00:00Z" || first.ImageURL == nil || *first.ImageURL != "https://publisher.example/atom-thumb.png" {
		t.Fatalf("Atom date/image mismatch: %+v", first)
	}
	if first.RawData["guid"] != "urn:atom:1" || first.RawData["author"] != "Редакция" {
		t.Fatalf("Atom raw metadata mismatch: %#v", first.RawData)
	}
	second := got[1]
	if second.Title != "No title" || second.RawData["guid"] != "urn:atom:2" || second.Summary == nil || *second.Summary != "Summary only" {
		t.Fatalf("missing-title/summary fallback mismatch: %+v", second)
	}
}

func TestParseRSSSourceGoldenPythonParity(t *testing.T) {
	baseline, err := os.ReadFile("../../tests/fixtures/go-source-manager/rss-parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var expected map[string]struct {
		Bozo     bool            `json:"bozo"`
		Articles json.RawMessage `json:"articles"`
	}
	if err := json.Unmarshal(baseline, &expected); err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 23, 11, 12, 13, 0, time.UTC)
	for filename, entry := range expected {
		t.Run(filename, func(t *testing.T) {
			if entry.Bozo {
				t.Fatalf("Python fixture baseline marks %s as malformed", filename)
			}
			articles, err := ParseRSSSource(context.Background(), readRSSFixture(t, filename), "https://publisher.example/feed.xml", "Fixture RSS", 0, now)
			if err != nil {
				t.Fatal(err)
			}
			actualJSON, err := json.Marshal(articles)
			if err != nil {
				t.Fatal(err)
			}
			var actualValue, expectedValue any
			if err := json.Unmarshal(actualJSON, &actualValue); err != nil {
				t.Fatal(err)
			}
			if err := json.Unmarshal(entry.Articles, &expectedValue); err != nil {
				t.Fatal(err)
			}
			if filename == "atom-base-sanitize.xml" {
				// Intentional Go security deviation from feedparser parity: image
				// URLs emitted into src attributes must be absolute HTTP(S).
				articles := expectedValue.([]any)
				articles[0].(map[string]any)["image_url"] = "https://publisher.example/images/thumb.jpg"
			}
			if !reflect.DeepEqual(actualValue, expectedValue) {
				t.Fatalf("Python Article baseline mismatch\nactual:   %s\nexpected: %s", actualJSON, entry.Articles)
			}
		})
	}
}

func TestParseRSSSourceSkipsUnsafeArticleURLsAndDropsUnsafeImages(t *testing.T) {
	tests := []struct {
		name      string
		feedURL   string
		wantURLs  []string
		wantImage []string
	}{
		{
			name:    "rss-unsafe-urls.xml",
			feedURL: "https://publisher.example/feeds/rss.xml",
			wantURLs: []string{
				"https://publisher.example/items/story-one.html",
				"https://publisher.example/items/story-two.html",
			},
			wantImage: []string{"", "https://publisher.example/images/safe.jpg"},
		},
		{
			name:    "atom-unsafe-urls.xml",
			feedURL: "https://publisher.example/feeds/atom.xml",
			wantURLs: []string{
				"https://publisher.example/%22%20onload=alert%281%29",
				"https://publisher.example/%3Csvg%20onload=x%3E",
				"https://publisher.example/%60onload=alert%281%29",
				"https://publisher.example/atom/stories/relative.html",
				"https://publisher.example/atom/stories/second.html",
			},
			wantImage: []string{"", "", "", "", "https://publisher.example/atom/images/atom-safe.jpg"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			articles, err := ParseRSSSource(context.Background(), readRSSFixture(t, test.name), test.feedURL, "Fixture", 0, time.Time{})
			if err != nil {
				t.Fatal(err)
			}
			if len(articles) != len(test.wantURLs) {
				t.Fatalf("got %d articles, want %d: %+v", len(articles), len(test.wantURLs), articles)
			}
			for i, article := range articles {
				if article.URL != test.wantURLs[i] {
					t.Errorf("article %d URL = %q, want %q", i, article.URL, test.wantURLs[i])
				}
				image := ""
				if article.ImageURL != nil {
					image = *article.ImageURL
				}
				if image != test.wantImage[i] {
					t.Errorf("article %d image = %q, want %q", i, image, test.wantImage[i])
				}
			}
		})
	}
}

func TestParseRSSSourceRejectsControlBearingAtomURLWithoutRewritingFeed(t *testing.T) {
	if _, err := ParseRSSSource(context.Background(), readRSSFixture(t, "atom-control-url.xml"), "https://publisher.example/feeds/atom.xml", "Fixture", 0, time.Time{}); !errors.Is(err, ErrInvalidRSSFeed) {
		t.Fatalf("control-bearing Atom URI should fail parsing without rewriting the XML, got %v", err)
	}
}

func TestParseRSSSourcePreservesGoFeedAlternateSelection(t *testing.T) {
	feed := `<feed xmlns="http://www.w3.org/2005/Atom"><title>Alternates</title><id>urn:feed:alternates</id><entry><title>Selected</title><id>urn:alternates:1</id><link rel="alternate" href="https://publisher.example/first"/><link rel="alternate" href="https://publisher.example/selected"/></entry></feed>`
	parsed, err := gofeed.NewParser().ParseString(feed)
	if err != nil || len(parsed.Items) != 1 {
		t.Fatalf("gofeed fixture parse: items=%v err=%v", parsed, err)
	}
	articles, err := ParseRSSSource(context.Background(), feed, "https://publisher.example/feed.xml", "Fixture", 0, time.Time{})
	if err != nil || len(articles) != 1 {
		t.Fatalf("source parse: articles=%v err=%v", articles, err)
	}
	want := safeRSSHTTPURL(parsed.Items[0].Link, "https://publisher.example/feed.xml")
	if articles[0].URL != want {
		t.Fatalf("selected alternate changed: got %q want gofeed-selected %q", articles[0].URL, want)
	}
}

func TestSafeRSSHTTPURLRejectsAttributeInjectionCharacters(t *testing.T) {
	for _, raw := range []string{
		"javascript:alert(1)",
		`https://publisher.example/" onerror=alert(1)`,
		"https://publisher.example/<svg>",
		"https://publisher.example/bad\npath",
		"https://publisher.example/with\u00a0space",
		"https://publisher.example/?q=bad\tvalue",
		"https://publisher.example/`onload=alert(1)",
		"https://user:pass@publisher.example/private",
		"https://publisher.example:65536/too-high",
		"https://publisher.example:abc/not-numeric",
		"https://publisher.example:/empty-port",
	} {
		if got := safeRSSHTTPURL(raw, "https://publisher.example/feed.xml"); got != "" {
			t.Errorf("safeRSSHTTPURL(%q) = %q, want rejection", raw, got)
		}
	}
	if got := safeRSSHTTPURL("../stories/item.html", "https://publisher.example/feeds/rss.xml"); got != "https://publisher.example/stories/item.html" {
		t.Fatalf("relative article URL = %q", got)
	}
	if got := safeRSSHTTPURL("//publisher.example/story", "https://publisher.example/feeds/rss.xml"); got != "https://publisher.example/story" {
		t.Fatalf("protocol-relative URL with valid base = %q", got)
	}
}

func TestParseRSSSourceLimitsAndEmptyFeed(t *testing.T) {
	rss := readRSSFixture(t, "rss-semantics.xml")
	limited, err := ParseRSSSource(context.Background(), rss, "https://publisher.example/feed.xml", "Fixture", 1, time.Time{})
	if err != nil || len(limited) != 1 {
		t.Fatalf("explicit limit: got %d articles, err=%v", len(limited), err)
	}
	empty, err := ParseRSSSource(context.Background(), `<rss version="2.0"><channel><title>empty</title></channel></rss>`, "https://publisher.example/empty.xml", "Empty", 0, time.Time{})
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty feed should succeed: %#v, %v", empty, err)
	}
	if empty == nil {
		t.Fatal("empty feed should return an initialized empty slice")
	}
	articleJSON, err := json.Marshal(RSSArticle{})
	if err != nil || !strings.Contains(string(articleJSON), `"summary":null`) || !strings.Contains(string(articleJSON), `"image_url":null`) {
		t.Fatalf("nullable fields should encode as JSON null: %s, %v", articleJSON, err)
	}
}

func rssTestString(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}

func TestParseRSSSourceRejectsMalformedAndNonXMLFeed(t *testing.T) {
	for _, input := range []string{
		`<rss version="2.0"><channel><item><title>broken</channel></rss>`,
		`<rss version="2.0"><channel></channel></rss><rss version="2.0"><channel></channel></rss>`,
		`{"version":"https://jsonfeed.org/version/1.1","items":[]}`,
		`<!DOCTYPE rss [<!ENTITY external SYSTEM "file:///etc/passwd">]><rss version="2.0"><channel><item><title>&external;</title></item></channel></rss>`,
	} {
		if _, err := ParseRSSSource(context.Background(), input, "https://publisher.example/feed", "Fixture", 0, time.Now()); !errors.Is(err, ErrInvalidRSSFeed) {
			t.Errorf("input %q: error = %v, want invalid feed", input, err)
		}
	}
}

func TestParseRSSSourceRejectsUnboundedAndCanceledRequests(t *testing.T) {
	if _, err := ParseRSSSource(context.Background(), `<rss version="2.0"><channel></channel></rss>`, "https://publisher.example/feed", "Fixture", MaxRSSItems+1, time.Now()); !errors.Is(err, ErrInvalidRSSInput) {
		t.Fatalf("oversized explicit limit error = %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ParseRSSSource(ctx, `<rss version="2.0"><channel></channel></rss>`, "https://publisher.example/feed", "Fixture", 0, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled context error = %v", err)
	}
	if _, err := ParseRSSSource(context.Background(), strings.Repeat(" ", 5*1024*1024+1), "https://publisher.example/feed", "Fixture", 0, time.Now()); !errors.Is(err, ErrRSSResourceLimit) {
		t.Fatalf("oversized XML body error = %v", err)
	}
	var entries strings.Builder
	entries.WriteString(`<rss version="2.0"><channel>`)
	for i := 0; i < MaxRSSItems+1; i++ {
		entries.WriteString(`<item><title>x</title><link>https://publisher.example/`)
		entries.WriteString(time.Unix(int64(i), 0).Format("150405"))
		entries.WriteString(`</link></item>`)
	}
	entries.WriteString(`</channel></rss>`)
	if _, err := ParseRSSSource(context.Background(), entries.String(), "https://publisher.example/feed", "Fixture", 0, time.Now()); !errors.Is(err, ErrRSSResourceLimit) {
		t.Fatalf("unbounded feed error = %v", err)
	}
}
