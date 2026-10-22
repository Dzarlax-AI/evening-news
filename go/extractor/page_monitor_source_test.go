package extractor

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParsePageMonitorSharedPythonOracle(t *testing.T) {
	type oracleCase struct {
		Name             string           `json:"name"`
		URL              string           `json:"url"`
		SourceName       string           `json:"source_name"`
		Now              string           `json:"now"`
		HTML             string           `json:"html"`
		Limit            int              `json:"limit"`
		MinTitleLength   int              `json:"min_title_length"`
		Article          []string         `json:"article_selectors"`
		Title            []string         `json:"title_selectors"`
		Date             []string         `json:"date_selectors"`
		Link             []string         `json:"link_selectors"`
		LearnedContainer []string         `json:"learned_container_selectors"`
		LearnedTitle     []string         `json:"learned_title_selectors"`
		LearnedLink      []string         `json:"learned_link_selectors"`
		LearnedDate      []string         `json:"learned_date_selectors"`
		General          [][]any          `json:"learned_general_selectors"`
		Expected         []map[string]any `json:"expected"`
	}
	var corpus struct {
		Cases []oracleCase `json:"cases"`
	}
	path := filepath.Join("..", "..", "tests", "fixtures", "page-monitor", "list-parity.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, tc := range corpus.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			now, err := time.Parse("2006-01-02T15:04:05", tc.Now)
			if err != nil {
				t.Fatal(err)
			}
			general := make([]PageMonitorGeneralSelector, 0, len(tc.General))
			for _, pair := range tc.General {
				general = append(general, PageMonitorGeneralSelector{Selector: pair[0].(string), Confidence: pair[1].(float64)})
			}
			got, _, err := ParsePageMonitorList(PageMonitorOptions{
				URL: tc.URL, SourceName: tc.SourceName, HTML: tc.HTML, Now: now.UTC(), Limit: tc.Limit, MinTitleLength: tc.MinTitleLength,
				Selectors:                 PageMonitorSelectors{Article: tc.Article, Title: tc.Title, Date: tc.Date, Link: tc.Link},
				LearnedContainerSelectors: tc.LearnedContainer, LearnedTitleSelectors: tc.LearnedTitle,
				LearnedLinkSelectors: tc.LearnedLink, LearnedDateSelectors: tc.LearnedDate, LearnedGeneral: general,
			})
			if err != nil {
				t.Fatal(err)
			}
			actual := make([]map[string]any, 0, len(got))
			for _, item := range got {
				published := item.Published.UTC().Format("2006-01-02T15:04:05")
				if item.Published.Nanosecond() != 0 {
					published = item.Published.UTC().Format("2006-01-02T15:04:05.000000")
				}
				actual = append(actual, map[string]any{
					"title": item.Title, "link": item.Link, "description": item.Description,
					"published":    published,
					"content_type": item.ContentType, "id": item.ID, "source": item.Source, "source_url": item.SourceURL,
				})
			}
			if !reflect.DeepEqual(actual, tc.Expected) {
				gotJSON, _ := json.MarshalIndent(actual, "", "  ")
				wantJSON, _ := json.MarshalIndent(tc.Expected, "", "  ")
				t.Fatalf("Go parser differs from Python oracle\n got: %s\nwant: %s", gotJSON, wantJSON)
			}
		})
	}
}

func TestParsePageMonitorListPriorityDedupeLinksAndDates(t *testing.T) {
	now := time.Date(2026, 10, 22, 8, 30, 0, 0, time.UTC)
	html := `<main><article class="configured"><h2>Release 2.0</h2><a href="/release/2">Release</a><time datetime="2026-10-20">Oct 20</time><p>Added feature and improved performance.</p></article><article class="learned"><h2>News: Launch today</h2><time>2 days ago</time><p>We launched an important update today.</p></article><a class="outer" href="/nested"><article class="nested"><h2>Nested article title</h2><p>Published yesterday</p></article></a></main>`
	got, used, err := ParsePageMonitorList(PageMonitorOptions{
		URL: "https://example.com/updates", HTML: html, Now: now, Limit: 20, MinTitleLength: 10,
		Selectors:                 PageMonitorSelectors{Article: []string{".configured", ".learned"}, Title: []string{"h2"}, Date: []string{"time"}},
		LearnedContainerSelectors: []string{".learned"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("items=%d: %#v", len(got), got)
	}
	if got[0].Title != "News: Launch today" || got[0].Link != "https://example.com/updates" {
		t.Fatalf("first item=%#v", got[0])
	}
	if !got[0].Published.Equal(now.Add(-48*time.Hour)) || got[0].ContentType != "news" {
		t.Fatalf("date/type=%#v", got[0])
	}
	if len(used) == 0 || used[0] != ".learned" {
		t.Fatalf("selectors used=%v", used)
	}
}

func TestParsePageMonitorConfiguredArticlesAndAncestorLink(t *testing.T) {
	got, _, err := ParsePageMonitorList(PageMonitorOptions{URL: "https://example.com/original", HTML: `<a href="/nested"><article><h2>Nested article title</h2><p>Published yesterday</p></article></a>`, Now: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC), Selectors: PageMonitorSelectors{Article: []string{"article"}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Link != "https://example.com/nested" {
		t.Fatalf("items=%#v", got)
	}
}

func TestParsePageMonitorListUsesPythonIDAndOriginalURL(t *testing.T) {
	got, _, err := ParsePageMonitorList(PageMonitorOptions{URL: "https://example.com/original", HTML: `<article><a href="/post"><h2>A sufficiently long title</h2></a></article>`, SourceName: "Source", Now: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC), Selectors: PageMonitorSelectors{Article: []string{"article"}}})
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Link != "https://example.com/post" || got[0].Source != "Source" || got[0].SourceURL != "https://example.com/original" {
		t.Fatalf("item=%#v", got[0])
	}
	if got[0].ID != pageMonitorMD5(got[0].Title+got[0].Link) {
		t.Fatalf("id=%q", got[0].ID)
	}
}

func TestParsePageMonitorListRejectsBadSelectorsAndBoundsDescription(t *testing.T) {
	_, _, err := ParsePageMonitorList(PageMonitorOptions{URL: "https://example.com", HTML: "<article/>", Selectors: PageMonitorSelectors{Article: []string{"article["}}})
	if !errors.Is(err, ErrPageMonitorSelector) {
		t.Fatalf("error=%v", err)
	}
	text := strings.Repeat("я", 510)
	got, _, err := ParsePageMonitorList(PageMonitorOptions{URL: "https://example.com", HTML: "<article><h2>Long enough title</h2><p>" + text + "</p></article>", Selectors: PageMonitorSelectors{Article: []string{"article"}}})
	if err != nil {
		t.Fatal(err)
	}
	if n := len([]rune(got[0].Description)); n != 503 {
		t.Fatalf("description runes=%d", n)
	}
}

func TestParsePageMonitorListBoundsRetainedOutputAndReturnsPrefix(t *testing.T) {
	var html strings.Builder
	for i := 0; i < 100; i++ {
		html.WriteString(`<article class="item"><div class="title">`)
		html.WriteString(strings.Repeat("x", i+1))
		html.WriteString(strings.Repeat("q", 1800))
	}
	for i := 0; i < 100; i++ {
		html.WriteString(`</div></article>`)
	}
	if html.Len() >= 5*1024*1024 {
		t.Fatalf("synthetic HTML unexpectedly large: %d bytes", html.Len())
	}

	items, _, err := ParsePageMonitorList(PageMonitorOptions{
		URL: "https://example.com/updates", HTML: html.String(), Limit: 100, MinTitleLength: 10,
		Selectors: PageMonitorSelectors{Article: []string{"article.item"}, Title: []string{"div.title"}},
	})
	if !errors.Is(err, ErrOutputLimit) {
		t.Fatalf("expected retained-output limit, got %v", err)
	}
	if len(items) == 0 || len(items) >= 100 {
		t.Fatalf("expected a bounded successful prefix, got %d items", len(items))
	}
	retained := 0
	for _, item := range items {
		retained += pageMonitorRetainedItemBytes(item)
	}
	if retained > pageMonitorRetainedOutputLimit {
		t.Fatalf("retained prefix exceeds budget: %d bytes", retained)
	}
}

func TestPageMonitorDateFormatsWindowsAndRelative(t *testing.T) {
	now := time.Date(2026, 10, 22, 8, 30, 0, 0, time.UTC)
	for _, tc := range []struct {
		in   string
		want time.Time
		ok   bool
	}{{"2026-10-20", time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC), true}, {"October 2026", time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC), true}, {"2 days ago", now.Add(-48 * time.Hour), true}, {"yesterday", now.Add(-24 * time.Hour), true}, {"2020-01-01", time.Time{}, false}, {"2027-10-22", time.Time{}, false}} {
		got, ok := pageMonitorDate(tc.in, now)
		if ok != tc.ok || ok && !got.Equal(tc.want) {
			t.Errorf("%q => %v,%v", tc.in, got, ok)
		}
	}
}

func BenchmarkParsePageMonitorList(b *testing.B) {
	type benchmarkCase struct {
		URL              string   `json:"url"`
		SourceName       string   `json:"source_name"`
		Now              string   `json:"now"`
		HTML             string   `json:"html"`
		Limit            int      `json:"limit"`
		MinTitleLength   int      `json:"min_title_length"`
		Article          []string `json:"article_selectors"`
		Title            []string `json:"title_selectors"`
		Date             []string `json:"date_selectors"`
		Link             []string `json:"link_selectors"`
		LearnedContainer []string `json:"learned_container_selectors"`
		LearnedTitle     []string `json:"learned_title_selectors"`
		LearnedLink      []string `json:"learned_link_selectors"`
		LearnedDate      []string `json:"learned_date_selectors"`
	}
	var corpus struct {
		Cases []benchmarkCase `json:"cases"`
	}
	bts, err := os.ReadFile(filepath.Join("..", "..", "tests", "fixtures", "page-monitor", "list-parity.json"))
	if err != nil {
		b.Fatal(err)
	}
	if err := json.Unmarshal(bts, &corpus); err != nil {
		b.Fatal(err)
	}
	tc := corpus.Cases[0]
	now, err := time.Parse("2006-01-02T15:04:05", tc.Now)
	if err != nil {
		b.Fatal(err)
	}
	in := PageMonitorOptions{
		URL: tc.URL, SourceName: tc.SourceName, HTML: tc.HTML, Now: now.UTC(), Limit: tc.Limit, MinTitleLength: tc.MinTitleLength,
		Selectors:                 PageMonitorSelectors{Article: tc.Article, Title: tc.Title, Date: tc.Date, Link: tc.Link},
		LearnedContainerSelectors: tc.LearnedContainer, LearnedTitleSelectors: tc.LearnedTitle,
		LearnedLinkSelectors: tc.LearnedLink, LearnedDateSelectors: tc.LearnedDate,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := ParsePageMonitorList(in); err != nil {
			b.Fatal(err)
		}
	}
}
