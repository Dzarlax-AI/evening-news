package selectors

import (
	"context"
	"strings"
	"testing"
	"time"
)

type generator struct {
	calls  int
	prompt string
}

func (g *generator) GenerateText(_ context.Context, prompt, domain string) (string, error) {
	g.calls++
	g.prompt = prompt
	return `prefix {"content_selectors":["article"],"title_selectors":["h2"],"date_selectors":[],"page_type":"news"} suffix`, nil
}
func TestAnalyzerCacheIdentityExpiryAndCopies(t *testing.T) {
	g := &generator{}
	a := NewAnalyzer(g)
	now := time.Unix(100, 0)
	a.now = func() time.Time { return now }
	ctx := context.Background()
	first, e := a.Analyze(ctx, "https://example.test/news", `<script>bad</script><p>first</p>`, "news", "example.test")
	if e != nil || g.calls != 1 || strings.Contains(g.prompt, "bad") {
		t.Fatalf("first %v %d %v", first, g.calls, e)
	}
	first.ContentSelectors[0] = "modified"
	cached, e := a.Analyze(ctx, "https://example.test/news", `<p>changed HTML</p>`, "news", "example.test")
	if e != nil || g.calls != 1 || cached.ContentSelectors[0] != "article" {
		t.Fatalf("cached %v %d %v", cached, g.calls, e)
	}
	now = now.Add(24 * time.Hour)
	if _, e = a.Analyze(ctx, "https://example.test/news", `<p>new</p>`, "news", "example.test"); e != nil || g.calls != 2 {
		t.Fatalf("expiry %d %v", g.calls, e)
	}
}
func TestAnalyzerBoundsAndParse(t *testing.T) {
	g := &generator{}
	a := NewAnalyzer(g)
	ctx := context.Background()
	for i := 0; i < 130; i++ {
		if _, e := a.Analyze(ctx, "https://example.test/"+string(rune(i+1000)), `<p>x</p>`, "news", "example.test"); e != nil {
			t.Fatal(e)
		}
	}
	if len(a.entries) != 128 {
		t.Fatalf("entries=%d", len(a.entries))
	}
	if _, e := parseAnalysis(`{"content_selectors":[],"page_type":"news"}`); e == nil {
		t.Fatal("missing required title accepted")
	}
	if cacheKey(strings.Repeat("я", 2050), "news") != "" {
		t.Fatal("oversized utf8 key accepted")
	}
}

func TestUnvalidatedAnalysisDoesNotPopulateCache(t *testing.T) {
	g := &generator{}
	a := NewAnalyzer(g)
	ctx := context.Background()
	url := "https://example.test/news"
	first, err := a.analyze(ctx, url, "<p>first</p>", "news", "example.test", false)
	if err != nil || g.calls != 1 {
		t.Fatal(err)
	}
	if _, err = a.analyze(ctx, url, "<p>second</p>", "news", "example.test", false); err != nil || g.calls != 2 {
		t.Fatalf("unvalidated result reused: calls=%d err=%v", g.calls, err)
	}
	a.put(cacheKey(url, "news"), first)
	if _, err = a.analyze(ctx, url, "<p>third</p>", "news", "example.test", false); err != nil || g.calls != 2 {
		t.Fatalf("validated result not cached: calls=%d err=%v", g.calls, err)
	}
}
