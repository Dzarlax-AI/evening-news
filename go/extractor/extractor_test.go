package extractor

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/andybalholm/brotli"
)

const prose = "Researchers released an open archive describing a method for repairing damaged river sensors. The team compared readings from four independent stations and published the calibration procedure, sample data, and known limitations so other laboratories can reproduce the result. Their report documents heavy rain, industrial discharge, electrical interference, and manual inspection requirements in a public engineering appendix."

func article(body string) string {
	return `<html><head><title>River measurements</title></head><body><nav>Menu categories</nav><article><p>` + body + `</p><aside>Unrelated sidebar</aside></article></body></html>`
}
func TestExtractionPriorityAndLearnedDegradation(t *testing.T) {
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: article(prose), LearnedSelector: ".missing", LearnedMethod: "css"})
	if r.State != "extracted" || r.Method != "enhanced_selectors" || strings.Contains(r.Content, "Unrelated") {
		t.Fatalf("unexpected result: %+v", r)
	}
	if strings.Join(r.Trace, ",") != "learned_pattern,learned_pattern_degraded,enhanced_selectors" {
		t.Fatalf("priority: %v", r.Trace)
	}
	if r.Title != "River measurements" {
		t.Fatalf("title: %q", r.Title)
	}
}
func TestLearnedMethodPrefixAndFirstMatch(t *testing.T) {
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: `<div class="story">` + prose + `</div>`, LearnedSelector: ".story", LearnedMethod: "learned_pattern_learned_pattern_css"})
	if r.Method != "learned_pattern_css" || r.SelectorUsed != ".story" {
		t.Fatalf("%+v", r)
	}
}
func TestJSONLDPoorCandidateShortCircuitsFallback(t *testing.T) {
	data, _ := json.Marshal(map[string]any{"@type": "Article", "articleBody": strings.Repeat("repeated ", 30)})
	h := `<html><head><script type="application/ld+json">` + string(data) + `</script><meta property="og:description" content="` + prose + `"></head></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: h, Strategy: "fallback_extraction"})
	if r.State != "failed" {
		t.Fatalf("bad JSON-LD must retain Python short-circuit behavior: %+v", r)
	}
}
func TestReadabilityIsIndependentFromCSS(t *testing.T) {
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: `<html><body><div><h1>River measurements</h1><div>` + prose + `</div></div></body></html>`, Strategy: "readability"})
	if r.State != "extracted" || r.Method != "readability" || !strings.Contains(r.Content, "four independent stations") {
		t.Fatalf("%+v", r)
	}
}
func TestRedditLinkedArticlePreservesPostMetadata(t *testing.T) {
	data := `[{"data":{"children":[{"data":{"title":"Discussion title","author":"reader","url":"https://example.test/article","is_self":false,"created_utc":1760000000}}]}},{"data":{"children":[]}}]`
	r := Extract(context.Background(), Input{URL: "https://www.reddit.com/r/example/comments/123/topic", RedditJSON: data, LinkedHTML: article(prose), Strategy: "reddit_json"})
	if r.Method != "reddit_json" || r.Title != "Discussion title" || r.Author != "reader" || !strings.Contains(r.Content, "calibration procedure") {
		t.Fatalf("%+v", r)
	}
}
func TestBrowserCaptureAndNoImplicitCustomHTTPFallback(t *testing.T) {
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: article(prose), SourceType: "custom"})
	if r.State != "failed" || r.Error != "browser_unavailable" {
		t.Fatalf("custom default must require captured browser: %+v", r)
	}
	r = Extract(context.Background(), Input{URL: "https://example.test/story", HTML: "<div>Loading</div>", RenderedHTML: article(prose), Strategy: "browser_rendering"})
	if r.State != "extracted" || r.Method != "browser_rendering" || r.Title != "River measurements" {
		t.Fatalf("%+v", r)
	}
}
func TestTelegramMetadataBeforeCleanupAndMediaExclusions(t *testing.T) {
	h := `<div class="tgme_widget_message" data-post="sample/42"><div class="tgme_widget_message_forwarded_from"><img src="https://cdn.test/avatar.png">Forwarded author</div><div class="tgme_widget_message_text">Water station update with enough useful message text.</div><div class="tgme_widget_message_photo" style="background-image:url('https://cdn.test/photo.jpg')"></div><div class="tgme_widget_message_footer"><a class="tgme_widget_message_date" href="https://t.me/sample/42"><time datetime="2026-10-15T10:00:00Z">Today</time></a><span class="tgme_widget_message_views">999 views</span></div></div>`
	r := Extract(context.Background(), Input{URL: "https://t.me/s/sample", TelegramHTML: h, Strategy: "telegram_http"})
	if r.State != "extracted" || r.URL != "https://t.me/sample/42" || r.PublicationDate != "2026-10-15T10:00:00Z" || r.ImageURL != "https://cdn.test/photo.jpg" || strings.Contains(r.Content, "999") || strings.Contains(r.Content, "Forwarded") {
		t.Fatalf("%+v", r)
	}
	for _, m := range r.Media {
		if strings.Contains(m.URL, "avatar") {
			t.Fatal("avatar escaped exclusion")
		}
	}
}
func TestTelegramAllRichMediaFamilies(t *testing.T) {
	h := `<div class="tgme_widget_message"><video src="https://cdn.test/v.mp4"></video><audio src="https://cdn.test/a.ogg"></audio><a class="tgme_widget_message_document" href="https://cdn.test/d.pdf">Document</a><div class="tgme_widget_message_sticker" style="background-image:url(https://cdn.test/s.webp)"></div><div class="tgme_widget_message_gif"><video src="https://cdn.test/g.gif"></video></div><div class="tgme_widget_message_poll"><div class="poll_question">Where?</div><div class="poll_option">Here</div></div><div class="tgme_widget_message_location"><a href="https://maps.test/location">Park</a></div><div class="tgme_widget_message_contact"><span class="phone">12345</span></div></div>`
	r := Extract(context.Background(), Input{URL: "https://t.me/s/sample", TelegramHTML: h, Strategy: "telegram_http"})
	types := map[string]bool{}
	for _, m := range r.Media {
		types[m.Type] = true
	}
	for _, typ := range []string{"video", "audio", "document", "animated_sticker", "gif", "poll", "location", "contact"} {
		if !types[typ] {
			t.Fatalf("missing %s in %+v", typ, r.Media)
		}
	}
}
func TestMetadataPriorityAndArticleMedia(t *testing.T) {
	h := `<html><head><meta property="og:title" content="OG heading"><meta name="twitter:title" content="Twitter heading"><meta name="author" content="A. Writer"><meta property="og:image" content="//cdn.test/cover.jpg"></head><body><article><p>` + prose + `</p><time datetime="2026-10-15">Date</time><img src="/photo.png" alt="River"><video><source src="/film.webm"></video><a href="/report.pdf">Report</a></article></body></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: h})
	if r.Title != "OG heading" || r.Author != "A. Writer" || r.ImageURL != "https://cdn.test/cover.jpg" || r.PublicationDate != "2026-10-15T00:00:00" || len(r.Media) != 3 {
		t.Fatalf("%+v", r)
	}
}
func TestLimitsRunesBytesAndNoSilentTruncation(t *testing.T) {
	for _, tt := range []struct {
		in    Input
		state string
	}{
		{Input{URL: "https://example.test/story", Content: "яя", MaxBytes: 3}, "resource_limit"},
		{Input{URL: "https://example.test/story", Content: "яя", MaxBytes: 4, MaxChars: 2}, "extracted"},
		{Input{URL: "https://example.test/story", Content: "яя", MaxChars: 1}, "resource_limit"},
		{Input{URL: "https://example.test/story", Strategy: "unknown"}, "unsupported"},
		{Input{URL: "file:///private", Content: prose}, "failed"},
	} {
		r := Extract(context.Background(), tt.in)
		if r.State != tt.state {
			t.Errorf("state %s want %s: %+v", r.State, tt.state, r)
		}
	}
	long := strings.Repeat(prose+" ", 23)
	r := Extract(context.Background(), Input{URL: "https://example.test/story", Content: long})
	if !r.Truncated || r.OriginalChars != runeLen(long) || runeLen(r.Content) > 8000 || r.TruncationPolicy == "" {
		t.Fatalf("truncation must be visible: %+v", r)
	}
}
func TestAlternativeURLRecovery(t *testing.T) {
	r := Extract(context.Background(), Input{URL: "https://example.test/blocked", HTML: `<link rel="canonical" href="/full"><p>Unavailable</p>`, AlternativeHTML: map[string]string{"https://example.test/full": article(prose)}})
	if r.State != "extracted" || r.URL != "https://example.test/blocked" || r.Trace[len(r.Trace)-1] != "alternative_url_success" {
		t.Fatalf("%+v", r)
	}
}

func TestShortOpenGraphFallsBackToStandardMeta(t *testing.T) {
	h := `<html><head><meta property="og:description" content="Short teaser"><meta name="description" content="` + prose + `"></head></html>`
	for _, strategy := range []string{"open_graph", "fallback_extraction"} {
		r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: h, Strategy: strategy})
		if r.State != "extracted" || r.Content != prose {
			t.Errorf("strategy %s blocked standard meta: %+v", strategy, r)
		}
	}
}

func TestAlternativeRecoveryUsesFullPipelineAndSemanticPriority(t *testing.T) {
	data, _ := json.Marshal(map[string]any{"@type": "NewsArticle", "articleBody": prose})
	canonical := `<html><head><title>Canonical only</title><script type="application/ld+json">` + string(data) + `</script></head></html>`
	h := `<html><head><link rel="amphtml" href="/amp"><link rel="canonical" href="/canonical"></head><body><a href="/more">Read more</a></body></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/blocked", HTML: h, AlternativeHTML: map[string]string{"https://example.test/canonical": canonical, "https://example.test/amp": article(prose), "https://example.test/more": article(prose)}})
	if r.State != "extracted" || r.URL != "https://example.test/blocked" || r.Method != "fallback_extraction" || r.Title != "Canonical only" {
		t.Fatalf("canonical JSON-LD recovery failed: %+v", r)
	}
	b, _ := json.Marshal(r)
	var fields map[string]any
	_ = json.Unmarshal(b, &fields)
	if fields["extracted_from"] != "https://example.test/canonical" {
		t.Fatalf("recovered URL provenance missing: %s", b)
	}
}

func TestCustomBrowserAliasUsesOnlyRenderedCapture(t *testing.T) {
	r := Extract(context.Background(), Input{URL: "https://example.test/list", SourceType: "custom", Strategy: "browser_rendering", RenderedHTML: `<article><h2><a href="/story">Captured browser card</a></h2><p>` + prose + `</p></article>`, Selectors: map[string][]string{"article": {"article"}, "title": {"h2"}, "content": {"p"}}})
	if r.State != "extracted" || r.Title != "Captured browser card" || r.URL != "https://example.test/story" {
		t.Fatalf("browser alias ignored rendered capture: %+v", r)
	}
}
func TestBoundedDecoders(t *testing.T) {
	for _, enc := range []string{"identity", "gzip", "deflate", "br"} {
		t.Run(enc, func(t *testing.T) {
			var b bytes.Buffer
			switch enc {
			case "identity":
				b.WriteString(prose)
			case "gzip":
				w := gzip.NewWriter(&b)
				_, _ = w.Write([]byte(prose))
				_ = w.Close()
			case "deflate":
				w := zlib.NewWriter(&b)
				_, _ = w.Write([]byte(prose))
				_ = w.Close()
			case "br":
				w := brotli.NewWriter(&b)
				_, _ = w.Write([]byte(prose))
				_ = w.Close()
			}
			out, e := DecodePage(bytes.NewReader(b.Bytes()), enc, 4096)
			if e != nil || string(out) != prose {
				t.Fatalf("decode: %v %q", e, out)
			}
			_, e = DecodePage(bytes.NewReader(b.Bytes()), enc, 100)
			if !errors.Is(e, ErrWireLimit) && !errors.Is(e, ErrOutputLimit) {
				t.Fatalf("bound: %v", e)
			}
			if enc != "identity" {
				_, e = DecodePage(bytes.NewReader(b.Bytes()[:b.Len()/2]), enc, 4096)
				if e == nil {
					t.Fatal("truncated stream accepted")
				}
			}
		})
	}
	_, e := DecodePage(strings.NewReader("body"), "zstd", 100)
	if e == nil {
		t.Fatal("unsupported encoding accepted")
	}
	var b bytes.Buffer
	w := gzip.NewWriter(&b)
	_, _ = w.Write([]byte(prose))
	_ = w.Close()
	b.WriteString("trailing")
	if _, e = DecodePage(&b, "gzip", 4096); e == nil {
		t.Fatal("trailing stream accepted")
	}
}
func TestCancellationAndConcurrentIsolation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if r := Extract(ctx, Input{}); r.Error != "context canceled" {
		t.Fatalf("%+v", r)
	}
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			r := Extract(context.Background(), Input{URL: fmt.Sprintf("https://example.test/%d", i), HTML: article(prose)})
			if r.State != "extracted" {
				t.Errorf("%+v", r)
			}
		}(i)
	}
	wg.Wait()
}

type cancelOnCheck struct {
	context.Context
	calls int
	after int
}

func (c *cancelOnCheck) Err() error {
	c.calls++
	if c.calls >= c.after {
		return context.Canceled
	}
	return nil
}
func TestCancellationAfterStrategyRejectNeverFallsBackToSource(t *testing.T) {
	ctx := &cancelOnCheck{Context: context.Background(), after: 3}
	r := Extract(ctx, Input{URL: "https://example.test/story", HTML: "<p>Loading</p>", Content: prose})
	if r.State == "extracted" || r.Content != "" || r.Error != "context canceled" {
		t.Fatalf("observed cancellation masked by source fallback: %+v", r)
	}
}

func TestTelegramTitleMatchesFirstLineAndSmartBoundary(t *testing.T) {
	for _, tt := range []struct{ content, want string }{{"Short first sentence. Another useful detail", "Short first sentence. Another useful detail"}, {"@channel: 📎 A useful field report title", "A useful field report title"}, {prose, "Researchers released an open archive describing a method for repairing damaged river sensors."}, {"", "Telegram Post"}} {
		h := `<div class="tgme_widget_message"><div class="tgme_widget_message_text">` + tt.content + `</div><div class="tgme_widget_message_photo"><img src="https://cdn.test/photo.jpg"></div></div>`
		r := Extract(context.Background(), Input{URL: "https://t.me/s/sample", TelegramHTML: h, Strategy: "telegram_http"})
		if r.Title != tt.want {
			t.Errorf("title=%q want=%q", r.Title, tt.want)
		}
	}
}

func TestTelegramLinkedReplayInheritsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	h := `<div class="tgme_widget_message"><div class="tgme_widget_message_text">Short station update with useful detail <a href="https://example.test/full">Full report</a></div></div>`
	r := extractTelegram(ctx, Input{URL: "https://t.me/s/sample", TelegramHTML: h, LinkedHTML: article(prose), MaxChars: 100000}, Result{})
	if strings.Contains(r.Content, "calibration procedure") {
		t.Fatal("cancelled parent context ignored by linked extraction")
	}
}

func TestSourceMissingDatesUseInjectedUTCClock(t *testing.T) {
	now := "2026-10-15T14:00:00+02:00"
	for _, tt := range []struct {
		name string
		in   Input
	}{
		{"rss", Input{URL: "https://example.test/feed", Strategy: "rss", RSSXML: `<rss><channel><item><title>Undated report</title><link>https://example.test/story</link><description>Captured description</description></item></channel></rss>`}},
		{"telegram", Input{URL: "https://t.me/s/sample", Strategy: "telegram_http", TelegramHTML: `<div class="tgme_widget_message"><div class="tgme_widget_message_text">Undated message with meaningful details</div></div>`}},
		{"custom", Input{URL: "https://example.test/list", Strategy: "custom_http", HTML: `<article><h2>Undated field report</h2><p>` + prose + `</p></article>`}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tt.in.Now = now
			r := Extract(context.Background(), tt.in)
			if r.State != "extracted" || r.PublicationDate != "2026-10-15T12:00:00" {
				t.Fatalf("missing-date source clock: %+v", r)
			}
		})
	}
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: article(prose), Now: now})
	if r.PublicationDate != "" {
		t.Fatalf("ordinary article acquired synthetic date: %+v", r)
	}
	r = Extract(context.Background(), Input{URL: "https://example.test/story", HTML: article(prose), Now: "invalid"})
	if r.State != "failed" || r.Error != "invalid_now" {
		t.Fatalf("bad injected clock accepted: %+v", r)
	}
}
