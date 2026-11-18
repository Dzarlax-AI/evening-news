package repositories

import (
	"reflect"
	"testing"
)

func TestProjectArticleMediaPreservesRawAndRSSProvenance(t *testing.T) {
	raw := []any{
		map[string]any{"type": "image", "url": "https://cdn.example/cover.jpg", "source": "rss_feed"},
		map[string]any{"type": "image", "url": "https://cdn.example/icon.svg", "source": "rss_feed", "data": map[string]any{"width": float64(60), "height": float64(60)}},
		map[string]any{"type": "image", "url": "https://cdn.example/chart.svg", "source": "rss_feed", "data": map[string]any{"width": float64(900), "height": float64(600)}},
		map[string]any{"type": "image", "url": "https://cdn.example/other.jpg", "source": "img_tag"},
	}
	got, primary := projectArticleMedia(raw, "rss")
	if len(got) != 2 || primary == nil || *primary != "https://cdn.example/cover.jpg" || len(raw) != 4 {
		t.Fatalf("projection=%v primary=%v raw=%v", got, primary, raw)
	}
	if !reflect.DeepEqual(got[1], raw[2]) {
		t.Fatal("editorial SVG lost")
	}
}

func TestProjectArticleMediaCanonicalAndSignedVariants(t *testing.T) {
	mk := func(u, c string) any {
		return map[string]any{"type": "image", "url": u, "data": map[string]any{"canonical_url": c}}
	}
	raw := []any{mk("https://cdn.example/a-small.jpg", "https://cdn.example/a.jpg"), mk("https://cdn.example/a-large.jpg", "https://cdn.example/a.jpg"), mk("https://cdn.example/b.jpg", "https://cdn.example/b.jpg"), mk("https://cdn.example/a.jpg?sig=1", "https://cdn.example/a.jpg"), mk("https://cdn.example/a.jpg?sig=2", "https://cdn.example/a.jpg")}
	got, _ := projectArticleMedia(raw, "telegram")
	if len(got) != 4 || !reflect.DeepEqual(got, []any{raw[0], raw[2], raw[3], raw[4]}) {
		t.Fatalf("unexpected projection: %v", got)
	}
}

func TestProjectArticleMediaEmpty(t *testing.T) {
	got, primary := projectArticleMedia([]any{map[string]any{"type": "image", "url": "https://cdn.example/pixel.png", "data": map[string]any{"width": 1, "height": 1}}}, "web")
	if len(got) != 0 || primary != nil {
		t.Fatalf("got %v, %v", got, primary)
	}
}

func TestProjectArticleMediaRollbackAndSelectedView(t *testing.T) {
	raw := []any{
		map[string]any{"type": "image", "url": "https://example.test/logo.png", "data": map[string]any{"article_body_scope": "outside"}},
		map[string]any{"type": "image", "url": "https://example.test/cover.jpg", "data": map[string]any{"width": float64(900), "height": float64(600)}},
		map[string]any{"type": "image", "url": "https://example.test/pixel.png", "data": map[string]any{"width": float64(1), "height": float64(1)}},
		map[string]any{"type": "video", "url": "https://example.test/clip.mp4"},
	}
	before := append([]any(nil), raw...)
	got, primary := projectArticleMedia(raw, "web")
	if len(got) != 2 || !reflect.DeepEqual(got, []any{raw[1], raw[3]}) || primary == nil || *primary != "https://example.test/cover.jpg" || !reflect.DeepEqual(raw, before) {
		t.Fatalf("selected=%v primary=%v raw=%v", got, primary, raw)
	}
}

func TestVergeOnlyConfirmedFullFrameVariantsMerge(t *testing.T) {
	mk := func(u string) any { return map[string]any{"type": "image", "source": "rss_feed", "url": u} }
	base := "https://platform.theverge.com/wp-content/uploads/sites/2/2026/11/EA-Sports-FC-27.png"
	raw := []any{mk(base + "?quality=90&strip=all&crop=0,0,100,100"), mk(base + "?w=300"), mk(base + "?crop=10,0,80,100&w=300"), mk(base + "?w=300&signature=abc"), mk("https://other.test/_next/image?url=one"), mk("https://other.test/_next/image?url=two")}
	got, _ := projectArticleMedia(raw, "rss")
	if len(got) != 5 || !reflect.DeepEqual(got, []any{raw[0], raw[2], raw[3], raw[4], raw[5]}) {
		t.Fatalf("projection=%v", got)
	}
}

func TestProjectArticleMediaOmitsConfirmedHTMLWrapper(t *testing.T) {
	raw := []any{map[string]any{"type": "image", "url": "https://huggingface.co/a/b/resolve/main/chart.png"}, map[string]any{"type": "image", "url": "https://huggingface.co/a/b/blob/main/chart.png", "data": map[string]any{"html_wrapper": true}}}
	got, _ := projectArticleMedia(raw, "web")
	if len(got) != 1 || !reflect.DeepEqual(got[0], raw[0]) || len(raw) != 2 {
		t.Fatalf("got=%v raw=%v", got, raw)
	}
}

func TestVergeFullSizeReplacesEarlierThumbnailInPlace(t *testing.T) {
	base := "https://platform.theverge.com/wp-content/uploads/sites/2/2026/11/photo.png"
	mk := func(u string) any { return map[string]any{"type": "image", "source": "rss_feed", "url": u} }
	raw := []any{mk(base + "?w=300"), mk("https://example.test/second.png"), mk(base + "?quality=90&strip=all&crop=0,0,100,100")}
	got, primary := projectArticleMedia(raw, "rss")
	if len(got) != 2 || !reflect.DeepEqual(got, []any{raw[2], raw[1]}) || primary == nil || *primary != base+"?quality=90&strip=all&crop=0,0,100,100" {
		t.Fatalf("got=%v primary=%v", got, primary)
	}
	if raw[0].(map[string]any)["url"] != base+"?w=300" {
		t.Fatal("raw changed")
	}
}
