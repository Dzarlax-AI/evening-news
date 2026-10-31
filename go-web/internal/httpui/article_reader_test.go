package httpui

import (
	"bytes"
	"html/template"
	"os"
	"strings"
	"testing"
)

func TestArticleReaderMediaAndSafeSSR(t *testing.T) {
	a := map[string]any{"title": "<script>unsafe</script>", "content": "<img src=x onerror=alert(1)>", "url": "javascript:alert(1)", "image_url": "https://example.test/fallback.jpg", "media_files": []any{map[string]any{"type": "video", "url": "https://example.test/video"}, map[string]any{"type": "image", "url": "https://example.test/one.jpg", "thumbnail_url": "https://example.test/thumb.jpg"}, map[string]any{"type": "image", "url": "javascript:alert(1)"}, map[string]any{"type": "image", "url": "https://example.test/two.jpg"}}}
	a["categories"] = []any{map[string]any{"display_name": "Primary", "color": "#ffffff"}}
	view := buildArticleView(1, a)
	if len(view.Images) != 2 || len(view.OtherMedia) != 1 || view.SourceURL != "" || view.Images[0].Thumbnail != "https://example.test/thumb.jpg" {
		t.Fatal("unexpected media normalization")
	}
	source, err := os.ReadFile("../../templates/public/fragments/article_reader.html")
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := template.New("reader").Funcs(template.FuncMap{"paragraphs": func(s string) []string { return []string{s} }, "addOne": func(i int) int { return i + 1 }}).Parse(string(source))
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = parsed.ExecuteTemplate(&out, "article-reader", map[string]any{"Article": view, "Locale": "ru"}); err != nil {
		t.Fatal(err)
	}
	html := out.String()
	for _, bad := range []string{"<script>unsafe</script>", "<img src=x onerror=alert(1)>", "javascript:alert"} {
		if strings.Contains(html, bad) {
			t.Errorf("unsafe markup: %s", bad)
		}
	}
	for _, want := range []string{"example.test/one.jpg", "example.test/two.jpg", "example.test/thumb.jpg", "example.test/video", "&lt;script&gt;", "background:#ffffff", "color:#000000"} {
		if !strings.Contains(html, want) {
			t.Errorf("missing %s", want)
		}
	}
	for _, tc := range []struct{ kind, url, want string }{
		{"video", "https://example.test/clip.mp4?x=1", "video"}, {"audio", "https://example.test/voice.WAV", "audio"}, {"video", "https://example.test/embed/1", ""}, {"document", "https://example.test/clip.mp4", ""},
	} {
		if got := playableMediaKind(tc.kind, tc.url); got != tc.want {
			t.Errorf("playableMediaKind(%q,%q)=%q", tc.kind, tc.url, got)
		}
	}

	fallback := buildArticleView(1, map[string]any{"image_url": "javascript:bad", "primary_image": "https://example.test/fallback.jpg", "media_files": []any{map[string]any{"type": "video", "url": "https://example.test/video"}}})
	if len(fallback.Images) != 1 || fallback.Images[0].URL != "https://example.test/fallback.jpg" {
		t.Fatal("missing safe image fallback")
	}
}

func TestReaderCategoryColorsRejectCSSInjection(t *testing.T) {
	for _, tc := range []struct{ input, color, text string }{
		{"#ffffff", "#ffffff", "#000000"},
		{"#000000", "#000000", "#ffffff"},
		{"#fff", "#6c757d", "#ffffff"},
		{"#12345+", "#6c757d", "#ffffff"},
		{"red; background:url(https://example.test)", "#6c757d", "#ffffff"},
	} {
		color, text := categoryColors(tc.input)
		if color != tc.color || text != tc.text {
			t.Errorf("color normalization got %q %q", color, text)
		}
	}
	view := buildArticleView(1, map[string]any{"category": "Legacy", "categories": []any{map[string]any{"display_name": "Primary", "color": "red;display:none"}}})
	if view.Category != "Primary" || view.CategoryColor != "#6c757d" {
		t.Fatal("invalid category normalization")
	}
}
