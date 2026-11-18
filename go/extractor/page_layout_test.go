package extractor

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go/pagelayout"
)

func TestReparseArticleLayoutRequiresSnapshotAndPreservesFallback(t *testing.T) {
	html := `<article><header><picture><img src="/hero.jpg"></picture></header><div class="body"><p>` + "Local officials announced a new public transport plan after several weeks of consultation with residents. The project includes additional routes and improved access for neighborhoods across the region. Construction will begin next spring, while existing services continue throughout the changes. Community members can inspect detailed proposals at the municipal offices and provide feedback." + `</p><img src="/photo.jpg"><aside class="related"><a href="/other"><img src="/other.jpg">Other article</a></aside></div></article>`
	input := Input{URL: "https://example.test/story", HTML: html}
	old := Result{Content: "original", State: "extracted"}
	layout := pagelayout.Layout{PageKind: "article", ArticleBody: ".body", EditorialCover: "header img", ExcludeBlocks: []string{"aside.related"}}
	sum := sha256.Sum256([]byte(html))
	digest := hex.EncodeToString(sum[:])
	if got, ok := ReparseArticleLayout(input, old, strings.Repeat("0", 64), layout); ok || got.Content != old.Content {
		t.Fatal("mismatched snapshot did not fall back")
	}
	got, ok := ReparseArticleLayout(input, old, digest, layout)
	if !ok || !strings.Contains(got.Content, "public transport plan") || strings.Contains(got.Content, "Other article") {
		t.Fatalf("validated article was not reparsed: ok=%v content=%q", ok, got.Content)
	}
	if got.ImageURL != "https://example.test/hero.jpg" || got.OriginalChars != len([]rune(got.Content)) || got.Quality <= 0 {
		t.Fatalf("incorrect cover or finalized metadata: %#v", got)
	}
	if len(got.Media) != 3 {
		t.Fatalf("raw candidates were not retained: %d", len(got.Media))
	}
	for _, m := range got.Media {
		if strings.Contains(m.URL, "hero.jpg") && m.Data["article_body_scope"] == "outside" {
			t.Fatalf("editorial hero was marked outside: %#v", m)
		}
		if strings.Contains(m.URL, "other.jpg") && m.Data["article_body_scope"] != "outside" {
			t.Fatalf("related image was not marked outside: %#v", m)
		}
	}
}
