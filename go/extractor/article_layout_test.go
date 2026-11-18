package extractor

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestVerifiedBlicLayoutAndStaleFallback(t *testing.T) {
	html := `<html><body><article class="article"><header><div class="news__image"><picture><source type="image/webp" srcset="https://ocdn.eu/small.webp 1x"><img src="https://ocdn.eu/cover.jpg"></picture></div></header><div class="article__text"><p>` + prose + `</p><div class="article__author"><img src="/logo.svg"></div><aside class="article__related"><img src="/related.jpg"></aside><div id="piano-story-related"><img src="/piano.png"></div><div class="article__community-groups-blic"><img src="/community.png"></div><div class="banner--article"><img src="/banner.png"></div><img src="https://ocdn.eu/small.webp"></div></article></body></html>`
	for _, strategy := range []string{"auto", "enhanced_selectors", "readability", "browser_rendering", "learned_pattern_browser_rendering"} {
		r := Extract(context.Background(), Input{URL: "https://www.blic.rs/vesti/politika/story/123", HTML: html, RenderedHTML: html, SourceType: "article", Strategy: strategy})
		if r.State != "extracted" || r.SelectorUsed != "article.article .article__text" || r.ImageURL != "https://ocdn.eu/cover.jpg" {
			t.Fatalf("%s result=%+v", strategy, r)
		}
		for _, m := range r.Media {
			switch {
			case strings.HasSuffix(m.URL, "/logo.svg") || strings.HasSuffix(m.URL, "/related.jpg") || strings.HasSuffix(m.URL, "/piano.png") || strings.HasSuffix(m.URL, "/community.png") || strings.HasSuffix(m.URL, "/banner.png"):
				if m.Data["article_body_scope"] != "outside" {
					t.Fatalf("%s retained service image: %+v", strategy, m)
				}
			case m.URL == "https://ocdn.eu/cover.jpg" || m.URL == "https://ocdn.eu/small.webp":
				if m.Data["canonical_url"] != "https://ocdn.eu/cover.jpg" {
					t.Fatalf("%s missing picture link: %+v", strategy, m)
				}
			}
		}
	}
	stale := strings.Replace(html, `class="news__image"`, `class="changed"`, 1)
	r := Extract(context.Background(), Input{URL: "https://www.blic.rs/vesti/politika/story/123", HTML: stale, SourceType: "article", Strategy: "auto"})
	if r.State != "extracted" || r.SelectorUsed == "article.article .article__text" {
		t.Fatalf("stale selector did not fall back: %+v", r)
	}
}

func TestStudentiArticleAndConfirmedIndex(t *testing.T) {
	articleHTML := `<main><article><div class="hero-image"><img src="/hero.png"></div><div class="prose"><article><p>` + prose + `</p><img src="/detail.png"></article></div></article></main>`
	list := `<main><section><ul>`
	for i := 0; i < 12; i++ {
		list += fmt.Sprintf(`<li><a class="clear" href="/vesti/story-%d/">Article</a></li>`, i)
	}
	list += `</ul></section></main>`
	for _, strategy := range []string{"auto", "enhanced_selectors", "readability", "browser_rendering", "learned_pattern_browser_rendering"} {
		article := Extract(context.Background(), Input{URL: "https://studenti.org.rs/vesti/cupic_gasic/", HTML: articleHTML, RenderedHTML: articleHTML, SourceType: "article", Strategy: strategy})
		if article.State != "extracted" || article.SelectorUsed != "main > article > div.prose > article" || article.ImageURL != "https://studenti.org.rs/hero.png" || len(article.Media) != 2 {
			t.Fatalf("%s article=%+v", strategy, article)
		}
		index := Extract(context.Background(), Input{URL: "https://studenti.org.rs/vesti/", HTML: list, RenderedHTML: list, Content: prose, SourceType: "article", Strategy: strategy})
		if index.State != "unsupported" || index.Error != "non_article_listing" || index.Content != "" || index.ImageURL != "" || len(index.Media) != 0 {
			t.Fatalf("%s index=%+v", strategy, index)
		}
	}
}

func TestVerifiedLayoutRejectsCoverInChromeAndWrongHost(t *testing.T) {
	html := `<article class="article"><header><nav><div class="news__image"><picture><img src="/logo.png"></picture></div></nav></header><div class="article__text"><p>` + prose + `</p></div></article>`
	d := parse(html)
	if _, _, _, _, ok := verifiedSourceLayout(d, "https://www.blic.rs/vesti/politika/story/123", 100000); ok {
		t.Fatal("navigation image accepted as cover")
	}
	if _, _, _, _, ok := verifiedSourceLayout(d, "https://www.blic.rs.attacker.test/vesti/politika/story/123", 100000); ok {
		t.Fatal("untrusted host accepted")
	}
}
