package extractor

import (
	"context"
	"strings"
	"testing"
)

func TestArticleIdentityIsSeparateFromNormalizedRequest(t *testing.T) {
	for _, invisible := range []string{"\u200b", "\u00a0"} {
		original := "https://example.test/a" + invisible + "b"
		r := Extract(context.Background(), Input{URL: original, HTML: article(prose), SourceType: "article"})
		if r.State != "extracted" || r.URL != original || r.ExtractedFrom != "https://example.test/ab" {
			t.Fatalf("article identity/request: %+v", r)
		}
	}
}

func TestFeedDoesNotInventMediaOrPreserveAnUnusedSummary(t *testing.T) {
	xml := `<rss xmlns:content="http://purl.org/rss/1.0/modules/content/"><channel><item><title>Report</title><link>https://example.test/report</link><description>Unused summary</description><content:encoded><![CDATA[<p>Full feed body.</p>]]></content:encoded><enclosure type="image/png" url="https://example.test/picture.png"/></item></channel></rss>`
	r := Extract(context.Background(), Input{URL: "https://example.test/feed", RSSXML: xml, Strategy: "rss"})
	if r.Content != "<p>Full feed body.</p>" || r.Description != "" || len(r.Media) != 0 || r.ImageURL != "https://example.test/picture.png" {
		t.Fatalf("feed contract: %+v", r)
	}
}

func TestArticleMediaKeepsSourceAndTypeMetadata(t *testing.T) {
	h := `<img src=""/><img src="/cover.png" alt="River diagram"><video src="/movie.mp4"></video><source src="/clip.webm"><a href="/notes.pdf">Download notes</a><p>https://youtu.be/aBc_12</p>`
	media := articleMedia(h, "https://example.test/report")
	if len(media) != 5 {
		t.Fatalf("media: %+v", media)
	}
	if media[0].Source != "img_tag" || media[0].AltText != "River diagram" || media[0].MediaType != "image/png" {
		t.Fatalf("image metadata: %+v", media[0])
	}
	if media[1].Source != "video_tag" || media[2].Source != "source_tag" || media[3].Source != "embedded_video" || media[3].MediaType != "video/embed" {
		t.Fatalf("video metadata: %+v", media)
	}
	if media[4].Source != "href_link" || media[4].MediaType != "document/pdf" || strings.Contains(media[4].URL, "Download") {
		t.Fatalf("document metadata: %+v", media[4])
	}
}

func TestArticleMediaPreservesLazyLoadedImages(t *testing.T) {
	h := `<figure><img src="data:image/svg+xml,placeholder" data-src="/lead.jpg" alt="Opening photograph"></figure><img data-lazy-src="/second.webp" alt="Second photograph"><img src="/actual.png" data-src="/unneeded.jpg" alt="Already loaded">`
	got := articleMedia(h, "https://example.test/article")
	if len(got) != 3 {
		t.Fatalf("lazy image count=%d want3: %+v", len(got), got)
	}
	for i, want := range []string{"https://example.test/lead.jpg", "https://example.test/second.webp", "https://example.test/actual.png"} {
		if got[i].URL != want {
			t.Errorf("media%d URL=%q want%q", i, got[i].URL, want)
		}
	}
	if got[0].AltText != "Opening photograph" || got[0].MediaType != "image/jpg" {
		t.Fatalf("lazy image metadata lost: %+v", got[0])
	}
}

func TestProxiedImageRetainsCanonicalExtensionType(t *testing.T) {
	got := articleMedia(`<img src="https://proxy.test/opaque-hash" data-canonical-src="https://badges.test/license.svg" alt="License badge">`, "https://example.test/article")
	if len(got) != 1 || got[0].URL != "https://proxy.test/opaque-hash" || got[0].MediaType != "image/svg" {
		t.Fatalf("proxy media lost canonical type: %+v", got)
	}
}

func TestArticleTextExcludesInertTemplate(t *testing.T) {
	doc := parse(`<article><p>` + prose + `</p><template><a href="/">Button</a></template></article>`)
	if got := text(doc.Find("article")); strings.Contains(got, "Button") || !strings.Contains(got, "independent stations") {
		t.Fatalf("inert template entered article: %q", got)
	}
}
