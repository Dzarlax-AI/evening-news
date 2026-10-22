package extractor

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"strings"
	"testing"
	"unicode/utf8"
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

func TestArticleMediaBoundsEscapedOutputAndPreservesArticle(t *testing.T) {
	base := "https://example.test/" + strings.Repeat("b", 6800) + "/"
	alt := strings.Repeat("é\"\\", 600)
	var page strings.Builder
	page.WriteString(article(prose))
	for i := 0; i < 400; i++ {
		fmt.Fprintf(&page, `<img src="asset-%d.jpg" alt="%s">`, i, html.EscapeString(alt))
	}
	r := Extract(context.Background(), Input{URL: base, HTML: page.String(), SourceType: "article"})
	if r.State != "extracted" || r.Content == "" {
		t.Fatalf("media bounding discarded article extraction: state=%s error=%s", r.State, r.Error)
	}
	if len(r.Media) == 0 || len(r.Media) >= 256 {
		t.Fatalf("media bounds/order not preserved: count=%d", len(r.Media))
	}
	if r.Media[0].URL != base+"asset-0.jpg" {
		t.Fatalf("first media item changed: %+v", r.Media[0])
	}
	if len(r.Media[0].AltText) > maxArticleMediaAltTextBytes || len(r.Media[0].AltText) != maxArticleMediaAltTextBytes || !strings.Contains(strings.Join(r.Trace, ","), "media_truncated") {
		t.Fatalf("alt text/output truncation was not made visible: alt_bytes=%d trace=%v", len(r.Media[0].AltText), r.Trace)
	}
	encoded, err := json.Marshal(r.Media)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > maxArticleMediaJSONBytes {
		t.Fatalf("serialized media exceeds budget: %d > %d", len(encoded), maxArticleMediaJSONBytes)
	}
}

func TestArticleMediaSkipsOversizeURLWithoutTruncatingIdentity(t *testing.T) {
	oversize := "https://cdn.example/" + strings.Repeat("x", maxArticleMediaURLBytes) + ".jpg"
	h := `<img src="/first.png"><img src="` + oversize + `"><img src="/last.png">`
	got, truncated := boundedArticleMedia(h, "https://example.test/article")
	if !truncated || len(got) != 2 || got[0].URL != "https://example.test/first.png" || got[1].URL != "https://example.test/last.png" {
		t.Fatalf("oversized media URL was not skipped intact: truncated=%v media=%+v", truncated, got)
	}
}

func TestEmbeddedMediaDedupDoesNotConsumeCandidateBudget(t *testing.T) {
	var page strings.Builder
	for i := 0; i < maxArticleMediaCount+1; i++ {
		page.WriteString(" https://youtu.be/repeated ")
	}
	page.WriteString(" https://youtu.be/unique ")
	got := articleMedia(page.String(), "https://example.test/article")
	if len(got) != 2 || got[0].URL != "https://youtu.be/repeated" || got[1].URL != "https://youtu.be/unique" {
		t.Fatalf("duplicate matches consumed media budget: %+v", got)
	}
}

func TestArticleMetadataBoundsQuoteHeavyOutputWithoutLosingArticle(t *testing.T) {
	quotes := strings.Repeat(`"`, 2*1024*1024)
	imageURL := "https://image.example/" + strings.Repeat("x", maxArticleImageURLBytes)
	page := "<html><head><meta property='og:title' content='" + quotes +
		"'><meta property='og:description' content='" + quotes +
		"'><meta property='og:image' content='" + imageURL + "'></head><body>" + article(prose) + "</body></html>"
	if len(page) > 5*1024*1024 {
		t.Fatalf("fixture exceeds extraction input cap: %d", len(page))
	}
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, SourceType: "article"})
	if r.State != "extracted" || r.Content == "" || !strings.Contains(r.Content, "independent stations") {
		t.Fatalf("metadata bounds discarded article content: state=%s error=%s", r.State, r.Error)
	}
	if len(r.Title) != maxArticleTitleBytes || len(r.Description) != maxArticleDescriptionBytes || r.ImageURL != "" {
		t.Fatalf("metadata limits not applied: title=%d description=%d image=%d", len(r.Title), len(r.Description), len(r.ImageURL))
	}
	if !strings.Contains(strings.Join(r.Trace, ","), "metadata_truncated") {
		t.Fatalf("metadata truncation is not visible: %v", r.Trace)
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= 5*1024*1024 {
		t.Fatalf("bounded result still exceeds client response cap: %d", len(encoded))
	}
}

func TestFailedArticleMetadataIsBoundedForSelectorStudyResponse(t *testing.T) {
	quotes := strings.Repeat(`"`, 2*1024*1024)
	page := "<html><head><meta property='og:title' content='" + quotes +
		"'><meta property='og:description' content='" + quotes +
		"'></head><body>Loading</body></html>"
	if len(page) > 5*1024*1024 {
		t.Fatalf("fixture exceeds extraction input cap: %d", len(page))
	}
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, SourceType: "article"})
	if r.State != "failed" || len(r.Title) != maxArticleTitleBytes || len(r.Description) != maxArticleDescriptionBytes {
		t.Fatalf("failed selector-study metadata was not bounded: state=%s title=%d description=%d", r.State, len(r.Title), len(r.Description))
	}
	if !strings.Contains(strings.Join(r.Trace, ","), "metadata_truncated") {
		t.Fatalf("failed metadata truncation is not visible: %v", r.Trace)
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= 5*1024*1024 {
		t.Fatalf("failed result still exceeds client response cap: %d", len(encoded))
	}
}

func TestArticleMetadataBoundsOnTextResourceLimit(t *testing.T) {
	quotes := strings.Repeat(`"`, 2*1024*1024)
	page := "<html><head><meta property='og:title' content='" + quotes +
		"'><meta property='og:description' content='" + quotes +
		"'></head><body>Loading</body></html>"
	if len(page) > 5*1024*1024 {
		t.Fatalf("fixture exceeds extraction input cap: %d", len(page))
	}

	// The web extractor leaves this selector-study page without content. The
	// caller content fallback then takes the MaxChars return after metadata has
	// already been parsed, exercising the early resource-limit path.
	r := Extract(context.Background(), Input{
		URL:        "https://example.test/story",
		HTML:       page,
		Content:    strings.Repeat("article fallback ", 20),
		MaxChars:   32,
		SourceType: "article",
	})
	if r.State != "resource_limit" || r.Error != "extraction_text_limit" || r.Content != "" {
		t.Fatalf("resource-limit outcome changed: state=%s error=%s content=%q", r.State, r.Error, r.Content)
	}
	if len(r.Title) != maxArticleTitleBytes || len(r.Description) != maxArticleDescriptionBytes {
		t.Fatalf("resource-limit metadata was not bounded: title=%d description=%d", len(r.Title), len(r.Description))
	}
	if !strings.Contains(strings.Join(r.Trace, ","), "metadata_truncated") {
		t.Fatalf("resource-limit metadata truncation is not visible: %v", r.Trace)
	}
	encoded, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) >= 5*1024*1024 {
		t.Fatalf("bounded resource-limit result exceeds client response cap: %d", len(encoded))
	}
}

func TestArticleMetadataBoundsAllFieldsOnUTF8BoundariesAndPreservesIdentity(t *testing.T) {
	result := Result{URL: "https://example.test/original", ExtractedFrom: "https://example.test/final"}
	result.Title = strings.Repeat("é", maxArticleTitleBytes/2) + "é"
	result.Author = strings.Repeat("a", maxArticleAuthorBytes+1)
	result.Description = strings.Repeat("d", maxArticleDescriptionBytes+1)
	result.PublicationDate = strings.Repeat("p", maxArticlePublicationDateBytes+1)
	result.ImageURL = "https://image.example/" + strings.Repeat("x", maxArticleImageURLBytes)
	if !boundArticleMetadata(&result) {
		t.Fatal("oversize metadata was not reported")
	}
	if len(result.Title) != maxArticleTitleBytes || !utf8.ValidString(result.Title) || len(result.Author) != maxArticleAuthorBytes || len(result.Description) != maxArticleDescriptionBytes || len(result.PublicationDate) != maxArticlePublicationDateBytes || result.ImageURL != "" {
		t.Fatalf("metadata did not honor UTF-8/URL bounds: %+v", result)
	}
	if result.URL != "https://example.test/original" || result.ExtractedFrom != "https://example.test/final" {
		t.Fatalf("metadata bounding changed article identity: %+v", result)
	}
}

func TestArticleTextExcludesInertTemplate(t *testing.T) {
	doc := parse(`<article><p>` + prose + `</p><template><a href="/">Button</a></template></article>`)
	if got := text(doc.Find("article")); strings.Contains(got, "Button") || !strings.Contains(got, "independent stations") {
		t.Fatalf("inert template entered article: %q", got)
	}
}
