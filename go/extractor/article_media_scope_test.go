package extractor

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func mediaViews(media []Media) (raw, selected []string) {
	for _, m := range media {
		raw = append(raw, m.URL)
		if m.Data["article_body_scope"] != "outside" && !(m.Data["width"] == 1 && m.Data["height"] == 1) && m.Data["decorative"] != true {
			selected = append(selected, m.URL)
		}
	}
	return
}
func TestWebMediaPreservesRollbackAndEvidence(t *testing.T) {
	page := `<html><body><nav><img src="/logo.png"></nav><article><p>` + prose + `</p><figure><img src="/cover.jpg" width="900" height="600" data-canonical-src="/cover-original.jpg"></figure><img src="/pixel.png" width="1" height="1"><img src="/icon.svg" aria-hidden="true"><img src="/chart.svg"><video src="/clip.mp4"></video><a href="/report.pdf">Report</a><aside><img src="/related.jpg"></aside></article><img src="/author.jpg"></body></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, SourceType: "web"})
	if r.State != "extracted" {
		t.Fatalf("extraction: %+v", r)
	}
	raw, selected := mediaViews(r.Media)
	if strings.Join(raw, ",") != "https://example.test/logo.png,https://example.test/cover.jpg,https://example.test/pixel.png,https://example.test/icon.svg,https://example.test/chart.svg,https://example.test/related.jpg,https://example.test/author.jpg,https://example.test/clip.mp4,https://example.test/report.pdf" {
		t.Fatalf("raw: %v", raw)
	}
	if strings.Join(selected, ",") != "https://example.test/cover.jpg,https://example.test/chart.svg,https://example.test/clip.mp4,https://example.test/report.pdf" {
		t.Fatalf("selected: %v", selected)
	}
	if r.Media[1].Data["canonical_url"] != "https://example.test/cover-original.jpg" || r.Media[1].Data["width"] != 900 {
		t.Fatalf("cover evidence: %+v", r.Media[1])
	}
}

func TestWebMediaNoTrustedBodyKeepsCandidatesUnmarked(t *testing.T) {
	page := `<html><head><meta name="description" content="` + prose + `"></head><body><img src="/author.jpg"></body></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, SourceType: "web", Strategy: "meta_description"})
	if r.State != "extracted" || len(r.Media) != 1 || r.Media[0].Data["article_body_scope"] != nil {
		t.Fatalf("unknown scope: %+v", r)
	}
}

func TestWebMediaReadabilityUsesSelectedNode(t *testing.T) {
	page := `<html><body><nav><img src="/logo.png"></nav><article><p>` + prose + `</p><p>` + prose + `</p><figure><img src="/editorial.jpg"></figure></article><aside><img src="/related.jpg"></aside></body></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, SourceType: "web", Strategy: "readability"})
	_, selected := mediaViews(r.Media)
	if r.State != "extracted" || strings.Join(selected, ",") != "https://example.test/editorial.jpg" {
		t.Fatalf("readability: %+v", r)
	}
}

func TestHeuristicsRetainSelectedDOMMedia(t *testing.T) {
	page := `<html><body><div class="story-body"><p>` + prose + `</p><img src="/editorial.jpg"></div><nav><img src="/logo.png"></nav></body></html>`
	for _, strategy := range []string{"enhanced_heuristics", "browser_heuristics"} {
		r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, RenderedHTML: page, SourceType: "web", Strategy: strategy})
		_, selected := mediaViews(r.Media)
		if r.State != "extracted" || strings.Join(selected, ",") != "https://example.test/editorial.jpg" {
			t.Fatalf("%s: %+v selected=%v", strategy, r, selected)
		}
	}
}

func TestWebMediaDuplicateOutsideEvidenceDoesNotTaintBody(t *testing.T) {
	page := `<html><body><nav><img src="/photo.jpg" width="1" height="1"></nav><article><p>` + prose + `</p><img src="/photo.jpg" width="800" height="600"></article></body></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, SourceType: "web"})
	if len(r.Media) != 1 || r.Media[0].Data["width"] != 800 || r.Media[0].Data["article_body_scope"] != nil {
		t.Fatalf("body evidence: %+v", r.Media)
	}
}

func TestLongAltDoesNotLoseKnownScope(t *testing.T) {
	page := `<html><body><nav><img src="/logo.png"></nav><article><p>` + prose + `</p><img src="/cover.jpg" alt="` + strings.Repeat("a", 1100) + `"></article></body></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, SourceType: "web"})
	_, selected := mediaViews(r.Media)
	if strings.Join(selected, ",") != "https://example.test/cover.jpg" || !strings.Contains(strings.Join(r.Trace, ","), "media_truncated") {
		t.Fatalf("scope or trace: %+v", r)
	}
}

func TestHeuristicMultipleContributingBlocks(t *testing.T) {
	page := `<html><body><section class="story">The council approved repairs to the old bridge after detailed inspections found several corroded joints. Work starts next week.<img src="/one.jpg"></section><section class="story">Engineers will install supports and keep one lane open while crews replace the damaged steel. Residents will receive a schedule.<img src="/two.jpg"></section><nav><img src="/logo.png"></nav></body></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, SourceType: "web", Strategy: "enhanced_heuristics"})
	_, selected := mediaViews(r.Media)
	if r.State != "extracted" || strings.Join(selected, ",") != "https://example.test/one.jpg,https://example.test/two.jpg" {
		t.Fatalf("multi-fragment: %+v selected=%v", r, selected)
	}
}

func TestMediaMetadataBudgetPreservesLegacyURLs(t *testing.T) {
	var plain, rich strings.Builder
	for i := 0; i < 235; i++ {
		u := fmt.Sprintf("https://example.test/%03d.jpg", i)
		fmt.Fprintf(&plain, `<img src="%s" alt="%s">`, u, strings.Repeat("a", 1000))
		fmt.Fprintf(&rich, `<img src="%s" alt="%s" width="900" height="600" data-canonical-src="https://example.test/canonical/%03d.jpg">`, u, strings.Repeat("a", 1000), i)
	}
	old, oldTruncated := boundedArticleMedia(plain.String(), "https://example.test/story")
	got, truncated := boundedArticleMedia(rich.String(), "https://example.test/story")
	if oldTruncated || !truncated || len(old) != len(got) {
		t.Fatalf("legacy count %d truncated %v, rich count %d truncated %v", len(old), oldTruncated, len(got), truncated)
	}
	for i := range old {
		if old[i].URL != got[i].URL {
			t.Fatalf("URL %d differs", i)
		}
	}
	b, _ := json.Marshal(got)
	if len(b) > maxArticleMediaJSONBytes {
		t.Fatalf("size %d", len(b))
	}
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: article(prose + rich.String()), SourceType: "web"})
	if len(r.Media) != len(old) || !strings.Contains(strings.Join(r.Trace, ","), "media_truncated") {
		t.Fatalf("bounded extraction count=%d trace=%v", len(r.Media), r.Trace)
	}
}

func TestScopeEvidenceSurvivesJSONTransport(t *testing.T) {
	page := `<html><body><nav><img src="/logo.png"></nav><article><p>` + prose + `</p><img src="/pixel.png" width="1" height="1"></article></body></html>`
	r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: page, SourceType: "web"})
	wire, err := json.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Result
	if err := json.Unmarshal(wire, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Media) != 2 || decoded.Media[0].Data["article_body_scope"] != "outside" || decoded.Media[1].Data["width"] != float64(1) {
		t.Fatalf("wire evidence: %+v", decoded.Media)
	}
}

func TestBlicArticleExcludesAuthorAndLinksPictureVariants(t *testing.T) {
	page := `<html><body><article><p>` + prose + `</p><figure><picture><source type="image/webp" srcset="https://ocdn.eu/photo-small.webp 1x, https://ocdn.eu/photo-small.webp 2x"><img src="https://ocdn.eu/photo.jpg"></picture></figure><div class="article__author"><img src="https://ocdn.eu/logo.svg"></div><div class="article__images"><img src="https://ocdn.eu/photo-small.webp"><img src="https://ocdn.eu/second.jpg"></div></article></body></html>`
	r := Extract(context.Background(), Input{URL: "https://www.blic.rs/story", HTML: page, SourceType: "web"})
	if len(r.Media) != 4 || r.Media[1].Data["article_body_scope"] != "outside" || r.Media[0].Data["canonical_url"] != "https://ocdn.eu/photo.jpg" || r.Media[2].Data["canonical_url"] != "https://ocdn.eu/photo.jpg" || r.Media[3].Data["canonical_url"] != nil {
		t.Fatalf("media: %+v", r.Media)
	}
}

func TestPictureEvidenceSkipsArtDirectionAndSignedURLs(t *testing.T) {
	page := `<article><p>` + prose + `</p><picture><source type="image/webp" media="(max-width: 500px)" srcset="https://ocdn.eu/mobile.jpg 1x"><img src="https://ocdn.eu/wide.jpg"></picture><img src="https://ocdn.eu/mobile.jpg"><picture><source type="image/webp" srcset="https://ocdn.eu/other.jpg?sig=x 1x"><img src="https://ocdn.eu/lead.jpg"></picture><img src="https://ocdn.eu/other.jpg?sig=x"></article>`
	r := Extract(context.Background(), Input{URL: "https://www.blic.rs/story", HTML: page, SourceType: "web"})
	for _, m := range r.Media {
		if m.Data["canonical_url"] != nil {
			t.Fatalf("unsafe association: %+v", r.Media)
		}
	}
}

func TestPictureEvidenceRejectsInvalidDescriptor(t *testing.T) {
	page := `<article><p>` + prose + `</p><picture><source type="image/webp" srcset="https://ocdn.eu/detail.webp bogus"><img src="https://ocdn.eu/cover.jpg"></picture><img src="https://ocdn.eu/detail.webp"></article>`
	r := Extract(context.Background(), Input{URL: "https://www.blic.rs/story", HTML: page, SourceType: "web"})
	if len(r.Media) != 2 || r.Media[0].Data["canonical_url"] != nil || r.Media[1].Data["canonical_url"] != nil {
		t.Fatalf("invalid descriptor linked: %+v", r.Media)
	}
}

func TestPictureEvidenceRejectsAmbiguousCommaInURL(t *testing.T) {
	page := `<article><p>` + prose + `</p><picture><source type="image/webp" srcset="https://ocdn.eu/path,https://ocdn.eu/detail.webp 1x"><img src="https://ocdn.eu/cover.jpg"></picture><img src="https://ocdn.eu/detail.webp"></article>`
	r := Extract(context.Background(), Input{URL: "https://www.blic.rs/story", HTML: page, SourceType: "web"})
	if len(r.Media) != 2 || r.Media[0].Data["canonical_url"] != nil || r.Media[1].Data["canonical_url"] != nil {
		t.Fatalf("ambiguous srcset linked: %+v", r.Media)
	}
}

func TestEditorialCoverEvidenceRequiresTrustedArticle(t *testing.T) {
	page := `<html><head><meta property="og:type" content="article"><meta property="og:image" content="https://cdn.test/cover.jpg"></head><body><article><p>` + prose + `</p></article></body></html>`
	r := Extract(context.Background(), Input{URL: "https://aisecret.us/story", HTML: page, SourceType: "web"})
	if r.MediaEvidence == nil || r.MediaEvidence.EditorialCover.URL != "https://cdn.test/cover.jpg" || r.MediaEvidence.EditorialCover.ArticleURL != "https://aisecret.us/story" {
		t.Fatalf("missing evidence: %+v", r.MediaEvidence)
	}
	for _, tc := range []struct{ url, html string }{{"https://aisecret.us/list", strings.Replace(page, `content="article"`, `content="website"`, 1)}, {"https://other.test/story", page}} {
		got := Extract(context.Background(), Input{URL: tc.url, HTML: tc.html, SourceType: "web"})
		if got.MediaEvidence != nil {
			t.Fatalf("untrusted evidence: %+v", got.MediaEvidence)
		}
	}
	noBody := `<html><head><meta property="og:type" content="article"><meta property="og:image" content="https://cdn.test/cover.jpg"><meta name="description" content="` + prose + `"></head><body><img src="/logo.svg"></body></html>`
	got := Extract(context.Background(), Input{URL: "https://aisecret.us/story", HTML: noBody, SourceType: "web", Strategy: "meta_description"})
	if got.MediaEvidence != nil {
		t.Fatalf("metadata-only fallback produced evidence: %+v", got.MediaEvidence)
	}
}

func TestN1ServiceControlsAndHuggingFaceBadgesDoNotHideEditorialSVG(t *testing.T) {
	tests := []struct{ url, body string }{
		{"https://n1info.rs/story", `<div class="social-share-bar"><img src="/facebook.svg"></div><section class="live-blog"><div class="header"><img src="/static/images/footer.avif"></div></section><img src="/media/images/Follow_Us.original.svg"><img src="/chart.svg">`},
		{"https://huggingface.co/team/model", `<img src="https://cdn-avatars.huggingface.co/avatar.png"><img src="https://img.shields.io/badge/test.svg"><img src="/chart.svg">`},
	}
	for _, tc := range tests {
		r := Extract(context.Background(), Input{URL: tc.url, HTML: article(prose + tc.body), SourceType: "web"})
		_, selected := mediaViews(r.Media)
		if len(selected) != 1 || !strings.HasSuffix(selected[0], "/chart.svg") {
			t.Fatalf("%s selected=%v", tc.url, selected)
		}
	}
}

func TestHuggingFaceBlobWrapperRequiresMatchingResolveImage(t *testing.T) {
	page := `<article><p>` + prose + `</p><a href="/owner/model/blob/main/plot.png"><img src="/owner/model/resolve/main/plot.png"></a><a href="/owner/model/blob/main/other.png"><img src="/owner/model/resolve/main/different.png"></a><a href="/owner/model/blob/main/signed.png?x=1"><img src="/owner/model/resolve/main/signed.png"></a></article>`
	r := Extract(context.Background(), Input{URL: "https://huggingface.co/owner/model", HTML: page, SourceType: "web"})
	got := map[string]bool{}
	for _, m := range r.Media {
		if m.Data["html_wrapper"] == true {
			got[m.URL] = true
		}
	}
	if len(got) != 1 || !got["https://huggingface.co/owner/model/blob/main/plot.png"] {
		t.Fatalf("wrapper evidence=%v media=%+v", got, r.Media)
	}
}
