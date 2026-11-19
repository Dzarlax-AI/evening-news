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
	if r.State != "extracted" || len(r.Media) != 1 || r.Media[0].Data["article_body_scope"] != nil ||
		r.MediaEvidence == nil || r.MediaEvidence.ArticleScope != "unknown" || r.ImageURL != "" {
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

func TestArticleExcludesSemanticAuthorAndLinksPictureVariants(t *testing.T) {
	page := `<html><body><article><p>` + prose + `</p><figure><picture><source type="image/webp" srcset="https://images.example.test/photo-small.webp 1x, https://images.example.test/photo-small.webp 2x"><img src="https://images.example.test/photo.jpg"></picture></figure><div itemprop="author"><img src="https://images.example.test/logo.svg"></div><div class="article__images"><img src="https://images.example.test/photo-small.webp"><img src="https://images.example.test/second.jpg"></div></article></body></html>`
	r := Extract(context.Background(), Input{URL: "https://publisher.example.test/story", HTML: page, SourceType: "web"})
	if len(r.Media) != 4 || r.Media[1].Data["article_body_scope"] != "outside" || r.Media[0].Data["canonical_url"] != "https://images.example.test/photo.jpg" || r.Media[2].Data["canonical_url"] != "https://images.example.test/photo.jpg" || r.Media[3].Data["canonical_url"] != nil {
		t.Fatalf("media: %+v", r.Media)
	}
}

func TestPictureEvidenceSkipsArtDirectionAndSignedURLs(t *testing.T) {
	page := `<article><p>` + prose + `</p><picture><source type="image/webp" media="(max-width: 500px)" srcset="https://images.example.test/mobile.jpg 1x"><img src="https://images.example.test/wide.jpg"></picture><img src="https://images.example.test/mobile.jpg"><picture><source type="image/webp" srcset="https://images.example.test/other.jpg?sig=x 1x"><img src="https://images.example.test/lead.jpg"></picture><img src="https://images.example.test/other.jpg?sig=x"></article>`
	r := Extract(context.Background(), Input{URL: "https://publisher.example.test/story", HTML: page, SourceType: "web"})
	for _, m := range r.Media {
		if m.Data["canonical_url"] != nil {
			t.Fatalf("unsafe association: %+v", r.Media)
		}
	}
}

func TestPictureEvidenceRejectsInvalidDescriptor(t *testing.T) {
	page := `<article><p>` + prose + `</p><picture><source type="image/webp" srcset="https://images.example.test/detail.webp bogus"><img src="https://images.example.test/cover.jpg"></picture><img src="https://images.example.test/detail.webp"></article>`
	r := Extract(context.Background(), Input{URL: "https://publisher.example.test/story", HTML: page, SourceType: "web"})
	if len(r.Media) != 2 || r.Media[0].Data["canonical_url"] != nil || r.Media[1].Data["canonical_url"] != nil {
		t.Fatalf("invalid descriptor linked: %+v", r.Media)
	}
}

func TestPictureEvidenceRejectsAmbiguousCommaInURL(t *testing.T) {
	page := `<article><p>` + prose + `</p><picture><source type="image/webp" srcset="https://images.example.test/path,https://images.example.test/detail.webp 1x"><img src="https://images.example.test/cover.jpg"></picture><img src="https://images.example.test/detail.webp"></article>`
	r := Extract(context.Background(), Input{URL: "https://publisher.example.test/story", HTML: page, SourceType: "web"})
	if len(r.Media) != 2 || r.Media[0].Data["canonical_url"] != nil || r.Media[1].Data["canonical_url"] != nil {
		t.Fatalf("ambiguous srcset linked: %+v", r.Media)
	}
}

func TestEditorialCoverEvidenceRequiresTrustedArticle(t *testing.T) {
	const page = "https://publisher.example.test/story"
	const cover = "https://cdn.test/cover.jpg"
	articleHTML := `<html><head><meta property="og:type" content="article"><meta property="og:image" content="` + cover + `"></head><body><article><p>` + prose + `</p><img src="` + cover + `"></article></body></html>`
	r := Extract(context.Background(), Input{URL: page, HTML: articleHTML, SourceType: "web"})
	if r.MediaEvidence == nil || r.MediaEvidence.EditorialCover.URL != cover || r.MediaEvidence.EditorialCover.ArticleURL != page {
		t.Fatalf("missing corroborated cover: %+v", r.MediaEvidence)
	}
	for _, html := range []string{
		strings.Replace(articleHTML, `content="article"`, `content="website"`, 1),
		strings.Replace(articleHTML, `<img src="`+cover+`">`, `<aside><img src="`+cover+`"></aside>`, 1),
		strings.Replace(articleHTML, `<img src="`+cover+`">`, `<img src="/some-other-photo.jpg">`, 1),
	} {
		got := Extract(context.Background(), Input{URL: page, HTML: html, SourceType: "web"})
		if got.MediaEvidence != nil && got.MediaEvidence.EditorialCover.URL != "" {
			t.Fatalf("untrusted cover evidence: %+v", got.MediaEvidence)
		}
	}
}

func TestArticleScopeKeepsEditorialSVGAndExcludesShareControls(t *testing.T) {
	body := prose + `<div class="social-share-bar"><img src="/social.svg"></div><img src="/chart.svg">`
	r := Extract(context.Background(), Input{URL: "https://publisher.example.test/story", HTML: article(body), SourceType: "web"})
	_, selected := mediaViews(r.Media)
	if len(selected) != 1 || !strings.HasSuffix(selected[0], "/chart.svg") {
		t.Fatalf("selected=%v", selected)
	}
}

func TestEditorialMediaUsesProseAndCardBoundaries(t *testing.T) {
	const story = "https://fixture.test/report"
	const before = `<p><!--<slot left 123 media>--></p>`
	const after = `<div><img src="/inline.jpg"><div>Bridge reconstruction</div></div>`
	source := `<meta property="og:type" content="article"><meta property="og:image" content="/wrong.jpg">
	<div id="content"><div><a href="/wrong-story"><img src="/wrong.jpg">An unrelated adjacent article</a></div>
	<article><audio><source src="/narration.mp3" type="audio/mpeg"></audio><h1>Bridge repairs</h1>
	<div class="renamed-prose"><p>` + prose + `</p>` + before +
		`<div class="renamed-widgets"><div class="alpha"><img src="/second.jpg"><h3><a href="/other">An unrelated older report</a></h3></div>
		<div class="beta"><img src="/wrong.jpg"><h3><a href="/third">A different regional story</a></h3></div></div>
		<p>` + prose + `</p>
		<figure><a href="/full-size?id=42"><img src="/first.jpg"></a><figcaption>An editorial photograph</figcaption></figure>
		<figure><a href="/second-large.jpg"><img src="/second.jpg"></a></figure>
		</div></article></div>`
	for _, tc := range []struct {
		name, markup, cover string
		pending             bool
		images              []string
	}{
		{"static", source, "/first.jpg", true, []string{"/first.jpg", "/second.jpg", "/second-large.jpg"}},
		{"rendered", strings.Replace(source, before, after, 1), "/inline.jpg", false, []string{"/inline.jpg", "/first.jpg", "/second.jpg", "/second-large.jpg"}},
		{"text_with_no_placeholder", strings.Replace(source, before, "<p></p>", 1), "/first.jpg", false, []string{"/first.jpg", "/second.jpg", "/second-large.jpg"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Extract(context.Background(), Input{URL: story, HTML: tc.markup})
			if got.State != "extracted" || got.PendingEditorialMedia != tc.pending ||
				got.ImageURL != "https://fixture.test"+tc.cover || got.MediaEvidence == nil ||
				got.MediaEvidence.ArticleScope != "verified" || got.MediaEvidence.EditorialCover.URL != "" {
				t.Fatalf("state=%s pending=%t cover=%q evidence=%+v", got.State, got.PendingEditorialMedia, got.ImageURL, got.MediaEvidence)
			}
			images := make(map[string]int)
			audio := 0
			for _, candidate := range got.Media {
				order, _ := candidate.Data["editorial_order"].(int)
				switch {
				case candidate.URL == "https://fixture.test/narration.mp3":
					if order > 0 && candidate.Type == "audio" && candidate.MediaType == "audio/mpeg" {
						audio++
					}
				case order > 0 && candidate.Type == "image":
					images[strings.TrimPrefix(candidate.URL, "https://fixture.test")] = order
				}
			}
			if audio != 1 || len(images) != len(tc.images) {
				t.Fatalf("images=%v, audio=%d", images, audio)
			}
			// Full-size image links are scanned separately from img occurrences.
			for index, image := range tc.images {
				if images[image] == 0 || (index > 0 && images[image] <= images[tc.images[index-1]]) {
					t.Fatalf("editorial image order=%v, want %v", images, tc.images)
				}
			}
		})
	}
}

func TestEditorialVideoInFigureBesideProse(t *testing.T) {
	const page = "https://fixture.test/report"
	markup := `<article><h1>Bridge repairs</h1><div><p>` + prose + `</p></div>
		<figure><video src="/bridge.mp4"><source src="/bridge.webm" type="video/webm"></video>
		<figcaption>Bridge repairs in progress</figcaption></figure>
		<aside><figure><video src="/related.mp4"></video></figure></aside>
		</article>`
	got := Extract(context.Background(), Input{URL: page, HTML: markup})
	if got.State != "extracted" || got.MediaEvidence == nil || got.MediaEvidence.ArticleScope != "verified" {
		t.Fatalf("state=%s evidence=%+v", got.State, got.MediaEvidence)
	}
	var videos []string
	for _, candidate := range got.Media {
		if order, _ := candidate.Data["editorial_order"].(int); candidate.Type == "video" && order > 0 {
			videos = append(videos, candidate.URL)
		}
	}
	if want := "https://fixture.test/bridge.mp4,https://fixture.test/bridge.webm"; strings.Join(videos, ",") != want {
		t.Fatalf("editorial videos=%v, want %s", videos, want)
	}
}

func TestEditorialMediaKeepsArticleQueryIdentity(t *testing.T) {
	const page = "https://fixture.test/story?id=1&edition=evening"
	markup := `<article><h1>Bridge repairs</h1><p>` + prose + `</p>
		<figure><a href="?id=2&amp;edition=evening"><img src="/other.jpg"></a></figure>
		<a href="?edition=evening&amp;id=1&amp;utm_source=newsletter#photo"><img id="photo" src="/editorial.jpg"></a>
		</article>`
	got := Extract(context.Background(), Input{URL: page, HTML: markup})
	_, selected := mediaViews(got.Media)
	if want := "https://fixture.test/editorial.jpg"; got.ImageURL != want || strings.Join(selected, ",") != want {
		t.Fatalf("cover=%q editorial media=%v, want %s", got.ImageURL, selected, want)
	}
}
