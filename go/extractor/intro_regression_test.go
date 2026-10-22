package extractor

import (
	"context"
	"strings"
	"testing"
)

func TestEditorialBodyExcludesSurroundingControls(t *testing.T) {
	for _, method := range []string{"auto", "readability"} {
		h := `<html><head><meta property="og:title" content="Research report"></head><body><article><header><h1>Research report</h1><p>A carefully researched introduction explains the investigation before readers examine the detailed scientific evidence.</p><div>Share post Playback speed Author biography</div></header><div class="body markup"><p>` + prose + `</p><p>Additional independent observations provide necessary context and explain the experimental uncertainty in this research.</p></div><div>Follow topics and authors</div></article></body></html>`
		r := Extract(context.Background(), Input{URL: "https://example.test/report", HTML: h, Strategy: method})
		if !strings.Contains(r.Content, "carefully researched introduction") || !strings.Contains(r.Content, "Additional independent observations") {
			t.Fatalf("lost prose in %s: %s", method, r.Content)
		}
		for _, ui := range []string{"Playback speed", "Author biography", "Follow topics"} {
			if strings.Contains(r.Content, ui) {
				t.Fatalf("retained %s in %s", ui, method)
			}
		}
	}
}

func TestPermalinkHeadingKeepsSourceOrder(t *testing.T) {
	h := `<article itemprop="text"><p>` + prose + `</p><div><h3>Required setup step</h3><a href="#setup" aria-label="Permalink"><svg></svg></a></div><p>Finish setup by reading the required instructions carefully before connecting this experimental device.</p></article>`
	r := Extract(context.Background(), Input{URL: "https://example.test/readme", HTML: h, Strategy: "readability"})
	before, heading, after := strings.Index(r.Content, prose[:40]), strings.Index(r.Content, "Required setup step"), strings.Index(r.Content, "Finish setup")
	if before < 0 || heading < before || after < heading {
		t.Fatalf("heading lost or reordered: %s", r.Content)
	}
}

func TestEditorialBodyKeepsBookmarkExcerptNotCardMetadata(t *testing.T) {
	for _, method := range []string{"auto", "readability"} {
		h := `<html><head><meta property="og:title" content="Daily research"></head><body><article><header><h1>Daily research</h1><a href="/">Latest Daily Newsletter</a><div class="post-share"><a href="#/share">Share control</a></div></header><div class="real-body"><p>` + prose + `</p><p>Author recommends independent research and related resources to interested readers.</p><figure class="kg-bookmark-card"><a href="https://example.test/related"><div class="kg-bookmark-title">Related research title</div><div class="kg-bookmark-description">Related research excerpt with useful scientific background.</div><div class="kg-bookmark-metadata"><span class="kg-bookmark-author">Card publisher metadata</span></div></a></figure></div><section class="next-story">Next edition unrelated material</section></article></body></html>`
		r := Extract(context.Background(), Input{URL: "https://example.test/daily", HTML: h, Strategy: method})
		for _, want := range []string{"Related research title", "Related research excerpt", "Author recommends independent research"} {
			if !strings.Contains(r.Content, want) {
				t.Fatalf("%s lost %q: %s", method, want, r.Content)
			}
		}
		for _, bad := range []string{"Latest Daily Newsletter", "Share control", "Card publisher metadata", "Next edition"} {
			if strings.Contains(r.Content, bad) {
				t.Fatalf("%s retained %q", method, bad)
			}
		}
	}
}

func TestRestoreIntroduction(t *testing.T) {
	lead := "This introduction explains the investigation and its important context before the detailed reporting begins."
	body := "Independent researchers examined the evidence carefully and published their observations for public scrutiny."
	for _, h := range []string{
		`<article><h1>Investigation</h1><div><h2>` + lead + `</h2><div><p>` + body + `</p></div></div></article>`,
		`<article><header><h2>Investigation</h2><div class="subtitle-random">` + lead + `</div></header><div><p>` + body + `</p></div></article>`,
	} {
		got := restoreIntroduction(parse(h), body, "Investigation")
		if got != lead+" "+body {
			t.Fatalf("missing introduction: %q", got)
		}
		if restoreIntroduction(parse(h), got, "Investigation") != got {
			t.Fatal("duplicate introduction")
		}
	}
	for _, h := range []string{
		`<article><nav><h2>` + lead + `</h2></nav><div><p>` + body + `</p></div></article>`,
		`<article><h2>` + lead + `</h2><div><p>Unselected unrelated content with a sufficiently long paragraph.</p></div></article><article><p>` + body + `</p></article>`,
		`<article><h1>Investigation</h1><figure><h2>` + lead + `</h2></figure><p>` + body + `</p></article>`,
	} {
		if got := restoreIntroduction(parse(h), body, "Investigation"); got != body {
			t.Fatalf("restored unrelated material: %q", got)
		}
	}
}
