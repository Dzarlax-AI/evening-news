package extractor

import (
	"context"
	"fmt"
	"strings"
	"testing"
)

func TestQualityPreservesLongTranscript(t *testing.T) {
	var b strings.Builder
	for i := 0; i < 60; i++ {
		fmt.Fprintf(&b, "Speaker discusses research topic%d and explains how independent laboratories compare evidence with practical engineering constraints. The audience asks about measurement uncertainty, calibration procedures, field observations, and future experiments. ", i)
		b.WriteString(prose)
	}
	if !good(b.String(), false, 50000) {
		t.Fatal("substantive long transcript rejected for global vocabulary reuse")
	}
}

func TestQualityRejectsMembershipAcrossMethods(t *testing.T) {
	modal := "Independent analysis membership. Choose your edge. Signal costs $69 / year billed annually. Weekly reports and your own cloud computer are included. Insider costs $199 / year billed annually with additional research tools. Ultimate costs $299 / year and offers priority support for research projects. Cancel anytime. AI token usage is billed separately. Continue with free."
	for _, method := range []string{"enhanced_selectors", "readability", "browser_rendering", "enhanced_heuristics", "json_ld", "open_graph", "learned_pattern"} {
		t.Run(method, func(t *testing.T) {
			h := `<html><head><meta property="og:description" content="` + modal + `"><script type="application/ld+json">{"@type":"Article","articleBody":"` + modal + `"}</script></head><body><article class="story"><p>` + modal + `</p></article></body></html>`
			r := Extract(context.Background(), Input{URL: "https://example.test/story", HTML: h, RenderedHTML: h, Strategy: method, LearnedSelector: ".story"})
			if r.Content != "" {
				t.Fatalf("accepted membership UI: %s", r.Content)
			}
		})
	}
}

func TestQualityRemovesStructuralBoilerplatePreservesByline(t *testing.T) {
	h := `<article><h2>A useful scientific introduction</h2><p>By Research Reporter, Oct 15, 2026.</p><p>` + prose + `</p><div class="newsletter-signup">Subscribe now for more updates.</div><footer>Privacy policy. All rights reserved.</footer></article>`
	s := text(parse(h).Find("article"))
	if strings.Contains(s, "Privacy") || strings.Contains(s, "updates") {
		t.Fatalf("boilerplate retained: %s", s)
	}
	if !strings.Contains(s, "Oct 15") || !strings.Contains(s, "scientific introduction") || !good(s, false, 50000) {
		t.Fatalf("legitimate introduction/byline lost: %s", s)
	}
}

func TestQualityPreservesEditorialSubscriptionDiscussion(t *testing.T) {
	for _, s := range []string{
		prose + " The investigation compares a $69 / year plan billed annually and explains why readers subscribe to independent journalism.",
		prose + " Researchers examined the privacy policy alongside the terms of service to identify inconsistencies in data retention promises.",
		prose + " The reporter criticized the request to follow topics and authors in a personalized homepage feed because it obscured independent reporting.",
	} {
		if !good(s, false, 50000) {
			t.Fatalf("editorial discussion rejected: %s", s)
		}
	}
}

func TestQualityRejectsNestedDialogCandidates(t *testing.T) {
	h := `<dialog><section class="plan"><p>Signal costs $69 / year billed annually. ` + prose + `</p><a href="/checkout">Subscribe</a></section></dialog>`
	d := parse(h)
	for _, q := range []string{"dialog", "section", "p"} {
		if s := text(d.Find(q)); s != "" {
			t.Fatalf("dialog descendant %s leaked: %s", q, s)
		}
	}
}

func TestQualityRejectsDuplicatePagination(t *testing.T) {
	words := strings.Fields(prose + " " + prose)
	chunk := strings.Join(words[:100], " ") + " "
	if good(strings.Repeat(chunk, 8), false, 50000) {
		t.Fatal("duplicate pagination accepted")
	}
}

func TestQualityPrunesRecommendationControlsPreservingShortParagraphs(t *testing.T) {
	for _, ui := range []string{
		`<div role="region" aria-label="Recent episodes"><a href="/one">Previous episode</a><a href="/two">Another episode</a></div>`,
		`<div><span><strong>Follow topics and authors</strong> from this story.</span><button>Follow</button></div>`,
	} {
		paragraphs := `<p>Researchers inspected river sensors.</p><p>Four stations supplied independent readings.</p><p>Heavy rain affected calibration.</p><p>Electrical interference distorted results.</p><p>Laboratories published reproducible procedures.</p><p>The public archive documents limitations.</p>`
		d := parse(`<article>` + paragraphs + ui + `</article>`)
		s := text(d.Find("article"))
		if !strings.Contains(s, "public archive") || !strings.Contains(s, "Researchers inspected") || !good(s, false, 50000) {
			t.Fatalf("short article paragraphs lost: %s", s)
		}
		if strings.Contains(s, "Follow topics") || strings.Contains(s, "Previous episode") {
			t.Fatalf("widget retained: %s", s)
		}
		if !strings.Contains(d.Text(), "Previous episode") && !strings.Contains(d.Text(), "Follow topics") {
			t.Fatal("original document mutated")
		}
	}
}
