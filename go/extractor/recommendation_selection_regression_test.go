package extractor

import (
	"context"
	"strings"
	"testing"
)

func TestRecommendationCardsDoNotReplaceMainArticle(t *testing.T) {
	main := "Election officials reported that ballot counting continued across several districts after polling stations closed. Observers said the process remained calm, while local teams published turnout figures and described the verification steps used before releasing each update. The commission plans to publish another report later in the evening."
	card := "A cultural organization announced a new mural project for the city center, with artists beginning work on a public wall next month. The project includes workshops for local residents and a small exhibition about the history of the neighborhood. Organizers said the artwork will remain in place through the summer."
	html := `<html><head><title>Election updates</title></head><body><article class="story"><h1>Election updates</h1><div class="storyBody"><div class="innerBody"><p>` + main + `</p></div></div><div class="box type3 moreCategory"><div class="storyCard"><a href="/mural-one"><h2>Mural project</h2><p>` + card + `</p></a></div><div class="storyCard"><a href="/mural-two"><h2>Neighborhood exhibition</h2><p>` + card + `</p></a></div></div></article></body></html>`
	for _, method := range []string{"enhanced_selectors", "readability", "browser_rendering", "learned_pattern"} {
		t.Run(method, func(t *testing.T) {
			input := Input{URL: "https://example.test/election", HTML: html, RenderedHTML: html, Strategy: method, MaxChars: 5000}
			if method == "learned_pattern" {
				input.LearnedSelector = ".innerBody"
			}
			result := Extract(context.Background(), input)
			if result.State != "extracted" || !strings.Contains(result.Content, "ballot counting") || strings.Contains(result.Content, "mural project") {
				t.Fatalf("recommendation card selected with %s: state=%s content=%q", method, result.State, result.Content)
			}
		})
	}
}

func TestRecommendationGridWithUnclassifiedArticleChildrenIsPruned(t *testing.T) {
	main := "Election officials reported that ballot counting continued across several districts after polling stations closed. Observers said the process remained calm, while local teams published turnout figures and described the verification steps used before releasing each update. The commission plans to publish another report later in the evening."
	card := "A cultural organization announced a new mural project for the city center, with artists beginning work on a public wall next month. The project includes workshops for local residents and a small exhibition about the history of the neighborhood. Organizers said the artwork will remain in place through the summer."
	html := `<html><body><article><h1>Election updates</h1><div class="innerBody"><p>` + main + `</p><p>` + main + `</p></div><div class="box type3 moreCategory"><h2>More stories</h2><div class="ad-slot"></div><article><div class="row g-0"><a href="/mural-one"><img src="/one.jpg"><h3>Mural project</h3></a><p>` + card + `</p></div></article><article><div class="row g-0"><a href="/mural-two"><img src="/two.jpg"><h3>Neighborhood exhibition</h3></a><p>` + card + `</p></div></article></div></article></body></html>`
	result := Extract(context.Background(), Input{URL: "https://example.test/election", HTML: html, Strategy: "auto", MaxChars: 5000})
	if result.State != "extracted" || !strings.Contains(result.Content, "ballot counting") || strings.Contains(result.Content, "mural project") {
		t.Fatalf("article recommendation grid replaced the story: state=%s content=%q", result.State, result.Content)
	}
}

func TestLearnedSelectorInsideRecommendationGridDegradesToMainArticle(t *testing.T) {
	main := "Election officials reported that ballot counting continued across several districts after polling stations closed. Observers said the process remained calm, while local teams published turnout figures and described the verification steps used before releasing each update. The commission plans to publish another report later in the evening."
	card := "A cultural organization announced a new mural project for the city center, with artists beginning work on a public wall next month. The project includes workshops for local residents and a small exhibition about the history of the neighborhood. Organizers said the artwork will remain in place through the summer."
	html := `<html><body><article><h1>Election updates</h1><div class="innerBody"><p>` + main + `</p><p>` + main + `</p></div><div class="box type3 moreCategory"><h2>More stories</h2><article><div class="row g-0"><a href="/mural-one"><img src="/one.jpg"><h3>Mural project</h3></a><p>` + card + `</p></div></article><article><div class="row g-0"><a href="/mural-two"><img src="/two.jpg"><h3>Neighborhood exhibition</h3></a><p>` + card + `</p></div></article></div></article></body></html>`
	result := Extract(context.Background(), Input{URL: "https://example.test/election", HTML: html, Strategy: "learned_pattern", LearnedSelector: ".moreCategory article", MaxChars: 5000})
	if result.State != "extracted" || !strings.Contains(result.Content, "ballot counting") || strings.Contains(result.Content, "mural project") {
		t.Fatalf("learned selector inside recommendations was accepted: state=%s content=%q", result.State, result.Content)
	}
}

func TestEditorialRelatedWorkIsNotPrunedByNameAlone(t *testing.T) {
	related := "Researchers published an analysis of prior field studies and explained how those results informed the current survey. The authors compared sampling methods, documented the limitations of each source, and linked the evidence to their conclusions. Their review gives readers enough detail to assess the research history."
	html := `<html><body><article class="related-work"><h1>Related Work</h1><p><a href="https://example.test/paper">Earlier paper</a></p><p>` + related + `</p></article></body></html>`
	result := Extract(context.Background(), Input{URL: "https://example.test/research", HTML: html, Strategy: "enhanced_selectors", MaxChars: 3000})
	if result.State != "extracted" || !strings.Contains(result.Content, "sampling methods") {
		t.Fatalf("editorial section with an innocuous class was pruned: state=%s content=%q", result.State, result.Content)
	}
}

func TestNestedWrappersInPopularArticleAreNotMistakenForCardGrid(t *testing.T) {
	prose := "Researchers published a detailed account of the regional water survey and described how samples were collected at independent stations. They compared measurements across several seasons, explained the limitations of the equipment, and released the calibration procedure with reproducible data. The report also outlines how neighboring laboratories can repeat the analysis and compare results with earlier observations."
	html := `<html><body><article class="popular-article"><div><div><h1>Water survey results</h1><a href="/author">Byline</a></div><p>` + prose + `</p></div><p>` + prose + `</p></article></body></html>`
	for _, method := range []string{"auto", "browser_rendering", "enhanced_selectors", "learned_pattern"} {
		t.Run(method, func(t *testing.T) {
			input := Input{URL: "https://example.test/water", HTML: html, RenderedHTML: html, Strategy: method, MaxChars: 3000}
			if method == "learned_pattern" {
				input.LearnedSelector = ".popular-article"
			}
			result := Extract(context.Background(), input)
			if result.State != "extracted" || !strings.Contains(result.Content, "regional water survey") {
				t.Fatalf("nested editorial wrappers were treated as a card grid: state=%s content=%q", result.State, result.Content)
			}
		})
	}
}
