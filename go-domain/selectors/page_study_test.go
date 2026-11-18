package selectors

import (
	"context"
	"strings"
	"testing"
)

type studyStub struct{ response, prompt string }

func (s *studyStub) GenerateText(_ context.Context, prompt, _ string) (string, error) {
	s.prompt = prompt
	return s.response, nil
}

func TestArticleStudyValidatesAndKeepsListContract(t *testing.T) {
	text := strings.Repeat("Independent reporting describes an important public decision and its practical consequences. ", 3)
	page := `<html><body><article><header><img class="cover" src="/cover.jpg"></header><div class="body">` + text + `<div class="author">Byline</div></div></article><aside class="related">Related</aside></body></html>`
	g := &studyStub{response: `{"content_selectors":[".cards"],"title_selectors":[],"page_type":"news","page_kind":"article","article_body_selector":"article .body","editorial_cover_selector":"img.cover","exclude_selectors":[".author",".related"]}`}
	got, err := NewAnalyzer(g).Analyze(context.Background(), "https://example.test/story", page, "news", "example.test")
	if err != nil || got.PageKind != "article" || got.ArticleBody != "article .body" || got.EditorialCover != "img.cover" || len(got.ExcludeBlocks) != 2 || len(got.ContentSelectors) != 1 || got.ContentSelectors[0] != ".cards" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if !strings.Contains(g.prompt, "content_selectors continue to select list entries") || !strings.Contains(g.prompt, "article.body") && !strings.Contains(g.prompt, "div.body") {
		t.Fatalf("prompt lacks instructions or DOM outline")
	}
}

func TestStudyUnknownForUnsafeOrNonexistentSelectors(t *testing.T) {
	page := `<html><body><article><div class="body">` + strings.Repeat("Distinct reporting about practical events. ", 6) + `</div><img class="cover" src="/image.png"></article></body></html>`
	for _, body := range []string{"[", "body", ".missing"} {
		g := &studyStub{response: `{"content_selectors":[],"title_selectors":[],"page_type":"news","page_kind":"article","article_body_selector":"` + body + `","editorial_cover_selector":"img.cover","exclude_selectors":[]}`}
		got, err := NewAnalyzer(g).Analyze(context.Background(), "https://example.test/story", page, "news", "example.test")
		if err != nil || got.PageKind != "unknown" || got.ArticleBody != "" {
			t.Fatalf("body=%q got=%+v err=%v", body, got, err)
		}
	}
	g := &studyStub{response: `{"content_selectors":[],"title_selectors":[],"page_type":"news","page_kind":"article","article_body_selector":"article .body","editorial_cover_selector":"img.cover","exclude_selectors":["article"]}`}
	got, _ := NewAnalyzer(g).Analyze(context.Background(), "https://example.test/story", page, "news", "example.test")
	if got.PageKind != "article" || len(got.ExcludeBlocks) != 0 {
		t.Fatalf("unsafe exclusion not removed: %+v", got)
	}
}

func TestArticleStudyDiscardsUnsafeOptionalHints(t *testing.T) {
	page := `<nav><img class="logo" src="/logo.svg"></nav><article><img class="cover" src="/cover.jpg"><div class="body">` + strings.Repeat("Public reporting explains a detailed event. ", 6) + `</div><div class="author">Byline</div></article>`
	g := &studyStub{response: `{"content_selectors":[],"title_selectors":[],"page_type":"news","page_kind":"article","article_body_selector":"article .body","editorial_cover_selector":"img.logo","exclude_selectors":["article","[",".author"]}`}
	got, err := NewAnalyzer(g).Analyze(context.Background(), "https://example.test/story", page, "news", "example.test")
	if err != nil || got.PageKind != "article" || got.ArticleBody != "article .body" || got.EditorialCover != "" || len(got.ExcludeBlocks) != 1 || got.ExcludeBlocks[0] != ".author" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestListingStudyRejectsNavigationLinks(t *testing.T) {
	page := `<nav><a class="card" href="/a">A</a><a class="card" href="/b">B</a></nav>`
	g := &studyStub{response: `{"content_selectors":["a.card"],"title_selectors":[],"page_type":"news","page_kind":"listing"}`}
	got, err := NewAnalyzer(g).Analyze(context.Background(), "https://example.test/list", page, "news", "example.test")
	if err != nil || got.PageKind != "unknown" || eligibleListStudy(got) {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestListingStudyKeepsOnlyValidatedCSSAndLegacyIsAccepted(t *testing.T) {
	page := `<main><section><a class="card" href="/a">A</a><a class="card" href="/b">B</a></section></main>`
	g := &studyStub{response: `{"content_selectors":["a.card","["],"title_selectors":["["],"page_type":"news","page_kind":"listing"}`}
	got, err := NewAnalyzer(g).Analyze(context.Background(), "https://example.test/list", page, "news", "example.test")
	if err != nil || got.PageKind != "listing" || len(got.ContentSelectors) != 1 || got.ContentSelectors[0] != "a.card" || len(got.TitleSelectors) != 0 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	g.response = `{"content_selectors":["article"],"title_selectors":[],"page_type":"news"}`
	got, err = NewAnalyzer(g).Analyze(context.Background(), "https://example.test/list", page, "news", "example.test")
	if err != nil || got.PageKind != "" || got.pageKindProvided {
		t.Fatalf("legacy: %+v %v", got, err)
	}
}

func TestListStudyEligibilityAndSnapshotRevalidation(t *testing.T) {
	if !eligibleListStudy(&Analysis{}) || !eligibleListStudy(&Analysis{pageKindProvided: true, PageKind: "listing"}) || eligibleListStudy(&Analysis{pageKindProvided: true, PageKind: "article"}) || eligibleListStudy(&Analysis{pageKindProvided: true, PageKind: "unknown"}) {
		t.Fatal("unsafe list study eligibility")
	}
	g := &studyStub{response: `{"content_selectors":["a.card"],"title_selectors":[],"page_type":"news","page_kind":"listing"}`}
	a := NewAnalyzer(g)
	page := `<a class="card" href="/a">A</a><a class="card" href="/b">B</a>`
	first, err := a.Analyze(context.Background(), "https://example.test/list", page, "news", "example.test")
	if err != nil || first.PageKind != "listing" {
		t.Fatalf("first=%+v err=%v", first, err)
	}
	second, err := a.Analyze(context.Background(), "https://example.test/list", `<main>No cards</main>`, "news", "example.test")
	if err != nil || second.PageKind != "unknown" || eligibleListStudy(second) {
		t.Fatalf("cached=%+v err=%v", second, err)
	}
}

func TestLongRelatedAsideIsAllowedWithoutRemovingArticleText(t *testing.T) {
	prose := strings.Repeat("The investigation explains a substantial verified public event. ", 6)
	page := `<article><img class="cover" src="/cover.jpg"><div class="body"><p class="ordinary">` + prose + `</p><aside class="article__related">` + prose + `</aside><aside class="unrelated">` + prose + `</aside></div></article>`
	g := &studyStub{response: `{"content_selectors":[],"title_selectors":[],"page_type":"news","page_kind":"article","article_body_selector":"article .body","editorial_cover_selector":"img.cover","exclude_selectors":[".article__related",".ordinary",".unrelated","article"]}`}
	got, err := NewAnalyzer(g).Analyze(context.Background(), "https://example.test/story", page, "news", "example.test")
	if err != nil || got.PageKind != "article" || len(got.ExcludeBlocks) != 1 || got.ExcludeBlocks[0] != ".article__related" || got.EditorialCover != "img.cover" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestRelatedBlockCannotExcludeCover(t *testing.T) {
	page := `<article><div class="promotion"><img class="cover" src="/photo.jpg"></div><div class="body">` + strings.Repeat("Detailed article reporting about an event. ", 5) + `</div></article>`
	g := &studyStub{response: `{"content_selectors":[],"title_selectors":[],"page_type":"news","page_kind":"article","article_body_selector":"article .body","editorial_cover_selector":"img.cover","exclude_selectors":[".promotion"]}`}
	got, err := NewAnalyzer(g).Analyze(context.Background(), "https://example.test/story", page, "news", "example.test")
	if err != nil || got.PageKind != "article" || len(got.ExcludeBlocks) != 0 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestStudyOutlineReportsRepeatedBranches(t *testing.T) {
	page := `<main><section><a class="clear" href="/featured">Featured</a><ul><li><a class="clear" href="/one">One</a></li><li><a class="clear" href="/two">Two</a></li><li><a class="clear" href="/three">Three</a></li></ul></section></main>`
	got, err := PrepareStudyHTML(page)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "section > a.clear [count=1]") || !strings.Contains(got, "ul > li > a.clear [count=3]") {
		t.Fatalf("outline lacks branch counts: %s", got)
	}
	prompt := StudyPrompt("https://example.test", got, "news")
	if !strings.Contains(prompt, "repeated sibling cards") || !strings.Contains(prompt, "not just a single featured card") {
		t.Fatal("prompt lacks listing guidance")
	}
}

func TestNestedArticleBodyFindsOuterHeroButNotOtherCards(t *testing.T) {
	text := strings.Repeat("Independent reporting explains an important event with detail. ", 4)
	page := `<main><article><h1>Headline</h1><div class="hero-image"><img id="hero" src="/hero.jpg"></div><div class="prose"><article><p>` + text + `</p></article></div><article class="card"><img id="nested" src="/nested.jpg"></article><div class="article__related"><img id="related" src="/related.jpg"></div></article><article><img id="sibling" src="/other.jpg"></article></main>`
	for _, tc := range []struct {
		id   string
		want bool
	}{{"hero", true}, {"nested", false}, {"related", false}, {"sibling", false}} {
		g := &studyStub{response: `{"content_selectors":[],"title_selectors":[],"page_type":"news","page_kind":"article","article_body_selector":"main > article > div.prose > article","editorial_cover_selector":"img#` + tc.id + `"}`}
		got, e := NewAnalyzer(g).Analyze(context.Background(), "https://example.test/story", page, "news", "example.test")
		if e != nil || got.PageKind != "article" || (got.EditorialCover != "") != tc.want {
			t.Fatalf("%s got=%+v err=%v", tc.id, got, e)
		}
	}
}

func TestOutlineReservesArticleHeaderImage(t *testing.T) {
	var cards strings.Builder
	for i := 0; i < 300; i++ {
		cards.WriteString(`<li class="news-card"><a class="clear" href="/story"><img class="news-image" src="/card.jpg"></a></li>`)
	}
	page := `<article class="article"><header><h1>Headline</h1><div class="news__image"><picture><img src="/hero.jpg"></picture></div></header><div class="prose">Text</div></article><ul>` + cards.String() + `</ul>`
	got, err := PrepareStudyHTML(page)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, "ARTICLE HEADER IMAGE: article.article > header > div.news__image > picture > img") || !strings.Contains(got, "count=300") {
		t.Fatalf("missing hero or repeated count: %s", got)
	}
	if len([]rune(got)) > 8000 {
		t.Fatalf("outline over budget: %d", len([]rune(got)))
	}
}
