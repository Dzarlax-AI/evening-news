package editorial

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

func syntheticSnapshot(n int) Snapshot {
	s := Snapshot{Date: "2026-10-27", CutoffUTC: time.Date(2026, 10, 27, 18, 0, 0, 0, time.UTC), SiteBaseURL: "https://news.example", EditionURL: "https://news.example/digest/2026-10-27?edition=fixture"}
	for i := 1; i <= n; i++ {
		title := fmt.Sprintf("Событие номер %d", i)
		content := fmt.Sprintf("Подтверждённый фрагмент материала %d.", i)
		source := fmt.Sprintf("Источник %d", i)
		category := "4"
		if i == 5 {
			title = "<script>alert(1)</script> & «заголовок»"
			content = "<img src=x onerror=alert(1)> заявляет: это проверяется."
		}
		if i == 6 {
			title = "Српске вести о мосту"
			content = "Независимый текст на српском језику — фрагмент."
		}
		if i == 7 {
			content = "Источник пересказывает неподтверждённый слух; редакция не подтверждает сообщение."
		}
		if n >= 40 {
			title = strings.Repeat("Новость ", 18) + fmt.Sprintf("номер %d", i)
		}
		s.Articles = append(s.Articles, Article{ID: fmt.Sprint(1000 + i), Title: title, Summary: "", Content: content, SourceName: source, URL: fmt.Sprintf("https://source.example/story/%d?a=1&b=2", i), CategoryID: category, CategoryName: "Технологии"})
	}
	if n >= 8 {
		s.Articles[0].Title = "Город начал строительство моста"
		s.Articles[0].Content = "Город сообщил о начале строительства нового моста через реку."
		s.Articles[1].Title = "Началось строительство нового моста"
		s.Articles[1].Content = "Работы по строительству моста через реку начались сегодня."
	}
	return s
}

func syntheticDraft(s Snapshot) modelDraft {
	d := modelDraft{}
	for i, a := range s.Articles {
		if len(s.Articles) >= 8 && i == 1 {
			continue
		}
		if i == 0 || i == 2 {
			story := modelMainStory{Title: a.Title, Summary: a.Content, PrimaryArticleID: a.ID, ArticleIDs: []string{a.ID}, Evidence: []evidenceReference{referenceFor(a)}}
			if len(s.Articles) >= 8 && i == 0 {
				other := s.Articles[1]
				story.ArticleIDs = append(story.ArticleIDs, other.ID)
				story.Evidence = append(story.Evidence, referenceFor(other))
			}
			d.MainStories = append(d.MainStories, story)
		} else {
			reference := referenceFor(a)
			d.BriefStories = append(d.BriefStories, modelBriefStory{
				Title: a.Title, Summary: a.Content, ArticleID: reference.ArticleID, SpanID: reference.SpanID,
			})
		}
	}
	return d
}

func referenceFor(a Article) evidenceReference {
	spans := splitSourceSpans(a.ID, preferredEvidenceText(a))
	return evidenceReference{ArticleID: a.ID, SpanID: spans[0].ID}
}

func syntheticStories(t *testing.T, s Snapshot) []Story {
	t.Helper()
	_, spans, err := buildPrompt(s)
	if err != nil {
		t.Fatal(err)
	}
	draft := syntheticDraft(s)
	stories, err := resolveModelStories(draft.MainStories, draft.BriefStories, spans)
	if err != nil {
		t.Fatal(err)
	}
	return stories
}

func fakeGemini(t *testing.T, draft modelDraft, status int) *httptest.Server {
	t.Helper()
	text, err := json.Marshal(draft)
	if err != nil {
		t.Fatal(err)
	}
	response, err := json.Marshal(map[string]any{"candidates": []any{map[string]any{"finishReason": "STOP", "content": map[string]any{"parts": []any{map[string]string{"text": string(text)}}}}}, "usageMetadata": map[string]int{"promptTokenCount": 321, "candidatesTokenCount": 123, "totalTokenCount": 444}})
	if err != nil {
		t.Fatal(err)
	}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Query().Get("key") != "fake-key" {
			t.Errorf("unexpected request metadata")
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("request JSON: %v", err)
		}
		if status != 0 {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`{"error":"do not expose me"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(response)
	}))
}

func TestGenerateWithFakeProviderAndRenderSafeLinks(t *testing.T) {
	s := syntheticSnapshot(8)
	server := fakeGemini(t, syntheticDraft(s), 0)
	defer server.Close()
	g, err := Generate(context.Background(), s, "ru", ProviderConfig{APIKey: "fake-key", Model: "fake-model", Endpoint: server.URL, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if g.Usage.Model != "fake-model" || g.Usage.TotalTokenCount != 444 {
		t.Fatalf("usage metadata missing: %+v", g.Usage)
	}
	if len(g.Edition.Stories) != 7 || g.Edition.Stories[0].Kind != "main" {
		t.Fatalf("unexpected story grouping: %d", len(g.Edition.Stories))
	}
	articles := make(map[string]Article, len(s.Articles))
	for _, article := range s.Articles {
		articles[article.ID] = article
	}
	for _, story := range g.Edition.Stories {
		for _, evidence := range story.Evidence {
			article := articles[evidence.ArticleID]
			if !strings.Contains(preferredEvidenceText(article), evidence.Quote) || utf8.RuneCountInString(evidence.Quote) > MaxEvidence {
				t.Fatalf("resolved evidence is not an exact bounded source span for article %s", evidence.ArticleID)
			}
		}
	}
	rendered, err := Render(s, "ru", g.Edition)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rendered.RichHTML, "https://news.example/article/1005") || !strings.Contains(rendered.RichHTML, "https://source.example/story/5?a=1&amp;b=2") {
		t.Fatal("expected internal and original source links")
	}
	if strings.Contains(rendered.RichHTML, "<script>") || strings.Contains(rendered.RichHTML, "<img src=x onerror=") {
		t.Fatal("untrusted article markup was not escaped")
	}
	if !strings.Contains(rendered.RichHTML, "https://news.example/digest/2026-10-27?edition=fixture") || !strings.Contains(rendered.PreviewText, s.EditionURL) {
		t.Fatal("edition URL missing")
	}
	if !strings.Contains(rendered.RichHTML, "Источник: <a") {
		t.Fatal("source links missing")
	}
}

func TestGenerateRejectsFabricatedEvidenceAndIncompleteCoverage(t *testing.T) {
	s := syntheticSnapshot(8)
	d := syntheticDraft(s)
	d.MainStories[0].Evidence[0].SpanID = "fabricated-span-id"
	server := fakeGemini(t, d, 0)
	_, err := Generate(context.Background(), s, "ru", ProviderConfig{APIKey: "fake-key", Model: "fake-model", Endpoint: server.URL, Client: server.Client()})
	server.Close()
	if err == nil || err.Error() != "provider_invalid_editorial_edition" {
		t.Fatalf("expected bounded validation failure, got %v", err)
	}
	d = syntheticDraft(s)
	d.BriefStories = d.BriefStories[:len(d.BriefStories)-1]
	server = fakeGemini(t, d, 0)
	defer server.Close()
	_, err = Generate(context.Background(), s, "ru", ProviderConfig{APIKey: "fake-key", Model: "fake-model", Endpoint: server.URL, Client: server.Client()})
	if err == nil || err.Error() != "provider_invalid_editorial_edition" {
		t.Fatalf("expected coverage failure, got %v", err)
	}
}

func TestEvidenceSpansCoverExactSourceTextAndPreferContentOverSummary(t *testing.T) {
	s := syntheticSnapshot(2)
	s.Articles[0].Content = strings.Repeat("English text starts here. Русский фрагмент следует. ", 24) + "Конец."
	s.Articles[0].Summary = "Не использовать: пересказ отличающийся от текста."
	s.Articles[1].Content = "  \n"
	s.Articles[1].Summary = "Summary fallback — точный источник."
	prompt, spanByID, err := buildPrompt(s)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(prompt, s.Articles[0].Summary) || !strings.Contains(prompt, s.Articles[1].Summary) {
		t.Fatal("prompt included alternate summary or omitted summary fallback")
	}
	if !strings.Contains(prompt, `"evidence_spans"`) || !strings.Contains(prompt, "span_id") {
		t.Fatal("prompt does not expose application-generated evidence span references")
	}
	for _, article := range s.Articles {
		spans := splitSourceSpans(article.ID, preferredEvidenceText(article))
		var joined strings.Builder
		for _, span := range spans {
			if utf8.RuneCountInString(span.Text) > MaxEvidence {
				t.Fatalf("span %s exceeds %d Unicode characters", span.ID, MaxEvidence)
			}
			if spanByID[span.ID].ArticleID != article.ID || !strings.Contains(preferredEvidenceText(article), span.Text) {
				t.Fatalf("span %s is not bound to its source article", span.ID)
			}
			joined.WriteString(span.Text)
		}
		if joined.String() != preferredEvidenceText(article) {
			t.Fatalf("span splitting dropped, changed, or reordered source text for article %s", article.ID)
		}
		if spans[0].ID != splitSourceSpans(article.ID, preferredEvidenceText(article))[0].ID {
			t.Fatalf("span IDs are not deterministic for article %s", article.ID)
		}
	}
	changed := s.Articles[0]
	changed.Content += " changed"
	if splitSourceSpans(changed.ID, preferredEvidenceText(changed))[0].ID == splitSourceSpans(s.Articles[0].ID, preferredEvidenceText(s.Articles[0]))[0].ID {
		t.Fatal("span ID did not change when bound source content changed")
	}
}

func TestModelDraftRejectsFreeformEvidenceQuote(t *testing.T) {
	response := `{"main_stories":[],"brief_stories":[{"title":"Заголовок","summary":"Пересказ","article_id":"1001","span_id":"span-1001-hash-0001","quote":"парафраз"}],"excluded":[]}`
	var draft modelDraft
	dec := json.NewDecoder(strings.NewReader(response))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&draft); err == nil {
		t.Fatal("provider-supplied quote text was accepted")
	}
}

func TestModelDraftCannotRepresentBundledBriefArticles(t *testing.T) {
	legacyBundle := `{"main_stories":[],"brief_stories":[{"title":"Дайджест","summary":"Общие новости","article_ids":["1001","1002"],"evidence":[{"article_id":"1001","span_id":"a"},{"article_id":"1002","span_id":"b"}]}],"excluded":[]}`
	var draft modelDraft
	dec := json.NewDecoder(strings.NewReader(legacyBundle))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&draft); err == nil {
		t.Fatal("bundled brief output fit the scalar brief schema")
	}
	briefProperties := geminiSchema()["properties"].(map[string]any)["brief_stories"].(map[string]any)["items"].(map[string]any)["properties"].(map[string]any)
	if _, hasArticleIDs := briefProperties["article_ids"]; hasArticleIDs {
		t.Fatal("brief schema still permits an article_ids array")
	}
	if _, hasEvidence := briefProperties["evidence"]; hasEvidence {
		t.Fatal("brief schema still permits grouped evidence arrays")
	}
}

func TestEvidenceSpanResolverRejectsUnknownCrossArticleAndDuplicateReferences(t *testing.T) {
	s := syntheticSnapshot(2)
	_, spanByID, err := buildPrompt(s)
	if err != nil {
		t.Fatal(err)
	}
	draft := syntheticDraft(s)
	draft.MainStories[0].Evidence[0].SpanID = "unknown-span"
	if _, err := resolveModelStories(draft.MainStories, draft.BriefStories, spanByID); err == nil {
		t.Fatal("unknown source span accepted")
	}
	draft = syntheticDraft(s)
	draft.MainStories[0].Evidence[0].SpanID = referenceFor(s.Articles[1]).SpanID
	if _, err := resolveModelStories(draft.MainStories, draft.BriefStories, spanByID); err == nil {
		t.Fatal("cross-article source span accepted")
	}
	draft = syntheticDraft(s)
	draft.MainStories[0].Evidence = append(draft.MainStories[0].Evidence, draft.MainStories[0].Evidence[0])
	if _, err := resolveModelStories(draft.MainStories, draft.BriefStories, spanByID); err == nil {
		t.Fatal("duplicate source span reference accepted")
	}
}

func TestBriefStoriesMustBeSingleArticleButMainStoriesMayGroup(t *testing.T) {
	s := syntheticSnapshot(2)
	brief := Story{
		Kind: "brief", Title: "Сводка", Summary: "Общие краткие новости.",
		PrimaryArticleID: s.Articles[0].ID, ArticleIDs: []string{s.Articles[0].ID, s.Articles[1].ID},
		Evidence: []Evidence{{ArticleID: s.Articles[0].ID, Quote: preferredEvidenceText(s.Articles[0])}, {ArticleID: s.Articles[1].ID, Quote: preferredEvidenceText(s.Articles[1])}},
	}
	if _, err := FinalizeEdition(s, RussianLocale, []Story{brief}, nil); err == nil {
		t.Fatal("multi-article brief accepted")
	}
	brief.Kind = "main"
	if _, err := FinalizeEdition(s, RussianLocale, []Story{brief}, nil); err != nil {
		t.Fatalf("grouped main story rejected: %v", err)
	}
}

func TestGenerateRejectsExclusionsForCallerSelectedArticlesIncludingLongContent(t *testing.T) {
	s := syntheticSnapshot(8)
	longContent := strings.Repeat("Полный исходный материал с подробностями и точными фактами. ", 150)
	s.Articles[7].Content = longContent
	prompt, spans, err := buildPrompt(s)
	if err != nil {
		t.Fatal(err)
	}
	var joined strings.Builder
	for _, span := range splitSourceSpans(s.Articles[7].ID, longContent) {
		joined.WriteString(span.Text)
	}
	if joined.String() != longContent || !strings.Contains(prompt, "Во входном снимке ровно 8 материалов.") ||
		!strings.Contains(prompt, "excluded всегда должен быть пустым массивом []") {
		t.Fatal("generation prompt omitted eligible long content or mandatory inclusion rule")
	}
	if len(spans) == 0 {
		t.Fatal("long article produced no source spans")
	}

	draft := syntheticDraft(s)
	draft.BriefStories = draft.BriefStories[:len(draft.BriefStories)-1]
	draft.Excluded = []Exclusion{{ArticleID: s.Articles[7].ID, Reason: "insufficient_content"}}
	server := fakeGemini(t, draft, 0)
	defer server.Close()
	_, err = Generate(context.Background(), s, RussianLocale, ProviderConfig{
		APIKey: "fake-key", Model: "fake-model", Endpoint: server.URL, Client: server.Client(),
	})
	if err == nil || err.Error() != "provider_invalid_editorial_edition" {
		t.Fatalf("provider exclusion of a caller-selected article was accepted: %v", err)
	}
}

func TestGenerateRejectsIncompleteProviderAndDoesNotFollowRedirects(t *testing.T) {
	s := syntheticSnapshot(1)
	partial := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"candidates":[{"finishReason":"MAX_TOKENS","content":{"parts":[{"text":"{}"}]}}]}`))
	}))
	_, err := Generate(context.Background(), s, RussianLocale, ProviderConfig{APIKey: "fake-key", Model: "fake-model", Endpoint: partial.URL, Client: partial.Client()})
	partial.Close()
	if err == nil || err.Error() != "provider_incomplete_response" {
		t.Fatalf("expected incomplete response failure, got %v", err)
	}

	var redirected bool
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { redirected = true; _, _ = w.Write([]byte(`{}`)) }))
	defer destination.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, destination.URL, http.StatusTemporaryRedirect)
	}))
	_, err = Generate(context.Background(), s, RussianLocale, ProviderConfig{APIKey: "fake-key", Model: "fake-model", Endpoint: redirect.URL, Client: redirect.Client()})
	redirect.Close()
	if err == nil || err.Error() != "provider_rejected_request" || redirected {
		t.Fatalf("redirect was followed or not rejected: err=%v followed=%v", err, redirected)
	}
}

func TestProviderEndpointAllowsConfiguredHTTPServiceNameAndRejectsMalformedURLs(t *testing.T) {
	cfg := ProviderConfig{Endpoint: "http://fake:8090/v1/models/fixture-model:generateContent"}
	got, err := providerEndpoint(cfg)
	if err != nil || got != cfg.Endpoint {
		t.Fatalf("trusted configured HTTP service endpoint rejected: %q %v", got, err)
	}
	for _, endpoint := range []string{
		"ftp://fake:8090/v1/models/fixture-model:generateContent",
		"http://user:secret@fake:8090/v1/models/fixture-model:generateContent",
		"http://fake:8090/v1/models/fixture-model:generateContent?key=embedded",
		"http://fake:8090/v1/models/fixture-model:generateContent#redirect",
	} {
		if _, err := providerEndpoint(ProviderConfig{Endpoint: endpoint}); err == nil {
			t.Errorf("expected invalid configured endpoint to fail: %s", endpoint)
		}
	}
}

func TestRenderLargeIssueSplitsOnlyAtStoryBoundaries(t *testing.T) {
	s := syntheticSnapshot(42)
	stories := syntheticStories(t, s)
	e, err := FinalizeEdition(s, "ru", stories, nil)
	if err != nil {
		t.Fatal(err)
	}
	r, err := Render(s, "ru", e)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.PlainParts) < 2 {
		t.Fatalf("expected split parts, got %d", len(r.PlainParts))
	}
	for i, part := range r.PlainParts {
		if visibleCount(part) > MaxPlainPart {
			t.Fatalf("part %d exceeds limit: %d", i, visibleCount(part))
		}
		if !strings.Contains(part, s.EditionURL) {
			t.Fatalf("part %d lacks full-issue link", i)
		}
	}
}

func TestRenderRejectsSingleOversizedStoryWithoutTruncation(t *testing.T) {
	s := syntheticSnapshot(4)
	story := Story{Kind: "main", Title: strings.Repeat("x", MaxStoryText), Summary: strings.Repeat("y", MaxStoryText), PrimaryArticleID: s.Articles[0].ID}
	for i := range s.Articles {
		s.Articles[i].SourceName = strings.Repeat("s", 300)
		a := s.Articles[i]
		story.ArticleIDs = append(story.ArticleIDs, a.ID)
		story.Evidence = append(story.Evidence, Evidence{ArticleID: a.ID, Quote: preferredEvidenceText(a)})
	}
	e, err := FinalizeEdition(s, RussianLocale, []Story{story}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Render(s, RussianLocale, e); err == nil || err.Error() != "render_story_too_large" {
		t.Fatalf("expected bounded oversized story failure, got %v", err)
	}
}

func TestRenderCountsLiteralAmpEntitiesInPlainHTMLParts(t *testing.T) {
	s := syntheticSnapshot(100)
	story := Story{Kind: "main", Title: "Один большой сюжет", Summary: "Короткий текст", PrimaryArticleID: s.Articles[0].ID}
	for _, a := range s.Articles {
		a.SourceName = strings.Repeat("&amp;", 60)
		story.ArticleIDs = append(story.ArticleIDs, a.ID)
		story.Evidence = append(story.Evidence, Evidence{ArticleID: a.ID, Quote: preferredEvidenceText(a)})
	}
	s.Articles = func() []Article {
		out := append([]Article(nil), s.Articles...)
		for i := range out {
			out[i].SourceName = strings.Repeat("&amp;", 60)
		}
		return out
	}()
	e, err := FinalizeEdition(s, RussianLocale, []Story{story}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Render(s, RussianLocale, e); err == nil || err.Error() != "render_story_too_large" {
		t.Fatalf("expected literal text to count toward limit, got %v", err)
	}
	if got := visibleCount(html.EscapeString(strings.Repeat("&amp;", 1000))); got != 5000 {
		t.Fatalf("escaped literal entity count = %d, want 5000", got)
	}
}

func TestRenderPlacesMainFirstAndGroupsBriefsBySuppliedCategory(t *testing.T) {
	s := syntheticSnapshot(5)
	s.Articles[0].CategoryID, s.Articles[0].CategoryName = "4", "Технологии"
	s.Articles[1].CategoryID, s.Articles[1].CategoryName = "8", "Культура"
	s.Articles[2].CategoryID, s.Articles[2].CategoryName = "4", "Технологии"
	s.Articles[3].CategoryID, s.Articles[3].CategoryName = "9", "Политика"
	s.Articles[4].CategoryID, s.Articles[4].CategoryName = "8", "Культура"
	s.Articles[4].Title = "Событие номер 5"
	story := func(i int, kind string) Story {
		a := s.Articles[i]
		return Story{Kind: kind, Title: a.Title, Summary: a.Content, PrimaryArticleID: a.ID, ArticleIDs: []string{a.ID}, Evidence: []Evidence{{ArticleID: a.ID, Quote: preferredEvidenceText(a)}}}
	}
	// Deliberately interleave provider output to exercise deterministic presentation order.
	stories := []Story{story(0, "brief"), story(1, "main"), story(2, "brief"), story(3, "main"), story(4, "brief")}
	e, err := FinalizeEdition(s, RussianLocale, stories, nil)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := Render(s, RussianLocale, e)
	if err != nil {
		t.Fatal(err)
	}
	main1 := strings.Index(rendered.RichHTML, "Событие номер 2")
	main2 := strings.Index(rendered.RichHTML, "Событие номер 4")
	briefHeader := strings.Index(rendered.RichHTML, "Ещё коротко")
	categoryTech := strings.Index(rendered.RichHTML, "<h3>Технологии</h3>")
	brief1 := strings.Index(rendered.RichHTML, "Событие номер 1")
	brief2 := strings.Index(rendered.RichHTML, "Событие номер 3")
	categoryCulture := strings.Index(rendered.RichHTML, "<h3>Культура</h3>")
	brief3 := strings.Index(rendered.RichHTML, "Событие номер 5")
	if !(main1 >= 0 && main1 < main2 && main2 < briefHeader && briefHeader < categoryTech && categoryTech < brief1 && brief1 < brief2 && brief2 < categoryCulture && categoryCulture < brief3) {
		t.Fatalf("unexpected rich order: %s", rendered.RichHTML)
	}
	part := rendered.PlainParts[0]
	if strings.Index(part, "Событие номер 2") > strings.Index(part, "Ещё коротко") {
		t.Fatal("plain parts did not put main stories first")
	}
	if !strings.Contains(part, `<a href="https://news.example/article/1001">Событие номер 1</a>`) || !strings.Contains(part, `<a href="https://source.example/story/1?a=1&amp;b=2">Источник 1</a>`) {
		t.Fatal("ordinary fallback lost internal or source links")
	}
	if strings.Contains(part, "<h2>") || strings.Contains(part, "<h3>") || strings.Contains(part, "<p>") || strings.Contains(part, "<ul>") {
		t.Fatal("ordinary fallback contains unsupported block markup")
	}
}

func TestSnapshotRejectsUnsafeURLsIDsAndLocales(t *testing.T) {
	s := syntheticSnapshot(1)
	if _, err := ValidateSnapshot(s, "sr"); err == nil {
		t.Fatal("unsupported locale accepted")
	}
	bad := s
	bad.Articles = append([]Article(nil), s.Articles...)
	bad.Articles[0].URL = "javascript:alert(1)"
	if _, err := ValidateSnapshot(bad, "ru"); err == nil {
		t.Fatal("unsafe URL accepted")
	}
	bad = s
	bad.Articles = append([]Article(nil), s.Articles...)
	bad.Articles[0].ID = "0"
	if _, err := ValidateSnapshot(bad, "ru"); err == nil {
		t.Fatal("zero article ID accepted")
	}
	for _, invalid := range []string{"-1", "0", "01", "2147483648", "18446744073709551615"} {
		bad = s
		bad.Articles = append([]Article(nil), s.Articles...)
		bad.Articles[0].ID = invalid
		if _, err := ValidateSnapshot(bad, "ru"); err == nil {
			t.Fatalf("invalid signed32 article ID %q accepted", invalid)
		}
	}
	good := s
	good.Articles = append([]Article(nil), s.Articles...)
	good.Articles[0].ID = "2147483647"
	if _, err := ValidateSnapshot(good, "ru"); err != nil {
		t.Fatalf("maximum positive signed32 article ID rejected: %v", err)
	}
	bad = s
	bad.CutoffUTC = time.Date(2026, 10, 27, 18, 0, 0, 0, time.FixedZone("offset", 3600))
	if _, err := ValidateSnapshot(bad, "ru"); err == nil {
		t.Fatal("non-UTC cutoff accepted")
	}
}

func TestEvidenceCanMatchMixedLanguageInputWithoutTranslatingSource(t *testing.T) {
	s := syntheticSnapshot(8)
	stories := syntheticStories(t, s)
	found := false
	for _, story := range stories {
		for _, ev := range story.Evidence {
			if ev.ArticleID == s.Articles[5].ID {
				found = true
				if ev.Quote != s.Articles[5].Content {
					t.Fatal("evidence was translated")
				}
			}
		}
	}
	if !found {
		t.Fatal("mixed-language source evidence missing")
	}
	e, err := FinalizeEdition(s, "ru", stories, nil)
	if err != nil {
		t.Fatal(err)
	}
	if e.Locale != "ru" {
		t.Fatal("unexpected output locale")
	}
}

func TestContractRequestExampleIsParseable(t *testing.T) {
	path := filepath.Join("..", "testdata", "generate-request.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var req Request
	if err := json.Unmarshal(b, &req); err != nil {
		t.Fatal(err)
	}
	if req.Operation != "generate" || req.Locale != RussianLocale || len(req.Snapshot.Articles) != 8 {
		t.Fatalf("unexpected contract example: %+v", req)
	}
	responseBytes, err := os.ReadFile(filepath.Join("..", "testdata", "generate-response.json"))
	if err != nil {
		t.Fatal(err)
	}
	var response Response
	if err := json.Unmarshal(responseBytes, &response); err != nil {
		t.Fatal(err)
	}
	if !response.OK || response.Edition == nil || response.Rendered == nil || response.ProviderUsage == nil || response.ResultHash != response.Edition.ContentHash {
		t.Fatal("response example is missing contract fields")
	}
	if err := ValidateEdition(req.Snapshot, req.Locale, *response.Edition); err != nil {
		t.Fatalf("response example edition invalid: %v", err)
	}
	rendered, err := Render(req.Snapshot, req.Locale, *response.Edition)
	if err != nil {
		t.Fatal(err)
	}
	if rendered.RichHTML != response.Rendered.RichHTML || strings.Join(rendered.PlainParts, "\n--PART--\n") != strings.Join(response.Rendered.PlainParts, "\n--PART--\n") || rendered.PreviewText != response.Rendered.PreviewText {
		t.Fatal("response example render is stale")
	}
}

func TestEvidenceLimitCountsUnicodeCharactersAndRequiresExactSourceText(t *testing.T) {
	for _, size := range []int{251, 300, MaxEvidence} {
		t.Run(fmt.Sprintf("accept_%d_cyrillic_characters", size), func(t *testing.T) {
			s := syntheticSnapshot(1)
			quote := strings.Repeat("я", size)
			s.Articles[0].Content = quote
			story := Story{
				Kind: "main", Title: "Проверка цитаты", Summary: "Короткий пересказ.",
				PrimaryArticleID: s.Articles[0].ID, ArticleIDs: []string{s.Articles[0].ID},
				Evidence: []Evidence{{ArticleID: s.Articles[0].ID, Quote: quote}},
			}
			e, err := FinalizeEdition(s, RussianLocale, []Story{story}, nil)
			if err != nil {
				t.Fatalf("expected exact %d-character Cyrillic evidence to pass: %v", size, err)
			}
			if err := ValidateEdition(s, RussianLocale, e); err != nil {
				t.Fatalf("finalized evidence no longer validates: %v", err)
			}
		})
	}

	t.Run("reject_501_cyrillic_characters", func(t *testing.T) {
		s := syntheticSnapshot(1)
		quote := strings.Repeat("я", MaxEvidence+1)
		s.Articles[0].Content = quote
		story := Story{
			Kind: "main", Title: "Проверка цитаты", Summary: "Короткий пересказ.",
			PrimaryArticleID: s.Articles[0].ID, ArticleIDs: []string{s.Articles[0].ID},
			Evidence: []Evidence{{ArticleID: s.Articles[0].ID, Quote: quote}},
		}
		if _, err := FinalizeEdition(s, RussianLocale, []Story{story}, nil); err == nil {
			t.Fatal("expected evidence over 500 Unicode characters to fail")
		}
	})
}

func TestRenderHasDateAndSnapshotCountsInEveryFormat(t *testing.T) {
	var req Request
	requestBytes, err := os.ReadFile(filepath.Join("..", "testdata", "generate-request.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(requestBytes, &req); err != nil {
		t.Fatal(err)
	}
	var response Response
	responseBytes, err := os.ReadFile(filepath.Join("..", "testdata", "generate-response.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(responseBytes, &response); err != nil {
		t.Fatal(err)
	}
	rendered, err := Render(req.Snapshot, req.Locale, *response.Edition)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{rendered.RichHTML, rendered.PlainParts[0], rendered.PreviewText} {
		if !strings.Contains(value, "Evening News · 27 октября 2026") || !strings.Contains(value, "8 исходных материалов · Русский") {
			t.Fatalf("expected dated source count in output: %s", value[:min(len(value), 200)])
		}
	}
	response.Edition.Excluded = []Exclusion{{ArticleID: req.Snapshot.Articles[7].ID, Reason: "not_news"}}
	response.Edition.Stories = response.Edition.Stories[:len(response.Edition.Stories)-1]
	response.Edition.ContentHash, err = editionContentHash(*response.Edition)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err = Render(req.Snapshot, req.Locale, *response.Edition)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(rendered.RichHTML, "Исключено: 1") || !strings.Contains(rendered.PlainParts[0], "Исключено: 1") || !strings.Contains(rendered.PreviewText, "Исключено: 1") {
		t.Fatal("exclusion count is missing from a renderer output")
	}
}
