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
	d := modelDraft{MainStories: []modelMainStory{}, BriefStories: []modelMainStory{}, ArticleStories: make(map[string]modelArticleStory, len(s.Articles))}
	for i, a := range s.Articles {
		d.ArticleStories[a.ID] = articleStoryFor(a)
		if len(s.Articles) >= 8 && i == 1 {
			continue
		}
		if i == 0 || i == 2 {
			story := modelMainStory{Title: a.Title, Summary: a.Content, PrimaryArticleID: a.ID, ArticleIDs: []string{a.ID}}
			if len(s.Articles) >= 8 && i == 0 {
				other := s.Articles[1]
				story.ArticleIDs = append(story.ArticleIDs, other.ID)
			}
			d.MainStories = append(d.MainStories, story)
		}
	}
	return d
}

func articleStoryFor(a Article) modelArticleStory {
	spans := splitSourceSpans(a.ID, preferredEvidenceText(a))
	return modelArticleStory{Title: a.Title, Summary: preferredEvidenceText(a), SpanID: spans[0].ID}
}

func syntheticStories(t *testing.T, s Snapshot) []Story {
	t.Helper()
	_, spans, err := buildPrompt(s)
	if err != nil {
		t.Fatal(err)
	}
	draft := syntheticDraft(s)
	stories, err := resolveModelStories(s, draft.MainStories, draft.BriefStories, draft.ArticleStories, spans)
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
	if !strings.Contains(rendered.RichHTML, "https://news.example/article/1005") || strings.Contains(rendered.RichHTML, "https://source.example/story/") {
		t.Fatal("expected app-owned story links and no original source URLs")
	}
	if strings.Contains(rendered.RichHTML, "<script>") || strings.Contains(rendered.RichHTML, "<img src=x onerror=") {
		t.Fatal("untrusted article markup was not escaped")
	}
	if !strings.Contains(rendered.RichHTML, "https://news.example/digest/2026-10-27?edition=fixture") || !strings.Contains(rendered.PreviewText, s.EditionURL) {
		t.Fatal("edition URL missing")
	}
	if !strings.Contains(rendered.RichHTML, " · Источник 5") || !strings.Contains(rendered.RichHTML, "&lt;img src=x onerror=alert(1)&gt; заявляет: это проверяется.") || strings.Contains(rendered.RichHTML, "&lt;script&gt;alert(1)&lt;/script&gt;") {
		t.Fatal("compact brief did not use its escaped summary sentence, omit title, and inline publisher")
	}
}

func TestRenderUsesAppOwnedHeadlineLinksAndDeduplicatesPlainSourceNames(t *testing.T) {
	s := syntheticSnapshot(8)
	s.Articles[1].SourceName = s.Articles[0].SourceName
	stories := syntheticStories(t, s)
	var grouped Story
	for _, story := range stories {
		if story.Kind == "main" && len(story.ArticleIDs) == 2 {
			grouped = story
			break
		}
	}
	if len(grouped.ArticleIDs) != 2 {
		t.Fatal("expected a two-article main story")
	}
	e, err := FinalizeEdition(s, RussianLocale, stories, nil)
	if err != nil {
		t.Fatal(err)
	}
	rendered, err := Render(s, RussianLocale, e)
	if err != nil {
		t.Fatal(err)
	}
	internalURL := "https://news.example/article/1001"
	for name, output := range map[string]string{"rich": rendered.RichHTML, "plain": strings.Join(rendered.PlainParts, "\n"), "preview": rendered.PreviewText} {
		if !strings.Contains(output, internalURL) {
			t.Errorf("%s output is missing app-owned headline URL", name)
		}
		if strings.Contains(output, "https://source.example/") {
			t.Errorf("%s output contains an original source URL", name)
		}
		if strings.Count(output, "Источник: "+s.Articles[0].SourceName) != 1 {
			t.Errorf("%s output should list the identical publisher once: %s", name, output)
		}
	}
	if !strings.Contains(rendered.RichHTML, `<a href="https://news.example/article/1001">Город начал строительство моста</a>`) {
		t.Fatal("rich headline does not link to the app-owned article")
	}
	if !strings.Contains(rendered.PlainParts[0], `<a href="https://news.example/article/1001">Город начал строительство моста</a>`) {
		t.Fatal("plain headline does not link to the app-owned article")
	}
}

func TestGenerateRejectsFabricatedEvidenceAndIncompleteCoverage(t *testing.T) {
	s := syntheticSnapshot(8)
	d := syntheticDraft(s)
	firstID := s.Articles[0].ID
	firstRecord := d.ArticleStories[firstID]
	firstRecord.SpanID = "fabricated-span-id"
	d.ArticleStories[firstID] = firstRecord
	server := fakeGemini(t, d, 0)
	_, err := Generate(context.Background(), s, "ru", ProviderConfig{APIKey: "fake-key", Model: "fake-model", Endpoint: server.URL, Client: server.Client()})
	server.Close()
	if err == nil || err.Error() != "provider_invalid_editorial_edition" {
		t.Fatalf("expected bounded validation failure, got %v", err)
	}
	d = syntheticDraft(s)
	delete(d.ArticleStories, s.Articles[len(s.Articles)-1].ID)
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
	response := `{"main_stories":[],"article_stories":{"1001":{"title":"Заголовок","summary":"Пересказ","span_id":"span-1001-hash-0001","quote":"парафраз"}}}`
	var draft modelDraft
	dec := json.NewDecoder(strings.NewReader(response))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&draft); err == nil {
		t.Fatal("provider-supplied quote text was accepted")
	}
}

func TestModelDraftRequiresPerArticleRecordsAndSupportsBriefGroups(t *testing.T) {
	s := syntheticSnapshot(3)
	schema := geminiSchema(s)
	properties := schema["properties"].(map[string]any)
	articleSchema := properties["article_stories"].(map[string]any)
	articleProperties := articleSchema["properties"].(map[string]any)
	if len(articleProperties) != len(s.Articles) {
		t.Fatalf("article schema has %d keys, expected %d", len(articleProperties), len(s.Articles))
	}
	required := articleSchema["required"].([]string)
	if len(required) != len(s.Articles) {
		t.Fatalf("required article key list has %d keys, expected %d", len(required), len(s.Articles))
	}
	requiredSet := make(map[string]bool, len(required))
	for _, id := range required {
		requiredSet[id] = true
	}
	for _, article := range s.Articles {
		if _, ok := articleProperties[article.ID]; !ok {
			t.Fatalf("schema missing article ID %s", article.ID)
		}
		if !requiredSet[article.ID] {
			t.Fatalf("schema does not require article ID %s", article.ID)
		}
	}
	if _, hasExcluded := properties["excluded"]; hasExcluded {
		t.Fatal("generation schema still permits article exclusions")
	}
	if _, hasBriefArray := properties["brief_stories"]; !hasBriefArray {
		t.Fatal("model schema does not permit brief event groups")
	}
	rootRequired := schema["required"].([]string)
	if !contains(rootRequired, "brief_stories") {
		t.Fatal("model schema does not require the brief group array")
	}
	legacyBundle := `{"main_stories":[],"brief_stories":[{"title":"Дайджест","summary":"Общие новости","article_ids":["1001","1002"],"primary_article_id":"1001"}],"article_stories":{}}`
	var draft modelDraft
	dec := json.NewDecoder(strings.NewReader(legacyBundle))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&draft); err != nil || len(draft.BriefStories) != 1 || len(draft.BriefStories[0].ArticleIDs) != 2 {
		t.Fatalf("valid grouped brief draft was rejected: %v", err)
	}
}

func TestArticleStoryMapRejectsMissingUnknownAndCrossArticleSpanKeys(t *testing.T) {
	s := syntheticSnapshot(2)
	_, spanByID, err := buildPrompt(s)
	if err != nil {
		t.Fatal(err)
	}
	draft := syntheticDraft(s)
	draft.ArticleStories[s.Articles[0].ID] = modelArticleStory{Title: s.Articles[0].Title, Summary: "Кратко", SpanID: "unknown-span"}
	if _, err := resolveModelStories(s, draft.MainStories, draft.BriefStories, draft.ArticleStories, spanByID); err == nil {
		t.Fatal("unknown source span accepted")
	}
	draft = syntheticDraft(s)
	draft.ArticleStories[s.Articles[0].ID] = modelArticleStory{Title: s.Articles[0].Title, Summary: "Кратко", SpanID: articleStoryFor(s.Articles[1]).SpanID}
	if _, err := resolveModelStories(s, draft.MainStories, draft.BriefStories, draft.ArticleStories, spanByID); err == nil {
		t.Fatal("cross-article source span accepted")
	}
	draft = syntheticDraft(s)
	delete(draft.ArticleStories, s.Articles[1].ID)
	if _, err := resolveModelStories(s, draft.MainStories, draft.BriefStories, draft.ArticleStories, spanByID); err == nil {
		t.Fatal("missing required article record accepted")
	}
	draft = syntheticDraft(s)
	draft.ArticleStories["999999"] = modelArticleStory{Title: "Other", Summary: "Other", SpanID: "foreign"}
	if _, err := resolveModelStories(s, draft.MainStories, draft.BriefStories, draft.ArticleStories, spanByID); err == nil {
		t.Fatal("unknown article record key accepted")
	}
}

func TestBriefStoriesMayGroupArticlesButRejectDuplicateMembership(t *testing.T) {
	s := syntheticSnapshot(2)
	brief := Story{
		Kind: "brief", Title: "Сводка", Summary: "Общие краткие новости.",
		PrimaryArticleID: s.Articles[0].ID, ArticleIDs: []string{s.Articles[0].ID, s.Articles[1].ID},
		Evidence: []Evidence{{ArticleID: s.Articles[0].ID, Quote: preferredEvidenceText(s.Articles[0])}, {ArticleID: s.Articles[1].ID, Quote: preferredEvidenceText(s.Articles[1])}},
	}
	if _, err := FinalizeEdition(s, RussianLocale, []Story{brief}, nil); err != nil {
		t.Fatalf("grouped brief story rejected: %v", err)
	}
	brief.ArticleIDs = []string{s.Articles[0].ID, s.Articles[0].ID, s.Articles[1].ID}
	brief.Evidence = append(brief.Evidence, Evidence{ArticleID: s.Articles[0].ID, Quote: preferredEvidenceText(s.Articles[0])})
	if _, err := FinalizeEdition(s, RussianLocale, []Story{brief}, nil); err == nil {
		t.Fatal("duplicate article membership in grouped brief accepted")
	}
}

func TestModelBriefGroupsPreserveCompletenessAndRejectOverlaps(t *testing.T) {
	s := syntheticSnapshot(3)
	_, spans, err := buildPrompt(s)
	if err != nil {
		t.Fatal(err)
	}
	draft := syntheticDraft(s)
	draft.MainStories = []modelMainStory{}
	draft.BriefStories = []modelMainStory{{
		Title: "Ремонт парка продолжается", Summary: "Город и подрядчик сообщают об одном этапе ремонта.",
		PrimaryArticleID: s.Articles[0].ID, ArticleIDs: []string{s.Articles[0].ID, s.Articles[1].ID},
	}}
	stories, err := resolveModelStories(s, draft.MainStories, draft.BriefStories, draft.ArticleStories, spans)
	if err != nil {
		t.Fatalf("valid brief event group rejected: %v", err)
	}
	if len(stories) != 2 || stories[0].Kind != "brief" || len(stories[0].ArticleIDs) != 2 || stories[1].ArticleIDs[0] != s.Articles[2].ID {
		t.Fatalf("group resolution lost coverage or snapshot ordering: %+v", stories)
	}
	if _, err := FinalizeEdition(s, RussianLocale, stories, nil); err != nil {
		t.Fatalf("complete grouped brief edition rejected: %v", err)
	}
	draft.MainStories = []modelMainStory{{Title: "Same", Summary: "Same event.", PrimaryArticleID: s.Articles[0].ID, ArticleIDs: []string{s.Articles[0].ID}}}
	if _, err := resolveModelStories(s, draft.MainStories, draft.BriefStories, draft.ArticleStories, spans); err == nil {
		t.Fatal("article assigned to both a main and brief group accepted")
	}
	draft.MainStories = []modelMainStory{}
	draft.BriefStories = append(draft.BriefStories, modelMainStory{Title: "Duplicate", Summary: "Duplicate membership.", PrimaryArticleID: s.Articles[1].ID, ArticleIDs: []string{s.Articles[1].ID}})
	if _, err := resolveModelStories(s, draft.MainStories, draft.BriefStories, draft.ArticleStories, spans); err == nil {
		t.Fatal("article assigned to two brief groups accepted")
	}
}

func TestGenerateRequiresRecordForLongCallerSelectedArticle(t *testing.T) {
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
		!strings.Contains(prompt, "article_stories") {
		t.Fatal("generation prompt omitted eligible long content or mandatory article records")
	}
	if len(spans) == 0 {
		t.Fatal("long article produced no source spans")
	}

	draft := syntheticDraft(s)
	delete(draft.ArticleStories, s.Articles[7].ID)
	server := fakeGemini(t, draft, 0)
	defer server.Close()
	_, err = Generate(context.Background(), s, RussianLocale, ProviderConfig{
		APIKey: "fake-key", Model: "fake-model", Endpoint: server.URL, Client: server.Client(),
	})
	if err == nil || err.Error() != "provider_invalid_editorial_edition" {
		t.Fatalf("provider omission of a caller-selected article was accepted: %v", err)
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
	for i := range s.Articles {
		s.Articles[i].Content = fmt.Sprintf("Новость %d: %s", i+1, strings.Repeat("событие ", 50))
	}
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
		a.SourceName = fmt.Sprintf("%s-%d", strings.Repeat("&amp;", 55), len(story.ArticleIDs))
		story.ArticleIDs = append(story.ArticleIDs, a.ID)
		story.Evidence = append(story.Evidence, Evidence{ArticleID: a.ID, Quote: preferredEvidenceText(a)})
	}
	s.Articles = func() []Article {
		out := append([]Article(nil), s.Articles...)
		for i := range out {
			out[i].SourceName = fmt.Sprintf("%s-%d", strings.Repeat("&amp;", 55), i+1)
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
	s.Articles[0].Content = "В Караджорджевом парке посадили 117 282 дерева."
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
	brief1 := strings.Index(rendered.RichHTML, "В Караджорджевом парке посадили 117 282 дерева.")
	brief2 := strings.Index(rendered.RichHTML, "Подтверждённый фрагмент материала 3.")
	categoryCulture := strings.Index(rendered.RichHTML, "<h3>Культура</h3>")
	brief3 := strings.Index(rendered.RichHTML, "заявляет: это проверяется.")
	if !(main1 >= 0 && main1 < main2 && main2 < briefHeader && briefHeader < categoryTech && categoryTech < brief1 && brief1 < brief2 && brief2 < categoryCulture && categoryCulture < brief3) {
		t.Fatalf("unexpected rich order: %s", rendered.RichHTML)
	}
	part := rendered.PlainParts[0]
	if strings.Index(part, "Событие номер 2") > strings.Index(part, "Ещё коротко") {
		t.Fatal("plain parts did not put main stories first")
	}
	if !strings.Contains(part, `<a href="https://news.example/article/1001">В Караджорджевом парке посадили 117 282 дерева.</a> · Источник 1`) || strings.Contains(part, "Событие номер 1") || strings.Contains(part, "https://source.example/") {
		t.Fatal("ordinary fallback lost internal headline link or included an original source URL")
	}
	if !strings.Contains(part, `Событие номер 2</a></b>`) || !strings.Contains(part, "Подтверждённый фрагмент материала 2.") {
		t.Fatal("main story title and summary were changed by compact brief rendering")
	}
	if !strings.Contains(rendered.RichHTML, `<p>• <a href="https://news.example/article/1001">В Караджорджевом парке посадили 117 282 дерева.</a> · Источник 1</p>`) || !strings.Contains(rendered.PreviewText, `• В Караджорджевом парке посадили 117 282 дерева. — https://news.example/article/1001 · Источник 1`) {
		t.Fatal("rich or preview brief did not render as one linked sentence with inline source")
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
