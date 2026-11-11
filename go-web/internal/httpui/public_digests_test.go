package httpui

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
)

type digestAPIFake struct {
	*fakeEditorialReader
	catalog      repositories.EditorialDigestCatalog
	catalogErr   error
	catalogCalls int
}

func (f *digestAPIFake) EditorialDigestCatalog(_ context.Context, locale, month string, _ time.Time) (repositories.EditorialDigestCatalog, error) {
	f.catalogCalls++
	if f.catalogErr != nil {
		return repositories.EditorialDigestCatalog{}, f.catalogErr
	}
	f.catalog.Month = month
	return f.catalog, nil
}

func TestPublicDigestCatalogValidatesAndProjectsAllowlistedMetadata(t *testing.T) {
	fake := &digestAPIFake{fakeEditorialReader: &fakeEditorialReader{fakeReader: &fakeReader{}}, catalog: repositories.EditorialDigestCatalog{EarliestDate: "2026-11-02", LatestDate: "2026-11-10", LatestID: "11111111-1111-4111-8111-111111111111", Issues: []repositories.EditorialDigestIssue{{Date: "2026-11-10", ID: "11111111-1111-4111-8111-111111111111"}}}}
	mux := http.NewServeMux()
	testHandler(fake).Register(mux)
	w := request(mux, "GET", "/api/public/digests?locale=ru&month=2026-11")
	if w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("status=%d headers=%v body=%s", w.Code, w.Header(), w.Body)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["version"] != "1" || body["timezone"] != "UTC" || body["locale"] != "ru" || body["month"] != "2026-11" || body["earliest_date"] != "2026-11-02" {
		t.Fatalf("unexpected response: %#v", body)
	}
	assertJSONKeys(t, body, "version", "locale", "timezone", "month", "earliest_date", "latest", "issues")
	assertJSONKeys(t, body["latest"].(map[string]any), "date", "edition_id")
	assertJSONKeys(t, body["issues"].([]any)[0].(map[string]any), "date", "edition_id")
	for _, bad := range []string{"locale=ru&locale=sr-Latn", "locale=ru&month=2026-13", "locale=ru&month=2026-1", "locale=ru&month=0000-01", "locale=ru&month=2026-11&month=2026-11", "month=2026-11", "locale=ru&other=x"} {
		w = request(mux, "GET", "/api/public/digests?"+bad)
		if w.Code != 400 {
			t.Errorf("query %q status=%d body=%s", bad, w.Code, w.Body)
		}
	}
}

func TestPublicDigestDetailUsesPinnedProjectionAndDoesNotExposeInternals(t *testing.T) {
	const id = "11111111-1111-4111-8111-111111111111"
	fake := &digestAPIFake{fakeEditorialReader: &fakeEditorialReader{fakeReader: &fakeReader{}, edition: &repositories.EditorialDigestEdition{ID: id, Date: "2026-11-10", Locale: "ru", SnapshotHash: "SECRET_HASH", Edition: repositories.EditorialDigestContent{Version: "1", Date: "2026-11-10", Locale: "ru", Stories: []repositories.EditorialDigestStory{{ID: "main-1", Kind: "main", Title: "Главная", Summary: "Сводка", PrimaryArticleID: "42", ArticleIDs: []string{"42"}, Evidence: []repositories.EditorialDigestEvidence{{Quote: "PRIVATE QUOTE"}}}, {ID: "brief-1", Kind: "brief", Title: "Коротко", Summary: "Текст", PrimaryArticleID: "43", ArticleIDs: []string{"43"}}}}, Snapshot: repositories.EditorialDigestSnapshot{Date: "2026-11-10", Articles: []repositories.EditorialDigestArticle{{ID: "42", SourceName: "Источник", URL: "https://example.org/a", CategoryID: "7", CategoryName: "Тема", Content: "PRIVATE CONTENT"}, {ID: "43", SourceName: "Опасный", URL: "https://user:secret@example.org/private"}}}}}}
	mux := http.NewServeMux()
	testHandler(fake).Register(mux)
	w := request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+strings.ToUpper(id))
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("detail cache header=%q", w.Header().Get("Cache-Control"))
	}
	body := w.Body.String()
	for _, secret := range []string{"PRIVATE QUOTE", "PRIVATE CONTENT", "SECRET_HASH", "secret@", "snapshot", "evidence", "provider_usage", "rendered"} {
		if strings.Contains(body, secret) {
			t.Errorf("response leaks %q: %s", secret, body)
		}
	}
	var result publicDigestDetail
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Stories) != 2 || result.Stories[0].Kind != "main" || result.Stories[1].Kind != "brief" || result.Stories[1].CategoryID != "other" || len(result.Stories[1].Sources) != 0 || result.Categories[0].StoryCount != 1 {
		t.Fatalf("projection mismatch: %+v", result)
	}
	var object map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &object); err != nil {
		t.Fatal(err)
	}
	assertJSONKeys(t, object, "version", "date", "locale", "edition_id", "issue_url", "categories", "stories")
	assertJSONKeys(t, object["categories"].([]any)[0].(map[string]any), "id", "name", "story_count")
	assertJSONKeys(t, object["stories"].([]any)[0].(map[string]any), "id", "kind", "title", "summary", "category_id", "primary_article_id", "article_ids", "sources")
	assertJSONKeys(t, object["stories"].([]any)[0].(map[string]any)["sources"].([]any)[0].(map[string]any), "name", "url")
	if fake.requestedID != id || fake.requestedLocale != "ru" {
		t.Fatalf("reader request not pinned: %+v", fake)
	}
}

func TestPublicDigestDetailRejectsMalformedOrMismatchedData(t *testing.T) {
	const id = "11111111-1111-4111-8111-111111111111"
	fake := &digestAPIFake{fakeEditorialReader: &fakeEditorialReader{fakeReader: &fakeReader{}, edition: &repositories.EditorialDigestEdition{ID: id, Date: "2026-11-10", Locale: "ru", Edition: repositories.EditorialDigestContent{Version: "1", Date: "2026-11-10", Locale: "ru", Stories: []repositories.EditorialDigestStory{{ID: "bad", Kind: "main", Title: "Missing article", PrimaryArticleID: "999"}}}, Snapshot: repositories.EditorialDigestSnapshot{Date: "2026-11-10", Articles: []repositories.EditorialDigestArticle{}}}}}
	mux := http.NewServeMux()
	testHandler(fake).Register(mux)
	for _, path := range []string{"/api/public/digests/0000-11-10?locale=ru", "/api/public/digests/2026-02-30?locale=ru", "/api/public/digests/2026-11-10?locale=ru&locale=ru", "/api/public/digests/2026-11-10?locale=ru&edition=bad", "/api/public/digests/2026-11-10?locale=ru&edition=" + id + "&edition=" + id, "/api/public/digests/2026-11-10?locale=%ZZ"} {
		w := request(mux, "GET", path)
		if w.Code != 400 {
			t.Errorf("%s status=%d", path, w.Code)
		}
	}
	w := request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "digest_unavailable") {
		t.Fatalf("corrupt empty projection status=%d body=%s", w.Code, w.Body)
	}
	fake.edition.Snapshot.Articles = []repositories.EditorialDigestArticle{{ID: "42"}}
	fake.edition.Edition.Stories = append(fake.edition.Edition.Stories, repositories.EditorialDigestStory{ID: "valid", Kind: "main", Title: "Valid saved story", PrimaryArticleID: "42", ArticleIDs: []string{"42"}})
	w = request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 503 {
		t.Fatalf("partially corrupt edition status=%d, want 503", w.Code)
	}
	fake.edition.Edition.Stories = fake.edition.Edition.Stories[1:]
	fake.edition.Date = "2026-11-09"
	w = request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 503 {
		t.Fatalf("mismatched date status=%d", w.Code)
	}
	fake.edition.Date = "2026-11-10"
	fake.edition.Locale = "sr-Latn"
	w = request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 503 {
		t.Fatalf("mismatched locale status=%d", w.Code)
	}
	fake.edition.Locale = "ru"
	fake.edition.Edition.Locale = "sr-Latn"
	w = request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 503 {
		t.Fatalf("mismatched content locale status=%d", w.Code)
	}
	fake.edition.Edition.Locale = "ru"
	fake.edition.ID = "22222222-2222-4222-8222-222222222222"
	w = request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 503 {
		t.Fatalf("mismatched edition ID status=%d", w.Code)
	}
	fake.edition.ID = id
	fake.edition.Snapshot.Date = "2026-11-09"
	w = request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 503 {
		t.Fatalf("mismatched snapshot date status=%d", w.Code)
	}
	fake.edition.Snapshot.Date = "2026-11-10"
	fake.edition.Edition.Version = ""
	w = request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 503 {
		t.Fatalf("missing content version status=%d", w.Code)
	}
	fake.edition = nil
	w = request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 404 || fake.digestCalls != 0 {
		t.Fatalf("missing pinned issue status=%d fallback calls=%d body=%s", w.Code, fake.digestCalls, w.Body)
	}
	for _, path := range []string{"/api/public/digests/2026-11-10?locale=ru", "/api/public/digests/2026-11-10?locale=sr-Latn", "/api/public/digests/2026-11-10?locale=ru&edition=22222222-2222-4222-8222-222222222222"} {
		w = request(mux, "GET", path)
		if w.Code != 404 || fake.digestCalls != 0 {
			t.Errorf("missing/wrong-locale/pin request %q status=%d legacy fallback=%d", path, w.Code, fake.digestCalls)
		}
	}
	fake.catalogErr = errors.New("private database diagnostic")
	w = request(mux, "GET", "/api/public/digests?locale=ru")
	if w.Code != 503 || strings.Contains(w.Body.String(), "private database") {
		t.Fatalf("catalog error status=%d body=%s", w.Code, w.Body)
	}
	noCatalogMux := http.NewServeMux()
	testHandler(&fakeReader{}).Register(noCatalogMux)
	w = request(noCatalogMux, "GET", "/api/public/digests?locale=ru")
	if w.Code != 503 || !strings.Contains(w.Body.String(), "digest_unavailable") {
		t.Fatalf("missing reader capability status=%d body=%s", w.Code, w.Body)
	}
	noReaderMux := http.NewServeMux()
	testHandler(nil).Register(noReaderMux)
	w = request(noReaderMux, "GET", "/api/public/digests/2026-11-10?locale=ru")
	if w.Code != 503 {
		t.Fatalf("nil reader status=%d body=%s", w.Code, w.Body)
	}
}

func TestPublicDigestStoryOrderMatchesSharedEditorialProjection(t *testing.T) {
	const id = "11111111-1111-4111-8111-111111111111"
	stories := []repositories.EditorialDigestStory{
		{ID: "main", Kind: "main", Title: "Main", Summary: "Main summary", PrimaryArticleID: "1", ArticleIDs: []string{"1", "2", "3"}},
		{ID: "duplicate", Kind: "brief", Title: "Duplicate", PrimaryArticleID: "2", ArticleIDs: []string{"2"}},
		{ID: "brief-a", Kind: "brief", Title: "Brief A", PrimaryArticleID: "4", ArticleIDs: []string{"4"}},
		{ID: "brief-b", Kind: "brief", Title: "Brief B", PrimaryArticleID: "5", ArticleIDs: []string{"5"}},
		{ID: "brief-c", Kind: "brief", Title: "Brief C", PrimaryArticleID: "6", ArticleIDs: []string{"6"}},
	}
	articles := []repositories.EditorialDigestArticle{
		{ID: "1", CategoryID: "7", CategoryName: "Технологии"}, {ID: "2", CategoryID: "7", CategoryName: "Технологии"}, {ID: "3", CategoryID: "7", CategoryName: "Технологии"},
		{ID: "4", CategoryID: "8", CategoryName: "Мир"}, {ID: "5", CategoryID: "8", CategoryName: "Мир"}, {ID: "6", CategoryID: "7", CategoryName: "Технологии"},
	}
	edition := &repositories.EditorialDigestEdition{ID: id, Date: "2026-11-10", Locale: "ru", Edition: repositories.EditorialDigestContent{Version: "1", Date: "2026-11-10", Locale: "ru", Stories: stories}, Snapshot: repositories.EditorialDigestSnapshot{Date: "2026-11-10", Articles: articles}}
	reader := &digestAPIFake{fakeEditorialReader: &fakeEditorialReader{fakeReader: &fakeReader{}, edition: edition}}
	mux := http.NewServeMux()
	testHandler(reader).Register(mux)
	w := request(mux, "GET", "/api/public/digests/2026-11-10?locale=ru&edition="+id)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body)
	}
	var got publicDigestDetail
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	view := buildEditorialDigestView(time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC), edition)
	want := append([]DigestStoryView{}, view.Main...)
	for _, group := range view.BriefGroups {
		want = append(want, group.Stories...)
	}
	if len(got.Stories) != len(want) || len(got.Stories) != 4 {
		t.Fatalf("stories=%+v, want shared projection %+v", got.Stories, want)
	}
	for i, story := range got.Stories {
		if story.ID != want[i].ID || story.Title != want[i].Title || story.CategoryID != want[i].CategoryID || story.PrimaryArticleID != want[i].PrimaryArticleID || strings.Join(story.ArticleIDs, ",") != strings.Join(want[i].ArticleIDs, ",") {
			t.Errorf("story[%d]=%+v; shared projection=%+v", i, story, want[i])
		}
	}
	if got.Stories[0].ID != "main" || got.Stories[1].ID != "brief-a" || got.Stories[2].ID != "brief-b" || got.Stories[3].ID != "brief-c" || got.Categories[0].StoryCount != 2 || got.Categories[1].StoryCount != 2 {
		t.Fatalf("dedupe/order/category counts mismatch: %+v", got)
	}
}

func assertJSONKeys(t *testing.T, got map[string]any, expected ...string) {
	t.Helper()
	if len(got) != len(expected) {
		t.Fatalf("JSON keys %v; want exactly %v", got, expected)
	}
	for _, key := range expected {
		if _, ok := got[key]; !ok {
			t.Fatalf("JSON keys %v; missing %q", got, key)
		}
	}
}
