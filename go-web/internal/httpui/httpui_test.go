package httpui

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
)

type fakeReader struct {
	feed                      repositories.FeedResponse
	article                   map[string]any
	digest                    repositories.DigestPage
	err                       error
	articleIncludeUnpublished bool
}

func (f *fakeReader) Feed(_ context.Context, q repositories.FeedQuery) (repositories.FeedResponse, error) {
	if f.err != nil {
		return repositories.FeedResponse{}, f.err
	}
	out := f.feed
	out.Limit = q.Limit
	out.Offset = q.Offset
	return out, nil
}
func (f *fakeReader) Article(_ context.Context, _ int64, include bool) (map[string]any, error) {
	f.articleIncludeUnpublished = include
	if f.err != nil {
		return nil, f.err
	}
	return f.article, nil
}
func (f *fakeReader) CategoriesConfig(context.Context) (map[string]any, error) {
	if f.err != nil {
		return nil, f.err
	}
	return map[string]any{"categories": map[string]any{"tech": map[string]string{"name": "Технологии", "color": "#123456"}}}, nil
}
func (f *fakeReader) Categories(context.Context, repositories.StatsQuery) (map[string]any, error) {
	if f.err != nil {
		return nil, f.err
	}
	return map[string]any{"categories": map[string]int{"all": 1, "tech": 1}, "total": 1, "filters": map[string]any{"since_hours": 24, "hide_ads": true}}, nil
}
func (f *fakeReader) Sources(context.Context, repositories.StatsQuery) (map[string]any, error) {
	if f.err != nil {
		return nil, f.err
	}
	return map[string]any{"sources": map[string]int{"7": 1}, "source_names": map[string]string{"7": "Fixture source"}, "total": 1, "filters": map[string]any{"since_hours": nil, "hide_ads": false}}, nil
}
func (f *fakeReader) Search(_ context.Context, q repositories.SearchQuery) (map[string]any, error) {
	if f.err != nil {
		return nil, f.err
	}
	return map[string]any{"articles": []any{}, "query": q.Query, "results_count": 0}, nil
}
func (f *fakeReader) Digest(context.Context, time.Time) (repositories.DigestPage, error) {
	if f.err != nil {
		return repositories.DigestPage{}, f.err
	}
	return f.digest, nil
}
func (f *fakeReader) APIFeed(context.Context, repositories.APIFeedQuery) (repositories.APIFeedResponse, error) {
	if f.err != nil {
		return repositories.APIFeedResponse{}, f.err
	}
	return repositories.APIFeedResponse{Version: "2.0", Title: "Evening News v2 Feed", Items: []map[string]any{}, Pagination: repositories.APIFeedPagination{Limit: 20, Count: 0}}, nil
}
func (f *fakeReader) RSS(context.Context, int) ([]repositories.RSSArticle, error) {
	if f.err != nil {
		return nil, f.err
	}
	return []repositories.RSSArticle{}, nil
}

func testHandler(reader *fakeReader) *Handler {
	return New(Config{Reader: reader, TemplatesDir: "../../templates", StaticDir: "../../../web/static", StartedAt: time.Date(2026, 10, 26, 10, 0, 0, 0, time.UTC), PublicAdmin: func(r *http.Request) bool { return r.Header.Get("X-Test-Admin") == "yes" }})
}

func request(h http.Handler, method, path string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest(method, path, nil))
	return w
}

func TestPublicReadRoutesUseReaderAndPreserveBasicShapes(t *testing.T) {
	reader := &fakeReader{feed: repositories.FeedResponse{Articles: []map[string]any{}, Total: 0}}
	mux := http.NewServeMux()
	h := testHandler(reader)
	h.Register(mux)
	w := request(mux, "GET", "/api/public/feed?limit=7&offset=3&since_hours=48&category=tech&source=5&hide_ads=false")
	if w.Code != 200 {
		t.Fatalf("feed status=%d body=%s", w.Code, w.Body)
	}
	if !strings.Contains(w.Body.String(), `"limit":7`) || !strings.Contains(w.Body.String(), `"offset":3`) {
		t.Fatalf("feed response shape changed: %s", w.Body)
	}
	w = request(mux, "GET", "/api/public/categories/config")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"tech"`) {
		t.Fatalf("category config: %d %s", w.Code, w.Body)
	}
	w = request(mux, "GET", "/api/public/sources?since_hours=24")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"source_names"`) {
		t.Fatalf("sources: %d %s", w.Code, w.Body)
	}
	w = request(mux, "GET", "/api/public/search?q=go&sort=date")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"query":"go"`) {
		t.Fatalf("search: %d %s", w.Code, w.Body)
	}
	w = request(mux, "GET", "/api/v1/feed.rss")
	if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "application/rss+xml") {
		t.Fatalf("rss: %d %s", w.Code, w.Header().Get("Content-Type"))
	}
}

func TestPublicValidationAndRepositoryFailureAreNotSuccess(t *testing.T) {
	mux := http.NewServeMux()
	h := testHandler(&fakeReader{err: errors.New("database unavailable")})
	h.Register(mux)
	for _, path := range []string{"/api/public/feed?limit=0", "/api/public/search?q=x", "/api/public/search?sort=bad", "/api/public/sources?since_hours=999"} {
		w := request(mux, "GET", path)
		if w.Code != http.StatusUnprocessableEntity {
			t.Errorf("%s status=%d body=%s", path, w.Code, w.Body)
		}
	}
	w := request(mux, "GET", "/api/public/feed")
	if w.Code != http.StatusServiceUnavailable || strings.Contains(w.Body.String(), `"articles":[{"id":1`) {
		t.Fatalf("repository failure was not explicit: %d %s", w.Code, w.Body)
	}
	w = request(mux, "GET", "/api/public/article/1")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("article failure status=%d body=%s", w.Code, w.Body)
	}
}

func TestPublicArticlePassesOnlyAdminVisibilityFlag(t *testing.T) {
	reader := &fakeReader{article: map[string]any{"id": 8, "title": "headline"}}
	h := testHandler(reader)
	mux := http.NewServeMux()
	h.Register(mux)
	w := request(mux, "GET", "/api/public/article/8")
	if w.Code != 200 || reader.articleIncludeUnpublished {
		t.Fatalf("anonymous article state code=%d include=%t", w.Code, reader.articleIncludeUnpublished)
	}
	req := httptest.NewRequest("GET", "/api/public/article/8", nil)
	req.Header.Set("X-Test-Admin", "yes")
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 || !reader.articleIncludeUnpublished {
		t.Fatalf("admin article state code=%d include=%t", w.Code, reader.articleIncludeUnpublished)
	}
}

func TestDigestTemplateEscapesUntrustedArticleText(t *testing.T) {
	reader := &fakeReader{digest: repositories.DigestPage{Summaries: []repositories.DigestSummary{{Category: "Tech", Text: "safe"}}, Categories: []repositories.DigestCategory{{Name: "Tech", Articles: []repositories.DigestArticle{{Title: `<script>alert(1)</script>`, URL: `javascript:alert(1)`, SourceName: `<b>source</b>`}}}}}}
	h := testHandler(reader)
	mux := http.NewServeMux()
	h.Register(mux)
	w := request(mux, "GET", "/digest/2026-10-26")
	if w.Code != 200 {
		t.Fatalf("digest status=%d body=%s", w.Code, w.Body)
	}
	if strings.Contains(w.Body.String(), "<script>alert(1)</script>") || strings.Contains(w.Body.String(), `href="javascript:alert(1)"`) {
		t.Fatalf("untrusted content escaped unsafely: %s", w.Body)
	}
}

func TestPublicPagesRedirectsAndHealth(t *testing.T) {
	h := testHandler(&fakeReader{})
	mux := http.NewServeMux()
	h.Register(mux)
	w := request(mux, "GET", "/news")
	if w.Code != 301 || w.Header().Get("Location") != "/" {
		t.Fatalf("news redirect=%d %s", w.Code, w.Header().Get("Location"))
	}
	w = request(mux, "GET", "/health")
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"status":"healthy"}` {
		t.Fatalf("health=%d %s", w.Code, w.Body)
	}
	w = request(mux, "GET", "/")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `id="news-list"`) || !strings.Contains(w.Body.String(), "window.loadMoreNews") || !strings.Contains(w.Body.String(), ".news-list-item") {
		t.Fatalf("root page incomplete: %d body=%s", w.Code, w.Body.String())
	}
	w = request(mux, "GET", "/search")
	if w.Code != 200 || !strings.Contains(w.Body.String(), `id="searchQuery"`) || !strings.Contains(w.Body.String(), `id="pagination"`) || !strings.Contains(w.Body.String(), `id="modalTitle"`) {
		t.Fatalf("search page incomplete: %d", w.Code)
	}
	w = request(mux, "GET", "/health/")
	if w.Code != 404 {
		t.Fatalf("trailing slash status=%d", w.Code)
	}
}

func TestPublicResponsesMatchPythonDataServiceGolden(t *testing.T) {
	_, sourceFile, _, _ := runtime.Caller(0)
	fixturePath := filepath.Join(filepath.Dir(sourceFile), "../../../tests/fixtures/web-contract-v1/public-read-golden.json")
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		t.Fatal(err)
	}
	var golden map[string]json.RawMessage
	if err := json.Unmarshal(data, &golden); err != nil {
		t.Fatal(err)
	}
	var feed repositories.FeedResponse
	if err := json.Unmarshal(golden["GET /api/public/feed?limit=4&offset=2"], &feed); err != nil {
		t.Fatal(err)
	}
	var article map[string]any
	if err := json.Unmarshal(golden["GET /api/public/article/42"], &article); err != nil {
		t.Fatal(err)
	}
	reader := &fakeReader{feed: feed, article: article}
	mux := http.NewServeMux()
	testHandler(reader).Register(mux)
	for key, want := range golden {
		path := strings.TrimPrefix(key, "GET ")
		w := request(mux, "GET", path)
		if w.Code != http.StatusOK {
			t.Fatalf("%s status=%d body=%s", path, w.Code, w.Body)
		}
		var got any
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		var expected any
		if err := json.Unmarshal(want, &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(got, expected) {
			t.Errorf("%s response differs from Python golden\nwant: %#v\n got: %#v", path, expected, got)
		}
	}
}
