package httpui

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
)

func TestManagementRoutesRequireAdmin(t *testing.T) {
	a := auth.New(auth.Config{Username: "fixture", Password: "fixture", JWTSecret: "fixture-secret"})
	mux := http.NewServeMux()
	RegisterManagement(mux, ManagementConfig{Auth: a})
	r := httptest.NewRequest("GET", "/api/v1/sources", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != 401 || !strings.Contains(w.Body.String(), "detail") {
		t.Fatalf("expected API auth rejection, got %d: %s", w.Code, w.Body.String())
	}
}

func TestManagementAuthPrecedesQueryParsingAndDatabaseAccess(t *testing.T) {
	a := auth.New(auth.Config{Username: "fixture", Password: "fixture", JWTSecret: "fixture-secret"})
	mux := http.NewServeMux()
	RegisterManagement(mux, ManagementConfig{Auth: a})
	request := httptest.NewRequest("GET", "/api/v1/stats/dashboard?days=broken", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated malformed query should fail auth first: %d %s", response.Code, response.Body.String())
	}
	token, err := a.Mint("fixture", auth.LevelAdmin)
	if err != nil {
		t.Fatal(err)
	}
	request = httptest.NewRequest("GET", "/api/v1/stats/dashboard?days=broken", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusUnprocessableEntity {
		t.Fatalf("authenticated malformed query should be rejected before DB access: %d %s", response.Code, response.Body.String())
	}
}

func TestEditorialDigestStatusRequiresAdminAndPinsOnlyReadyEditions(t *testing.T) {
	a := auth.New(auth.Config{Username: "fixture", Password: "fixture", JWTSecret: "fixture-secret"})
	token, err := a.Mint("fixture", auth.LevelAdmin)
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	ready := true
	mux := http.NewServeMux()
	RegisterManagement(mux, ManagementConfig{
		Auth: a,
		EditorialDigestStatus: func(_ context.Context, date time.Time, locale string) (repositories.EditorialDigestStatus, error) {
			calls++
			status := repositories.EditorialDigestStatus{Date: date.Format("2006-01-02"), Locale: locale, Ready: ready}
			if ready {
				status.EditionID = "8c93bb34-383d-449c-9e85-28aa9121a4fe"
				status.MainCount, status.BriefCount, status.StoryCount, status.ArticleCount = 1, 40, 41, 42
			}
			return status, nil
		},
	})
	call := func(path string, authorized bool) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, path, nil)
		if authorized {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	if w := call("/api/v1/summaries/editorial-status?date=2026-10-27", false); w.Code != http.StatusUnauthorized || calls != 0 {
		t.Fatalf("unauthenticated status read reached repository: status=%d calls=%d", w.Code, calls)
	}
	for _, path := range []string{
		"/api/v1/summaries/editorial-status",
		"/api/v1/summaries/editorial-status?date=2026-02-30",
		"/api/v1/summaries/editorial-status?date=2026-10-27&date=2026-10-27",
		"/api/v1/summaries/editorial-status?date=2026-10-27&locale=",
		"/api/v1/summaries/editorial-status?date=2026-10-27&locale=ru&locale=ru",
		"/api/v1/summaries/editorial-status?date=2026-10-27&locale=sr",
	} {
		if w := call(path, true); w.Code != http.StatusUnprocessableEntity || calls != 0 {
			t.Errorf("invalid status query %q reached repository: status=%d calls=%d body=%s", path, w.Code, calls, w.Body)
		}
	}
	w := call("/api/v1/summaries/editorial-status?date=2026-10-27&locale=ru", true)
	var result repositories.EditorialDigestStatus
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil {
		t.Fatalf("ready status: %d %s", w.Code, w.Body)
	}
	if !result.Ready || result.EditionID != "8c93bb34-383d-449c-9e85-28aa9121a4fe" || result.PreviewURL != "/digest/2026-10-27?locale=ru&edition="+result.EditionID || result.MainCount != 1 || result.BriefCount != 40 || result.ArticleCount != 42 {
		t.Fatalf("ready edition metadata/link mismatch: %+v", result)
	}
	ready = false
	w = call("/api/v1/summaries/editorial-status?date=2026-10-27", true)
	result = repositories.EditorialDigestStatus{}
	if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Ready || result.EditionID != "" || result.PreviewURL != "" || result.MainCount != 0 || result.BriefCount != 0 || result.ArticleCount != 0 {
		t.Fatalf("missing edition was presented as ready: status=%d result=%+v body=%s", w.Code, result, w.Body)
	}
	ready = true
	if w = call("/api/v1/summaries/editorial-status?date=2026-10-27", true); w.Code != http.StatusOK {
		t.Fatalf("status read after no legacy rows should succeed: %d %s", w.Code, w.Body)
	}
}

func TestScheduledJobsReceiverRejectsBadEnvelopeBeforeService(t *testing.T) {
	mux := http.NewServeMux()
	RegisterManagement(mux, ManagementConfig{ScheduledJobsToken: "fixture-token"})
	r := httptest.NewRequest("POST", "/api/v1/internal/scheduled-jobs", strings.NewReader(`{"version":1,"unexpected":true}`))
	r.Header.Set("Authorization", "Bearer fixture-token")
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != 422 || !strings.Contains(w.Body.String(), "invalid scheduled job envelope") {
		t.Fatalf("expected strict envelope rejection, got %d: %s", w.Code, w.Body.String())
	}
}

func TestPublicReprocessRejectsCrossOriginCookieRequestsBeforeCommand(t *testing.T) {
	a := auth.New(auth.Config{Username: "fixture", Password: "fixture", JWTSecret: "fixture-secret"})
	token, err := a.Mint("fixture", auth.LevelAdmin)
	if err != nil {
		t.Fatal(err)
	}
	readerToken, err := a.Mint("reader", auth.LevelAPIRead)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	RegisterManagement(mux, ManagementConfig{
		Auth: a,
		PublicAdmin: func(r *http.Request) bool {
			_, ok := a.PublicAdmin(r)
			return ok
		},
	})

	request := func(origin, fetchSite, authorization string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, "https://news.fixture.test/api/public/article/not-an-id/reprocess", strings.NewReader(`{}`))
		r.Header.Set("Content-Type", "application/json")
		r.AddCookie(&http.Cookie{Name: auth.AdminCookie, Value: token})
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if fetchSite != "" {
			r.Header.Set("Sec-Fetch-Site", fetchSite)
		}
		if authorization != "" {
			r.Header.Set("Authorization", authorization)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}

	for _, tc := range []struct {
		name, origin, fetchSite, authorization string
	}{
		{name: "same-site different host", origin: "https://evil.fixture.test", fetchSite: "same-site", authorization: "Bearer " + readerToken},
		{name: "null origin", origin: "null"},
		{name: "cross-site fetch metadata", fetchSite: "cross-site"},
		{name: "malformed origin", origin: "https://news.fixture.test/attacker-path"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := request(tc.origin, tc.fetchSite, tc.authorization)
			if w.Code != http.StatusForbidden {
				t.Fatalf("cross-origin cookie request must be rejected before article parsing/enqueue: %d %s", w.Code, w.Body.String())
			}
			var body map[string]string
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["detail"] != "Cross-site cookie request rejected" {
				t.Fatalf("unexpected CSRF rejection body: %s (%v)", w.Body.String(), err)
			}
		})
	}

	goodOrigin := request("https://news.fixture.test", "same-origin", "")
	if goodOrigin.Code != http.StatusUnprocessableEntity {
		t.Fatalf("same-origin admin cookie should pass auth and origin checks before invalid path validation, got %d %s", goodOrigin.Code, goodOrigin.Body.String())
	}

	anonymous := httptest.NewRequest(http.MethodPost, "https://news.fixture.test/api/public/article/not-an-id/reprocess", strings.NewReader(`{}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, anonymous)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("request without public-admin cookie or trusted identity should remain unauthorized, got %d %s", w.Code, w.Body.String())
	}
}
