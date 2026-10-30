package httpui

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
)

func adminTestService() *auth.Service {
	return auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "admin-test-jwt-secret", Development: true})
}

func adminBasicHeader() string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte("admin:secret"))
}

func adminMux(t *testing.T, service *auth.Service) *http.ServeMux {
	t.Helper()
	renderer := New(Config{TemplatesDir: "../../templates"})
	mux := http.NewServeMux()
	RegisterAdmin(mux, service, renderer)
	return mux
}

func TestAdminRoutesRenderExistingPagesAndSetCookie(t *testing.T) {
	service := adminTestService()
	mux := adminMux(t, service)
	tests := []struct {
		path, title, marker, activeHref string
	}{
		{"/admin/", "Evening News v2 - Admin", "Дашборд", "/admin"},
		{"/admin/sources", "Источники - Evening News v2", "Источники", "/admin/sources"},
		{"/admin/summaries", "Дневные сводки - Evening News v2", "Сводки", "/admin/summaries"},
		{"/admin/stats", "Статистика - Evening News v2", "stats-header", "/admin/stats"},
		{"/admin/backup", "Резервные Копии", "backups-list", "/admin/backup"},
		{"/admin/categories", "Категории", "add-category-form", "/admin/categories"},
		{"/admin/settings", "Настройки - Evening News v2", "chat_id", "/admin/settings"},
	}
	for _, tc := range tests {
		t.Run(tc.path, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodGet, tc.path, nil)
			request.Header.Set("Authorization", adminBasicHeader())
			response := httptest.NewRecorder()
			mux.ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			if !strings.Contains(response.Header().Get("Content-Type"), "text/html") {
				t.Fatalf("content type=%q", response.Header().Get("Content-Type"))
			}
			if !strings.Contains(response.Body.String(), "<title>"+tc.title+" — Evening News</title>") {
				t.Fatalf("page title not preserved: %s", response.Body.String()[:min(response.Body.Len(), 160)])
			}
			if !strings.Contains(response.Body.String(), tc.marker) {
				t.Fatalf("missing %q marker", tc.marker)
			}
			cookie := response.Header().Get("Set-Cookie")
			for _, attr := range []string{"admin_token=", "HttpOnly", "SameSite=Lax", "Path=/"} {
				if !strings.Contains(cookie, attr) {
					t.Fatalf("cookie missing %q: %s", attr, cookie)
				}
			}
			if !strings.Contains(response.Body.String(), `href="`+tc.activeHref+`" class="sidenav-link active"`) {
				t.Fatalf("active navigation missing for %s", tc.activeHref)
			}
		})
	}
}

func TestAdminCookieAndTrustedForwardAuthPagePaths(t *testing.T) {
	service := adminTestService()
	token, err := service.Mint("cookie-admin", auth.LevelAdmin)
	if err != nil {
		t.Fatal(err)
	}
	mux := adminMux(t, service)
	request := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	request.AddCookie(&http.Cookie{Name: auth.AdminCookie, Value: token})
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusUnauthorized || response.Header().Get("WWW-Authenticate") != "Basic" {
		t.Fatalf("admin page must retain Basic auth boundary, status=%d headers=%v", response.Code, response.Header())
	}

	forwarded := auth.New(auth.Config{TrustForwardAuth: true, JWTSecret: "forward-jwt-secret"})
	forwardMux := adminMux(t, forwarded)
	forwardRequest := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	forwardRequest.Header.Set("X-authentik-username", "proxy-admin")
	forwardResponse := httptest.NewRecorder()
	forwardMux.ServeHTTP(forwardResponse, forwardRequest)
	if forwardResponse.Code != 200 || !strings.Contains(forwardResponse.Header().Get("Set-Cookie"), "admin_token=") {
		t.Fatalf("forward auth status=%d headers=%v", forwardResponse.Code, forwardResponse.Header())
	}
}

func TestAdminRedirectsAndUnknownPaths(t *testing.T) {
	mux := adminMux(t, adminTestService())
	request := httptest.NewRequest(http.MethodGet, "/admin", nil)
	response := httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusTemporaryRedirect || response.Header().Get("Location") != "/admin/" {
		t.Fatalf("admin redirect=%d %v", response.Code, response.Header())
	}
	request = httptest.NewRequest(http.MethodGet, "/admin/unknown", nil)
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown admin status=%d", response.Code)
	}
	request = httptest.NewRequest(http.MethodGet, "/admin/schedule", nil)
	request.Header.Set("Authorization", adminBasicHeader())
	response = httptest.NewRecorder()
	mux.ServeHTTP(response, request)
	if response.Code != http.StatusFound || response.Header().Get("Location") != "/admin/settings" {
		t.Fatalf("schedule redirect=%d %v", response.Code, response.Header())
	}
}

func TestAdminTemplateEscapesDynamicPageTitle(t *testing.T) {
	renderer := New(Config{TemplatesDir: "../../templates"})
	r := httptest.NewRequest(http.MethodGet, "/admin/", nil)
	w := httptest.NewRecorder()
	payload := `"><script>alert(1)</script>`
	if err := renderer.RenderTemplate(w, r, "admin/dashboard.html", map[string]any{"Title": payload, "ActiveMenu": "dashboard"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(w.Body.String(), payload) || !strings.Contains(w.Body.String(), "&lt;script&gt;") {
		t.Fatalf("template did not escape dynamic title: %s", w.Body.String()[:min(w.Body.Len(), 240)])
	}
}

func TestRedirectedPageTemplatesRemainRenderable(t *testing.T) {
	renderer := New(Config{TemplatesDir: "../../templates"})
	for _, tc := range []struct{ name, marker string }{
		{"admin/telegram.html", "id=\"connection-status\""},
		{"admin/schedule.html", "id=\"schedule-container\""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			err := renderer.RenderTemplate(w, httptest.NewRequest(http.MethodGet, "/admin/", nil), tc.name, map[string]any{"Title": "Page", "ActiveMenu": "settings"})
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(w.Body.String(), tc.marker) {
				t.Fatalf("missing page marker %q", tc.marker)
			}
		})
	}
}

func TestRequireAPIAdminCSRFAndNativeCredentials(t *testing.T) {
	service := adminTestService()
	adminToken, _ := service.Mint("admin", auth.LevelAdmin)
	readerToken, _ := service.Mint("reader", auth.LevelAPIRead)
	mutated := 0
	protected := RequireAPIAdmin(service, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mutated++
		w.WriteHeader(http.StatusNoContent)
	}))
	tests := []struct {
		name, origin, fetchSite, authorization, cookie string
		status                                         int
	}{
		{"cookie hostile origin", "https://evil.example", "", "", adminToken, 403},
		{"cookie cross-site metadata", "", "cross-site", "", adminToken, 403},
		{"cookie with invalid bearer", "https://evil.example", "cross-site", "Bearer invalid", adminToken, 403},
		{"cookie same origin", "https://news.example", "same-origin", "", adminToken, 204},
		{"bearer native client", "https://evil.example", "cross-site", "Bearer " + adminToken, adminToken, 204},
		{"basic native client", "https://evil.example", "cross-site", adminBasicHeader(), adminToken, 204},
		{"reader token", "", "", "Bearer " + readerToken, "", 403},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "https://news.example/api/v1/schedule/settings", nil)
			r.Host = "news.example"
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if tc.fetchSite != "" {
				r.Header.Set("Sec-Fetch-Site", tc.fetchSite)
			}
			if tc.authorization != "" {
				r.Header.Set("Authorization", tc.authorization)
			}
			if tc.cookie != "" {
				r.AddCookie(&http.Cookie{Name: auth.AdminCookie, Value: tc.cookie})
			}
			w := httptest.NewRecorder()
			protected.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
	if mutated != 3 {
		t.Fatalf("mutation handler calls=%d, want 3", mutated)
	}
}

func TestCSRFOriginParserRejectsMalformedOriginAndAllowsAbsentOrigin(t *testing.T) {
	for _, tc := range []struct {
		origin string
		want   bool
	}{{"", false}, {"null", true}, {"javascript://news.example", true}, {"https://evil.example", true}, {"https://news.example", false}} {
		r := httptest.NewRequest(http.MethodPost, "https://news.example/x", nil)
		r.Host = "news.example"
		if tc.origin != "" {
			r.Header.Set("Origin", tc.origin)
		}
		if got := hasCrossSiteOrigin(r); got != tc.want {
			t.Fatalf("Origin=%q got %v want %v", tc.origin, got, tc.want)
		}
	}
}

func TestManagedRecoveryPageHasNoMutationControls(t *testing.T) {
	renderer := New(Config{TemplatesDir: "../../templates"})
	response := httptest.NewRecorder()
	err := renderer.RenderTemplate(response, httptest.NewRequest("GET", "/admin/backup", nil), "admin/backup.html", map[string]any{"Title": "Резервные Копии", "Managed": true, "ActiveMenu": "backup", "AdminUser": "fixture-admin"})
	if err != nil {
		t.Fatal(err)
	}
	body := response.Body.String()
	if !strings.Contains(body, "managed-recovery-help") || !strings.Contains(body, "managed-settings-recovery.md") || !strings.Contains(body, "configuration.md#fresh-managed-installation") || !strings.Contains(body, "HTTP-only") {
		t.Fatal("managed recovery guidance missing")
	}
	if !strings.Contains(body, `href="/admin/backup" class="sidenav-link active"`) || !strings.Contains(body, "Справка по бэкапу") {
		t.Fatal("managed backup help navigation missing")
	}
	for _, forbidden := range []string{"createBackup()", "showUploadModal()", "restore-modal", "schedule-enabled", "backups-list", "/api/v1/backup"} {
		if strings.Contains(body, forbidden) {
			t.Fatalf("managed guidance exposes mutation UI: %s", forbidden)
		}
	}
}
