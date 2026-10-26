package httpui

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
)

type adminTemplateRenderer interface {
	RenderTemplate(http.ResponseWriter, *http.Request, string, any) error
}

type AdminHandler struct {
	auth     *auth.Service
	renderer adminTemplateRenderer
}

func RegisterAdmin(mux *http.ServeMux, service *auth.Service, renderer adminTemplateRenderer) {
	(&AdminHandler{auth: service, renderer: renderer}).Register(mux)
}

func (h *AdminHandler) Register(mux *http.ServeMux) {
	mux.Handle("GET /admin/", http.NotFoundHandler())
	mux.HandleFunc("GET /admin", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/admin/", http.StatusTemporaryRedirect)
	})
	mux.HandleFunc("GET /admin/{$}", h.page("dashboard.html", "Evening News v2 - Admin", "dashboard"))
	mux.HandleFunc("GET /admin/sources", h.page("sources.html", "Источники - Evening News v2", "sources"))
	mux.HandleFunc("GET /admin/summaries", h.page("summaries.html", "Дневные сводки - Evening News v2", "summaries"))
	mux.HandleFunc("GET /admin/stats", h.page("stats.html", "Статистика Системы", "stats"))
	mux.HandleFunc("GET /admin/backup", h.page("backup.html", "Резервные Копии", ""))
	mux.HandleFunc("GET /admin/categories", h.page("categories.html", "Категории", "categories"))
	mux.HandleFunc("GET /admin/settings", h.page("settings.html", "Настройки - Evening News v2", "settings"))
	mux.HandleFunc("GET /admin/telegram", h.redirectSettings)
	mux.HandleFunc("GET /admin/schedule", h.redirectSettings)
}

func (h *AdminHandler) page(templateName, title, activeMenu string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		principal, err := h.adminPagePrincipal(r)
		if err != nil {
			writeAdminAuthError(w, err)
			return
		}
		if h.renderer == nil {
			http.Error(w, "page unavailable", http.StatusInternalServerError)
			return
		}
		buffer := httptest.NewRecorder()
		data := map[string]any{"Title": title, "ActiveMenu": activeMenu, "AdminUser": principal.Username}
		if err := h.renderer.RenderTemplate(buffer, r, "admin/"+templateName, data); err != nil {
			http.Error(w, "page unavailable", http.StatusInternalServerError)
			return
		}
		cookie, err := h.auth.AdminCookie(principal.Username)
		if err != nil {
			http.Error(w, "page unavailable", http.StatusInternalServerError)
			return
		}
		for name, values := range buffer.Header() {
			for _, value := range values {
				w.Header().Add(name, value)
			}
		}
		http.SetCookie(w, cookie)
		w.WriteHeader(buffer.Code)
		_, _ = w.Write(buffer.Body.Bytes())
	}
}

func (h *AdminHandler) redirectSettings(w http.ResponseWriter, r *http.Request) {
	if _, err := h.adminPagePrincipal(r); err != nil {
		writeAdminAuthError(w, err)
		return
	}
	http.Redirect(w, r, "/admin/settings", http.StatusFound)
}

func (h *AdminHandler) adminPagePrincipal(r *http.Request) (auth.Principal, error) {
	return h.auth.AdminPage(r)
}

func RequireAPIAdmin(service *auth.Service, next http.Handler) http.Handler {
	return auth.RequireAPIAdmin(service, next)
}

func mutatingMethod(method string) bool {
	switch method {
	case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		return true
	default:
		return false
	}
}

func hasValidExplicitAuthorization(service *auth.Service, r *http.Request) bool {
	if strings.TrimSpace(r.Header.Get("Authorization")) == "" {
		return false
	}
	request := r.Clone(r.Context())
	request.Header = r.Header.Clone()
	request.Header.Del("Cookie")
	request.Header.Del("X-authentik-username")
	_, err := service.API(request)
	return err == nil
}

func hasCrossSiteOrigin(r *http.Request) bool {
	if site := strings.ToLower(strings.TrimSpace(r.Header.Get("Sec-Fetch-Site"))); site == "cross-site" {
		return true
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return false
	}
	parsed, err := url.Parse(origin)
	if err != nil || parsed.Host == "" || parsed.User != nil || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return true
	}
	if parsed.Scheme != "https" && parsed.Scheme != "http" {
		return true
	}
	if !sameOriginHost(parsed.Host, r.Host) {
		return true
	}
	if r.TLS != nil && parsed.Scheme != "https" {
		return true
	}
	return false
}

func sameOriginHost(a, b string) bool {
	return strings.EqualFold(strings.TrimSuffix(a, "."), strings.TrimSuffix(b, "."))
}

func writeAdminAuthError(w http.ResponseWriter, err error) {
	var authErr *auth.AuthError
	if errors.As(err, &authErr) {
		if authErr.Challenge != "" {
			w.Header().Set("WWW-Authenticate", authErr.Challenge)
		}
		writeAdminJSON(w, authErr.Status, map[string]string{"detail": authErr.Detail})
		return
	}
	writeAdminJSON(w, http.StatusUnauthorized, map[string]string{"detail": "Invalid or missing credentials"})
}

func writeAdminJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
