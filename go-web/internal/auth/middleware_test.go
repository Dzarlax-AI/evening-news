package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestAdminMiddlewareUsesCanonicalSchemeForCSRF(t *testing.T) {
	service := New(Config{Username: "admin", Password: "fixture", JWTSecret: "fixture-secret"})
	cookie, err := service.AdminCookie("admin")
	if err != nil {
		t.Fatal(err)
	}
	handler := RequireAPIAdmin(service, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) }))
	request := httptest.NewRequest(http.MethodPost, "http://news.example/api", nil)
	request.Host = "news.example"
	request.URL.Scheme = "https"
	request.Header.Set("Origin", "http://news.example")
	request.AddCookie(cookie)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("canonical HTTPS origin mismatch was accepted: %d", response.Code)
	}
}
