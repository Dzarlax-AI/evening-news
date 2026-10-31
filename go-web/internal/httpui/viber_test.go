package httpui

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/backend"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/transport"
)

func TestViberPublicCallbackAndAdminBoundary(t *testing.T) {
	body := []byte("{ \"event\" : \"webhook\",\n \"timestamp\":1 }")
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		switch r.URL.Path {
		case "/internal/web/v1/viber/webhook":
			var in struct {
				Body      []byte `json:"body"`
				Signature string `json:"signature"`
			}
			if json.NewDecoder(r.Body).Decode(&in) != nil || !bytes.Equal(in.Body, body) || in.Signature != strings.Repeat("a", 64) {
				t.Fatal("callback bytes or signature changed")
			}
		case "/internal/web/v1/viber/test":
			if r.Method != "POST" {
				t.Fatal("test method changed")
			}
		case "/internal/web/v1/viber/preview":
			if r.URL.Query().Get("edition_id") != "00000000-0000-4000-8000-000000000001" {
				t.Fatal("preview edition query lost")
			}
		default:
			t.Fatalf("unexpected private route: %s", r.URL.Path)
		}
		io.WriteString(w, `{"ok":true}`)
	}))
	defer upstream.Close()
	client, e := backend.NewClient(backend.Config{BaseURL: upstream.URL, Token: "synthetic-private-token"})
	if e != nil {
		t.Fatal(e)
	}
	service := auth.New(auth.Config{Username: "admin", Password: "password", JWTSecret: "synthetic-signing-key"})
	mux := http.NewServeMux()
	RegisterBackend(mux, service, client)
	handler, e := (transport.Policy{TrustedHosts: []string{"example.com"}, PublicOrigin: "https://example.com"}).Wrap(mux)
	if e != nil {
		t.Fatal(e)
	}
	for _, path := range []string{"/api/v1/viber/verify", "/api/v1/viber/connect", "/api/v1/viber/test", "/api/v1/configuration/viber"} {
		w := httptest.NewRecorder()
		method := "POST"
		if strings.Contains(path, "configuration") {
			method = "PUT"
		}
		handler.ServeHTTP(w, httptest.NewRequest(method, path, strings.NewReader(`{}`)))
		if w.Code != 401 || calls != 0 {
			t.Fatal("unauthorized Viber configuration reached domain")
		}
	}
	req := httptest.NewRequest("POST", "/integrations/viber/webhook", bytes.NewReader(body))
	req.Header.Set("X-Viber-Content-Signature", strings.Repeat("a", 64))
	req.Header.Set("X-Forwarded-Proto", "https")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 403 || calls != 0 {
		t.Fatal("untrusted HTTP callback was accepted")
	}
	req = httptest.NewRequest("POST", "https://example.com/integrations/viber/webhook", bytes.NewReader(body))
	req.Header.Set("X-Viber-Content-Signature", strings.Repeat("a", 64))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 || calls != 1 {
		t.Fatalf("signed callback relay status %d", w.Code)
	}
	req = httptest.NewRequest("POST", "https://example.com/integrations/viber/webhook", bytes.NewReader(body))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 401 || calls != 1 {
		t.Fatal("missing signature reached private domain")
	}
	req = httptest.NewRequest("POST", "https://example.com/integrations/viber/webhook", bytes.NewReader(bytes.Repeat([]byte("x"), 16385)))
	req.Header.Set("X-Viber-Content-Signature", strings.Repeat("a", 64))
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 413 || calls != 1 {
		t.Fatal("oversized callback reached domain")
	}
	token, e := service.Mint("admin", auth.LevelAdmin)
	if e != nil {
		t.Fatal(e)
	}
	req = httptest.NewRequest("GET", "/api/v1/viber/preview?edition_id=00000000-0000-4000-8000-000000000001", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 || calls != 2 {
		t.Fatalf("preview relay status %d", w.Code)
	}
	req = httptest.NewRequest("POST", "/api/v1/viber/test", strings.NewReader(`{"request_id":"00000000-0000-4000-8000-000000000001","expected_revision":1}`))
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 || calls != 3 {
		t.Fatalf("manual test relay status %d", w.Code)
	}
}
