package httpui

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/backend"
)

func TestCredentialProxyNeverReturnsSecret(t *testing.T) {
	calls := 0
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/internal/web/v1/credentials/gemini" {
			t.Errorf("bad path")
		}
		if r.Method == "PUT" {
			body, _ := io.ReadAll(r.Body)
			if !strings.Contains(string(body), `"expected_revision":0`) {
				t.Error("missing CAS")
			}
		}
		io.WriteString(w, `{"provider":"gemini","revision":1,"configured":true,"value":"fixture-secret"}`)
	}))
	defer upstream.Close()
	c, err := backend.NewClient(backend.Config{BaseURL: upstream.URL, Token: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	service := auth.New(auth.Config{Username: "admin", Password: "password", JWTSecret: "fixture"})
	token, err := service.Mint("admin", auth.LevelAdmin)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	RegisterBackend(mux, service, c)
	req := httptest.NewRequest("PUT", "/api/v1/credentials/gemini", strings.NewReader(`{"expected_revision":0,"value":"fixture-secret"}`))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 401 || calls != 0 {
		t.Fatal("unauthorized write passed")
	}
	req = httptest.NewRequest("PUT", "/api/v1/credentials/gemini", strings.NewReader(`{"expected_revision":0,"value":"fixture-secret"}`))
	req.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 || calls != 1 || strings.Contains(w.Body.String(), "fixture-secret") {
		t.Fatalf("unexpected response %d %s", w.Code, w.Body.String())
	}
}
