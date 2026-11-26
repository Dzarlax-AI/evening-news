package httpui

import (
	"context"
	"encoding/json"
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

type jevCredentialOps struct {
	*fakeBackendOps
	method, provider string
	body             json.RawMessage
}

func (f *jevCredentialOps) Credential(_ context.Context, method, provider string, body json.RawMessage) (json.RawMessage, error) {
	f.method, f.provider, f.body = method, provider, body
	return json.RawMessage(`{"provider":"jev","revision":2,"configured":true,"value":"must-not-escape"}`), nil
}
func TestJevCredentialProxyAuthCASAndMetadata(t *testing.T) {
	ops := &jevCredentialOps{fakeBackendOps: &fakeBackendOps{}}
	service := auth.New(auth.Config{Username: "admin", Password: "password", JWTSecret: "fixture"})
	token, _ := service.Mint("admin", auth.LevelAdmin)
	mux := http.NewServeMux()
	RegisterBackend(mux, service, ops)
	for _, method := range []string{"GET", "PUT", "DELETE"} {
		body := `{"expected_revision":1}`
		if method == "PUT" {
			body = `{"expected_revision":1,"value":"fixture"}`
		}
		req := httptest.NewRequest(method, "/api/v1/credentials/jev", strings.NewReader(body))
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized || ops.provider != "" {
			t.Fatalf("unauthorized %s status=%d", method, w.Code)
		}
		req = httptest.NewRequest(method, "/api/v1/credentials/jev", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		w = httptest.NewRecorder()
		mux.ServeHTTP(w, req)
		if w.Code != http.StatusOK || ops.provider != "jev" || ops.method != method || strings.Contains(w.Body.String(), "must-not-escape") {
			t.Fatalf("%s status=%d body=%s", method, w.Code, w.Body.String())
		}
		if method != "GET" && !strings.Contains(string(ops.body), `"expected_revision":1`) {
			t.Fatal("missing revision")
		}
		ops.provider = ""
	}
}
