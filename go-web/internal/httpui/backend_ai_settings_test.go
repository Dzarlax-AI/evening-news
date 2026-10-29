package httpui

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/backend"
)

const aiPayload = `{"expected_revision":7,"active_provider":"openai","profiles":{"gemini":{"models":{"article":"g-a","digest":"g-d","text":"g-t"}},"openai":{"models":{"article":"o-a","digest":"o-d","text":"o-t"}}},"reset_to_env":true}`

func TestAISettingsAdminProxy(t *testing.T) {
	var calls int
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/internal/web/v1/ai/settings" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Errorf("invalid private request")
		}
		if r.Method == "PUT" {
			b, _ := io.ReadAll(r.Body)
			var v map[string]any
			_ = json.Unmarshal(b, &v)
			if v["expected_revision"] != float64(7) || v["reset_to_env"] != true {
				t.Errorf("payload %s", b)
			}
			w.WriteHeader(409)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, `{"revision":7}`)
	}))
	defer upstream.Close()
	c, err := backend.NewClient(backend.Config{BaseURL: upstream.URL, Token: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "test-signing-key"})
	token, err := service.Mint("admin", auth.LevelAdmin)
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	RegisterBackend(mux, service, c)
	call := func(method, body, authHeader string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, "/api/v1/ai/settings", strings.NewReader(body))
		if authHeader != "" {
			r.Header.Set("Authorization", "Bearer "+authHeader)
		}
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w
	}
	if w := call("GET", "", ""); w.Code != 401 || calls != 0 {
		t.Fatalf("unauthorized: %d %d", w.Code, calls)
	}
	if w := call("GET", "", token); w.Code != 200 || calls != 1 {
		t.Fatalf("get: %d %d", w.Code, calls)
	}
	if w := call("PUT", aiPayload, token); w.Code != 409 || calls != 2 {
		t.Fatalf("conflict: %d %d", w.Code, calls)
	}
	for _, bad := range []string{strings.Replace(aiPayload, `"expected_revision":7`, `"expected_revision":7,"expected_revision":8`, 1), strings.Replace(aiPayload, `"active_provider":"openai"`, `"active_provider":"openai","active_provider":"gemini"`, 1), strings.Replace(aiPayload, `"article":"g-a"`, `"article":"g-a","article":"other"`, 1), strings.Replace(aiPayload, `"reset_to_env":true`, `"reset_to_env":null`, 1), strings.Replace(aiPayload, `"article":"g-a"`, `"article":"g-a","key":"secret"`, 1), strings.Replace(aiPayload, `"article":"g-a"`, `"article":null`, 1)} {
		if w := call("PUT", bad, token); w.Code != 422 || calls != 2 {
			t.Fatalf("invalid: %d %d", w.Code, calls)
		}
	}
}
