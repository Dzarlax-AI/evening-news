package webbackend

import (
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAISettingsPrivateBoundaryAndValidation(t *testing.T) {
	server := &Server{Guard: testGuard{}}
	boundary := Boundary{Token: "fixture", Next: server.Handler()}
	call := func(method, body, token string) int {
		r := httptest.NewRequest(method, "/internal/web/v1/ai/settings", strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		boundary.ServeHTTP(w, r)
		return w.Code
	}
	if got := call("GET", "", "wrong"); got != 401 {
		t.Fatal(got)
	}
	if got := call("GET", "", "fixture"); got != 503 {
		t.Fatal(got)
	}
	valid := `{"expected_revision":7,"active_provider":"openai","profiles":{"gemini":{"models":{"article":"ga","digest":"gd","text":"gt"}},"openai":{"models":{"article":"oa","digest":"od","text":"ot"}}},"reset_to_env":false}`
	for _, body := range []string{`{}`, `{"expected_revision":null}`, `{"key":"secret"}`,
		strings.Replace(valid, `"expected_revision":7`, `"expected_revision":7,"expected_revision":8`, 1),
		strings.Replace(valid, `"active_provider":"openai"`, `"active_provider":"openai","active_provider":"gemini"`, 1),
		strings.Replace(valid, `"article":"ga"`, `"article":"ga","article":"other"`, 1),
		valid + ` {}`, strings.Replace(valid, `"article":"ga"`, `"article":null`, 1),
	} {
		if got := call("PUT", body, "fixture"); got != 422 {
			t.Fatalf("%s: %d", body, got)
		}
	}
	if got := call("PUT", valid, "fixture"); got != 503 {
		t.Fatalf("valid request should reach unavailable runtime: %d", got)
	}
	if got := call("POST", "{}", "fixture"); got != 405 {
		t.Fatal(got)
	}
}
