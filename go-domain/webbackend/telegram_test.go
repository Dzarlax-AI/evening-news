package webbackend

import (
	"encoding/json"
	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTelegramTestUsesServiceDestination(t *testing.T) {
	received := false
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		if r.URL.Path != "/botfixture/sendMessage" || payload["chat_id"] != "service-fixture" || !strings.Contains(payload["text"].(string), "Test message from Evening News v2") {
			t.Errorf("unexpected fixture payload %v", payload)
		}
		received = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true,"result":{"message_id":1}}`))
	}))
	defer fake.Close()
	runtime := airuntime.NewServer(airuntime.Config{RPS: 3, TelegramToken: "fixture", TelegramMain: "main-fixture", TelegramService: "service-fixture", TelegramEndpoint: fake.URL}, nil)
	s := Server{Guard: testGuard{}, Runtime: runtime}
	handler := Boundary{Token: "fixture", Next: s.Handler()}
	req := httptest.NewRequest("POST", "/internal/web/v1/telegram/test", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer fixture")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 || !received || !strings.Contains(w.Body.String(), `"success":true`) {
		t.Fatalf("status=%d received=%v body=%s", w.Code, received, w.Body.String())
	}
}
