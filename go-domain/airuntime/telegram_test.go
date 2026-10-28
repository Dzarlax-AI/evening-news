package airuntime

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type telegramRoundTrip func(*http.Request) (*http.Response, error)

func (f telegramRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestAlertSettingsUnavailableUsesConfiguredServiceOnce(t *testing.T) {
	s := NewServer(Config{Token: "01234567890123456789012345678901", RPS: 3, TelegramToken: "fixture", TelegramMain: "main-default", TelegramService: "service-default", TelegramEndpoint: "https://fixture.invalid"}, nil)
	calls := 0
	s.client = &http.Client{Transport: telegramRoundTrip(func(r *http.Request) (*http.Response, error) {
		calls++
		var payload map[string]any
		if json.NewDecoder(r.Body).Decode(&payload) != nil || payload["chat_id"] != "service-default" {
			t.Error("wrong logical destination")
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"ok":true,"result":{"message_id":1}}`)), Request: r}, nil
	})}
	body, _ := json.Marshal(map[string]any{"version": 1, "request_id": "test", "deadline_at": time.Now().Add(time.Minute).Format(time.RFC3339Nano), "channel": "service", "method": "sendMessage", "text": "synthetic alert"})
	req := httptest.NewRequest(http.MethodPost, "/v1/telegram/send", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+s.config.Token)
	out := httptest.NewRecorder()
	s.Handler().ServeHTTP(out, req)
	if out.Code != 200 || calls != 1 {
		t.Fatalf("status=%d posts=%d", out.Code, calls)
	}
}
