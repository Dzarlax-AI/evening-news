package service

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type telegramTestRunner struct {
	seen TelegramRequest
	call func(context.Context, TelegramRequest) TelegramResponse
}

func (r *telegramTestRunner) Run(context.Context, Request) Response {
	return Response{Outcome: "success"}
}
func (r *telegramTestRunner) RunTelegram(ctx context.Context, req TelegramRequest) TelegramResponse {
	r.seen = req
	if r.call != nil {
		return r.call(ctx, req)
	}
	return TelegramResponse{Outcome: "success"}
}

func telegramPost(s *Server, body string, token bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/telegram/fetch", strings.NewReader(body))
	if token {
		r.Header.Set("Authorization", "Bearer "+testToken)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func TestTelegramRequestValidationAndCanonicalization(t *testing.T) {
	runner := &telegramTestRunner{}
	s, err := New(Config{Token: testToken, MaxConcurrent: 1}, runner)
	if err != nil {
		t.Fatal(err)
	}
	valid := `{"contract_version":"1","request_id":"abc","channel_url":"https://Telegram.me/s/News_1","source_name":"News","limit":10,"budget_ms":2500}`
	tests := []struct {
		name, body string
		status     int
	}{
		{"valid", valid, 200},
		{"unauthorized", valid, 401},
		{"unknown_field", strings.TrimSuffix(valid, "}") + `,"html":"<div>"}`, 400},
		{"wrong_version", strings.Replace(valid, `"1"`, `"2"`, 1), 400},
		{"zero_limit", strings.Replace(valid, `"limit":10`, `"limit":0`, 1), 400},
		{"large_limit", strings.Replace(valid, `"limit":10`, `"limit":101`, 1), 400},
		{"large_budget", strings.Replace(valid, `"budget_ms":2500`, `"budget_ms":30001`, 1), 400},
		{"query", strings.Replace(valid, `News_1"`, `News_1?x=1"`, 1), 400},
		{"fragment", strings.Replace(valid, `News_1"`, `News_1#x"`, 1), 400},
		{"credentials", strings.Replace(valid, `https://Telegram.me`, `https://u:p@Telegram.me`, 1), 400},
		{"port", strings.Replace(valid, `Telegram.me`, `Telegram.me:444`, 1), 400},
		{"unknown_host", strings.Replace(valid, `Telegram.me`, `telegram.example.org`, 1), 400},
		{"arbitrary_path", strings.Replace(valid, `/s/News_1`, `/s/News_1/extra`, 1), 400},
		{"private_literal", strings.Replace(valid, `Telegram.me`, `127.0.0.1`, 1), 400},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := telegramPost(s, tc.body, tc.name != "unauthorized")
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
			}
		})
	}
	if runner.seen.ChannelURL != "https://t.me/s/news_1" || runner.seen.Limit != 10 {
		t.Fatalf("runner did not receive canonical request: %+v", runner.seen)
	}
}

func TestTelegramPartialResponseAndSharedDrainCancellation(t *testing.T) {
	started := make(chan struct{})
	runner := &telegramTestRunner{call: func(ctx context.Context, req TelegramRequest) TelegramResponse {
		close(started)
		<-ctx.Done()
		return TelegramResponse{Outcome: "success"}
	}}
	s, err := New(Config{Token: testToken, MaxConcurrent: 1}, runner)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"contract_version":"1","request_id":"slow","channel_url":"https://t.me/channel","source_name":"News","limit":1,"budget_ms":30000}`
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- telegramPost(s, body, true) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("runner not started")
	}
	drainCtx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Drain(drainCtx); err == nil {
		t.Fatal("expected drain timeout/cancellation")
	}
	select {
	case response := <-done:
		var got TelegramResponse
		if err := json.Unmarshal(response.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if got.Outcome != "failed" || got.Error == nil || got.Error.Code != "cancelled" {
			t.Fatalf("unexpected cancellation response: %s", response.Body.String())
		}
	case <-time.After(time.Second):
		t.Fatal("drain failed to cancel active Telegram request")
	}
}
