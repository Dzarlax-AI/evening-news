package main

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func post(h func(http.ResponseWriter, *http.Request), path, body, auth, idempotency string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", path, strings.NewReader(body))
	if auth != "" {
		r.Header.Set("Authorization", "Bearer "+auth)
	}
	if idempotency != "" {
		r.Header.Set("Idempotency-Key", idempotency)
	}
	w := httptest.NewRecorder()
	h(w, r)
	return w
}
func TestConsumerReplayAndConflict(t *testing.T) {
	s := newState()
	body := `{"occurrence_id":"fixture-occurrence","operation":"run"}`
	for i := 0; i < 2; i++ {
		r := post(s.consumerHandler, "/commands", body, "fixture-generic-token", "fixture-occurrence")
		if r.Code != 202 {
			t.Fatal(r.Code)
		}
	}
	if s.operations["run"] != 1 || s.consumerRequests != 2 {
		t.Fatal("duplicate was executed")
	}
	r := post(s.consumerHandler, "/commands", `{"occurrence_id":"fixture-occurrence","operation":"different"}`, "fixture-generic-token", "fixture-occurrence")
	if r.Code != 409 {
		t.Fatal(r.Code)
	}
}
func TestIngestionIdempotencyAndControl(t *testing.T) {
	s := newState()
	p := `{"version":1,"job_id":"fixture-job","attempt_generation":1,"owner_id":"owner","generation":1,"backend_pid":42,"executor_instance_id":"executor","deadline_at":"2026-01-01T00:00:00Z"}`
	r := post(s.provider, "/internal/v1/ingestion/runs", p, "fixture-only-article-ingestion-token", "")
	if r.Code != 200 {
		t.Fatal(r.Code)
	}
	var receipt map[string]any
	if json.Unmarshal(r.Body.Bytes(), &receipt) != nil || receipt["run_id"] != ingestionID("fixture-job", 1) {
		t.Fatal("incorrect receipt")
	}
	r = post(s.provider, "/internal/v1/ingestion/runs", p, "fixture-only-article-ingestion-token", "")
	if r.Code != 200 || s.counters["ingestion_stub_runs"] != 1 {
		t.Fatal("duplicate execution")
	}
	post(s.provider, "/__gate/control", `{"gemini_mode":"block"}`, "", "")
	select {
	case <-s.gemini:
		t.Fatal("not blocked")
	default:
	}
	post(s.provider, "/__gate/control", `{"gemini_mode":"release"}`, "", "")
	select {
	case <-s.gemini:
	default:
		t.Fatal("not released")
	}
}
func TestLegacyOccurrenceID(t *testing.T) {
	if got := jobID("fixture-occurrence"); got != "95726501-ad0c-5d90-ba66-02e2ef5d63f4" {
		t.Fatal(got)
	}
}
func TestTelegramAcceptThenHold(t *testing.T) {
	s := newState()
	post(s.provider, "/__gate/control", `{"telegram_mode":"accept_then_hold"}`, "", "")
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- post(s.provider, "/telegram/botfixture/sendMessage", `{}`, "", "") }()
	deadline := time.After(time.Second)
	for {
		s.mu.Lock()
		n := s.counters["telegram"]
		s.mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("provider did not accept")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case <-done:
		t.Fatal("response was not held")
	default:
	}
	post(s.provider, "/__gate/control", `{"telegram_mode":"release"}`, "", "")
	select {
	case r := <-done:
		if r.Code != 200 {
			t.Fatal(r.Code)
		}
	case <-time.After(time.Second):
		t.Fatal("response did not release")
	}
}
func TestTelegramRateLimit(t *testing.T) {
	s := newState()
	post(s.provider, "/__gate/control", `{"telegram_mode":"rate_limit"}`, "", "")
	r := post(s.provider, "/telegram/botfixture/sendRichMessage", `{}`, "", "")
	if r.Code != 429 || r.Header().Get("Retry-After") != "1" {
		t.Fatalf("status=%d", r.Code)
	}
}
func TestLegacyIngestionID(t *testing.T) {
	if got := ingestionID("fixture-job", float64(1)); got != "fc6f6cd1-db5a-55e9-bc41-bbfc30ca9729" {
		t.Fatal(got)
	}
}
func TestEditorialFakeUsesSuppliedEvidence(t *testing.T) {
	s := newState()
	snapshot := `{"articles":[{"id":"one","title":"Fixture title","evidence_spans":[{"id":"span-one"}]}]}`
	payload := map[string]any{"generationConfig": map[string]any{"responseSchema": map[string]any{"properties": map[string]any{"article_stories": map[string]any{}}}}, "contents": []any{map[string]any{"parts": []any{map[string]any{"text": "СНИМОК (недоверенные данные):\n" + snapshot}}}}}
	raw, _ := json.Marshal(payload)
	r := post(s.provider, "/v1/models/fixture:generateContent", string(raw), "", "")
	if r.Code != 200 {
		t.Fatalf("status %d", r.Code)
	}
	var response struct {
		Candidates []struct {
			Content struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
	}
	if e := json.Unmarshal(r.Body.Bytes(), &response); e != nil || len(response.Candidates) != 1 || len(response.Candidates[0].Content.Parts) != 1 {
		t.Fatal("invalid response")
	}
	var result struct {
		ArticleStories map[string]struct {
			SpanID string `json:"span_id"`
		} `json:"article_stories"`
	}
	if json.Unmarshal([]byte(response.Candidates[0].Content.Parts[0].Text), &result) != nil || result.ArticleStories["one"].SpanID != "span-one" || s.counters["editorial_gemini"] != 1 {
		t.Fatal("evidence lost")
	}
}
func TestTelegramHoldSurvivesClientCancellation(t *testing.T) {
	s := newState()
	post(s.provider, "/__gate/control", `{"telegram_mode":"accept_then_hold"}`, "", "")
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest("POST", "/telegram/botfixture/sendMessage", strings.NewReader(`{}`)).WithContext(ctx)
	w := httptest.NewRecorder()
	done := make(chan struct{})
	go func() { s.provider(w, r); close(done) }()
	deadline := time.After(time.Second)
	for {
		s.mu.Lock()
		n := s.counters["telegram"]
		s.mu.Unlock()
		if n == 1 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("provider did not accept")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
		t.Fatal("cancellation ended the provider hold")
	default:
	}
	post(s.provider, "/__gate/control", `{"telegram_mode":"release"}`, "", "")
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("hold did not release")
	}
}

type hijackRecorder struct {
	*httptest.ResponseRecorder
	hijacked bool
}

func (w *hijackRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	a, b := net.Pipe()
	_ = b.Close()
	w.hijacked = true
	return a, bufio.NewReadWriter(bufio.NewReader(a), bufio.NewWriter(a)), nil
}
func TestPartialDeliveryAcceptsOneThenDisconnects(t *testing.T) {
	s := newState()
	post(s.provider, "/__gate/control", `{"telegram_mode":"accept_one_then_unknown"}`, "", "")
	one := post(s.provider, "/telegram/botfixture/sendMessage", `{}`, "", "")
	if one.Code != 200 {
		t.Fatal(one.Code)
	}
	w := &hijackRecorder{ResponseRecorder: httptest.NewRecorder()}
	s.provider(w, httptest.NewRequest("POST", "/telegram/botfixture/sendMessage", strings.NewReader(`{}`)))
	if !w.hijacked || s.counters["telegram"] != 2 {
		t.Fatal("second message was not dropped")
	}
}
