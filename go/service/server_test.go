package service

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testToken = "test-token-with-32-secret-bytes"

func testServer(t *testing.T, f RunnerFunc) *Server {
	t.Helper()
	s, e := New(Config{Token: testToken, MaxConcurrent: 1}, f)
	if e != nil {
		t.Fatal(e)
	}
	return s
}
func post(s *Server, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/extract", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer "+testToken)
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

const valid = `{"request_id":"one","url":"https://example.org/article","content":"hello"}`

func TestNativeOuterDeadlineCannotEnablePythonFallback(t *testing.T) {
	s, err := New(Config{Token: testToken, MaxConcurrent: 1, NativeBrowser: true, BrowserAvailable: func() bool { return true }}, RunnerFunc(func(ctx context.Context, _ Request) Response { <-ctx.Done(); return Response{} }))
	if err != nil {
		t.Fatal(err)
	}
	w := post(s, `{"request_id":"native-timeout","url":"https://example.org/article","use_browser":true,"budget_ms":10}`)
	var resp Response
	if err = json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || resp.Error == nil || resp.Error.Code != "native_browser_timeout" || !resp.Error.Retryable {
		t.Fatalf("native timeout lost policy: %s", w.Body.String())
	}
}

func TestWireValidation(t *testing.T) {
	var calls atomic.Int32
	s := testServer(t, func(_ context.Context, r Request) Response { calls.Add(1); return Response{Outcome: "success"} })
	tests := []struct {
		body   string
		status int
	}{
		{valid, 200},
		{`{"request_id":"one","url":"https://example.org/","cancel_at":"browser"}`, 400},
		{`{"request_id":"one","url":"https://example.org/","now":"2026"}`, 400},
		{`{"request_id":"one","url":"https://example.org/","alternative_html":{}}`, 400},
		{`{"request_id":"one","url":"https://example.org/","budget_ms":30001}`, 400},
		{`{"request_id":"one","url":"https://example.org/","strategy":"unknown_method"}`, 400},
		{`{"request_id":"one","url":"https://example.org/","source_type":"custom"}`, 400},
		{`{"request_id":"one","url":"https://example.org/","max_bytes":1,"content":"ab"}`, 413},
		{`{"request_id":"one","url":"http://user:pass@example.org/"}`, 400},
		{valid + ` {}`, 400},
		{strings.Repeat(" ", MaxJSONBytes+1), 413},
	}
	for _, tt := range tests {
		if w := post(s, tt.body); w.Code != tt.status {
			t.Fatalf("got %d want %d: %s", w.Code, tt.status, w.Body.String())
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("invalid inputs reached runner: %d", calls.Load())
	}
	r := httptest.NewRequest("POST", "/v1/extract", strings.NewReader(valid))
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 401 {
		t.Fatal(w.Code)
	}
}
func TestAdmissionAndDrain(t *testing.T) {
	started := make(chan struct{})
	done := make(chan struct{})
	s := testServer(t, func(ctx context.Context, _ Request) Response {
		close(started)
		<-ctx.Done()
		close(done)
		return Response{Outcome: "failed"}
	})
	result := make(chan int, 1)
	go func() { result <- post(s, valid).Code }()
	<-started
	if w := post(s, valid); w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("missing admission rejection: %v", w)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if s.Drain(ctx) != context.DeadlineExceeded {
		t.Fatal("drain should time out")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("owned request not cancelled")
	}
	<-result
	if w := post(s, valid); w.Code != 503 {
		t.Fatal(w.Code)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health/ready", nil))
	if w.Code != 503 {
		t.Fatal(w.Code)
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/health/live", nil))
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
}
func TestRequestBudgetAndClientCancellation(t *testing.T) {
	s := testServer(t, func(ctx context.Context, _ Request) Response { <-ctx.Done(); return Response{Outcome: "success"} })
	w := post(s, `{"request_id":"one","url":"https://example.org/","budget_ms":1}`)
	var got Response
	if e := json.Unmarshal(w.Body.Bytes(), &got); e != nil {
		t.Fatal(e)
	}
	if got.Error == nil || got.Error.Code != "deadline_exceeded" || got.Outcome != "failed" {
		t.Fatal(w.Body.String())
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r := httptest.NewRequest("POST", "/v1/extract", strings.NewReader(valid)).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+testToken)
	w = httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if e := json.Unmarshal(w.Body.Bytes(), &got); e != nil {
		t.Fatal(e)
	}
	if got.Error.Code != "cancelled" {
		t.Fatal(w.Body.String())
	}
}
func TestResponseCapAndMetadata(t *testing.T) {
	s := testServer(t, func(_ context.Context, _ Request) Response {
		return Response{Outcome: "success", FinalURL: "https://example.org/final"}
	})
	w := post(s, valid)
	var got Response
	if e := json.Unmarshal(w.Body.Bytes(), &got); e != nil {
		t.Fatal(e)
	}
	if got.ContractVersion != "1" || got.BuildVersion != "dev" || got.RequestID != "one" || got.OriginalURL != "https://example.org/article" || got.FinalURL != "https://example.org/final" || got.Observations == nil {
		t.Fatal(w.Body.String())
	}
	s.runner = RunnerFunc(func(_ context.Context, _ Request) Response {
		r := Response{Outcome: "success"}
		r.Result.Content = strings.Repeat("x", MaxJSONBytes+1)
		return r
	})
	if w := post(s, valid); w.Code != 500 {
		t.Fatalf("unbounded response: %d", w.Code)
	}
}

func TestCapabilitiesAndBoundedStats(t *testing.T) {
	s := testServer(t, func(_ context.Context, _ Request) Response {
		r := Response{Outcome: "success"}
		r.Result.MethodUsed = "secret-url-label"
		return r
	})
	w := post(s, `{"request_id":"browser","url":"https://example.org/","strategy":"browser_rendering"}`)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "browser_unavailable") {
		t.Fatal(w.Body.String())
	}
	// Browser failure leaves HTTP extraction and general readiness available.
	if post(s, valid).Code != 200 {
		t.Fatal("HTTP-only request failed")
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/health/ready", nil))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"browser":false`) {
		t.Fatal(w.Body.String())
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/stats", nil))
	if strings.Contains(w.Body.String(), "secret-url-label") || !strings.Contains(w.Body.String(), `"other":1`) || !strings.Contains(w.Body.String(), `"completed":1`) || !strings.Contains(w.Body.String(), `"inflight":0`) {
		t.Fatal(w.Body.String())
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, httptest.NewRequest("GET", "/version", nil))
	if !strings.Contains(w.Body.String(), `"contract_version":"1"`) {
		t.Fatal(w.Body.String())
	}
}

func TestRealClientCancellationAndGracefulDrain(t *testing.T) {
	started, stopped := make(chan struct{}), make(chan struct{})
	s := testServer(t, func(ctx context.Context, _ Request) Response {
		close(started)
		<-ctx.Done()
		close(stopped)
		return Response{Outcome: "failed"}
	})
	ts := httptest.NewServer(s)
	defer ts.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, e := http.NewRequestWithContext(ctx, "POST", ts.URL+"/v1/extract", strings.NewReader(valid))
	if e != nil {
		t.Fatal(e)
	}
	req.Header.Set("Authorization", "Bearer "+testToken)
	finished := make(chan error, 1)
	go func() {
		resp, err := ts.Client().Do(req)
		if resp != nil {
			resp.Body.Close()
		}
		finished <- err
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("request not admitted")
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("disconnect did not cancel owned work")
	}
	if <-finished == nil {
		t.Fatal("cancelled client unexpectedly received response")
	}
	drain, done := context.WithTimeout(context.Background(), time.Second)
	defer done()
	if err := s.Drain(drain); err != nil {
		t.Fatal(err)
	}
	resp, err := ts.Client().Get(ts.URL + "/health/ready")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatal(resp.StatusCode)
	}
}
