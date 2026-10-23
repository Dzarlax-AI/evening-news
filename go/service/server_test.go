package service

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
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

func postBody(s *Server, body io.Reader) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/extract", body)
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

func TestNativeAdmissionBusySurvivesOuterDeadlineOnlyForValidRunnerResponse(t *testing.T) {
	busy := Response{Outcome: "rejected", Result: extractor.Result{State: "failed", Error: "native_browser_busy"}, Error: &Failure{Code: "native_browser_busy", Retryable: true}}
	for _, tc := range []struct {
		name string
		resp Response
		want string
	}{
		{name: "valid admission busy", resp: busy, want: "native_browser_busy"},
		{name: "arbitrary failed response", resp: Response{Outcome: "failed", Error: &Failure{Code: "native_browser_busy", Retryable: true}}, want: "native_browser_timeout"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, err := New(Config{Token: testToken, MaxConcurrent: 1, NativeBrowser: true, BrowserAvailable: func() bool { return true }}, RunnerFunc(func(ctx context.Context, _ Request) Response {
				<-ctx.Done()
				return tc.resp
			}))
			if err != nil {
				t.Fatal(err)
			}
			w := post(s, `{"request_id":"native-wait","url":"https://example.org/article","use_browser":true,"budget_ms":10}`)
			var resp Response
			if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if resp.Error == nil || resp.Error.Code != tc.want || !resp.Error.Retryable {
				t.Fatalf("deadline mapping = %+v, want %s retryable", resp.Error, tc.want)
			}
		})
	}
}

func TestExternalCancellationDoesNotPreserveAdmissionBusy(t *testing.T) {
	started := make(chan struct{})
	s, err := New(Config{Token: testToken, MaxConcurrent: 1, NativeBrowser: true, BrowserAvailable: func() bool { return true }}, RunnerFunc(func(ctx context.Context, _ Request) Response {
		close(started)
		<-ctx.Done()
		return Response{Outcome: "rejected", Result: extractor.Result{State: "failed", Error: "native_browser_busy"}, Error: &Failure{Code: "native_browser_busy", Retryable: true}}
	}))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	req := httptest.NewRequest(http.MethodPost, "/v1/extract", strings.NewReader(`{"request_id":"native-cancel","url":"https://example.org/article","use_browser":true,"budget_ms":1000}`)).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer "+testToken)
	response := httptest.NewRecorder()
	done := make(chan struct{})
	go func() {
		s.ServeHTTP(response, req)
		close(done)
	}()
	<-started
	cancel()
	<-done
	var resp Response
	if err := json.Unmarshal(response.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || resp.Error.Code != "cancelled" || resp.Error.Retryable {
		t.Fatalf("external cancellation was reclassified: %+v", resp.Error)
	}
}

func TestReadinessFailureRejectsBrowserRequestBeforeCapabilityOrRunner(t *testing.T) {
	var readyChecks, browserChecks, runnerCalls atomic.Int32
	s, err := New(Config{
		Token: testToken, MaxConcurrent: 1,
		Ready:            func() bool { readyChecks.Add(1); return false },
		BrowserAvailable: func() bool { browserChecks.Add(1); return true },
	}, RunnerFunc(func(context.Context, Request) Response {
		runnerCalls.Add(1)
		return Response{Outcome: "success"}
	}))
	if err != nil {
		t.Fatal(err)
	}
	w := post(s, `{"request_id":"browser","url":"https://example.org/","use_browser":true}`)
	if w.Code != 503 || readyChecks.Load() != 1 || browserChecks.Load() != 0 || runnerCalls.Load() != 0 {
		t.Fatalf("unready browser request was admitted: status=%d ready=%d browser=%d runner=%d body=%s",
			w.Code, readyChecks.Load(), browserChecks.Load(), runnerCalls.Load(), w.Body.String())
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

func TestRequestBodyBoundariesAndStrictJSON(t *testing.T) {
	s := testServer(t, func(context.Context, Request) Response { return Response{Outcome: "success"} })
	validBody := valid + strings.Repeat(" ", MaxJSONBytes-len(valid))
	if w := post(s, validBody); w.Code != 200 {
		t.Fatalf("exact 8 MiB request: status=%d body=%s", w.Code, w.Body.String())
	}
	if w := postBody(s, &leadingWhitespaceBody{value: []byte(valid), remaining: MaxJSONBytes - len(valid), size: MaxJSONBytes}); w.Code != 200 {
		t.Fatalf("near-limit leading whitespace request: status=%d body=%s", w.Code, w.Body.String())
	}
	for _, tc := range []struct {
		name   string
		body   io.Reader
		status int
	}{
		{"oversized_whitespace", strings.NewReader(valid + strings.Repeat(" ", MaxJSONBytes+1-len(valid))), 413},
		{"malformed_prefix_then_oversized", &repeatedBodyReader{prefix: []byte("{bad"), size: MaxJSONBytes + 1}, 413},
		{"unknown_field_then_oversized", &repeatedBodyReader{prefix: []byte(`{"unknown":true}`), size: MaxJSONBytes + 1}, 413},
		{"multiple_values", strings.NewReader(valid + ` {}`), 400},
		{"leading_nbsp", strings.NewReader("\u00a0" + valid), 400},
		{"leading_bom", strings.NewReader("\ufeff" + valid), 400},
		{"whitespace_only", strings.NewReader(" \t\r\n"), 400},
		{"invalid_nested_field_type", strings.NewReader(`{"request_id":"one","url":"https://example.org/","selectors":{"title":[1]}}`), 400},
		{"reader_error_after_valid_json", &bodyThenErrorReader{body: []byte(valid), err: errors.New("synthetic body read error")}, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if w := postBody(s, tc.body); w.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
			}
		})
	}
}

type leadingWhitespaceBody struct {
	value       []byte
	remaining   int
	valueRead   int
	size        int
	leadingDone bool
}

func (r *leadingWhitespaceBody) Read(p []byte) (int, error) {
	if r.leadingDone && r.valueRead == len(r.value) {
		return 0, io.EOF
	}
	if !r.leadingDone {
		if r.remaining > 0 {
			n := min(len(p), r.remaining)
			for i := 0; i < n; i++ {
				p[i] = ' '
			}
			r.remaining -= n
			return n, nil
		}
		r.leadingDone = true
	}
	n := copy(p, r.value[r.valueRead:])
	r.valueRead += n
	return n, nil
}

type repeatedBodyReader struct {
	prefix []byte
	size   int
	read   int
}

func (r *repeatedBodyReader) Read(p []byte) (int, error) {
	if r.read < len(r.prefix) {
		n := copy(p, r.prefix[r.read:])
		r.read += n
		return n, nil
	}
	if r.read >= r.size {
		return 0, io.EOF
	}
	n := min(len(p), r.size-r.read)
	for i := 0; i < n; i++ {
		p[i] = ' '
	}
	r.read += n
	return n, nil
}

type bodyThenErrorReader struct {
	body []byte
	err  error
}

func (r *bodyThenErrorReader) Read(p []byte) (int, error) {
	if r.body == nil {
		return 0, io.EOF
	}
	n := copy(p, r.body)
	r.body = nil
	return n, r.err
}

func TestConcurrentNearLimitWhitespaceRequestsKeepAdmissionCapacity(t *testing.T) {
	const maxConcurrent = 5
	var calls atomic.Int32
	s, err := New(Config{Token: testToken, MaxConcurrent: maxConcurrent}, RunnerFunc(func(context.Context, Request) Response {
		calls.Add(1)
		return Response{Outcome: "success"}
	}))
	if err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{}, maxConcurrent)
	release := make(chan struct{})
	responses := make(chan int, maxConcurrent)
	for i := 0; i < maxConcurrent; i++ {
		body := &gatedWhitespaceBody{prefix: []byte(valid), size: MaxJSONBytes, started: started, release: release}
		go func() { responses <- postBody(s, body).Code }()
	}
	for i := 0; i < maxConcurrent; i++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("not all request bodies reached the admission-held read")
		}
	}
	blockedBody := &countingReader{r: strings.NewReader(valid)}
	if w := postBody(s, blockedBody); w.Code != 429 || blockedBody.reads.Load() != 0 {
		close(release)
		t.Fatalf("sixth request status=%d body_reads=%d", w.Code, blockedBody.reads.Load())
	}
	close(release)
	for i := 0; i < maxConcurrent; i++ {
		select {
		case status := <-responses:
			if status != 200 {
				t.Fatalf("admitted request status=%d", status)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("admitted request did not finish")
		}
	}
	if calls.Load() != maxConcurrent {
		t.Fatalf("runner calls=%d want=%d", calls.Load(), maxConcurrent)
	}
}

type gatedWhitespaceBody struct {
	prefix  []byte
	read    int
	size    int
	started chan struct{}
	release chan struct{}
	once    atomic.Bool
}

func (r *gatedWhitespaceBody) Read(p []byte) (int, error) {
	if r.once.CompareAndSwap(false, true) {
		r.started <- struct{}{}
		<-r.release
	}
	if r.read < len(r.prefix) {
		n := copy(p, r.prefix[r.read:])
		r.read += n
		return n, nil
	}
	if r.read >= r.size {
		return 0, io.EOF
	}
	n := min(len(p), r.size-r.read)
	for i := 0; i < n; i++ {
		p[i] = ' '
	}
	r.read += n
	return n, nil
}

type countingReader struct {
	r     io.Reader
	reads atomic.Int32
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	return r.r.Read(p)
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
