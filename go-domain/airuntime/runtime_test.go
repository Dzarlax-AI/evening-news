package airuntime

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func response(status int, headers http.Header, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: nil}
}

type transportFixture struct {
	Name             string            `json:"name"`
	Mode             string            `json:"mode"`
	Status           int               `json:"status"`
	Headers          map[string]string `json:"headers"`
	ProviderResponse json.RawMessage   `json:"provider_response"`
	ExpectedResult   any               `json:"expected_result"`
	ExpectedError    struct {
		Status     int      `json:"status_code"`
		RetryAfter *float64 `json:"retry_after_seconds"`
	} `json:"expected_error"`
	Request struct {
		Prompt       string          `json:"prompt"`
		Model        string          `json:"model"`
		AnalysisType string          `json:"analysis_type"`
		Domain       string          `json:"domain"`
		Temperature  float64         `json:"temperature"`
		MaxTokens    int             `json:"max_tokens"`
		Schema       json.RawMessage `json:"schema"`
	} `json:"request"`
	ExpectedRequest struct {
		URL     string `json:"url"`
		JSON    any    `json:"json"`
		Timeout int    `json:"timeout"`
	} `json:"expected_request"`
	ExpectedUsageEvents int `json:"expected_usage_events"`
}

func TestLegacyTransportFixtureParity(t *testing.T) {
	data, err := os.ReadFile("../../tests/fixtures/ai-runtime-v1/legacy_transport.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []transportFixture
	if err = json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) != 10 {
		t.Fatalf("fixtures=%d", len(fixtures))
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Name, func(t *testing.T) {
			var gotBody map[string]any
			var gotURL string
			var gotDeadline time.Time
			var calls atomic.Int32
			status := fixture.Status
			if status == 0 {
				status = 200
			}
			headers := http.Header{}
			for k, v := range fixture.Headers {
				headers.Set(k, v)
			}
			transport := roundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls.Add(1)
				gotURL = req.URL.Scheme + "://" + req.URL.Host + req.URL.Path
				gotDeadline, _ = req.Context().Deadline()
				raw, _ := io.ReadAll(req.Body)
				_ = json.Unmarshal(raw, &gotBody)
				return response(status, headers, string(fixture.ProviderResponse)), nil
			})
			s := NewServer(Config{APIKey: "synthetic-secret", Endpoint: "https://fixture.invalid/v1/models", Model: fixture.Request.Model, RPS: 100}, nil)
			s.client = &http.Client{Transport: transport}
			req := generateRequest{Mode: fixture.Mode, Prompt: fixture.Request.Prompt, Model: fixture.Request.Model, AnalysisType: fixture.Request.AnalysisType, Domain: fixture.Request.Domain, Schema: fixture.Request.Schema, Options: generateOptions{Temperature: fixture.Request.Temperature, MaxTokens: fixture.Request.MaxTokens}}
			if fixture.Mode == "structured" {
				req.Options = generateOptions{}
			}
			value, callErr := s.callGenerate(context.Background(), req)
			if wantErr, exists := map[string]bool{"text_http429": true, "structured_http500": true, "text_empty_candidates": true}[fixture.Name]; exists && wantErr {
				if callErr == nil {
					t.Fatal("expected provider error")
				}
				var pf *providerFailure
				if !errors.As(callErr, &pf) {
					t.Fatalf("error type %T", callErr)
				}
				if fixture.ExpectedError.Status != 0 && (pf.status == nil || *pf.status != fixture.ExpectedError.Status) {
					t.Fatalf("status=%v", pf.status)
				}
				if fixture.ExpectedError.RetryAfter != nil && (pf.retryAfter == nil || *pf.retryAfter != *fixture.ExpectedError.RetryAfter) {
					t.Fatalf("retry_after=%v", pf.retryAfter)
				}
				if calls.Load() != 1 {
					t.Fatalf("calls=%d; status/decode failures must not retry", calls.Load())
				}
				return
			}
			if callErr != nil {
				t.Fatal(callErr)
			}
			out := value.(generateOutput)
			wantAccounting := out.usage.TotalTokens
			if wantAccounting == 0 {
				wantAccounting = out.usage.PromptTokens + out.usage.CompletionTokens
			}
			if out.accountingTokens != wantAccounting {
				t.Fatalf("usage accounting tokens=%d want=%d", out.accountingTokens, wantAccounting)
			}
			got := out.result
			wantJSON, _ := json.Marshal(fixture.ExpectedResult)
			gotJSON, _ := json.Marshal(got)
			var want, actual any
			_ = json.Unmarshal(wantJSON, &want)
			_ = json.Unmarshal(gotJSON, &actual)
			if !reflect.DeepEqual(actual, want) {
				t.Fatalf("result mismatch\n got: %s\nwant: %s", gotJSON, wantJSON)
			}
			if gotURL != fixture.ExpectedRequest.URL {
				t.Fatalf("url=%q want=%q", gotURL, fixture.ExpectedRequest.URL)
			}
			wantRequest, _ := json.Marshal(fixture.ExpectedRequest.JSON)
			gotRequest, _ := json.Marshal(gotBody)
			if !reflect.DeepEqual(gotRequest, wantRequest) {
				t.Fatalf("provider request mismatch\n got: %s\nwant: %s", gotRequest, wantRequest)
			}
			if gotDeadline.IsZero() {
				t.Fatal("fixture transport observed no finite request context deadline")
			}
			if fixture.ExpectedUsageEvents != 1 {
				t.Fatalf("fixture usage=%d", fixture.ExpectedUsageEvents)
			}
		})
	}
}

func TestLegacyUsageAccountingFixtureParity(t *testing.T) {
	data, err := os.ReadFile("../../tests/fixtures/ai-runtime-v1/legacy_usage.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Rates struct {
			Input  float64 `json:"input_per_1m"`
			Output float64 `json:"output_per_1m"`
			Cached float64 `json:"cached_per_1m"`
		} `json:"rates"`
		Cases []struct {
			Name     string         `json:"name"`
			Usage    map[string]any `json:"usage"`
			Expected map[string]any `json:"expected_insert"`
		} `json:"cases"`
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	config := Config{InputRate: fixture.Rates.Input, OutputRate: fixture.Rates.Output, CachedRate: fixture.Rates.Cached}
	for _, tc := range fixture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			get := func(key string) int { v, _ := tc.Usage[key].(float64); return int(v) }
			accounting := get("prompt_tokens") + get("completion_tokens")
			if total, ok := tc.Usage["total_tokens"].(float64); ok && total > 0 {
				accounting = int(total)
			}
			rec := usageRecord{domain: "fixture.invalid", analysisType: "synthetic_analysis", usage: analysis.Usage{PromptTokens: get("prompt_tokens"), CompletionTokens: get("completion_tokens"), CachedTokens: get("cached_tokens"), TotalTokens: get("total_tokens")}, accountingTokens: accounting, accountingTokenSet: true}
			actual := usageInsert(rec, config)
			want := tc.Expected
			if actual["domain"] != want["domain"] || actual["analysis_type"] != want["analysis_type"] || actual["tokens_used"] != int(want["tokens_used"].(float64)) {
				t.Fatalf("insert mismatch got=%#v want=%#v", actual, want)
			}
			if math.Abs(actual["cost"].(float64)-want["cost"].(float64)) > 1e-12 {
				t.Fatalf("cost=%v want=%v", actual["cost"], want["cost"])
			}
			gotJSON, _ := json.Marshal(actual["result"])
			wantJSON, _ := json.Marshal(want["result"])
			if !reflect.DeepEqual(gotJSON, wantJSON) {
				t.Fatalf("token detail got=%s want=%s", gotJSON, wantJSON)
			}
		})
	}
}

func TestSharedPermitAndProviderUseOneWindow(t *testing.T) {
	limiter := NewSlidingWindow(2)
	s := NewServer(Config{APIKey: "test", Endpoint: "https://fixture.invalid/v1/models", Model: "gemini-2", RPS: 2}, nil)
	s.limiter = limiter
	s.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		return response(200, nil, `{"candidates":[{"content":{"parts":[{"text":"ok"}]}}],"usageMetadata":{}}`), nil
	})}
	request := generateRequest{Mode: "text", Prompt: "x", Model: "gemini-2", AnalysisType: "x", Domain: "x", Options: generateOptions{Temperature: .1, MaxTokens: 20}}
	start := time.Now()
	done := make(chan error, 1)
	go func() { _, err := s.callGenerate(context.Background(), request); done <- err }()
	if err := limiter.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := limiter.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Fatalf("third admission did not wait for shared window: %v", elapsed)
	}
}

func TestProviderCancellationDoesNotReplay(t *testing.T) {
	var calls atomic.Int32
	s := NewServer(Config{APIKey: "test", Endpoint: "https://fixture.invalid/v1/models", Model: "gemini-2", RPS: 10}, nil)
	s.client = &http.Client{Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
		calls.Add(1)
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	req := generateRequest{Mode: "text", Prompt: "x", Model: "gemini-2", AnalysisType: "x", Domain: "x", Options: generateOptions{Temperature: .1, MaxTokens: 20}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := s.callGenerate(ctx, req); done <- err }()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		var pf *providerFailure
		if !errors.As(err, &pf) || pf.code != "provider_cancelled" {
			t.Fatalf("cancel outcome=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("provider request did not cancel")
	}
	if calls.Load() != 1 {
		t.Fatalf("provider request replayed %d times", calls.Load())
	}
}

func TestAdmissionCancellationAndBreakerThreshold(t *testing.T) {
	limiter := NewSlidingWindow(1)
	if err := limiter.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := limiter.Acquire(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("cancelled admission error=%v", err)
	}
	b := NewBreaker()
	for i := 0; i < 5; i++ {
		_, _ = b.Do(context.Background(), func() (any, error) { return nil, errors.New("synthetic") })
	}
	state, count := b.Snapshot()
	if state != "open" || count != 5 {
		t.Fatalf("breaker state=%s failures=%d", state, count)
	}
	if _, err := b.Do(context.Background(), func() (any, error) { return nil, nil }); err == nil || !strings.HasPrefix(err.Error(), "breaker_open:") {
		t.Fatalf("open breaker admitted request: %v", err)
	}
}

func TestSlidingWindowWaitUsesExactRemainderAndCanceledAcquireDoesNotConsume(t *testing.T) {
	limiter := NewSlidingWindow(1)
	base := time.Unix(100, 0)
	now := base.Add(600 * time.Millisecond)
	limiter.now = func() time.Time { return now }
	limiter.calls = []time.Time{base}
	var waits []time.Duration
	limiter.wait = func(_ context.Context, d time.Duration) error { waits = append(waits, d); now = now.Add(d); return nil }
	if err := limiter.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(waits) != 1 || waits[0] != 400*time.Millisecond {
		t.Fatalf("waits=%v want exactly 400ms", waits)
	}
	empty := NewSlidingWindow(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := empty.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-cancelled acquire=%v", err)
	}
	if len(empty.calls) != 0 {
		t.Fatalf("pre-cancelled acquire consumed permit: %v", empty.calls)
	}
}

func TestDomainPreservesAuthorityAndGenericRequiresStatus200(t *testing.T) {
	if got := domainFor("https://News.Example:8443/story%2"); got != "News.Example:8443" {
		t.Fatalf("domain=%q", got)
	}
	var calls atomic.Int32
	s := NewServer(Config{APIKey: "test", Endpoint: "https://fixture.invalid/v1/models", Model: "gemini-2", RPS: 10}, nil)
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return response(http.StatusCreated, nil, `{"candidates":[{"content":{"parts":[{"text":"unexpected"}]}}]}`), nil
	})}
	req := generateRequest{Mode: "text", Prompt: "x", Model: "gemini-2", AnalysisType: "x", Domain: "x", Options: generateOptions{Temperature: .1, MaxTokens: 20}}
	_, err := s.callGenerate(context.Background(), req)
	var pf *providerFailure
	if !errors.As(err, &pf) || pf.status == nil || *pf.status != http.StatusCreated {
		t.Fatalf("201 status outcome=%v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("non-200 response retried %d times", calls.Load())
	}
}

func runGenerateOverUnix(t *testing.T, providerStatus int, domain string) (int, map[string]any, int, []usageRecord) {
	t.Helper()
	var providerCalls atomic.Int32
	provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providerCalls.Add(1)
		if providerStatus == http.StatusOK {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"candidates":[{"content":{"parts":[{"text":"generated"}]}}],"usageMetadata":{"promptTokenCount":2,"candidatesTokenCount":1}}`)
			return
		}
		w.WriteHeader(providerStatus)
	}))
	t.Cleanup(provider.Close)

	dir, err := os.MkdirTemp("/tmp", "nai-test-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "runtime.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	listener.(*net.UnixListener).SetUnlinkOnClose(false)
	if err := os.Chmod(socket, 0600); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Remove(socket) })

	s := NewServer(Config{Token: "synthetic-token", APIKey: "synthetic-key",
		Endpoint: provider.URL + "/v1/models", Model: "fixture-model", RPS: 100}, nil)
	httpServer := &http.Server{Handler: s.Handler()}
	serveDone := make(chan error, 1)
	go func() { serveDone <- httpServer.Serve(listener) }()
	t.Cleanup(func() {
		_ = httpServer.Close()
		<-serveDone
	})

	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client := &http.Client{Transport: transport}
	body, err := json.Marshal(map[string]any{
		"version": 1, "request_id": "fixture-request",
		"deadline_at": time.Now().Add(time.Minute).Format(time.RFC3339Nano),
		"mode":        "text", "prompt": "fixture prompt", "model": "fixture-model",
		"options":       map[string]any{"temperature": 0.1, "max_tokens": 20},
		"analysis_type": "publication_date", "domain": domain,
	})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequest(http.MethodPost, "http://unix/v1/generate", strings.NewReader(string(body)))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer synthetic-token")
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	var envelope map[string]any
	if err := json.Unmarshal(responseBody, &envelope); err != nil {
		t.Fatalf("decode RPC response (status %d, body %q): %v", resp.StatusCode, responseBody, err)
	}
	var records []usageRecord
	for len(s.usage) > 0 {
		records = append(records, <-s.usage)
	}
	return resp.StatusCode, envelope, int(providerCalls.Load()), records
}

func TestProvider204And304SurviveRealUnixHTTPRoundTrip(t *testing.T) {
	for _, status := range []int{http.StatusNoContent, http.StatusNotModified} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			rpcStatus, envelope, calls, _ := runGenerateOverUnix(t, status, "https://news.example/story")
			if rpcStatus != http.StatusBadGateway {
				t.Fatalf("RPC status=%d want %d", rpcStatus, http.StatusBadGateway)
			}
			if envelope["ok"] != false {
				t.Fatalf("unexpected envelope: %#v", envelope)
			}
			detail, ok := envelope["error"].(map[string]any)
			if !ok || detail["code"] != "provider_http_error" || detail["status_code"] != float64(status) {
				t.Fatalf("provider error was lost: %#v", envelope)
			}
			if calls != 1 {
				t.Fatalf("provider calls=%d; expected one", calls)
			}
		})
	}
}

func TestLongUsageDomainDoesNotPreventProviderCallOrGetTruncated(t *testing.T) {
	domain := "https://news.example/" + strings.Repeat("a", 400)
	rpcStatus, envelope, calls, records := runGenerateOverUnix(t, http.StatusOK, domain)
	if rpcStatus != http.StatusOK || envelope["ok"] != true {
		t.Fatalf("RPC status=%d envelope=%#v", rpcStatus, envelope)
	}
	if calls != 1 {
		t.Fatalf("provider calls=%d; expected one", calls)
	}
	if len(records) != 1 || records[0].domain != domain {
		t.Fatalf("usage domain was not preserved: %#v", records)
	}
}

func TestCategoryMetadataCacheExpiryAndFallback(t *testing.T) {
	store := NewMetadataStore(nil)
	got, local := store.Load(context.Background())
	if local != "" || len(got) != 5 || got[0].Name != "Business" || got[4].Name != "Other" {
		t.Fatalf("fallback metadata=%#v local=%q", got, local)
	}
	now := time.Unix(100, 0)
	store.now = func() time.Time { return now }
	calls := 0
	name := "Local"
	store.load = func(context.Context) ([]analysis.CategoryMetadata, *string, error) {
		calls++
		return []analysis.CategoryMetadata{{Name: "Local", DisplayName: "Local display", Examples: []string{"Example"}}}, &name, nil
	}
	got, local = store.Load(context.Background())
	if calls != 1 || local != "Local" || got[0].Examples[0] != "Example" {
		t.Fatalf("loaded metadata=%#v local=%q calls=%d", got, local, calls)
	}
	got[0].Examples[0] = "caller mutation"
	cached, _ := store.Load(context.Background())
	if calls != 1 || cached[0].Examples[0] != "Example" {
		t.Fatalf("cache not isolated: %#v calls=%d", cached, calls)
	}
	now = now.Add(301 * time.Second)
	store.Load(context.Background())
	if calls != 2 {
		t.Fatalf("expired cache load calls=%d", calls)
	}
	store.Invalidate()
	store.Load(context.Background())
	if calls != 3 {
		t.Fatalf("invalidated cache load calls=%d", calls)
	}
}

func TestCanceledMetadataRefreshWaiterDoesNotWaitForDatabaseLoad(t *testing.T) {
	store := NewMetadataStore(nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	store.load = func(ctx context.Context) ([]analysis.CategoryMetadata, *string, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		select {
		case <-release:
			return []analysis.CategoryMetadata{{Name: "Loaded", DisplayName: "Loaded"}}, nil, nil
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		}
	}
	first := make(chan struct{})
	go func() { store.Load(context.Background()); close(first) }()
	<-entered
	ctx, cancel := context.WithCancel(context.Background())
	second := make(chan struct{})
	go func() { store.Load(ctx); close(second) }()
	cancel()
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("canceled waiter remained blocked behind metadata refresh")
	}
	select {
	case <-first:
		t.Fatal("database refresh was unexpectedly unblocked")
	default:
	}
	close(release)
	select {
	case <-first:
	case <-time.After(time.Second):
		t.Fatal("metadata refresh did not finish")
	}
}

func TestMetadataInvalidationDuringRefreshDoesNotPublishStaleSnapshot(t *testing.T) {
	store := NewMetadataStore(nil)
	entered := make(chan struct{})
	release := make(chan struct{})
	calls := 0
	name := "Old"
	store.load = func(context.Context) ([]analysis.CategoryMetadata, *string, error) {
		calls++
		if calls == 1 {
			close(entered)
			<-release
			return []analysis.CategoryMetadata{{Name: "Old", DisplayName: "Old"}}, &name, nil
		}
		return []analysis.CategoryMetadata{{Name: "New", DisplayName: "New"}}, nil, nil
	}
	done := make(chan struct{})
	go func() { store.Load(context.Background()); close(done) }()
	<-entered
	store.Invalidate()
	close(release)
	<-done
	got, _ := store.Load(context.Background())
	if calls != 2 || got[0].Name != "New" {
		t.Fatalf("in-flight stale load was cached after invalidation: calls=%d result=%#v", calls, got)
	}
}

func TestUDSAuthenticationSocketModeAndCleanup(t *testing.T) {
	dir, err := os.MkdirTemp("/private/tmp", "ai-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "ai.sock")
	ctx, cancel := context.WithCancel(context.Background())
	s := NewServer(Config{Socket: socket, Token: "boot-token", RPS: 2}, nil)
	done := make(chan error, 1)
	go func() { done <- ServeUnix(ctx, s) }()
	var conn net.Conn
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn, _ = net.Dial("unix", socket)
		if conn != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if conn == nil {
		select {
		case err := <-done:
			t.Fatalf("UDS did not open: %v", err)
		default:
			t.Fatal("UDS did not open")
		}
	}
	_ = conn.Close()
	info, err := os.Stat(socket)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("socket mode=%o", info.Mode().Perm())
	}
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
	statusReq, _ := http.NewRequest("GET", "http://localhost/v1/status", nil)
	statusReq.Header.Set("Authorization", "Bearer boot-token")
	statusResp, err := client.Do(statusReq)
	if err != nil {
		t.Fatal(err)
	}
	if statusResp.StatusCode != 200 {
		t.Fatalf("authenticated status=%d", statusResp.StatusCode)
	}
	statusResp.Body.Close()
	statusReq, _ = http.NewRequest("GET", "http://localhost/v1/status", nil)
	statusReq.Header.Set("Authorization", "Bearer stale-token")
	statusResp, err = client.Do(statusReq)
	if err != nil {
		t.Fatal(err)
	}
	if statusResp.StatusCode != 401 {
		t.Fatalf("stale token status=%d", statusResp.StatusCode)
	}
	statusResp.Body.Close()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("shutdown did not complete")
	}
	if _, err = os.Lstat(socket); !os.IsNotExist(err) {
		t.Fatalf("owned socket remained after shutdown: %v", err)
	}
}

func TestUDSCleanupNeverRemovesPreexistingOrReplacedPath(t *testing.T) {
	dir, err := os.MkdirTemp("/private/tmp", "ai-path-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err = os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	socket := filepath.Join(dir, "ai.sock")
	if err = os.WriteFile(socket, []byte("original"), 0600); err != nil {
		t.Fatal(err)
	}
	s := NewServer(Config{Socket: socket, Token: "boot-token"}, nil)
	if err = ServeUnix(context.Background(), s); err == nil {
		t.Fatal("preexisting path accepted")
	}
	contents, err := os.ReadFile(socket)
	if err != nil || string(contents) != "original" {
		t.Fatalf("preexisting path changed: %q err=%v", contents, err)
	}
	_ = os.Remove(socket)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ServeUnix(ctx, s) }()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err = os.Lstat(socket); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err = os.Remove(socket); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(socket, []byte("replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err = <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("runtime shutdown timed out")
	}
	contents, err = os.ReadFile(socket)
	if err != nil || string(contents) != "replacement" {
		t.Fatalf("replacement path was removed: %q err=%v", contents, err)
	}
}

func TestDeadlineIsBoundedAndStatusRequiresBearer(t *testing.T) {
	s := NewServer(Config{Token: "secret", RPS: 1}, nil)
	req := httptest.NewRequest("GET", "/v1/status", nil)
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 401 {
		t.Fatalf("unauthenticated status=%d", w.Code)
	}
	body := `{"version":1,"request_id":"id","deadline_at":"` + time.Now().Add(6*time.Minute).Format(time.RFC3339) + `"}`
	req = httptest.NewRequest("POST", "/v1/permit", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	w = httptest.NewRecorder()
	s.Handler().ServeHTTP(w, req)
	if w.Code != 400 {
		t.Fatalf("overlong deadline status=%d", w.Code)
	}
}

var _ = analysis.Usage{}
