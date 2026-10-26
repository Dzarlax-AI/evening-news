package processing

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

func TestInputFingerprintMatchesPythonUTF8CanonicalEncoding(t *testing.T) {
	input := map[string]any{"title": "Заголовок", "content": "A <b> & \u2028middle\u2029", "url": "https://fixture.invalid/a"}
	const want = "2a85e4e5915452f0645d9a6dd3e962c5bd4634fabb88bd466770f5da19752a01"
	if got := InputFingerprint(input); got != want {
		t.Fatalf("fingerprint=%s want %s", got, want)
	}
}

func TestExtractUsesPythonCallbackInputAllowlists(t *testing.T) {
	var operations []map[string]any
	aiServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req map[string]any
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Errorf("decode callback request: %v", err)
			return
		}
		operations = append(operations, req)
		result := map[string]any{"recorded": true}
		if req["operation"] == "extraction_context" {
			result = map[string]any{"selector": "article", "method": "readability"}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"version": req["version"], "run_id": req["run_id"], "article_id": req["article_id"],
			"article_attempt": req["article_attempt"], "input_fingerprint": req["input_fingerprint"],
			"operation": req["operation"], "result": result,
		})
	}))
	defer aiServer.Close()
	const articleURL = "https://fixture.invalid/story"
	extractorServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req map[string]any
		_ = json.Unmarshal(body, &req)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"contract_version": "1", "request_id": req["request_id"], "original_url": articleURL,
			"outcome": "success", "observations": []any{map[string]any{"selector": "article", "method": "http", "success": true}},
			"result": map[string]any{"content": "A sufficiently useful extracted article body.", "method_used": "readability", "selector_used": "article"},
		})
	}))
	defer extractorServer.Close()
	processor := &Processor{
		AI:        NewAIClient(aiServer.URL, "fixture-token"),
		Extractor: NewExtractorClient(extractorServer.URL, "fixture-token"),
	}
	article := store.Article{ID: 7, URL: articleURL, Title: "Fixture title", SourceType: "rss"}
	fence := store.Fence{RunID: "10000000-0000-4000-8000-000000000001", Envelope: store.Envelope{Version: 1}}
	_, err := processor.extract(context.Background(), fence, article, 1, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "original source body")
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 2 {
		t.Fatalf("callback operations=%d want 2", len(operations))
	}
	contextInput := operations[0]["input"].(map[string]any)
	if len(contextInput) != 1 || contextInput["url"] != articleURL {
		t.Fatalf("extraction_context input violates callback allowlist: %#v", contextInput)
	}
	feedback := operations[1]["input"].(map[string]any)
	allowed := map[string]bool{"url": true, "method": true, "success": true, "selector_used": true, "observations": true, "content_length": true, "extraction_time_ms": true, "quality_score": true}
	for key := range feedback {
		if !allowed[key] {
			t.Fatalf("extraction_feedback input has unsupported key %q: %#v", key, feedback)
		}
	}
	if feedback["url"] != articleURL || feedback["method"] != "readability" || feedback["success"] != true {
		t.Fatalf("unexpected extraction feedback: %#v", feedback)
	}
}

func TestExtractorClientSendsBoundedContractAndPreservesObservations(t *testing.T) {
	url := "https://fixture.invalid/story"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/extract" || r.Header.Get("Authorization") != "Bearer fixture-token" || r.Header.Get("Accept-Encoding") != "identity" {
			t.Errorf("unexpected extractor request: %s %s", r.Method, r.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode extractor request: %v", err)
			return
		}
		if body["url"] != url || body["strategy"] != "auto" || body["source_type"] != "article" || body["use_browser"] != false || body["max_bytes"] != float64(5*1024*1024) || body["max_chars"] != float64(1000) || body["learned_selector"] != "article" || body["learned_method"] != "http" {
			t.Errorf("unexpected extractor request payload: %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"contract_version": "1", "request_id": body["request_id"], "original_url": url,
			"final_url": url, "outcome": "success",
			"result":       map[string]any{"content": "synthetic article content", "title": "fixture", "media": []any{map[string]any{"type": "image", "url": "https://fixture.invalid/image.jpg"}}, "method": "http", "method_used": "http", "selector_used": "article"},
			"observations": []any{map[string]any{"selector": "article", "method": "http", "success": true}},
		})
	}))
	defer server.Close()
	c := NewExtractorClient(server.URL, "fixture-token")
	c.MaxChars = 1000
	out, err := c.Extract(context.Background(), url, "title", "rss", "article", "http", 1000)
	if err != nil {
		t.Fatal(err)
	}
	if out.Content != "synthetic article content" || len(out.MediaFiles) != 1 || out.MethodUsed != "http" || len(out.Observations) != 1 || !out.Observations[0].Success {
		t.Fatalf("extraction result lost fields: %#v", out)
	}
}

func TestExtractorClientRejectsCorrelationMismatchAndRedirects(t *testing.T) {
	url := "https://fixture.invalid/story"
	t.Run("mismatched request id", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"contract_version": "1", "request_id": "different", "original_url": url, "outcome": "success", "result": map[string]any{"content": "fixture content"}})
		}))
		defer server.Close()
		_, err := NewExtractorClient(server.URL, "fixture-token").Extract(context.Background(), url, "title", "rss", "", "", 1000)
		if err == nil || !isFatal(err) {
			t.Fatalf("mismatched request id should be a fatal protocol error: %v", err)
		}
	})
	t.Run("no redirect following", func(t *testing.T) {
		targetHits := 0
		target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { targetHits++ }))
		defer target.Close()
		redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }))
		defer redirect.Close()
		out, err := NewExtractorClient(redirect.URL, "fixture-token").Extract(context.Background(), url, "title", "rss", "", "", 1000)
		if err != nil || out.Error != "http_error" || targetHits != 0 {
			t.Fatalf("redirect should be rejected without contacting its target: err=%v target_hits=%d", err, targetHits)
		}
	})
}

func TestExtractorClientRetries429OnlyWithinTotalBudget(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.Header().Set("Retry-After", "0")
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		_ = json.NewEncoder(w).Encode(map[string]any{"contract_version": "1", "request_id": body["request_id"], "original_url": "https://fixture.invalid/a", "outcome": "success", "result": map[string]any{"content": "fixture content"}})
	}))
	defer server.Close()
	c := NewExtractorClient(server.URL, "fixture-token")
	c.TotalBudget = time.Second
	c.AttemptTimeout = time.Second
	out, err := c.Extract(context.Background(), "https://fixture.invalid/a", "title", "rss", "", "", 1000)
	if err != nil || out.Content != "fixture content" || calls != 2 {
		t.Fatalf("busy retry failed within budget: out=%#v calls=%d err=%v", out, calls, err)
	}
}

func TestExtractorClientUsesNativeBrowserOnlyAfterEligibleHTTPFailure(t *testing.T) {
	var browserCalls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if body["use_browser"] == true {
			browserCalls++
			_ = json.NewEncoder(w).Encode(map[string]any{"contract_version": "1", "request_id": body["request_id"], "original_url": body["url"], "outcome": "success", "result": map[string]any{"content": "browser extracted content"}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"contract_version": "1", "request_id": body["request_id"], "original_url": body["url"], "outcome": "failed", "error": map[string]any{"code": "challenge", "retryable": false}, "result": map[string]any{}})
	}))
	defer server.Close()
	c := NewExtractorClient(server.URL, "fixture-token")
	c.UseBrowser = true
	out, err := c.Extract(context.Background(), "https://fixture.invalid/a", "title", "rss", "article", "learned_pattern_browser_rendering", 1000)
	if err != nil || out.Content != "browser extracted content" || browserCalls != 1 {
		t.Fatalf("eligible browser retry failed: out=%#v browser_calls=%d err=%v", out, browserCalls, err)
	}
}

func TestExtractorClientDoesNotRetryBusyAfterRetryAfterExceedsBudget(t *testing.T) {
	var calls int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Retry-After", "2")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	c := NewExtractorClient(server.URL, "fixture-token")
	c.TotalBudget = 50 * time.Millisecond
	c.AttemptTimeout = 20 * time.Millisecond
	start := time.Now()
	out, err := c.Extract(context.Background(), "https://fixture.invalid/a", "title", "rss", "", "", 1000)
	if err != nil || out.Error != "busy" || calls != 1 || time.Since(start) > time.Second {
		t.Fatalf("busy retry exceeded budget: out=%#v calls=%d elapsed=%s err=%v", out, calls, time.Since(start), err)
	}
}

func TestAIClientStaleFenceIsFatal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusConflict) }))
	defer server.Close()
	c := NewAIClient(server.URL, "fixture-token")
	f := store.Fence{RunID: "10000000-0000-4000-8000-000000000001"}
	a := store.Article{ID: 7}
	_, err := c.Call(context.Background(), f, a, 1, "fingerprint", "analyze", map[string]any{"title": "", "content": "", "url": ""})
	if err == nil || !isFatal(err) {
		t.Fatalf("stale callback fence must abort the run: %v", err)
	}
}
