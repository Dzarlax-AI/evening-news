package analysis

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func testRequest(endpoint string) Request {
	return Request{Version: 1, Title: "Заголовок", Content: strings.Repeat("текст новости ", 6), URL: "https://example.com/news",
		Categories: []CategoryMetadata{{Name: "Business", DisplayName: "Бизнес", Examples: []string{"рынок"}}},
		Model:      "gemini-2.5-flash", Endpoint: endpoint, MaxTextChars: 100000}
}

func providerResponse(metadata string) string {
	return `{"candidates":[{"content":{"parts":[{"text":"{\"summary\":\"ok\",\"categories\":[\"Business\"],\"is_advertisement\":false}"}]}}],"usageMetadata":{` + metadata + `}}`
}

func TestAnalyzeGeminiRequestAndUsage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s", r.Method)
		}
		if !strings.Contains(r.URL.Path, "gemini-2.5-flash:generateContent") {
			t.Errorf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("key") != "test-key" {
			t.Errorf("provider key missing from query")
		}
		var request map[string]any
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		contents := request["contents"].([]any)
		part := contents[0].(map[string]any)["parts"].([]any)[0].(map[string]any)
		prompt := part["text"].(string)
		if !strings.Contains(prompt, "from an UNKNOWN source") || !strings.Contains(prompt, "рынок") || !strings.Contains(prompt, "SUMMARIZATION") {
			t.Errorf("prompt is missing expected source/category/summary context")
		}
		_, _ = w.Write([]byte(providerResponse(`"promptTokenCount":10,"candidatesTokenCount":20,"cachedContentTokenCount":3,"totalTokenCount":0`)))
	}))
	defer server.Close()
	r := testRequest(server.URL + "/v1/models")
	result, usage, err := Analyze(context.Background(), r, ProviderConfig{APIKey: "test-key", Model: r.Model, Endpoint: r.Endpoint, Client: server.Client()})
	if err != nil {
		t.Fatal(err)
	}
	if usage == nil || usage.TotalTokens != 0 || usage.CachedTokens != 3 {
		t.Fatalf("usage = %#v", usage)
	}
	if result["summary"] != "ok" || result["category"] != "Business" {
		t.Fatalf("normalized result = %#v", result)
	}
	parsed := result["categories_parsed"].([]any)[0].(map[string]any)
	if parsed["name"] != "Business" || parsed["confidence"] != 1.0 {
		t.Fatalf("categories_parsed = %#v", parsed)
	}
}

func TestMissingTotalUsageFallsBackButExplicitZeroDoesNot(t *testing.T) {
	for _, tc := range []struct {
		name, metadata string
		want           int
	}{
		{"missing", `"promptTokenCount":7,"candidatesTokenCount":11,"cachedContentTokenCount":2`, 20},
		{"zero", `"promptTokenCount":7,"candidatesTokenCount":11,"cachedContentTokenCount":2,"totalTokenCount":0`, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(providerResponse(tc.metadata))) }))
			defer server.Close()
			r := testRequest(server.URL + "/v1/models")
			_, usage, err := Analyze(context.Background(), r, ProviderConfig{APIKey: "key", Model: r.Model, Endpoint: r.Endpoint, Client: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			if usage.TotalTokens != tc.want {
				t.Fatalf("total tokens=%d, want %d", usage.TotalTokens, tc.want)
			}
		})
	}
}

func TestProviderErrorsPreserveRetryPhase(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "12")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	r := testRequest(server.URL + "/v1/models")
	_, _, err := Analyze(context.Background(), r, ProviderConfig{APIKey: "key", Model: r.Model, Endpoint: r.Endpoint, Client: server.Client()})
	var ae *AnalysisError
	if !errors.As(err, &ae) {
		t.Fatalf("error = %T %v", err, err)
	}
	if ae.Detail.StatusCode == nil || *ae.Detail.StatusCode != 429 || ae.Detail.RetryAfterSeconds == nil || *ae.Detail.RetryAfterSeconds != 12 || ae.Detail.RetryableTransport {
		t.Fatalf("provider error = %#v", ae.Detail)
	}
}

func TestTransportErrorIsEligibleForImmediateRetry(t *testing.T) {
	r := testRequest("http://127.0.0.1:1/v1/models")
	_, _, err := Analyze(context.Background(), r, ProviderConfig{APIKey: "key", Model: r.Model, Endpoint: r.Endpoint})
	var ae *AnalysisError
	if !errors.As(err, &ae) || !ae.Detail.RetryableTransport {
		t.Fatalf("error = %#v", err)
	}
}

func TestMalformedProviderJSONIsNotTransportRetryable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write([]byte(`not json`)) }))
	defer server.Close()
	r := testRequest(server.URL + "/v1/models")
	_, _, err := Analyze(context.Background(), r, ProviderConfig{APIKey: "key", Model: r.Model, Endpoint: r.Endpoint, Client: server.Client()})
	var ae *AnalysisError
	if !errors.As(err, &ae) || ae.Detail.RetryableTransport || ae.Detail.Code != CodeProviderInvalid {
		t.Fatalf("error = %#v", err)
	}
}

func TestInsufficientContentAvoidsProvider(t *testing.T) {
	r := testRequest("https://generativelanguage.googleapis.com/v1/models")
	r.Content = "tiny"
	result, usage, err := Analyze(context.Background(), r, ProviderConfig{})
	if err != nil || usage != nil || result["_insufficient_content"] != true {
		t.Fatalf("result=%#v usage=%#v err=%v", result, usage, err)
	}
}

func TestStructuredJSONPreservesLegacyFencedAndPrefixedExtraction(t *testing.T) {
	for _, input := range []string{
		`{"summary":"plain"}`,
		"```json\n{\"summary\":\"fenced\"}\n```",
		"Structured result follows: {\"summary\":\"prefixed\"}",
	} {
		if _, err := parseStructuredResult(input); err != nil {
			t.Errorf("parseStructuredResult(%q): %v", input, err)
		}
	}
}

func TestAnalyzeTrimsWhitespaceInLegacyNumericStrings(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := json.Marshal(map[string]any{
			"candidates": []any{map[string]any{"content": map[string]any{"parts": []any{
				map[string]string{"text": `{"summary":"ok","categories":["Business"],"is_advertisement":false,"category_confidence":" 0.72 "}`},
			}}}},
			"usageMetadata": map[string]any{"promptTokenCount": 1, "candidatesTokenCount": 1, "totalTokenCount": 2},
		})
		_, _ = w.Write(body)
	}))
	defer server.Close()
	r := testRequest(server.URL + "/v1/models")
	result, _, err := Analyze(context.Background(), r, ProviderConfig{
		APIKey: "key", Model: r.Model, Endpoint: r.Endpoint, Client: server.Client(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := result["category_confidence"]; got != 0.72 {
		t.Fatalf("category_confidence=%v, want 0.72", got)
	}
}
