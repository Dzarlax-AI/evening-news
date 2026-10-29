package gemini

import (
	"context"
	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"io"
	"net/http"
	"strings"
	"testing"
)

type usageTransport func(*http.Request) (*http.Response, error)

func (f usageTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func intPtr(n int) *int                                                    { return &n }
func same(got, want *int) bool {
	return got == nil && want == nil || got != nil && want != nil && *got == *want
}
func TestUsageProtoJSONDefaults(t *testing.T) {
	cases := []struct {
		name, metadata                         string
		input, output, total, cache, reasoning *int
	}{
		{"omitted zero subsets", `,"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":4,"totalTokenCount":14}`, intPtr(10), intPtr(4), intPtr(14), intPtr(0), intPtr(0)},
		{"explicit zero subsets", `,"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":4,"cachedContentTokenCount":0,"thoughtsTokenCount":0,"totalTokenCount":14}`, intPtr(10), intPtr(4), intPtr(14), intPtr(0), intPtr(0)},
		{"explicit zero parents", `,"usageMetadata":{"promptTokenCount":0,"candidatesTokenCount":0,"totalTokenCount":0}`, intPtr(0), intPtr(0), intPtr(0), intPtr(0), intPtr(0)},
		{"thinking and cache", `,"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":4,"cachedContentTokenCount":3,"thoughtsTokenCount":2,"totalTokenCount":16}`, intPtr(10), intPtr(6), intPtr(16), intPtr(3), intPtr(2)},
		{"no metadata", ``, nil, nil, nil, nil, nil},
		{"missing parent counts", `,"usageMetadata":{"cachedContentTokenCount":3,"thoughtsTokenCount":2,"totalTokenCount":20}`, nil, nil, intPtr(20), nil, nil},
		{"partial prompt only", `,"usageMetadata":{"promptTokenCount":10}`, intPtr(10), nil, nil, intPtr(0), nil},
		{"partial candidate only", `,"usageMetadata":{"candidatesTokenCount":4}`, nil, intPtr(4), nil, nil, intPtr(0)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			body := `{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"answer"}]}}]` + tc.metadata + `}`
			client := &http.Client{Transport: usageTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			result, err := New(ai.Config{APIKey: "test", BaseURL: "https://fake.invalid", HTTPClient: client}).Generate(context.Background(), ai.Request{Model: "model", Prompt: "prompt"})
			if err != nil {
				t.Fatal(err)
			}
			u := result.Usage
			if !same(u.InputTokens, tc.input) || !same(u.OutputTokens, tc.output) || !same(u.TotalTokens, tc.total) || !same(u.CachedInputTokens, tc.cache) || !same(u.ReasoningTokens, tc.reasoning) {
				t.Fatalf("unexpected usage %+v", u)
			}
		})
	}
}
