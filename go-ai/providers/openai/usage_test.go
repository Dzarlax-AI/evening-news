package openai

import (
	"context"
	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"io"
	"net/http"
	"strings"
	"testing"
)

type fakeRoundTrip func(*http.Request) (*http.Response, error)

func (f fakeRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestMissingUsageSubsetsRemainUnknown(t *testing.T) {
	client := &http.Client{Transport: fakeRoundTrip(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":"completed","output":[{"content":[{"type":"output_text","text":"ok"}]}],"usage":{"input_tokens":10,"output_tokens":4}}`))}, nil
	})}
	response, err := New(ai.Config{APIKey: "test", BaseURL: "https://fake.invalid", HTTPClient: client}).Generate(context.Background(), ai.Request{Model: "model"})
	if err != nil {
		t.Fatal(err)
	}
	u := response.Usage
	if u.InputTokens == nil || u.OutputTokens == nil || u.TotalTokens != nil || u.CachedInputTokens != nil || u.ReasoningTokens != nil {
		t.Fatalf("unexpected usage %+v", u)
	}
}
