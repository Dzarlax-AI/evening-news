package airuntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
	ai "github.com/Dzarlax-AI/go-ai"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestProfileSnapshotKeepsProviderAndModel(t *testing.T) {
	s := NewServer(Config{ActiveProvider: "openai", OpenAIKey: "fake", OpenAIArticleModel: "article-model", OpenAIDigestModel: "digest-model", OpenAITextModel: "text-model", APIKey: "", Model: "gemini-model", RPS: 100}, nil)
	var models []string
	s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		models = append(models, body.Model)
		return response(200, nil, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"hello"}]}]}`), nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	snap, err := s.snapshot(ctx, "text")
	if err != nil {
		t.Fatal(err)
	}
	s.config.ActiveProvider = "gemini"
	s.config.APIKey = "fake"
	s.config.Model = "other-model"
	out, err := s.generateWithSnapshot(ctx, snap, generateRequest{Mode: "text", Prompt: "hello", Options: generateOptions{Temperature: .1, MaxTokens: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 1 || models[0] != "text-model" || out.usage.Provider != "openai" {
		t.Fatalf("models=%v usage=%+v", models, out.usage)
	}
}
func TestUnknownCostAndCacheAccounting(t *testing.T) {
	input, output, cached := 100, 20, 30
	rec := usageRecord{usage: analysisUsage("openai", "model", &input, &output, &cached)}
	if got := usageInsert(rec, Config{}); got["cost"] != nil {
		t.Fatalf("unknown cost=%v", got["cost"])
	}
	a, b, c := 1.0, 2.0, .5
	got := usageInsert(rec, Config{Prices: map[string]ModelPrice{"openai/model": {Input: &a, Output: &b, CachedInput: &c}}})
	if got["cost"] != .000125 {
		t.Fatalf("cost=%v", got["cost"])
	}
	rec.usage.Model = "actual-snapshot"
	rec.usage.RequestedModel = "model"
	alias := usageInsert(rec, Config{Prices: map[string]ModelPrice{"openai/model": {Input: &a, Output: &b, CachedInput: &c}}})
	if alias["cost"] != .000125 {
		t.Fatalf("alias price=%v", alias["cost"])
	}
	if missing := usageInsert(rec, Config{}); missing["cost"] != nil {
		t.Fatalf("unknown alias=%v", missing["cost"])
	}

}
func TestOpenAIArticleIgnoresLegacyInputModel(t *testing.T) {
	s := NewServer(Config{ActiveProvider: "openai", OpenAIKey: "fake", OpenAIArticleModel: "article-model", OpenAIDigestModel: "digest-model", OpenAITextModel: "text-model", Model: "legacy-gemini", RPS: 100}, nil)
	s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		if !strings.Contains(string(body), `"model":"article-model"`) {
			t.Fatalf("wrong model")
		}
		return response(200, nil, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"{\"summary\":\"ok\",\"categories\":[\"Business\"],\"is_advertisement\":false}"}]}]}`), nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, usage, err := s.AnalyzeArticle(ctx, ArticleInput{Title: "title", Content: strings.Repeat("synthetic content ", 6), URL: "https://example.test", Model: "legacy-gemini"})
	if err != nil || result["summary"] != "ok" || usage.Model != "article-model" {
		t.Fatalf("result=%v usage=%v err=%v", result, usage, err)
	}
}

func analysisUsage(provider, model string, input, output, cached *int) analysis.Usage {
	return analysis.Usage{Provider: provider, Model: model, Normalized: &ai.Usage{InputTokens: input, OutputTokens: output, CachedInputTokens: cached}}
}

func TestGeminiEndpointPolicy(t *testing.T) {
	for _, tc := range []struct {
		url            string
		fixture, valid bool
	}{
		{"https://generativelanguage.googleapis.com/v1/models", false, true},
		{"https://generativelanguage.googleapis.com/v1beta/models", false, true},
		{"http://127.0.0.1:1234/v1/models", true, true},
		{"http://fixture-services:8090/v1/models", true, true},
		{"http://127.0.0.1:1234/v1/models", false, false},
		{"https://evil.example/v1/models", true, false},
		{"https://generativelanguage.googleapis.com:443/v1/models", false, false},
		{"https://generativelanguage.googleapis.com/v1/models?key=secret", false, false},
	} {
		if got := validateGeminiEndpoint(tc.url, tc.fixture) == nil; got != tc.valid {
			t.Errorf("endpoint %q fixture=%v valid=%v", tc.url, tc.fixture, got)
		}
	}
}

type deadlineProvider struct {
	deadline time.Time
	seen     chan struct{}
}

func (p *deadlineProvider) Generate(ctx context.Context, _ ai.Request) (ai.Response, error) {
	p.deadline, _ = ctx.Deadline()
	close(p.seen)
	<-ctx.Done()
	return ai.Response{}, ctx.Err()
}
func TestManagedProviderAttemptDeadlineAndCancellation(t *testing.T) {
	for _, tc := range []struct {
		purpose string
		schema  json.RawMessage
		seconds int
	}{{"article", nil, 40}, {"text", nil, 30}, {"text", json.RawMessage(`{"type":"object","properties":{}}`), 40}, {"digest", nil, 180}} {
		t.Run(tc.purpose+fmt.Sprint(tc.seconds), func(t *testing.T) {
			base, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			ctx, stop := context.WithCancel(base)
			defer stop()
			fake := &deadlineProvider{seen: make(chan struct{})}
			s := NewServer(Config{RPS: 100}, nil)
			p := managedProvider{server: s, provider: fake, breaker: NewBreaker()}
			done := make(chan error, 1)
			started := time.Now()
			go func() { _, err := p.Generate(ctx, ai.Request{Purpose: tc.purpose, Schema: tc.schema}); done <- err }()
			<-fake.seen
			remaining := fake.deadline.Sub(started)
			if remaining < time.Duration(tc.seconds)*time.Second-time.Second || remaining > time.Duration(tc.seconds)*time.Second+time.Second {
				t.Fatalf("attempt deadline %v", remaining)
			}
			stop()
			if err := <-done; !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel=%v", err)
			}
		})
	}
}
func TestRetryAfterCannotOutliveOperationDeadline(t *testing.T) {
	s := NewServer(Config{ActiveProvider: "openai", OpenAIKey: "fake", OpenAIArticleModel: "a", OpenAIDigestModel: "d", OpenAITextModel: "t", RPS: 100}, nil)
	var calls int
	s.client = &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		calls++
		return response(429, http.Header{"Retry-After": []string{"30"}}, `{}`), nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	snap, err := s.snapshot(ctx, "text")
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	_, err = s.generateWithSnapshot(ctx, snap, generateRequest{Mode: "text", Prompt: "fixture", Options: generateOptions{MaxTokens: 10}})
	if err == nil || calls != 1 || time.Since(start) > time.Second/2 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestManagedDigestHonorsEarlierJobDeadline(t *testing.T) {
	fake := &deadlineProvider{seen: make(chan struct{})}
	s := NewServer(Config{RPS: 100}, nil)
	p := managedProvider{server: s, provider: fake, breaker: NewBreaker()}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	jobDeadline, _ := ctx.Deadline()
	_, err := p.Generate(ctx, ai.Request{Purpose: "digest"})
	if !errors.Is(err, context.DeadlineExceeded) || !fake.deadline.Equal(jobDeadline) {
		t.Fatal("digest attempt did not preserve the earlier job deadline")
	}
}
