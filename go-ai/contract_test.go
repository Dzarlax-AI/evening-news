package ai_test

import (
	"context"
	"encoding/json"
	"errors"
	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"github.com/Dzarlax-AI/evening-news/go-ai/providers/gemini"
	"github.com/Dzarlax-AI/evening-news/go-ai/providers/openai"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProviderContracts(t *testing.T) {
	for _, name := range []string{"openai", "gemini"} {
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			var response string
			var status int
			handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if name == "openai" {
					if r.URL.Path != "/responses" || r.Header.Get("Authorization") != "Bearer secret" {
						t.Error("wrong auth/path")
					}
				} else if r.URL.Path != "/model:generateContent" || r.Header.Get("x-goog-api-key") != "secret" {
					t.Error("wrong auth/path")
				}
				var body map[string]any
				if json.NewDecoder(r.Body).Decode(&body) != nil {
					t.Error("bad request")
				}
				if name == "openai" && body["store"] != false {
					t.Error("store enabled")
				}
				w.Header().Set("Retry-After", "3")
				w.Header().Set("x-request-id", "request-id")
				w.WriteHeader(status)
				io.WriteString(w, response)
			})
			client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				if err := r.Context().Err(); err != nil {
					return nil, err
				}
				w := httptest.NewRecorder()
				handler.ServeHTTP(w, r)
				return w.Result(), nil
			})}
			cfg := ai.Config{APIKey: "secret", BaseURL: "https://fake.invalid", HTTPClient: client}
			var p ai.Provider
			if name == "openai" {
				p = openai.New(cfg)
			} else {
				p = gemini.New(cfg)
			}
			request := ai.Request{Model: "model", Prompt: "prompt"}
			status = 200
			if name == "openai" {
				response = `{"status":"completed","model":"actual","output":[{"content":[{"type":"output_text","text":"hello"}]}],"usage":{"input_tokens":10,"output_tokens":5,"total_tokens":15,"input_tokens_details":{"cached_tokens":2},"output_tokens_details":{"reasoning_tokens":3}}}`
			} else {
				response = `{"modelVersion":"actual","candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"hello"}]}}],"usageMetadata":{"promptTokenCount":10,"candidatesTokenCount":2,"thoughtsTokenCount":3,"cachedContentTokenCount":2,"totalTokenCount":15}}`
			}
			got, err := p.Generate(context.Background(), request)
			if err != nil || got.Status != ai.Complete || got.Text != "hello" || got.Model != "actual" || got.RequestID != "request-id" || got.Usage.OutputTokens == nil || *got.Usage.OutputTokens != 5 || got.Usage.TotalTokens == nil || *got.Usage.TotalTokens != 15 || *got.Usage.CachedInputTokens != 2 || *got.Usage.ReasoningTokens != 3 {
				t.Fatalf("unexpected result: %+v %v", got, err)
			}
			if name == "openai" {
				response = `{"status":"completed","output":[{"content":[{"type":"refusal","refusal":"no"}]}]}`
			} else {
				response = `{"promptFeedback":{"blockReason":"SAFETY"}}`
			}
			got, err = p.Generate(context.Background(), request)
			if err != nil || got.Status != ai.Refusal || got.Usage.InputTokens != nil {
				t.Fatalf("refusal: %+v %v", got, err)
			}
			if name == "openai" {
				response = `{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}`
			} else {
				response = `{"candidates":[{"finishReason":"MAX_TOKENS"}]}`
			}
			got, err = p.Generate(context.Background(), request)
			if err != nil || got.Status != ai.Incomplete {
				t.Fatalf("incomplete: %+v %v", got, err)
			}
			request.Schema = json.RawMessage(`{"type":"OBJECT","properties":{"optional":{"type":"STRING"}},"required":[]}`)
			if name == "openai" {
				response = `{"status":"completed","output":[{"content":[{"type":"output_text","text":"{\"optional\":null}"}]}]}`
			} else {
				response = `{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"{\"optional\":null}"}]}}]}`
			}
			got, err = p.Generate(context.Background(), request)
			if err != nil || got.Status != ai.Complete {
				t.Fatalf("json: %+v %v", got, err)
			}
			if name == "openai" {
				response = `{"status":"completed","output":[{"content":[{"type":"output_text","text":"not json"}]}]}`
			} else {
				response = `{"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"not json"}]}}]}`
			}
			_, err = p.Generate(context.Background(), request)
			var e *ai.Error
			if !errors.As(err, &e) || e.Kind != "invalid_json" {
				t.Fatalf("invalid JSON: %v", err)
			}
			before := calls.Load()
			request.Schema = json.RawMessage(`{"type":"object","$ref":"x"}`)
			_, err = p.Generate(context.Background(), request)
			if !errors.As(err, &e) || e.Kind != "invalid_schema" || calls.Load() != before {
				t.Fatal("schema must reject before call")
			}
			request.Schema = nil
			before = calls.Load()
			invalid := request
			invalid.Model = "invalid/model"
			_, err = p.Generate(context.Background(), invalid)
			if !errors.As(err, &e) || e.Kind != "configuration" || calls.Load() != before {
				t.Fatal("invalid model reached provider")
			}
			zero := 0
			invalid = request
			invalid.MaxOutputTokens = &zero
			_, err = p.Generate(context.Background(), invalid)
			if !errors.As(err, &e) || e.Kind != "invalid_options" || calls.Load() != before {
				t.Fatal("invalid token limit reached provider")
			}
			for _, code := range []int{202, 204, 401, 403, 429, 500, 503} {
				status = code
				response = "secret error"
				_, err = p.Generate(context.Background(), request)
				if !errors.As(err, &e) || e.StatusCode != code || e.Retryable != (code == 429 || code >= 500) || e.RetryAfter != 3*time.Second || strings.Contains(err.Error(), "secret") {
					t.Fatalf("http %d: %+v", code, err)
				}
			}
			status = 200
			response = strings.Repeat("x", (2<<20)+1)
			_, err = p.Generate(context.Background(), request)
			if !errors.As(err, &e) || e.Kind != "response_too_large" {
				t.Fatalf("large: %v", err)
			}
			ctxTimeout, stopTimeout := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			defer stopTimeout()
			_, err = p.Generate(ctxTimeout, request)
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("timeout: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			_, err = p.Generate(ctx, request)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("cancel: %v", err)
			}
		})
	}
}
