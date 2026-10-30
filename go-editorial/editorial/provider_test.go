package editorial

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"

	ai "github.com/Dzarlax-AI/go-ai"
	"github.com/Dzarlax-AI/go-ai/providers/gemini"
	"github.com/Dzarlax-AI/go-ai/providers/openai"
)

type fakeTransport func(*http.Request) (*http.Response, error)

func (f fakeTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNeutralProvidersPreserveEditorialValidationAndLinks(t *testing.T) {
	s := syntheticSnapshot(8)
	draft, _ := json.Marshal(syntheticDraft(s))
	for _, name := range []string{"gemini", "openai"} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: fakeTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				var payload map[string]any
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Fatal(err)
				}
				var b []byte
				if name == "gemini" {
					b, _ = json.Marshal(map[string]any{"candidates": []any{map[string]any{"finishReason": "STOP", "content": map[string]any{"parts": []any{map[string]string{"text": string(draft)}}}}}})
				} else {
					b, _ = json.Marshal(map[string]any{"status": "completed", "output": []any{map[string]any{"type": "message", "content": []any{map[string]any{"type": "output_text", "text": string(draft)}}}}})
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(b)))}, nil
			})}
			config := ai.Config{APIKey: "fixture", HTTPClient: client}
			var provider ai.Provider
			if name == "gemini" {
				provider = gemini.New(config)
			} else {
				provider = openai.New(config)
			}
			result, err := Generate(context.Background(), s, RussianLocale, ProviderConfig{Provider: provider, Model: "fixture-model", Revision: 7})
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || result.Usage.Provider != name || result.Usage.Revision != 7 || len(result.Edition.Stories) != 7 {
				t.Fatalf("unexpected generation: calls=%d usage=%+v stories=%d", calls, result.Usage, len(result.Edition.Stories))
			}
			rendered, err := Render(s, RussianLocale, result.Edition)
			if err != nil || !strings.Contains(rendered.RichHTML, "https://news.example/article/1005") {
				t.Fatalf("render: %v", err)
			}
		})
	}
}

func TestNeutralProvidersShareBilingualEditorialCall(t *testing.T) {
	ru, sr := bilingualFixture()
	draft, _ := json.Marshal(map[string]any{"ru": syntheticDraft(ru), "sr-Latn": syntheticDraft(sr)})
	for _, name := range []string{"gemini", "openai"} {
		t.Run(name, func(t *testing.T) {
			calls := 0
			client := &http.Client{Transport: fakeTransport(func(r *http.Request) (*http.Response, error) {
				calls++
				var payload map[string]any
				if json.NewDecoder(r.Body).Decode(&payload) != nil {
					t.Fatal("invalid provider request")
				}
				raw, _ := json.Marshal(payload)
				if !strings.Contains(string(raw), "sr-Latn") {
					t.Fatal("missing Serbian request")
				}
				var b []byte
				if name == "gemini" {
					b, _ = json.Marshal(map[string]any{"candidates": []any{map[string]any{"finishReason": "STOP", "content": map[string]any{"parts": []any{map[string]string{"text": string(draft)}}}}}})
				} else {
					b, _ = json.Marshal(map[string]any{"status": "completed", "output": []any{map[string]any{"type": "message", "content": []any{map[string]any{"type": "output_text", "text": string(draft)}}}}})
				}
				return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(string(b)))}, nil
			})}
			var provider ai.Provider
			config := ai.Config{APIKey: "fixture", HTTPClient: client}
			if name == "gemini" {
				provider = gemini.New(config)
			} else {
				provider = openai.New(config)
			}
			out, err := GenerateBilingual(context.Background(), ru, sr, ProviderConfig{Provider: provider, Model: "fixture", Revision: 7})
			if err != nil || calls != 1 || out.Serbian == nil || out.Usage.Provider != name {
				t.Fatalf("bilingual provider calls=%d error=%v", calls, err)
			}
		})
	}
}
