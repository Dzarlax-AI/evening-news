package analysis

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"testing"
)

type parityFixture struct {
	Case  string `json:"case"`
	Input struct {
		Title           string  `json:"title"`
		Content         string  `json:"content"`
		URL             string  `json:"url"`
		OriginalContext *string `json:"original_context"`
	} `json:"input"`
	Categories              []CategoryMetadata `json:"categories"`
	LocalCategory           string             `json:"local_category"`
	ConfiguredLocalCategory string             `json:"configured_local_category"`
	ProviderResult          map[string]any     `json:"provider_result"`
	ExpectedPrompt          string             `json:"expected_prompt"`
	ExpectedSchema          any                `json:"expected_schema"`
	ExpectedResult          map[string]any     `json:"expected_result"`
}

func TestLegacyArticleAnalysisParityFixtures(t *testing.T) {
	fixtureBytes, err := os.ReadFile("../../tests/fixtures/article-analysis-v1/legacy_parity.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixtures []parityFixture
	if err := json.Unmarshal(fixtureBytes, &fixtures); err != nil {
		t.Fatal(err)
	}
	if len(fixtures) != 6 {
		t.Fatalf("fixture count=%d, want 6", len(fixtures))
	}
	for _, fixture := range fixtures {
		t.Run(fixture.Case, func(t *testing.T) {
			providerJSON, err := json.Marshal(fixture.ProviderResult)
			if err != nil {
				t.Fatal(err)
			}
			providerBody, err := json.Marshal(map[string]any{
				"candidates":    []any{map[string]any{"content": map[string]any{"parts": []any{map[string]string{"text": string(providerJSON)}}}}},
				"usageMetadata": map[string]any{"promptTokenCount": 10, "candidatesTokenCount": 5, "cachedContentTokenCount": 2, "totalTokenCount": 0},
			})
			if err != nil {
				t.Fatal(err)
			}
			var gotPrompt string
			var gotSchema any
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
					t.Errorf("decode provider request: %v", err)
					return
				}
				content := body["contents"].([]any)[0].(map[string]any)
				gotPrompt = content["parts"].([]any)[0].(map[string]any)["text"].(string)
				gotSchema = body["generationConfig"].(map[string]any)["responseJsonSchema"]
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write(providerBody)
			}))
			defer server.Close()
			r := Request{Version: 1, Title: fixture.Input.Title, Content: fixture.Input.Content, URL: fixture.Input.URL,
				OriginalContext: fixture.Input.OriginalContext, Categories: fixture.Categories,
				LocalCategory: fixture.LocalCategory, ConfiguredLocalCategory: fixture.ConfiguredLocalCategory,
				Model: "gemini-3.5-flash-lite", Endpoint: server.URL + "/v1/models", MaxTextChars: 100000}
			result, usage, err := Analyze(context.Background(), r, ProviderConfig{APIKey: "synthetic-key", Model: r.Model, Endpoint: r.Endpoint, Client: server.Client()})
			if err != nil {
				t.Fatal(err)
			}
			if gotPrompt != fixture.ExpectedPrompt {
				offset := firstDifference(gotPrompt, fixture.ExpectedPrompt)
				start := offset - 30
				if start < 0 {
					start = 0
				}
				end := offset + 75
				if end > len(gotPrompt) {
					end = len(gotPrompt)
				}
				expectedEnd := offset + 75
				if expectedEnd > len(fixture.ExpectedPrompt) {
					expectedEnd = len(fixture.ExpectedPrompt)
				}
				t.Errorf("legacy prompt differs at byte %d: got %q, want %q", offset, gotPrompt[start:end], fixture.ExpectedPrompt[start:expectedEnd])
			}
			if !equalJSON(gotSchema, fixture.ExpectedSchema) {
				t.Errorf("legacy JSON schema differs")
			}
			if !equalJSON(result, fixture.ExpectedResult) {
				t.Errorf("normalized result differs\n got: %#v\nwant: %#v", result, fixture.ExpectedResult)
			}
			if usage == nil || usage.TotalTokens != 0 {
				t.Errorf("explicit zero total token usage changed: %#v", usage)
			}
		})
	}
}

func firstDifference(a, b string) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] != b[i] {
			return i
		}
	}
	return n
}

func equalJSON(a, b any) bool {
	aj, err := json.Marshal(a)
	if err != nil {
		return false
	}
	bj, err := json.Marshal(b)
	if err != nil {
		return false
	}
	var av, bv any
	if json.Unmarshal(aj, &av) != nil || json.Unmarshal(bj, &bv) != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}
