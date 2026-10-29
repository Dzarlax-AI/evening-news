package analysis

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"

	ai "github.com/Dzarlax-AI/go-ai"
	"github.com/Dzarlax-AI/go-ai/providers/gemini"
)

const (
	maxGeminiRequest = 5 << 20
)

type ProviderConfig struct {
	Provider      ai.Provider
	Revision      int64
	APIKey        string
	Model         string
	Endpoint      string
	Client        *http.Client
	BeforeAttempt func(context.Context) error
}

func Analyze(ctx context.Context, r Request, config ProviderConfig) (map[string]any, *Usage, error) {
	if err := validateRequest(r); err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(r.Content) == "" || len([]rune(strings.TrimSpace(r.Content))) < 30 {
		if scientificTitleFallback(r.Title, r.URL) && len([]rune(strings.TrimSpace(r.Title))) > 20 {
			r.Content = r.Title
		} else {
			return map[string]any{"summary": nil, "categories": []any{}, "is_advertisement": nil,
				"_insufficient_content": true}, nil, nil
		}
	}
	if config.Provider == nil {
		if err := requiredConfig(config); err != nil {
			return nil, nil, err
		}
		if r.Model != "" {
			config.Model = r.Model
		}
		if r.Endpoint != "" {
			config.Endpoint = r.Endpoint
		}
		endpoint := config.Endpoint
		if strings.Contains(strings.ToLower(config.Model), "gemini-3") {
			endpoint = strings.Replace(endpoint, "/v1/models", "/v1beta/models", 1)
		}
		config.Provider = gemini.New(ai.Config{APIKey: config.APIKey, BaseURL: endpoint, HTTPClient: config.Client})
	}
	return analyzeWithProvider(ctx, r, config)
}

func analysisSchema() map[string]any {
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"optimized_title":      map[string]any{"type": []string{"string", "null"}},
			"summary":              map[string]any{"type": []string{"string", "null"}},
			"categories":           map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"category_confidences": map[string]any{"type": "array", "items": map[string]any{"type": "number"}},
			"category":             map[string]any{"type": []string{"string", "null"}},
			"category_confidence":  map[string]any{"type": "number"},
			"summary_confidence":   map[string]any{"type": "number"},
			"original_categories":  map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"is_advertisement":     map[string]any{"type": "boolean"},
			"ad_type":              map[string]any{"type": "string"},
			"ad_confidence":        map[string]any{"type": "number"},
			"ad_reasoning":         map[string]any{"type": "string"},
			"publication_date":     map[string]any{"type": []string{"string", "null"}},
			"content_quality":      map[string]any{"type": "number"},
			"confidence":           map[string]any{"type": "number"},
		},
		"required":             []string{"summary", "categories", "is_advertisement"},
		"additionalProperties": true,
	}
}

// analyzeWithProvider keeps domain validation separate from transport.
func analyzeWithProvider(ctx context.Context, r Request, cfg ProviderConfig) (map[string]any, *Usage, error) {
	schema, err := json.Marshal(analysisSchema())
	if err != nil {
		return nil, nil, errCode(CodeProviderConfiguration)
	}
	if cfg.BeforeAttempt != nil {
		if err := cfg.BeforeAttempt(ctx); err != nil {
			return nil, nil, err
		}
	}
	temp, maxTokens := 0.1, 2000
	response, err := cfg.Provider.Generate(ctx, ai.Request{Purpose: "article", Model: cfg.Model, Prompt: buildPrompt(r), Schema: schema, Temperature: &temp, MaxOutputTokens: &maxTokens})
	if err != nil {
		return nil, nil, err
	}
	if response.Status != ai.Complete {
		return nil, nil, errCode(CodeProviderInvalid)
	}
	parsed, err := parseStructuredResult(response.Text)
	if err != nil {
		return nil, nil, errCode(CodeProviderInvalidJSON)
	}
	value := func(p *int) int {
		if p == nil {
			return 0
		}
		return *p
	}
	u := &Usage{RequestedModel: cfg.Model, Model: response.Model, Provider: response.Provider, ProfileRevision: cfg.Revision, Normalized: &response.Usage, PromptTokens: value(response.Usage.InputTokens), CompletionTokens: value(response.Usage.OutputTokens), CachedTokens: value(response.Usage.CachedInputTokens), TotalTokens: value(response.Usage.TotalTokens)}
	return validateAnalysisResult(parsed, r), u, nil
}

var structuredJSONCodeBlock = regexp.MustCompile("(?s)```(json)?\\s*\\n?(.*?)\\n?```")

func parseStructuredResult(response string) (map[string]any, error) {
	text := strings.TrimSpace(response)
	result, err := decodeStructuredObject(text)
	if err == nil {
		return result, nil
	}
	if match := structuredJSONCodeBlock.FindStringSubmatch(text); len(match) > 2 {
		text = strings.TrimSpace(match[2])
	}
	if !strings.HasPrefix(text, "{") {
		start, end := strings.Index(text, "{"), strings.LastIndex(text, "}")
		if start >= 0 && end > start {
			text = text[start : end+1]
		}
	}
	return decodeStructuredObject(text)
}

func decodeStructuredObject(text string) (map[string]any, error) {
	decoder := json.NewDecoder(strings.NewReader(text))
	decoder.UseNumber()
	var result map[string]any
	if err := decoder.Decode(&result); err != nil || result == nil {
		return nil, fmt.Errorf("invalid structured object")
	}
	if err := ensureEOF(decoder); err != nil {
		return nil, err
	}
	return result, nil
}

func providerEndpoint(baseURL, model string) (string, error) {
	if strings.TrimSpace(model) == "" || len(model) > 128 || strings.ContainsAny(model, "/?#") {
		return "", fmt.Errorf("%s", CodeProviderConfiguration)
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" ||
		parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return "", fmt.Errorf("%s", CodeProviderConfiguration)
	}
	if strings.Contains(strings.ToLower(model), "gemini-3") {
		parsed.Path = strings.Replace(parsed.Path, "/v1/models", "/v1beta/models", 1)
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/") + "/" + url.PathEscape(model) + ":generateContent"
	return parsed.String(), nil
}

func ensureEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return fmt.Errorf("unexpected trailing JSON")
	}
	return nil
}

func scientificTitleFallback(title, rawURL string) bool {
	urlLower := strings.ToLower(rawURL)
	for _, domain := range []string{"nplus1.ru", "arxiv.org", "nature.com", "sciencedirect.com"} {
		if strings.Contains(urlLower, domain) {
			return true
		}
	}
	return false
}
