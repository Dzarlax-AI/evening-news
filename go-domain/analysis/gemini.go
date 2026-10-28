package analysis

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	maxGeminiRequest  = 5 << 20
	maxGeminiResponse = 2 << 20
)

type ProviderConfig struct {
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
	if err := requiredConfig(config); err != nil {
		return nil, nil, err
	}
	if r.Model != "" {
		config.Model = r.Model
	}
	if r.Endpoint != "" {
		config.Endpoint = r.Endpoint
	}
	prompt := buildPrompt(r)
	response, err := callGemini(ctx, prompt, config)
	if err != nil {
		return nil, nil, err
	}
	result := validateAnalysisResult(response.Result, r)
	usage := &Usage{Model: config.Model, PromptTokens: response.Usage.PromptTokenCount,
		CompletionTokens: response.Usage.CandidatesTokenCount,
		CachedTokens:     response.Usage.CachedContentTokenCount,
		TotalTokens:      0}
	if response.Usage.TotalTokenCount != nil {
		usage.TotalTokens = *response.Usage.TotalTokenCount
	} else {
		usage.TotalTokens = usage.PromptTokens + usage.CompletionTokens + usage.CachedTokens
	}
	return result, usage, nil
}

type geminiResult struct {
	Result map[string]any
	Usage  GeminiUsage
}

func callGemini(ctx context.Context, prompt string, config ProviderConfig) (geminiResult, error) {
	var empty geminiResult
	endpoint, err := providerEndpoint(config.Endpoint, config.Model)
	if err != nil {
		return empty, errCode(CodeProviderConfiguration)
	}
	schema := map[string]any{
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
	body, err := json.Marshal(map[string]any{
		"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]string{"text": prompt}}}},
		"generationConfig": map[string]any{
			"temperature": 0.1, "maxOutputTokens": 2000,
			"responseMimeType": "application/json", "responseJsonSchema": schema,
		},
	})
	if err != nil || len(body) > maxGeminiRequest {
		return empty, errCode(CodeProviderRequestTooLarge)
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return empty, errCode(CodeProviderConfiguration)
	}
	query := u.Query()
	query.Set("key", config.APIKey)
	u.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return empty, errCode(CodeProviderConfiguration)
	}
	req.Header.Set("Content-Type", "application/json")
	client := config.Client
	if client == nil {
		client = &http.Client{Timeout: 40 * time.Second}
	}
	clientCopy := *client
	clientCopy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	if config.BeforeAttempt != nil {
		if err := config.BeforeAttempt(ctx); err != nil {
			return empty, err
		}
	}
	resp, err := clientCopy.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return empty, errTransport(CodeProviderCancelled, false)
		}
		if errors.Is(err, context.DeadlineExceeded) {
			return empty, errTransport(CodeProviderTimeout, true)
		}
		return empty, errTransport(CodeProviderTransport, true)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		var retryAfter *float64
		if resp.StatusCode == http.StatusTooManyRequests {
			retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
		}
		return empty, errStatus(CodeProviderHTTP, resp.StatusCode, retryAfter)
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxGeminiResponse+1))
	if err != nil {
		return empty, errCode(CodeProviderRead)
	}
	if len(responseBody) > maxGeminiResponse {
		return empty, errCode(CodeProviderResponseTooBig)
	}
	var raw struct {
		Candidates []struct {
			Content *struct {
				Parts []struct {
					Text string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		Usage GeminiUsage `json:"usageMetadata"`
	}
	if err := json.Unmarshal(responseBody, &raw); err != nil {
		return empty, errStatus(CodeProviderInvalid, http.StatusOK, nil)
	}
	if len(raw.Candidates) == 0 || raw.Candidates[0].Content == nil || len(raw.Candidates[0].Content.Parts) == 0 || raw.Candidates[0].Content.Parts[0].Text == "" {
		return empty, errStatus(CodeProviderEmpty, http.StatusOK, nil)
	}
	result, err := parseStructuredResult(raw.Candidates[0].Content.Parts[0].Text)
	if err != nil {
		return empty, errStatus(CodeProviderInvalidJSON, http.StatusOK, nil)
	}
	return geminiResult{Result: result, Usage: raw.Usage}, nil
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

func parseRetryAfter(raw string) *float64 {
	if raw == "" {
		return nil
	}
	if seconds, err := strconv.ParseFloat(strings.TrimSpace(raw), 64); err == nil && seconds >= 0 && !math.IsInf(seconds, 0) && !math.IsNaN(seconds) {
		return &seconds
	}
	if date, err := http.ParseTime(raw); err == nil {
		seconds := time.Until(date).Seconds()
		if seconds < 0 {
			seconds = 0
		}
		return &seconds
	}
	return nil
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
