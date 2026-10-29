package airuntime

// Historical fixtures retain the previous wire format for parity comparison.
import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func (s *Server) callGenerate(ctx context.Context, v generateRequest) (any, error) {
	if err := s.limiter.Acquire(ctx); err != nil {
		return nil, &providerFailure{code: "provider_timeout", retryable: errors.Is(err, context.DeadlineExceeded)}
	}
	urlString, err := providerURL(s.config.Endpoint, v.Model)
	if err != nil {
		return nil, &providerFailure{code: "provider_invalid_configuration"}
	}
	temp, maxTokens := v.Options.Temperature, v.Options.MaxTokens
	generation := map[string]any{"temperature": temp, "maxOutputTokens": maxTokens}
	if v.Mode == "structured" {
		temp, maxTokens = .1, 2000
		generation = map[string]any{"temperature": temp, "maxOutputTokens": maxTokens, "responseMimeType": "application/json"}
		var schema any
		if json.Unmarshal(v.Schema, &schema) != nil {
			return nil, &providerFailure{code: "invalid_input"}
		}
		generation["responseJsonSchema"] = schema
	}
	payload := map[string]any{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]string{"text": v.Prompt}}}}, "generationConfig": generation}
	body, err := json.Marshal(payload)
	if err != nil || len(body) > maxRPCBody {
		return nil, &providerFailure{code: "provider_request_too_large"}
	}
	u, _ := url.Parse(urlString)
	query := u.Query()
	query.Set("key", s.config.APIKey)
	u.RawQuery = query.Encode()
	timeout := 30 * time.Second
	if v.Mode == "structured" {
		timeout = 40 * time.Second
	}
	attemptCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(attemptCtx, http.MethodPost, u.String(), bytes.NewReader(body))
	if err != nil {
		return nil, &providerFailure{code: "provider_invalid_configuration"}
	}
	req.Header.Set("Content-Type", "application/json")
	client := *s.client
	client.Timeout = timeout
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	resp, err := client.Do(req)
	if err != nil {
		if errors.Is(err, context.Canceled) && ctx.Err() != nil {
			return nil, &providerFailure{code: "provider_cancelled"}
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(attemptCtx.Err(), context.DeadlineExceeded) {
			return nil, &providerFailure{code: "provider_timeout", retryable: true}
		}
		return nil, &providerFailure{code: "provider_transport_error", retryable: true}
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
		status := resp.StatusCode
		var after *float64
		if status == 429 {
			after = parseRetryAfter(resp.Header.Get("Retry-After"))
		}
		return nil, &providerFailure{code: "provider_http_error", status: &status, retryAfter: after}
	}
	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxRPCBody+1))
	if err != nil {
		return nil, &providerFailure{code: "provider_response_read_error"}
	}
	if len(responseBody) > maxRPCBody {
		return nil, &providerFailure{code: "provider_response_too_large"}
	}
	var raw struct {
		Candidates []struct {
			Content *struct {
				Parts []struct {
					Text *string `json:"text"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		Usage analysis.GeminiUsage `json:"usageMetadata"`
	}
	if json.Unmarshal(responseBody, &raw) != nil {
		return nil, &providerFailure{code: "provider_invalid_response"}
	}
	if len(raw.Candidates) == 0 {
		status := 200
		return nil, &providerFailure{code: "provider_empty_response", status: &status}
	}
	text := ""
	if raw.Candidates[0].Content != nil && len(raw.Candidates[0].Content.Parts) > 0 && raw.Candidates[0].Content.Parts[0].Text != nil {
		text = *raw.Candidates[0].Content.Parts[0].Text
	}
	usage := normalizeUsage(raw.Usage)
	var result map[string]any
	if v.Mode == "text" {
		result = map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": strings.TrimSpace(text)}}}, "usage": usage}
	} else {
		if raw.Candidates[0].Content == nil || len(raw.Candidates[0].Content.Parts) == 0 || raw.Candidates[0].Content.Parts[0].Text == nil {
			status := 200
			return nil, &providerFailure{code: "provider_empty_response", status: &status}
		}
		structured, e := parseLegacyStructured(text)
		if e != nil {
			status := 200
			return nil, &providerFailure{code: "provider_invalid_json", status: &status}
		}
		result = map[string]any{"result": structured, "usage": usage}
	}
	accountingTokens := usage["total_tokens"].(int)
	if accountingTokens == 0 {
		accountingTokens = raw.Usage.PromptTokenCount + raw.Usage.CandidatesTokenCount
	}
	return generateOutput{result: result, usage: analysis.Usage{Model: v.Model, PromptTokens: usage["prompt_tokens"].(int), CompletionTokens: usage["completion_tokens"].(int), CachedTokens: usage["cached_tokens"].(int), TotalTokens: usage["total_tokens"].(int)}, accountingTokens: accountingTokens, accountingTokenSet: true}, nil
}
