// Package gemini implements native generateContent text and structured output.
package gemini

import (
	"context"
	"encoding/json"
	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"github.com/Dzarlax-AI/evening-news/go-ai/internal/transport"
	"net/url"
	"strings"
)

type Client struct{ cfg ai.Config }

func New(cfg ai.Config) *Client { return &Client{cfg: cfg} }
func (c *Client) Generate(ctx context.Context, r ai.Request) (ai.Response, error) {
	out := ai.Response{Provider: "gemini", Model: r.Model}
	if err := transport.Validate(c.cfg, r, "gemini"); err != nil {
		return out, err
	}
	schema, err := transport.Schema(r.Schema, false)
	if err != nil {
		return out, ai.NewError("gemini", "invalid_schema", nil)
	}
	config := map[string]any{}
	if r.Temperature != nil {
		config["temperature"] = *r.Temperature
	}
	if r.MaxOutputTokens != nil {
		config["maxOutputTokens"] = *r.MaxOutputTokens
	}
	if schema != nil {
		config["responseMimeType"] = "application/json"
		config["responseJsonSchema"] = schema
	}
	body := map[string]any{"contents": []any{map[string]any{"role": "user", "parts": []any{map[string]string{"text": r.Prompt}}}}, "generationConfig": config}
	endpoint := strings.TrimRight(c.cfg.BaseURL, "/")
	if endpoint == "" {
		endpoint = "https://generativelanguage.googleapis.com/v1beta/models"
	}
	endpoint += "/" + url.PathEscape(r.Model) + ":generateContent"
	b, id, err := transport.Do(ctx, c.cfg, "gemini", endpoint, "x-goog-api-key", c.cfg.APIKey, body)
	out.RequestID = id
	if err != nil {
		return out, err
	}
	var v struct {
		Model          string `json:"modelVersion"`
		PromptFeedback struct {
			BlockReason string `json:"blockReason"`
		} `json:"promptFeedback"`
		Candidates []struct {
			FinishReason string `json:"finishReason"`
			Content      struct {
				Parts []struct {
					Text    string `json:"text"`
					Thought bool   `json:"thought"`
				} `json:"parts"`
			} `json:"content"`
		} `json:"candidates"`
		Usage *struct {
			Total     *int `json:"totalTokenCount"`
			Input     *int `json:"promptTokenCount"`
			Output    *int `json:"candidatesTokenCount"`
			Cached    *int `json:"cachedContentTokenCount"`
			Reasoning *int `json:"thoughtsTokenCount"`
		} `json:"usageMetadata"`
	}
	if json.Unmarshal(b, &v) != nil {
		return out, ai.NewError("gemini", "invalid_response", nil)
	}
	if v.Model != "" {
		out.Model = v.Model
	}
	if v.Usage != nil {
		out.Usage.TotalTokens = v.Usage.Total
		out.Usage.InputTokens = v.Usage.Input
		out.Usage.CachedInputTokens = v.Usage.Cached
		out.Usage.ReasoningTokens = v.Usage.Reasoning
		if v.Usage.Output != nil && v.Usage.Reasoning != nil {
			x := *v.Usage.Output + *v.Usage.Reasoning
			out.Usage.OutputTokens = &x
		}
	}
	if v.PromptFeedback.BlockReason != "" {
		out.Status = ai.Refusal
		out.FinishReason = v.PromptFeedback.BlockReason
		return out, nil
	}
	if len(v.Candidates) == 0 {
		return out, ai.NewError("gemini", "invalid_response", nil)
	}
	candidate := v.Candidates[0]
	out.FinishReason = candidate.FinishReason
	for _, p := range candidate.Content.Parts {
		if !p.Thought {
			out.Text += p.Text
		}
	}
	switch candidate.FinishReason {
	case "STOP":
		out.Status = ai.Complete
	case "SAFETY", "RECITATION", "BLOCKLIST", "PROHIBITED_CONTENT", "SPII":
		out.Status = ai.Refusal
	default:
		out.Status = ai.Incomplete
	}
	if out.Status == ai.Complete {
		if out.Text == "" {
			return out, ai.NewError("gemini", "invalid_response", nil)
		}
		if schema != nil && !json.Valid([]byte(out.Text)) {
			return out, ai.NewError("gemini", "invalid_json", nil)
		}
	}
	return out, nil
}
