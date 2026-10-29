// Package openai implements the Responses API text and structured output subset.
package openai

import (
	"context"
	"encoding/json"
	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"github.com/Dzarlax-AI/evening-news/go-ai/internal/transport"
	"strings"
)

type Client struct{ cfg ai.Config }

func New(cfg ai.Config) *Client { return &Client{cfg: cfg} }
func (c *Client) Generate(ctx context.Context, r ai.Request) (ai.Response, error) {
	out := ai.Response{Provider: "openai", Model: r.Model}
	if err := transport.Validate(c.cfg, r, "openai"); err != nil {
		return out, err
	}
	schema, err := transport.Schema(r.Schema, true)
	if err != nil {
		return out, ai.NewError("openai", "invalid_schema", nil)
	}
	body := map[string]any{"model": r.Model, "input": r.Prompt, "store": false}
	if r.Temperature != nil {
		body["temperature"] = *r.Temperature
	}
	if r.MaxOutputTokens != nil {
		body["max_output_tokens"] = *r.MaxOutputTokens
	}
	if schema != nil {
		body["text"] = map[string]any{"format": map[string]any{"type": "json_schema", "name": "response", "strict": true, "schema": schema}}
	}
	endpoint := strings.TrimRight(c.cfg.BaseURL, "/")
	if endpoint == "" {
		endpoint = "https://api.openai.com/v1"
	}
	b, id, err := transport.Do(ctx, c.cfg, "openai", endpoint+"/responses", "Authorization", "Bearer "+c.cfg.APIKey, body)
	out.RequestID = id
	if err != nil {
		return out, err
	}
	var v struct {
		Status            string `json:"status"`
		Model             string `json:"model"`
		IncompleteDetails struct {
			Reason string `json:"reason"`
		} `json:"incomplete_details"`
		Output []struct {
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
		Usage *struct {
			Total        *int `json:"total_tokens"`
			Input        *int `json:"input_tokens"`
			Output       *int `json:"output_tokens"`
			InputDetails *struct {
				Cached *int `json:"cached_tokens"`
			} `json:"input_tokens_details"`
			OutputDetails *struct {
				Reasoning *int `json:"reasoning_tokens"`
			} `json:"output_tokens_details"`
		} `json:"usage"`
	}
	if json.Unmarshal(b, &v) != nil {
		return out, ai.NewError("openai", "invalid_response", nil)
	}
	if v.Model != "" {
		out.Model = v.Model
	}
	if v.Usage != nil {
		out.Usage.TotalTokens = v.Usage.Total
		out.Usage.InputTokens = v.Usage.Input
		out.Usage.OutputTokens = v.Usage.Output
		if v.Usage.InputDetails != nil {
			out.Usage.CachedInputTokens = v.Usage.InputDetails.Cached
		}
		if v.Usage.OutputDetails != nil {
			out.Usage.ReasoningTokens = v.Usage.OutputDetails.Reasoning
		}
	}
	for _, item := range v.Output {
		for _, part := range item.Content {
			if part.Type == "refusal" {
				out.Status = ai.Refusal
				out.FinishReason = "refusal"
				return out, nil
			}
			if part.Type == "output_text" {
				out.Text += part.Text
			}
		}
	}
	if v.Status == "incomplete" {
		out.Status = ai.Incomplete
		out.FinishReason = v.IncompleteDetails.Reason
		return out, nil
	}
	if v.Status != "completed" || out.Text == "" {
		return out, ai.NewError("openai", "invalid_response", nil)
	}
	if schema != nil && !json.Valid([]byte(out.Text)) {
		return out, ai.NewError("openai", "invalid_json", nil)
	}
	out.Status = ai.Complete
	out.FinishReason = "completed"
	return out, nil
}
