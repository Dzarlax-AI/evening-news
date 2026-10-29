// Package ai defines provider-neutral text and structured generation contracts.
package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Provider interface {
	Generate(context.Context, Request) (Response, error)
}
type Request struct {
	Purpose, Model, Prompt string
	Schema                 json.RawMessage
	Temperature            *float64
	MaxOutputTokens        *int
}
type Status string

const (
	Complete   Status = "complete"
	Refusal    Status = "refusal"
	Incomplete Status = "incomplete"
)

// CachedInputTokens is a subset of InputTokens, and ReasoningTokens a subset of OutputTokens. Nil means unknown.
type Usage struct {
	InputTokens       *int `json:"input_tokens,omitempty"`
	OutputTokens      *int `json:"output_tokens,omitempty"`
	TotalTokens       *int `json:"total_tokens,omitempty"`
	CachedInputTokens *int `json:"cached_input_tokens,omitempty"`
	ReasoningTokens   *int `json:"reasoning_tokens,omitempty"`
}
type Response struct {
	Text, Provider, Model, FinishReason, RequestID string
	Status                                         Status
	Usage                                          Usage
}
type Config struct {
	APIKey, BaseURL string
	HTTPClient      *http.Client
}
type Error struct {
	Kind, Provider, FinishReason, RequestID string
	StatusCode                              int
	RetryAfter                              time.Duration
	Retryable                               bool
	cause                                   error
}

func (e *Error) Error() string {
	return fmt.Sprintf("ai %s: %s (status %d)", e.Provider, e.Kind, e.StatusCode)
}
func (e *Error) Unwrap() error { return e.cause }

// NewError constructs a safe error without exposing upstream bodies or transport details.
func NewError(provider, kind string, cause error) *Error {
	return &Error{Provider: provider, Kind: kind, cause: cause}
}
