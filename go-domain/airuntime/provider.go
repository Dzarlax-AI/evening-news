package airuntime

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
	ai "github.com/Dzarlax-AI/go-ai"
	"github.com/Dzarlax-AI/go-ai/providers/gemini"
	"github.com/Dzarlax-AI/go-ai/providers/openai"
)

type providerSnapshot struct {
	provider    ai.Provider
	name, model string
	revision    int64
}

func (s *Server) snapshot(ctx context.Context, purpose string) (providerSnapshot, error) {
	settings, err := s.GetAISettings(ctx)
	if err != nil {
		return providerSnapshot{}, err
	}
	if err = validateSettings(settings); err != nil {
		return providerSnapshot{}, err
	}
	profile := settings.Profiles.Gemini
	key, endpoint := s.config.APIKey, s.config.Endpoint
	if settings.ActiveProvider == "openai" {
		profile = settings.Profiles.OpenAI
		key, endpoint = s.config.OpenAIKey, s.config.OpenAIEndpoint
	}
	if s.credentials != nil {
		secret, _, resolveErr := s.credentials.Resolve(ctx, settings.ActiveProvider)
		if resolveErr != nil || len(secret) == 0 {
			return providerSnapshot{}, ErrAISettingsInvalid
		}
		key = string(secret)
	}
	model := profile.Models.Text
	if purpose == "article" {
		model = profile.Models.Article
	}
	if purpose == "digest" {
		model = profile.Models.Digest
	}
	cfg := ai.Config{APIKey: key, BaseURL: endpoint, HTTPClient: s.client}
	var p ai.Provider
	if settings.ActiveProvider == "openai" {
		p = openai.New(cfg)
	} else {
		if strings.Contains(strings.ToLower(model), "gemini-3") {
			cfg.BaseURL = strings.Replace(cfg.BaseURL, "/v1/models", "/v1beta/models", 1)
		}
		p = gemini.New(cfg)
	}
	return providerSnapshot{provider: &managedProvider{server: s, provider: p, breaker: s.providerBreaker(settings.ActiveProvider), name: settings.ActiveProvider}, name: settings.ActiveProvider, model: model, revision: settings.Revision}, nil
}

type managedProvider struct {
	server   *Server
	provider ai.Provider
	breaker  *Breaker
	name     string
}

func (p *managedProvider) Generate(ctx context.Context, req ai.Request) (ai.Response, error) {
	if p.name == "openai" {
		req.Temperature = nil
	}
	value, err := p.breaker.Do(ctx, func() (any, error) {
		for attempt := 0; attempt < 3; attempt++ {
			if err := p.server.limiter.Acquire(ctx); err != nil {
				return nil, err
			}
			timeout := 30 * time.Second
			if req.Purpose == "article" || len(req.Schema) > 0 {
				timeout = 40 * time.Second
			}
			if req.Purpose == "digest" {
				timeout = 60 * time.Second
			}
			attemptCtx, cancel := context.WithTimeout(ctx, timeout)
			response, err := p.provider.Generate(attemptCtx, req)
			cancel()
			if err == nil {
				if response.Status != ai.Complete {
					return nil, ai.NewError(response.Provider, string(response.Status), nil)
				}
				return response, nil
			}
			var failure *ai.Error
			if !errors.As(err, &failure) || !failure.Retryable || attempt == 2 {
				return nil, err
			}
			delay := 4 * time.Second
			if failure.RetryAfter > delay {
				delay = failure.RetryAfter
			}
			if deadline, ok := ctx.Deadline(); ok && time.Until(deadline) <= delay {
				return nil, err
			}
			if err = waitContext(ctx, delay); err != nil {
				return nil, err
			}
		}
		return nil, errors.New("provider_failed")
	})
	if err != nil {
		return ai.Response{}, err
	}
	return value.(ai.Response), nil
}
func (s *Server) providerBreaker(name string) *Breaker {
	if name == "openai" {
		return s.openAIBreaker
	}
	return s.breaker
}

func (s *Server) generateWithSnapshot(ctx context.Context, snap providerSnapshot, v generateRequest) (generateOutput, error) {
	temp, maxTokens := v.Options.Temperature, v.Options.MaxTokens
	if v.Mode == "structured" {
		temp, maxTokens = .1, 2000
	}
	response, err := snap.provider.Generate(ctx, ai.Request{Purpose: "text", Model: snap.model, Prompt: v.Prompt, Schema: v.Schema, Temperature: &temp, MaxOutputTokens: &maxTokens})
	if err != nil {
		return generateOutput{}, err
	}
	val := func(p *int) int {
		if p == nil {
			return 0
		}
		return *p
	}
	usage := analysis.Usage{RequestedModel: snap.model, Provider: response.Provider, Model: response.Model, ProfileRevision: snap.revision, Normalized: &response.Usage, PromptTokens: val(response.Usage.InputTokens), CompletionTokens: val(response.Usage.OutputTokens), CachedTokens: val(response.Usage.CachedInputTokens), TotalTokens: val(response.Usage.TotalTokens)}
	display := map[string]any{"prompt_tokens": usage.PromptTokens, "completion_tokens": usage.CompletionTokens, "cached_tokens": usage.CachedTokens, "total_tokens": usage.TotalTokens}
	var result map[string]any
	if v.Mode == "text" {
		result = map[string]any{"choices": []any{map[string]any{"message": map[string]string{"content": strings.TrimSpace(response.Text)}}}, "usage": display}
	} else {
		parsed, e := parseLegacyStructured(response.Text)
		if e != nil {
			return generateOutput{}, &providerFailure{code: "provider_invalid_json"}
		}
		result = map[string]any{"result": parsed, "usage": display}
	}
	return generateOutput{result: result, usage: usage, accountingTokens: usage.TotalTokens, accountingTokenSet: response.Usage.TotalTokens != nil}, nil
}
