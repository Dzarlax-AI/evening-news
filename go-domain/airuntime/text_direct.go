package airuntime

import (
	"context"
	"errors"
	"time"
)

// GenerateText is the in-process equivalent of the text generation RPC. It
// shares provider admission, retries, circuit breaker and usage accounting.
func (s *Server) GenerateText(ctx context.Context, prompt, domain string) (string, error) {
	return s.generateDirect(ctx, prompt, domain, "combined_analysis", 0.1, 2000)
}

// GenerateSummary preserves the legacy summary prompt options and usage labels.
func (s *Server) GenerateSummary(ctx context.Context, prompt string) (string, error) {
	return s.generateDirect(ctx, prompt, "summary", "summary", 0.2, 1000)
}

func (s *Server) generateDirect(ctx context.Context, prompt, domain, analysisType string, temperature float64, maxTokens int) (string, error) {
	if s.draining.Load() {
		return "", errors.New("runtime_draining")
	}
	if deadline, ok := ctx.Deadline(); !ok || !deadline.After(time.Now()) {
		return "", errors.New("invalid_deadline")
	}
	ctx, cancel := context.WithTimeout(ctx, 98*time.Second)
	stop := make(chan struct{})
	go func() {
		select {
		case <-s.closing:
			cancel()
		case <-ctx.Done():
		case <-stop:
		}
	}()
	defer func() { close(stop); cancel() }()
	if _, err := providerURL(s.config.Endpoint, s.config.Model); err != nil || len(prompt) > maxRPCBody/2 || domain == "" || len(domain) > 8192 {
		return "", errors.New("invalid_input")
	}
	req := generateRequest{Mode: "text", Prompt: prompt, Model: s.config.Model, AnalysisType: analysisType, Domain: domain, Options: generateOptions{Temperature: temperature, MaxTokens: maxTokens}}
	value, err := s.breaker.Do(ctx, func() (any, error) {
		return s.retry(ctx, func(attempt context.Context) (any, error) { return s.callGenerate(attempt, req) })
	})
	if err != nil {
		return "", err
	}
	out := value.(generateOutput)
	s.enqueueUsage(usageRecord{domain: domain, analysisType: req.AnalysisType, usage: out.usage, accountingTokens: out.accountingTokens, accountingTokenSet: out.accountingTokenSet})
	choices, ok := out.result["choices"].([]any)
	if !ok || len(choices) == 0 {
		return "", errors.New("invalid_result")
	}
	choice, ok := choices[0].(map[string]any)
	if !ok {
		return "", errors.New("invalid_result")
	}
	message, ok := choice["message"].(map[string]string)
	if !ok {
		return "", errors.New("invalid_result")
	}
	return message["content"], nil
}
