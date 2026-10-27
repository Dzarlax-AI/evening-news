package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
)

const maxRequest = 10 << 20

func main() {
	response := run(os.Stdin)
	_ = json.NewEncoder(os.Stdout).Encode(response)
	if !response.OK {
		os.Exit(1)
	}
}

func run(input io.Reader) editorial.Response {
	data, err := io.ReadAll(io.LimitReader(input, maxRequest+1))
	if err != nil {
		return failure("invalid_request")
	}
	if len(data) > maxRequest {
		return failure("request_too_large")
	}
	var req editorial.Request
	dec := json.NewDecoder(strings.NewReader(string(data)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil || ensureEOF(dec) != nil {
		return failure("invalid_request")
	}
	if req.Locale != editorial.RussianLocale {
		return failure("unsupported_locale")
	}
	switch req.Operation {
	case "generate":
		ctx, cancel := context.WithTimeout(context.Background(), 65*time.Second)
		defer cancel()
		generation, err := editorial.Generate(ctx, req.Snapshot, req.Locale, editorial.ProviderConfig{APIKey: os.Getenv("GEMINI_API_KEY"), Model: os.Getenv("GEMINI_MODEL"), Endpoint: os.Getenv("GEMINI_ENDPOINT")})
		if err != nil {
			return failure(codeFor(err))
		}
		rendered, err := editorial.Render(req.Snapshot, req.Locale, generation.Edition)
		if err != nil {
			return failure(codeFor(err))
		}
		return editorial.Response{OK: true, Edition: &generation.Edition, Rendered: &rendered, ProviderUsage: &generation.Usage, ResultHash: generation.Edition.ContentHash}
	case "validate":
		if req.Edition == nil {
			return failure("invalid_request")
		}
		if err := editorial.ValidateEdition(req.Snapshot, req.Locale, *req.Edition); err != nil {
			return failure(codeFor(err))
		}
		return editorial.Response{OK: true, Edition: req.Edition, ResultHash: req.Edition.ContentHash}
	case "render":
		if req.Edition == nil {
			return failure("invalid_request")
		}
		rendered, err := editorial.Render(req.Snapshot, req.Locale, *req.Edition)
		if err != nil {
			return failure(codeFor(err))
		}
		return editorial.Response{OK: true, Edition: req.Edition, Rendered: &rendered, ResultHash: req.Edition.ContentHash}
	default:
		return failure("unsupported_operation")
	}
}

func failure(code string) editorial.Response { return editorial.Response{OK: false, ErrorCode: code} }

func codeFor(err error) string {
	if err == nil {
		return "unknown_error"
	}
	code := err.Error()
	switch code {
	case "provider_not_configured", "provider_invalid_configuration", "provider_request_too_large", "provider_cancelled", "provider_timeout", "provider_transport_error", "provider_response_read_error", "provider_response_too_large", "provider_invalid_response", "provider_empty_response", "provider_incomplete_response", "provider_invalid_editorial_json", "provider_invalid_editorial_edition", "provider_rejected_credentials", "provider_rate_limited", "provider_unavailable", "provider_rejected_request":
		return code
	case "render_story_too_large":
		return code
	}
	if errors.Is(err, editorial.ErrInvalidInput) {
		return "invalid_snapshot"
	}
	if errors.Is(err, editorial.ErrInvalidEdition) {
		return "invalid_edition"
	}
	return "invalid_operation_result"
}

func ensureEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err != io.EOF {
		return errors.New("trailing data")
	}
	return nil
}
