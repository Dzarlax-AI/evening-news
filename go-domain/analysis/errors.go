package analysis

import (
	"errors"
	"net/url"
)

const (
	CodeInvalidInput            = "invalid_input"
	CodeResourceLimit           = "resource_limit"
	CodeInsufficientContent     = "insufficient_content"
	CodeProviderNotConfigured   = "provider_not_configured"
	CodeProviderConfiguration   = "provider_invalid_configuration"
	CodeProviderRequestTooLarge = "provider_request_too_large"
	CodeProviderTimeout         = "provider_timeout"
	CodeProviderCancelled       = "provider_cancelled"
	CodeProviderTransport       = "provider_transport_error"
	CodeProviderRead            = "provider_response_read_error"
	CodeProviderResponseTooBig  = "provider_response_too_large"
	CodeProviderInvalid         = "provider_invalid_response"
	CodeProviderEmpty           = "provider_empty_response"
	CodeProviderInvalidJSON     = "provider_invalid_json"
	CodeProviderHTTP            = "provider_http_error"
)

type AnalysisError struct {
	Detail Error
}

func (e *AnalysisError) Error() string { return e.Detail.Code }

func errCode(code string) error { return &AnalysisError{Detail: Error{Code: code}} }

func errStatus(code string, status int, retryAfter *float64) error {
	return &AnalysisError{Detail: Error{Code: code, StatusCode: &status, RetryAfterSeconds: retryAfter}}
}

func errTransport(code string, retryable bool) error {
	return &AnalysisError{Detail: Error{Code: code, RetryableTransport: retryable}}
}

func ErrorResponse(err error) Response {
	var analysisErr *AnalysisError
	if errors.As(err, &analysisErr) {
		return Response{Version: 1, OK: false, Error: &analysisErr.Detail}
	}
	return Response{Version: 1, OK: false, Error: &Error{Code: "analysis_failed"}}
}

func validateRequest(r Request) error {
	if r.Version != 1 || r.MaxTextChars < 1 || r.MaxTextChars > maxGeminiRequest ||
		r.Model == "" || len(r.Model) > 128 || len(r.Endpoint) > 2048 ||
		len(r.URL) > 8192 || !validHTTPURL(r.URL) {
		return errCode(CodeInvalidInput)
	}
	if len([]rune(r.Title)) > r.MaxTextChars || len([]rune(r.Content)) > r.MaxTextChars ||
		(r.OriginalContext != nil && len([]rune(*r.OriginalContext)) > r.MaxTextChars) {
		return errCode(CodeResourceLimit)
	}
	if len(r.Categories) > 128 || len(r.LocalCategory) > 256 || len(r.ConfiguredLocalCategory) > 256 {
		return errCode(CodeResourceLimit)
	}
	for _, category := range r.Categories {
		if len(category.Name) > 256 || len(category.DisplayName) > 512 || len(category.Examples) > 5 {
			return errCode(CodeResourceLimit)
		}
		for _, example := range category.Examples {
			if len(example) > 1024 {
				return errCode(CodeResourceLimit)
			}
		}
	}
	return nil
}

func validHTTPURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil
}

func requiredConfig(config ProviderConfig) error {
	if config.Model == "" || config.APIKey == "" {
		return errCode(CodeProviderNotConfigured)
	}
	if _, err := providerEndpoint(config.Endpoint, config.Model); err != nil {
		return errCode(CodeProviderConfiguration)
	}
	return nil
}
