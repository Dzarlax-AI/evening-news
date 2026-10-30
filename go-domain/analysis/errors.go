package analysis

import (
	"errors"
	"net/url"
	"strings"
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

// RetryableTransport reports whether an error happened before a provider response
// and is safe for the runtime's bounded transport retry policy.
func RetryableTransport(err error) bool {
	var analysisErr *AnalysisError
	return errors.As(err, &analysisErr) && analysisErr.Detail.RetryableTransport
}

func validateRequest(r Request) error {
	if (r.Version != 1 && r.Version != 2) || r.MaxTextChars < 1 || r.MaxTextChars > maxGeminiRequest ||
		r.Model == "" || len(r.Model) > 128 || len(r.Endpoint) > 2048 ||
		len(r.URL) > 8192 || !validHTTPURL(r.URL) {
		return errCode(CodeInvalidInput)
	}
	if r.Version == 1 && len(r.Locales) != 0 {
		return errCode(CodeInvalidInput)
	}
	seen := map[string]bool{}
	for _, locale := range r.Locales {
		if (locale != "ru" && locale != "sr-Latn") || seen[locale] {
			return errCode(CodeInvalidInput)
		}
		seen[locale] = true
	}
	if r.Version == 2 && (!seen["sr-Latn"] || len(seen) > 2) {
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
	if err == nil {
		return (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil
	}
	var escapeErr url.EscapeError
	if !errors.As(err, &escapeErr) {
		return false
	}
	return validHTTPURLWithPathOrFragmentEscape(raw)
}

func isValidHTTPAuthority(u *url.URL) bool {
	return (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil
}

func validHTTPURLWithPathOrFragmentEscape(raw string) bool {
	schemeEnd := strings.Index(raw, "://")
	if schemeEnd <= 0 {
		return false
	}
	rest := raw[schemeEnd+3:]
	authorityEnd := strings.IndexAny(rest, "/?#")
	if authorityEnd < 0 {
		authorityEnd = len(rest)
	}
	if authorityEnd == 0 {
		return false
	}
	authority := rest[:authorityEnd]
	parsedAuthority, err := url.Parse(raw[:schemeEnd+3] + authority)
	if err != nil || !isValidHTTPAuthority(parsedAuthority) {
		return false
	}
	return true
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
