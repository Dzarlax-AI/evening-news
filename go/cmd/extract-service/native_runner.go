package main

import (
	"context"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/service"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

type nativeRender func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error)
type nativeHTTPFetch func(context.Context, string, transport.Options) (transport.Document, error)

// Native requests never enter the HTTP method graph or an implicit retry path.
func nativeArticleResponse(ctx context.Context, r service.Request, render nativeRender, fallbackFetch ...nativeHTTPFetch) service.Response {
	fail := func(code string, retryable bool) service.Response {
		return service.Response{Outcome: "rejected", Result: extractor.Result{URL: r.URL, State: "failed", Error: code}, Error: &service.Failure{Code: code, Retryable: retryable}}
	}
	if r.SourceType != "" && r.SourceType != "article" && r.SourceType != "web" {
		return fail("native_browser_unsupported", false)
	}
	u, err := url.Parse(r.URL)
	if err != nil || u.Scheme != "https" || u.User != nil || !strings.Contains(u.Hostname(), ".") || u.Fragment != "" || (u.Port() != "" && u.Port() != "443") {
		return fail("native_browser_unsupported", false)
	}
	if err = transport.ValidatePublicURL(r.URL); err != nil {
		return fail("public_url_denied", false)
	}
	allowed := map[string]bool{"": true, "auto": true, "pipeline": true, "browser_rendering": true, "browser_heuristics": true, "readability": true, "enhanced_selectors": true, "learned_pattern": true, "learned_pattern_browser_rendering": true}
	if !allowed[r.Strategy] || r.HTML != "" || r.Content != "" {
		return fail("native_browser_unsupported", false)
	}
	// A stale learned selector must not become a mandatory navigation condition.
	// Controlled benchmark readiness is configured on the server, independently.
	doc, err := render(ctx, r.URL, transport.BrowserOptions{MaxDOMBytes: int64(r.MaxBytes), MaxWireBytes: int64(r.MaxBytes)}, "")
	if err != nil {
		if reason := nativeResourceReason(err); reason != "" && len(fallbackFetch) > 0 && fallbackFetch[0] != nil && nativeFallbackAllowed(ctx, err, reason) {
			if recovered := recoverNativeResourceFailure(ctx, r, reason, fallbackFetch[0]); recovered.Outcome == "success" {
				return recovered
			}
		}
		resp := fail("native_browser_unavailable", true)
		ownershipLost := errors.Is(err, transport.ErrNativeOwnershipLost)
		switch {
		case ownershipLost:
			resp = fail("native_browser_unavailable", false)
		case errors.Is(err, transport.ErrNativeTargetCrashed):
			resp = fail("native_browser_target_crashed", false)
			resp.Outcome = "failed"
		case errors.Is(err, transport.ErrNativePolicy):
			resp = fail("native_browser_policy_denied", false)
		case errors.Is(err, transport.ErrNativeUnsupported):
			resp = fail("native_browser_unsupported", false)
		case errors.Is(err, transport.ErrNativeBusy):
			resp = fail("native_browser_busy", true)
		case errors.Is(err, context.Canceled):
			resp = fail("cancelled", false)
		case errors.Is(err, context.DeadlineExceeded):
			resp = fail("native_browser_timeout", true)
		case errors.Is(err, transport.ErrUnsafePublicURL):
			resp = fail("public_url_denied", false)
		case errors.Is(err, extractor.ErrOutputLimit), errors.Is(err, extractor.ErrWireLimit):
			resp = fail("resource_limit", false)
			resp.Result.State = "resource_limit"
			resp.Error.Reason = nativeResourceReason(err)
		}
		var status *transport.PublicHTTPStatusError
		if !ownershipLost && errors.As(err, &status) {
			code := "native_browser_http_rejected"
			retry := status.StatusCode >= 500
			if status.StatusCode == 429 {
				code, retry = "upstream_rate_limited", true
			}
			if status.StatusCode == 401 || status.StatusCode == 407 {
				code, retry = "upstream_auth_required", false
			}
			resp = fail(code, retry)
			resp.Error.UpstreamStatus, resp.Error.RetryAfterSeconds = status.StatusCode, status.RetryAfterSeconds
		}
		return resp
	}
	limit := r.MaxBytes
	if limit == 0 {
		limit = service.MaxBytes
	}
	if len(doc.HTML) > limit {
		resp := fail("resource_limit", false)
		resp.Result.State = "resource_limit"
		resp.Error.Reason = "dom_output"
		return resp
	}
	if transport.IsChallengePage(doc.HTML) {
		return fail("native_browser_challenge", false)
	}
	final := doc.URL
	if final == "" {
		final = r.URL
	}
	if err = transport.ValidatePublicURL(final); err != nil || !strings.HasPrefix(final, "https://") {
		return fail("public_url_denied", false)
	}
	strategy := r.Strategy
	if strategy == "" || strategy == "auto" || strategy == "pipeline" {
		strategy = "browser_rendering"
	}
	if strategy == "learned_pattern" {
		strategy = "learned_pattern_browser_rendering"
	}
	result := extractor.Extract(ctx, extractor.Input{URL: final, HTML: string(doc.HTML), RenderedHTML: string(doc.HTML), Title: r.Title, SourceType: r.SourceType, Strategy: strategy, Selectors: r.Selectors, LearnedSelector: r.LearnedSelector, LearnedMethod: "browser_rendering", MaxBytes: limit, MaxChars: r.MaxChars})
	result.URL = r.URL
	if final != r.URL {
		result.ExtractedFrom = final
	}
	resp := service.Response{Outcome: "success", Result: result, FinalURL: final}
	if result.State == "resource_limit" {
		resp.Outcome, resp.Error = "rejected", &service.Failure{Code: "resource_limit"}
		resp.Error.Reason = extractedResourceReason(result.Error)
	} else if result.State != "extracted" {
		resp.Outcome, resp.Error = "rejected", &service.Failure{Code: "native_browser_no_content"}
	}
	if r.LearnedSelector != "" && (r.Strategy == "learned_pattern" || r.Strategy == "learned_pattern_browser_rendering") {
		learned := extractor.Extract(ctx, extractor.Input{URL: final, HTML: string(doc.HTML), Strategy: "learned_pattern", LearnedSelector: r.LearnedSelector, LearnedMethod: r.LearnedMethod, MaxBytes: limit, MaxChars: r.MaxChars})
		if learned.State != "resource_limit" && ctx.Err() == nil {
			method := r.LearnedMethod
			if method == "" {
				method = "browser_rendering"
			}
			resp.Observations = []service.Observation{{Selector: r.LearnedSelector, Method: method, Success: learned.State == "extracted" && learned.SelectorUsed == r.LearnedSelector && strings.HasPrefix(learned.Method, "learned_pattern_")}}
		}
	}
	return resp
}

func extractedResourceReason(reason string) string {
	switch reason {
	case "extraction_byte_limit":
		return "extracted_byte_limit"
	case "extraction_text_limit":
		return "extracted_text_limit"
	default:
		return "extracted_resource_limit"
	}
}

func nativeResourceReason(err error) string {
	if reason := transport.NativeResourceLimitReason(err); reason != "" {
		return reason
	}
	if errors.Is(err, extractor.ErrOutputLimit) {
		return "resource_limit_unknown"
	}
	if errors.Is(err, extractor.ErrWireLimit) {
		return "resource_limit_unknown"
	}
	return ""
}

func nativeFallbackAllowed(ctx context.Context, err error, reason string) bool {
	switch reason {
	case "wire_bytes", "dom_output", "frame_count", "request_count", "event_rpc_admission":
	default:
		return false
	}
	if ctx.Err() != nil ||
		errors.Is(err, transport.ErrNativeOwnershipLost) || errors.Is(err, transport.ErrNativePolicy) ||
		errors.Is(err, transport.ErrNativeTargetCrashed) || errors.Is(err, transport.ErrNativeNotReady) ||
		errors.Is(err, transport.ErrNativeUnsupported) || errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) || errors.Is(err, transport.ErrUnsafePublicURL) {
		return false
	}
	var status *transport.PublicHTTPStatusError
	var challenge *transport.PublicChallengeError
	return !errors.As(err, &status) && !errors.As(err, &challenge)
}

func recoverNativeResourceFailure(ctx context.Context, r service.Request, reason string, fetch nativeHTTPFetch) service.Response {
	limit := r.MaxBytes
	if limit <= 0 || limit > service.MaxBytes {
		limit = service.MaxBytes
	}
	maxChars := r.MaxChars
	if maxChars <= 0 {
		maxChars = 100000
	}
	if maxChars > 100000 {
		maxChars = 100000
	}
	timeout := 15 * time.Second
	if deadline, ok := ctx.Deadline(); ok {
		timeout = time.Until(deadline)
		if timeout <= 0 {
			return service.Response{}
		}
		if timeout > 30*time.Second {
			timeout = 30 * time.Second
		}
	}
	doc, err := fetch(ctx, r.URL, transport.Options{MaxWireBytes: int64(limit), MaxDecodedBytes: int64(limit), Timeout: timeout})
	if err != nil || ctx.Err() != nil || doc.StatusCode != 200 || len(doc.HTML) > limit || transport.IsChallengePage(doc.HTML) {
		return service.Response{}
	}
	final := doc.URL
	if final == "" {
		final = r.URL
	}
	if err = transport.ValidatePublicURL(final); err != nil || !strings.HasPrefix(final, "https://") {
		return service.Response{}
	}
	result := extractor.Extract(ctx, extractor.Input{URL: final, HTML: string(doc.HTML), Title: r.Title, SourceType: r.SourceType, Strategy: "readability", MaxBytes: limit, MaxChars: maxChars})
	if ctx.Err() != nil || result.State != "extracted" || result.Content == "" || !extractor.HasArticleBodyProvenance(string(doc.HTML), result.Content) {
		return service.Response{}
	}
	result.URL = r.URL
	if final != r.URL {
		result.ExtractedFrom = final
	}
	result.Trace = append(result.Trace, "native_resource_recovered:"+reason)
	return service.Response{Outcome: "success", Result: result, FinalURL: final}
}
