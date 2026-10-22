package main

import (
	"context"
	"errors"
	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go/service"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

func TestNativeRunnerRejectsUnsupportedBeforeNavigation(t *testing.T) {
	for _, r := range []service.Request{{URL: "http://example.org/story"}, {URL: "https://example.org:8443/story"}, {URL: "https://example.org/story", SourceType: "telegram"}, {URL: "https://example.org/story", Strategy: "reddit_json"}, {URL: "https://example.org/story", HTML: "injected"}, {URL: "https://127.0.0.1/story"}} {
		called := false
		resp := nativeArticleResponse(context.Background(), r, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
			called = true
			return transport.RenderedDocument{}, nil
		})
		if called || resp.Error == nil || resp.Outcome == "success" {
			t.Fatalf("unsupported navigation admitted: %+v", resp)
		}
	}
}

func TestNativeRunnerDoesNotTurnErrorsIntoPythonFallbackCodes(t *testing.T) {
	retry := 47
	for _, tc := range []struct {
		err   error
		code  string
		retry bool
	}{{errors.New("CDP disconnected"), "native_browser_unavailable", true}, {context.DeadlineExceeded, "native_browser_timeout", true}, {context.Canceled, "cancelled", false}, {transport.ErrNativeTargetCrashed, "native_browser_target_crashed", false}, {errors.Join(transport.ErrNativeTargetCrashed, transport.ErrNativeOwnershipLost), "native_browser_unavailable", false}, {errors.Join(transport.ErrNativeOwnershipLost, &transport.PublicHTTPStatusError{StatusCode: 503}), "native_browser_unavailable", false}, {&transport.PublicHTTPStatusError{StatusCode: 403}, "native_browser_http_rejected", false}, {&transport.PublicHTTPStatusError{StatusCode: 429, RetryAfterSeconds: &retry}, "upstream_rate_limited", true}} {
		calls := 0
		resp := nativeArticleResponse(context.Background(), service.Request{URL: "https://example.org/story"}, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
			calls++
			return transport.RenderedDocument{}, tc.err
		})
		if calls != 1 || resp.Error == nil || resp.Error.Code != tc.code || resp.Error.Retryable != tc.retry {
			t.Fatalf("failure changed: %+v", resp)
		}
		if tc.code == "native_browser_target_crashed" && (resp.Outcome != "failed" || resp.Result.Content != "") {
			t.Fatalf("owned crash must be one empty failed job: %+v", resp)
		}
		if tc.code == "upstream_rate_limited" && (resp.Error.RetryAfterSeconds == nil || *resp.Error.RetryAfterSeconds != 47) {
			t.Fatal("retry timing lost")
		}
	}
}

func TestNativeWireLimitIsResourceFailure(t *testing.T) {
	resp := nativeArticleResponse(context.Background(), service.Request{URL: "https://example.org/story"}, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
		return transport.RenderedDocument{}, extractor.ErrWireLimit
	})
	if resp.Error == nil || resp.Error.Code != "resource_limit" || resp.Error.Retryable {
		t.Fatalf("wire cap lost: %+v", resp)
	}
}

func TestNativeLearnedObservationUsesActualCandidateAndIdentity(t *testing.T) {
	prose := "Researchers at the Northstar Institute published a detailed account of river measurements gathered from four independent stations. They released the full calibration procedure and reproducible sample data, described the limitations of their equipment, and explained why unusually heavy rainfall requires additional manual review. The report contains sufficient evidence for neighboring laboratories to repeat the experiment and compare the results with historical observations."
	for _, selector := range []string{".missing", ".short"} {
		r := service.Request{URL: "https://example.org/story", Strategy: "learned_pattern_browser_rendering", LearnedSelector: selector, LearnedMethod: "learned_pattern_browser_rendering"}
		resp := nativeArticleResponse(context.Background(), r, func(_ context.Context, _ string, _ transport.BrowserOptions, ready string) (transport.RenderedDocument, error) {
			if ready != "" {
				t.Fatal("stale learned selector became readiness condition")
			}
			return transport.RenderedDocument{Document: transport.Document{URL: r.URL, HTML: []byte(`<html><body><article><p>` + prose + `</p><p>` + prose + `</p><div class="short">` + prose + `</div></article></body></html>`)}}, nil
		})
		if resp.Outcome != "success" || len(resp.Observations) != 1 || resp.Observations[0].Method != r.LearnedMethod || resp.Observations[0].Success != (selector == ".short") {
			t.Fatalf("false learned observation for %s: %+v", selector, resp)
		}
	}
}

func TestNativeRunnerParsesRenderedSourceAndKeepsArticleIdentity(t *testing.T) {
	prose := "Researchers at the Northstar Institute published a detailed account of river measurements gathered from four independent stations. They released the full calibration procedure and reproducible sample data, described the limitations of their equipment, and explained why unusually heavy rainfall requires additional manual review. The report contains sufficient evidence for neighboring laboratories to repeat the experiment and compare the results with historical observations."
	r := service.Request{URL: "https://example.org/story", Strategy: "browser_rendering", MaxBytes: 10000, MaxChars: 10000}
	resp := nativeArticleResponse(context.Background(), r, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
		return transport.RenderedDocument{Document: transport.Document{URL: "https://example.org/final", HTML: []byte(`<html><head><title>River report</title></head><body><article><h1>River report</h1><p>` + prose + `</p></article></body></html>`), StatusCode: 200}}, nil
	})
	if resp.Outcome != "success" || resp.Result.URL != r.URL || resp.Result.ExtractedFrom != "https://example.org/final" || !strings.Contains(resp.Result.Content, "four independent stations") || resp.Result.Title != "River report" {
		t.Fatalf("native result lost source: %+v", resp)
	}
}
