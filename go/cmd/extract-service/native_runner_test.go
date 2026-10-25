package main

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
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
	}{{errors.New("CDP disconnected"), "native_browser_unavailable", true}, {context.DeadlineExceeded, "native_browser_timeout", true}, {transport.ErrNativeAdmissionDeadline, "native_browser_busy", true}, {context.Canceled, "cancelled", false}, {transport.ErrNativeTargetCrashed, "native_browser_target_crashed", false}, {errors.Join(transport.ErrNativeTargetCrashed, transport.ErrNativeOwnershipLost), "native_browser_unavailable", false}, {errors.Join(transport.ErrNativeOwnershipLost, &transport.PublicHTTPStatusError{StatusCode: 503}), "native_browser_unavailable", false}, {&transport.PublicHTTPStatusError{StatusCode: 403}, "native_browser_http_rejected", false}, {&transport.PublicHTTPStatusError{StatusCode: 429, RetryAfterSeconds: &retry}, "upstream_rate_limited", true}} {
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
		return transport.RenderedDocument{}, &transport.NativeResourceLimitError{Reason: "wire_bytes", Cause: extractor.ErrWireLimit}
	})
	if resp.Error == nil || resp.Error.Code != "resource_limit" || resp.Error.Retryable || resp.Error.Reason != "wire_bytes" || resp.Result.State != "resource_limit" {
		t.Fatalf("wire cap lost: %+v", resp)
	}
}

func TestUnknownNativeResourceFailureDoesNotTriggerFallback(t *testing.T) {
	fallbackCalls := 0
	resp := nativeArticleResponse(context.Background(), service.Request{URL: "https://example.org/story"}, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
		return transport.RenderedDocument{}, extractor.ErrWireLimit
	}, func(context.Context, string, transport.Options) (transport.Document, error) {
		fallbackCalls++
		return transport.Document{}, nil
	})
	if fallbackCalls != 0 || resp.Error == nil || resp.Error.Code != "resource_limit" || resp.Error.Reason != "resource_limit_unknown" {
		t.Fatalf("unclassified resource failure was recovered or misreported: calls=%d response=%+v", fallbackCalls, resp)
	}
}

func TestNativeResourceFailureUsesOneBoundedReadabilityFallback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fallbackCalls := 0
	r := service.Request{URL: "https://example.org/story", MaxBytes: 4096, MaxChars: 3000}
	resp := nativeArticleResponse(ctx, r, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
		return transport.RenderedDocument{}, &transport.NativeResourceLimitError{Reason: "wire_bytes", Cause: extractor.ErrWireLimit}
	}, func(ctx context.Context, raw string, opts transport.Options) (transport.Document, error) {
		fallbackCalls++
		if raw != r.URL || opts.MaxWireBytes != int64(r.MaxBytes) || opts.MaxDecodedBytes != int64(r.MaxBytes) || opts.Timeout <= 0 || opts.Timeout > 5*time.Second {
			t.Fatalf("fallback bounds or target changed: url=%s opts=%+v", raw, opts)
		}
		return knownGoodFallbackDocument(), nil
	})
	if fallbackCalls != 1 || resp.Outcome != "success" || !strings.Contains(strings.Join(resp.Result.Trace, ","), "native_resource_recovered:wire_bytes") || !strings.Contains(resp.Result.Content, "regional water measurements") {
		t.Fatalf("fallback recovery failed: calls=%d response=%+v", fallbackCalls, resp)
	}
}

func TestNativeFallbackProvenanceAllowsMultipleInlineBookmarks(t *testing.T) {
	prose := "Researchers published a detailed account of the regional water survey and described how samples were collected at independent stations. They compared measurements across several seasons, explained the limitations of the equipment, and released the calibration procedure with reproducible data. The report also outlines how neighboring laboratories can repeat the analysis and compare results with earlier observations."
	html := `<html><body><article><h1>Water survey results</h1><div class="real-body"><p>` + prose + `</p><p>` + prose + `</p><figure class="kg-bookmark-card"><a href="https://example.org/related-one"><div class="kg-bookmark-title">Previous water measurements</div><div class="kg-bookmark-description">A related research summary with useful background.</div><div class="kg-bookmark-metadata">Publisher details</div></a></figure><figure class="kg-bookmark-card"><a href="https://example.org/related-two"><div class="kg-bookmark-title">Seasonal data report</div><div class="kg-bookmark-description">Another related report with its sample notes.</div><div class="kg-bookmark-metadata">More publisher details</div></a></figure></div></article></body></html>`
	resp := nativeArticleResponse(context.Background(), service.Request{URL: "https://example.org/story", MaxBytes: 4096, MaxChars: 3000}, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
		return transport.RenderedDocument{}, &transport.NativeResourceLimitError{Reason: "wire_bytes", Cause: extractor.ErrWireLimit}
	}, func(context.Context, string, transport.Options) (transport.Document, error) {
		return transport.Document{HTML: []byte(html), URL: "https://example.org/story", StatusCode: 200}, nil
	})
	if resp.Outcome != "success" || !strings.Contains(resp.Result.Content, "regional water survey") {
		t.Fatalf("multiple inline bookmark excerpts invalidated main article provenance: %+v", resp)
	}
}

func TestNativeFallbackProvenanceAllowsPopularArticleCitationSections(t *testing.T) {
	first := "Researchers compared measurements from independent stations and explained the calibration method used during the regional survey. The report documents seasonal variation, equipment limitations, and the checks used to confirm the final results. A linked citation provides the original dataset for readers who want to inspect the measurements."
	second := "The authors also describe how neighboring laboratories can repeat the analysis and compare new samples with earlier observations. Their conclusions distinguish measured changes from seasonal effects, and the appendices explain how the team handled missing records and uncertainty. The review includes a second citation to its source material."
	html := `<html><body><article class="popular-article"><h1>Regional survey findings</h1><div class="popular-section"><h2>Measurement method</h2><p>` + first + ` <a href="https://example.org/citation-one">[1]</a></p></div><div class="popular-section"><h2>Comparison and limits</h2><p>` + second + ` <a href="https://example.org/citation-two">[2]</a></p></div></article></body></html>`
	resp := nativeArticleResponse(context.Background(), service.Request{URL: "https://example.org/story", MaxBytes: 4096, MaxChars: 3000}, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
		return transport.RenderedDocument{}, &transport.NativeResourceLimitError{Reason: "wire_bytes", Cause: extractor.ErrWireLimit}
	}, func(context.Context, string, transport.Options) (transport.Document, error) {
		return transport.Document{HTML: []byte(html), URL: "https://example.org/story", StatusCode: 200}, nil
	})
	if resp.Outcome != "success" || !strings.Contains(resp.Result.Content, "regional survey") || !strings.Contains(resp.Result.Content, "neighboring laboratories") {
		t.Fatalf("citation links invalidated fallback article provenance: %+v", resp)
	}
}

func TestNativeFallbackDoesNotMaskUnsafeOrNonResourceFailures(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"ownership", errors.Join(&transport.NativeResourceLimitError{Reason: "wire_bytes", Cause: extractor.ErrWireLimit}, transport.ErrNativeOwnershipLost)},
		{"policy", errors.Join(&transport.NativeResourceLimitError{Reason: "wire_bytes", Cause: extractor.ErrWireLimit}, transport.ErrNativePolicy)},
		{"auth", errors.Join(&transport.NativeResourceLimitError{Reason: "wire_bytes", Cause: extractor.ErrWireLimit}, &transport.PublicHTTPStatusError{StatusCode: 401})},
		{"challenge", errors.Join(&transport.NativeResourceLimitError{Reason: "dom_output", Cause: extractor.ErrOutputLimit}, &transport.PublicChallengeError{})},
		{"crash", errors.Join(&transport.NativeResourceLimitError{Reason: "dom_output", Cause: extractor.ErrOutputLimit}, transport.ErrNativeTargetCrashed)},
		{"not_ready", errors.Join(&transport.NativeResourceLimitError{Reason: "wire_bytes", Cause: extractor.ErrWireLimit}, transport.ErrNativeNotReady)},
		{"cancelled", errors.Join(&transport.NativeResourceLimitError{Reason: "dom_output", Cause: extractor.ErrOutputLimit}, context.Canceled)},
		{"timeout", errors.Join(&transport.NativeResourceLimitError{Reason: "wire_bytes", Cause: extractor.ErrWireLimit}, context.DeadlineExceeded)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fallbackCalls := 0
			resp := nativeArticleResponse(context.Background(), service.Request{URL: "https://example.org/story"}, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
				return transport.RenderedDocument{}, tc.err
			}, func(context.Context, string, transport.Options) (transport.Document, error) {
				fallbackCalls++
				return transport.Document{}, nil
			})
			if fallbackCalls != 0 || resp.Outcome == "success" {
				t.Fatalf("unsafe fallback was attempted: calls=%d response=%+v", fallbackCalls, resp)
			}
		})
	}
}

func TestNativeResourceReasonsAreFiniteAndPreserveLimitSentinels(t *testing.T) {
	for _, reason := range []string{"wire_bytes", "dom_output", "frame_count", "auth_event_count", "request_count", "event_rpc_admission"} {
		cause := error(extractor.ErrWireLimit)
		if reason == "dom_output" {
			cause = extractor.ErrOutputLimit
		}
		err := &transport.NativeResourceLimitError{Reason: reason, Cause: cause}
		if got := transport.NativeResourceLimitReason(err); got != reason || !errors.Is(err, cause) {
			t.Fatalf("resource reason/sentinel changed for %s: reason=%q err=%v", reason, got, err)
		}
	}
	if got := nativeResourceReason(extractor.ErrWireLimit); got != "resource_limit_unknown" {
		t.Fatalf("untyped wire limit invented a specific cap: %q", got)
	}
}

func knownGoodFallbackDocument() transport.Document {
	prose := "Researchers published a detailed report about regional water measurements gathered at independent stations. The team compared readings across several seasons, explained the equipment limits, and released its calibration method with reproducible sample data. The report documents how neighboring laboratories can repeat the analysis and compare results with earlier observations."
	html := `<html><body><article><h1>Regional water report</h1><p>` + prose + `</p><p>` + prose + `</p></article></body></html>`
	return transport.Document{HTML: []byte(html), URL: "https://example.org/story", StatusCode: 200}
}

func TestNativeFallbackRejectsHTTPErrorAndUnusableBodies(t *testing.T) {
	good := knownGoodFallbackDocument()
	challenge := good
	challenge.HTML = append([]byte(`<div id="challenge-form"></div>`), challenge.HTML...)
	privateRedirect := good
	privateRedirect.URL = "http://127.0.0.1/private"
	httpStatus := good
	httpStatus.StatusCode = 403
	tooLarge := good
	tooLarge.HTML = append(tooLarge.HTML, bytes.Repeat([]byte("x"), 400)...)
	short := transport.Document{HTML: []byte(`<html><body><article><h1>Regional water report</h1><p>A short teaser only.</p></article></body></html>`), URL: good.URL, StatusCode: 200}
	markedCard := transport.Document{HTML: []byte(`<html><body><article class="related-article"><a href="/other"><h2>Related article</h2><p>Researchers published a separate analysis of regional transit changes. The report compares service schedules across several cities and explains how the updated routes affect local commuters. Reviewers collected additional comments from riders during the past month.</p><p>The team will publish a follow-up with further detail about the survey, its sampling limits, and the next stages of the project. The linked story describes those results and includes background on the organization that prepared the report.</p></a></article></body></html>`), URL: good.URL, StatusCode: 200}
	unmarkedCard := transport.Document{HTML: []byte(`<html><body><article><h2><a href="/other">Other headline</a></h2><p>Researchers published a separate analysis of regional transit changes. The report compares service schedules across several cities and explains how the updated routes affect local commuters. Reviewers collected additional comments from riders during the past month.</p><p>The team will publish a follow-up with further detail about the survey, its sampling limits, and the next stages of the project. The linked story describes those results and includes background on the organization that prepared the report.</p></article></body></html>`), URL: good.URL, StatusCode: 200}
	listing := transport.Document{HTML: []byte(`<html><body><main><h1>Latest stories</h1><article><h2><a href="/one">Transit report</a></h2><p>Researchers published a separate analysis of regional transit changes. The report compares service schedules across several cities and explains how the updated routes affect local commuters. Reviewers collected additional comments from riders during the past month.</p><p>The team will publish a follow-up with further detail about the survey, its sampling limits, and the next stages of the project. The linked story describes those results and includes background on the organization that prepared the report.</p></article><article><h2><a href="/two">Transit update</a></h2><p>Researchers published a separate analysis of regional transit changes. The report compares service schedules across several cities and explains how the updated routes affect local commuters. Reviewers collected additional comments from riders during the past month.</p><p>The team will publish a follow-up with further detail about the survey, its sampling limits, and the next stages of the project. The linked story describes those results and includes background on the organization that prepared the report.</p></article></main></body></html>`), URL: good.URL, StatusCode: 200}
	mainWithCard := transport.Document{HTML: []byte(`<html><body><main><h1>Page title</h1><article class="related-article"><h2><a href="/other">Other headline</a></h2><p>Researchers published a separate analysis of regional transit changes. The report compares service schedules across several cities and explains how the updated routes affect local commuters. Reviewers collected additional comments from riders during the past month.</p><p>The team will publish a follow-up with further detail about the survey, its sampling limits, and the next stages of the project. The linked story describes those results and includes background on the organization that prepared the report.</p></article></main></body></html>`), URL: good.URL, StatusCode: 200}
	for _, tc := range []struct {
		name    string
		doc     transport.Document
		err     error
		request service.Request
	}{
		{name: "http_error", doc: good, err: &transport.PublicHTTPStatusError{StatusCode: 403}},
		{name: "http_status", doc: httpStatus},
		{name: "challenge", doc: challenge},
		{name: "private_redirect", doc: privateRedirect},
		{name: "teaser_only", doc: short},
		{name: "oversized_html", doc: tooLarge, request: service.Request{MaxBytes: 1024}},
		{name: "max_chars", doc: good, request: service.Request{MaxChars: 100}},
		{name: "single_recommendation_card", doc: markedCard},
		{name: "unmarked_linked_teaser", doc: unmarkedCard},
		{name: "main_with_single_marked_card", doc: mainWithCard},
		{name: "repeated_linked_listing", doc: listing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fallbackCalls := 0
			r := tc.request
			r.URL = good.URL
			resp := nativeArticleResponse(context.Background(), r, func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
				return transport.RenderedDocument{}, &transport.NativeResourceLimitError{Reason: "dom_output", Cause: extractor.ErrOutputLimit}
			}, func(context.Context, string, transport.Options) (transport.Document, error) {
				fallbackCalls++
				return tc.doc, tc.err
			})
			if fallbackCalls != 1 || resp.Outcome == "success" || resp.Error == nil || resp.Error.Code != "resource_limit" || resp.Result.State != "resource_limit" {
				t.Fatalf("unusable fallback changed resource failure: calls=%d response=%+v", fallbackCalls, resp)
			}
		})
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
