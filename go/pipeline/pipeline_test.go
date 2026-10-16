package pipeline

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

const body = "The river research team measured water quality across several districts this spring. Their instruments recorded changes after rainfall and compared samples from nearby farms. Local authorities published the findings with a detailed explanation of the measurement process. Residents can now review the evidence and understand how upstream activity affects their drinking water."

func doc(html string) transport.Document {
	return transport.Document{HTML: []byte(html), StatusCode: 200}
}

func TestRuntimeOrderingAndLearnedObservation(t *testing.T) {
	var calls []string
	r := Runner{Fetch: func(ctx context.Context, u string, o transport.Options) (transport.Document, error) {
		calls = append(calls, "http")
		return doc(`<html><body><div id="story">` + body + `</div></body></html>`), nil
	}, Render: func(context.Context, string, string, transport.BrowserOptions) (transport.RenderedDocument, error) {
		t.Fatal("unexpected browser")
		return transport.RenderedDocument{}, nil
	}}
	got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/story", LearnedSelector: "#story", LearnedMethod: "enhanced_selectors"}})
	if got.Result.State != "extracted" || !strings.HasPrefix(got.Result.Method, "learned_pattern_") || len(calls) != 1 || len(got.Observations) == 0 || !got.Observations[0].Success {
		t.Fatalf("unexpected outcome: %+v calls=%v", got, calls)
	}
}

func TestBrowserPrecedesMetadataAndSharesHTTP(t *testing.T) {
	var calls []string
	r := Runner{Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
		calls = append(calls, "http")
		return doc(`<html><head><meta name="description" content="` + body + `"></head><body>Loading</body></html>`), nil
	}, Render: func(context.Context, string, string, transport.BrowserOptions) (transport.RenderedDocument, error) {
		calls = append(calls, "browser")
		return transport.RenderedDocument{Document: doc(`<html><body><article>` + body + `</article></body></html>`)}, nil
	}}
	got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/story"}, ChromeEndpoint: "http://configured:9222"})
	if got.Result.State != "extracted" || got.Result.Method != "browser_rendering" || strings.Join(calls, ",") != "http,browser" {
		t.Fatalf("outcome=%+v calls=%v", got, calls)
	}
}

func TestAlternateOnlyWhenInitialUnavailable(t *testing.T) {
	var profiles []bool
	r := Runner{Fetch: func(_ context.Context, _ string, o transport.Options) (transport.Document, error) {
		profiles = append(profiles, o.AlternateHeaders)
		if !o.AlternateHeaders {
			return transport.Document{}, errors.New("offline")
		}
		return doc(`<meta name="description" content="` + body + `">`), nil
	}}
	got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/story"}})
	if got.Result.State != "extracted" || len(profiles) != 2 || !profiles[1] {
		t.Fatalf("outcome=%+v calls=%v", got, profiles)
	}
}

func TestAlternativeAdmissionAndIdentity(t *testing.T) {
	var urls []string
	r := Runner{Fetch: func(_ context.Context, u string, _ transport.Options) (transport.Document, error) {
		urls = append(urls, u)
		if strings.HasSuffix(u, "/full") {
			return doc(`<article>` + body + `</article>`), nil
		}
		return doc(`<link rel="canonical" href="http://169.254.169.254/latest"><link rel="amphtml" href="/full"><body>Loading</body>`), nil
	}}
	got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/summary"}})
	if got.Result.State != "extracted" || got.Result.URL != "https://news.example/summary" || got.Result.ExtractedFrom != "https://news.example/full" || len(urls) != 2 {
		t.Fatalf("outcome=%+v urls=%v", got, urls)
	}
}

func TestCancelDoesNotFallbackToSource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	r := Runner{Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
		cancel()
		return transport.Document{}, context.Canceled
	}}
	got := r.Extract(ctx, Request{Input: extractor.Input{URL: "https://news.example/story", Content: body}})
	if got.Result.State == "extracted" || got.NeedsSelectorStudy || got.Result.Content != "" {
		t.Fatalf("cancelled work accepted: %+v", got)
	}
}

func TestSelectorStudyAndForcedHTML(t *testing.T) {
	r := Runner{Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
		t.Fatal("supplied HTML triggered network")
		return transport.Document{}, nil
	}}
	got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/story", HTML: `<meta name="description" content="` + body + `">`, Strategy: "meta_description"}})
	if got.Result.State != "extracted" || got.Result.SelectorUsed != "meta_description" {
		t.Fatalf("forced strategy: %+v", got)
	}
	got = r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/story", HTML: `<body>Loading</body>`}})
	if !got.NeedsSelectorStudy || got.StudyHTML == "" {
		t.Fatalf("missing study signal: %+v", got)
	}
}

func TestRedditRetainsPostWhenLinkedFetchDenied(t *testing.T) {
	var urls []string
	r := Runner{Fetch: func(_ context.Context, u string, _ transport.Options) (transport.Document, error) {
		urls = append(urls, u)
		return doc(`[{"data":{"children":[{"data":{"title":"River report","author":"reporter","is_self":false,"url":"http://127.0.0.1/private"}}]}}]`), nil
	}}
	got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://www.reddit.com/r/science/comments/123/story"}})
	if got.Result.State != "extracted" || got.Result.Author != "reporter" || len(urls) != 1 || !strings.Contains(urls[0], ".json?") {
		t.Fatalf("outcome=%+v urls=%v", got, urls)
	}
}

func TestTransportFailuresAreClassified(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want string
	}{{transport.ErrUnsafePublicURL, "public_url_denied"}, {&transport.PublicHTTPStatusError{StatusCode: 429}, "upstream_rate_limited"}, {&transport.PublicHTTPStatusError{StatusCode: 503}, "upstream_unavailable"}, {errors.New("dial failed with secret url"), "transport_failed"}} {
		r := Runner{Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{}, tc.err
		}}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/story"}})
		if got.Result.Error != tc.want || got.NeedsSelectorStudy {
			t.Errorf("%v: %+v", tc.err, got)
		}
	}
}

func TestRedirectProvenanceAndRelativeMedia(t *testing.T) {
	markup := `<html><head><title>Redirected report</title><meta property="article:published_time" content="2026-10-14T10:00:00Z"></head><body><article>` + body + `<img src="photo.jpg"></article></body></html>`
	for _, browser := range []bool{false, true} {
		r := Runner{Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			if browser {
				return doc(`<body>Loading</body>`), nil
			}
			d := doc(markup)
			d.URL = "https://publisher.example/reports/final"
			return d, nil
		}, Render: func(context.Context, string, string, transport.BrowserOptions) (transport.RenderedDocument, error) {
			d := doc(markup)
			d.URL = "https://publisher.example/reports/final"
			return transport.RenderedDocument{Document: d}, nil
		}}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://short.example/start"}, ChromeEndpoint: "http://configured:9222"})
		if got.Result.URL != "https://short.example/start" || got.Result.ExtractedFrom != "https://publisher.example/reports/final" || got.Result.Title != "Redirected report" || got.Result.PublicationDate == "" || len(got.Result.Media) != 1 || got.Result.Media[0].URL != "https://publisher.example/reports/photo.jpg" {
			t.Fatalf("browser=%v: %+v", browser, got)
		}
	}
}

func TestRuntimeCompletesMatchingTableBeforeStaticSuccess(t *testing.T) {
	static := `<main><article>` + body + `<table id="measurements"></table></article></main>`
	rendered := strings.Replace(static, `<table id="measurements"></table>`, `<table id="measurements"><tr><th>Station</th><th>Reading</th></tr><tr><td>North station</td><td>42 units</td></tr></table>`, 1)
	r := Runner{Fetch: func(context.Context, string, transport.Options) (transport.Document, error) { return doc(static), nil }, Render: func(context.Context, string, string, transport.BrowserOptions) (transport.RenderedDocument, error) {
		return transport.RenderedDocument{Document: doc(rendered)}, nil
	}}
	got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/report"}, ChromeEndpoint: "http://configured:9222"})
	if got.Result.Method != "browser_rendering" || !strings.Contains(got.Result.Content, "North station 42 units") {
		t.Fatalf("runtime table facts lost: %+v", got)
	}
}

func TestTransportOutageDoesNotDegradeLearnedSelector(t *testing.T) {
	for _, method := range []string{"enhanced_selectors", "browser_rendering"} {
		r := Runner{Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{}, &transport.PublicHTTPStatusError{StatusCode: 429}
		}, Render: func(context.Context, string, string, transport.BrowserOptions) (transport.RenderedDocument, error) {
			return transport.RenderedDocument{}, errors.New("browser_busy")
		}}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/report", LearnedMethod: method, LearnedSelector: "#learned"}, ChromeEndpoint: "http://configured:9222"})
		if len(got.Observations) != 0 {
			t.Fatalf("outage degraded %s learned selector: %+v", method, got)
		}
	}
}

func TestRedditLinkedRedirectPreservesPostAndResolvesMedia(t *testing.T) {
	r := Runner{Fetch: func(_ context.Context, u string, _ transport.Options) (transport.Document, error) {
		if strings.Contains(u, "reddit.com") {
			return doc(`[{"data":{"children":[{"data":{"title":"Original Reddit title","author":"reddit_author","created_utc":1791972000,"is_self":false,"url":"https://short.example/linked"}}]}}]`), nil
		}
		d := doc(`<html><head><title>Publisher title</title><meta name="author" content="publisher_author"></head><body><article>` + body + `<img src="photo.jpg"></article></body></html>`)
		d.URL = "https://publisher.example/reports/final"
		return d, nil
	}}
	got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://www.reddit.com/r/science/comments/123/report"}})
	if got.Result.URL != "https://www.reddit.com/r/science/comments/123/report" || got.Result.ExtractedFrom != "https://publisher.example/reports/final" || got.Result.Title != "Original Reddit title" || got.Result.Author != "reddit_author" || got.Result.PublicationDate == "" || len(got.Result.Media) != 1 || got.Result.Media[0].URL != "https://publisher.example/reports/photo.jpg" {
		t.Fatalf("Reddit linked redirect lost provenance or post metadata: %+v", got)
	}
}

func TestHTTPOnlyTerminalHTTPStatusDoesNotTryOtherMethods(t *testing.T) {
	for _, tc := range []struct {
		status int
		code   string
	}{{403, "upstream_forbidden"}, {429, "upstream_rate_limited"}, {401, "upstream_auth_required"}, {407, "upstream_auth_required"}, {503, "upstream_unavailable"}} {
		calls := 0
		retry := 30
		r := Runner{HTTPOnly: true, Admit: func(context.Context, string) error { return nil }, Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			calls++
			return transport.Document{}, &transport.PublicHTTPStatusError{StatusCode: tc.status, RetryAfterSeconds: &retry}
		}, Render: func(context.Context, string, string, transport.BrowserOptions) (transport.RenderedDocument, error) {
			t.Fatal("HTTP-only attempted browser")
			return transport.RenderedDocument{}, nil
		}}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/report", Content: body, LearnedSelector: "#learned"}, ChromeEndpoint: "http://must-not-connect:9222"})
		if got.Result.Error != tc.code || got.UpstreamStatus != tc.status || got.RetryAfterSeconds == nil || *got.RetryAfterSeconds != 30 || calls != 1 || got.NeedsSelectorStudy || len(got.Observations) != 0 {
			t.Fatalf("status %d continued graph or lost contract: calls=%d %+v", tc.status, calls, got)
		}
	}
}

func TestHTTPOnlyLearnedBrowserReturnsNativeRequirementWithoutNetwork(t *testing.T) {
	for _, strategy := range []string{"", "browser_rendering", "browser_heuristics"} {
		r := Runner{HTTPOnly: true, Admit: func(context.Context, string) error { return nil }, Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			t.Fatal("browser strategy attempted HTTP")
			return transport.Document{}, nil
		}, Render: func(context.Context, string, string, transport.BrowserOptions) (transport.RenderedDocument, error) {
			t.Fatal("HTTP-only attempted browser")
			return transport.RenderedDocument{}, nil
		}}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/report", HTML: `<article>` + body + `</article>`, LearnedSelector: "article", LearnedMethod: "browser_rendering", Strategy: strategy}})
		if got.Result.Error != "browser_required" || len(got.Observations) != 0 {
			t.Fatalf("learned browser replaced by static content: %+v", got)
		}
	}
}

func TestHTTPOnlyChallengeAndIncompleteTableRequireNativeBrowser(t *testing.T) {
	for _, html := range []string{`<html><form id="challenge-form"></form></html>`, `<main><article>` + body + `<table id="measurements"></table></article></main>`} {
		calls := 0
		r := Runner{HTTPOnly: true, Admit: func(context.Context, string) error { return nil }, Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			calls++
			return doc(html), nil
		}}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/report", Content: body}})
		if got.Result.Error != "browser_required" || got.NeedsSelectorStudy || calls != 1 {
			t.Fatalf("HTTP incomplete article consumed as success: %+v calls=%d", got, calls)
		}
	}
}

func TestHTTPOnlyPolicyResourceAndCancelNeverBecomeNativeRequirement(t *testing.T) {
	for _, err := range []error{transport.ErrUnsafePublicURL, extractor.ErrOutputLimit, context.Canceled, context.DeadlineExceeded} {
		calls := 0
		r := Runner{HTTPOnly: true, Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			calls++
			return transport.Document{}, err
		}}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/report", Content: body}})
		if got.Result.State == "extracted" || got.Result.Error == "browser_required" || got.NeedsSelectorStudy || calls != 1 {
			t.Fatalf("terminal error %v bypassed: %+v calls=%d", err, got, calls)
		}
	}
}

func TestHTTPOnlyGoodStaticAndMissingContentStayDistinct(t *testing.T) {
	for _, tc := range []struct {
		html, state string
		study       bool
	}{{`<article>` + body + `</article>`, "extracted", false}, {`<body>Loading</body>`, "failed", true}} {
		r := Runner{HTTPOnly: true, Fetch: func(context.Context, string, transport.Options) (transport.Document, error) { return doc(tc.html), nil }, Render: func(context.Context, string, string, transport.BrowserOptions) (transport.RenderedDocument, error) {
			t.Fatal("HTTP-only attempted browser")
			return transport.RenderedDocument{}, nil
		}}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/report", Content: body}})
		if got.Result.State != tc.state || got.NeedsSelectorStudy != tc.study {
			t.Fatalf("HTTP-only source fallback hid result: %+v", got)
		}
	}
}

func TestRedditLinkedTerminalFailureRetainsRetryContract(t *testing.T) {
	retry := 45
	for _, tc := range []struct {
		err    error
		code   string
		status int
	}{{&transport.PublicHTTPStatusError{StatusCode: 429, RetryAfterSeconds: &retry}, "upstream_rate_limited", 429}, {&transport.PublicHTTPStatusError{StatusCode: 401}, "upstream_auth_required", 401}, {extractor.ErrOutputLimit, "resource_limit", 0}} {
		calls := 0
		r := Runner{HTTPOnly: true, Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			calls++
			if calls == 1 {
				return doc(`[{"data":{"children":[{"data":{"title":"Valid Reddit post title","author":"reporter","is_self":false,"url":"https://publisher.example/report"}}]}}]`), nil
			}
			return transport.Document{}, tc.err
		}}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://www.reddit.com/r/science/comments/123/report"}})
		if got.Result.Error != tc.code || got.UpstreamStatus != tc.status || got.Result.State == "extracted" || calls != 2 {
			t.Fatalf("linked failure swallowed by Reddit text: %+v calls=%d", got, calls)
		}
		if tc.status == 429 && (got.RetryAfterSeconds == nil || *got.RetryAfterSeconds != 45) {
			t.Fatalf("linked rate-limit delay lost: %+v", got)
		}
	}
}

func TestBrowserRequirementCannotBypassDNSAdmission(t *testing.T) {
	for _, strategy := range []string{"browser_rendering", "auto"} {
		admissions := 0
		r := Runner{HTTPOnly: true, Admit: func(context.Context, string) error { admissions++; return transport.ErrUnsafePublicURL }, Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			t.Fatal("unadmitted browser strategy fetched")
			return transport.Document{}, nil
		}}
		html := ""
		if strategy == "auto" {
			html = `<form id="challenge-form"></form>`
		}
		got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://private-dns.example/report", Strategy: strategy, HTML: html}})
		if got.Result.Error != "public_url_denied" || admissions != 1 || got.NeedsSelectorStudy {
			t.Fatalf("private DNS delegated to native: %+v admissions=%d", got, admissions)
		}
	}
}

func TestUnavailableBrowserAdmissionIsNotPolicyDenial(t *testing.T) {
	r := Runner{HTTPOnly: true, Admit: func(context.Context, string) error { return transport.ErrPublicAdmissionUnavailable }}
	got := r.Extract(context.Background(), Request{Input: extractor.Input{URL: "https://news.example/report", LearnedMethod: "browser_rendering"}})
	if got.Result.Error != "admission_unavailable" || got.NeedsSelectorStudy || len(got.Observations) != 0 {
		t.Fatalf("DNS outage delegated or made permanent: %+v", got)
	}
}
