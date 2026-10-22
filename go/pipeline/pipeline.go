// Package pipeline runs the bounded article method graph. Persistence, selector
// study by AI, durable retries, and delivery remain the caller's responsibility.
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/transport"
	"github.com/PuerkitoBio/goquery"
)

type Request struct {
	Input          extractor.Input
	ChromeEndpoint string
}
type Observation struct {
	Selector string `json:"selector"`
	Method   string `json:"method"`
	Success  bool   `json:"success"`
	Reason   string `json:"reason,omitempty"`
}
type Outcome struct {
	Result             extractor.Result `json:"result"`
	Observations       []Observation    `json:"observations"`
	NeedsSelectorStudy bool             `json:"needs_selector_study"`
	// StudyHTML is bounded input for the caller's optional AI round trip. It is
	// returned only on failure and must never be included in logs or metrics.
	StudyHTML         string `json:"study_html,omitempty"`
	UpstreamStatus    int    `json:"upstream_status,omitempty"`
	RetryAfterSeconds *int   `json:"retry_after_seconds,omitempty"`
	Reason            string `json:"reason,omitempty"`
}
type Runner struct {
	// HTTPOnly disables every Go browser path, including learned strategies.
	HTTPOnly bool
	// Admit is a trusted runtime/test dependency, never a wire option.
	Admit  func(context.Context, string) error
	Fetch  func(context.Context, string, transport.Options) (transport.Document, error)
	Render func(context.Context, string, string, transport.BrowserOptions) (transport.RenderedDocument, error)
}

const maxAlternativeURLBytes = 8 * 1024

func failed(raw, reason string) extractor.Result {
	return extractor.Result{URL: raw, State: "failed", Error: reason, Trace: []string{}, Media: []extractor.Media{}}
}

// Extract makes at most one primary and one alternate HTTP request per page,
// one browser attempt per page and two alternative-page attempts. All attempts
// share a 30-second maximum request context, including caller cancellation.
func (r Runner) Extract(ctx context.Context, request Request) Outcome {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out := Outcome{Observations: []Observation{}}
	in := request.Input
	u, ok := extractor.NormalizeArticleURL(in.URL)
	if !ok {
		out.Result = failed(in.URL, "invalid_url")
		return out
	}
	in.URL = u
	admit := r.Admit
	if admit == nil {
		admit = transport.AdmitPublicURL
	}
	finishFailure := func(err error) Outcome {
		var browserChallenge *transport.PublicChallengeError
		if errors.As(err, &browserChallenge) {
			if admissionErr := admit(ctx, u); admissionErr != nil {
				err = admissionErr
			}
		}
		out.Result = failed(u, classifyFailure(err))
		var status *transport.PublicHTTPStatusError
		if errors.As(err, &status) {
			out.UpstreamStatus = status.StatusCode
			out.RetryAfterSeconds = status.RetryAfterSeconds
		}
		var challenge *transport.PublicChallengeError
		if errors.As(err, &challenge) {
			out.UpstreamStatus = 200
			out.Reason = "upstream_challenge"
		}
		if IsTransportLimit(err) {
			out.Result.State = "resource_limit"
		}
		return out
	}
	if err := transport.ValidatePublicURL(u); err != nil {
		out.Result = failed(u, "public_url_denied")
		return out
	}
	allowed := map[string]bool{"": true, "auto": true, "pipeline": true, "source_content": true, "learned_pattern": true, "learned_pattern_browser_rendering": true, "enhanced_selectors": true, "readability": true, "browser_rendering": true, "browser_heuristics": true, "json_ld": true, "open_graph": true, "meta_description": true, "enhanced_heuristics": true, "fallback_extraction": true, "reddit_json": true, "reddit_linked_article": true}
	if !allowed[in.Strategy] {
		out.Result = failed(u, "unsupported_strategy")
		out.Result.State = "unsupported"
		return out
	}
	if in.SourceType != "" && in.SourceType != "article" && in.SourceType != "web" {
		out.Result = failed(u, "unsupported_source_type")
		out.Result.State = "unsupported"
		return out
	}
	if in.Limits != nil {
		in.MaxBytes = in.Limits.MaxBytes
		in.MaxChars = in.Limits.MaxChars
	}
	if in.MaxBytes == 0 {
		in.MaxBytes = transport.MaxPageBytes
	}
	if in.MaxChars == 0 {
		in.MaxChars = 100000
	}
	if in.MaxBytes < 1 || in.MaxBytes > transport.MaxPageBytes || in.MaxChars < 1 || in.MaxChars > 100000 {
		out.Result = failed(u, "invalid_limits")
		return out
	}
	for _, s := range []string{in.HTML, in.RenderedHTML, in.RedditJSON, in.LinkedHTML, in.Content} {
		if len(s) > in.MaxBytes {
			out.Result = failed(u, "extraction_byte_limit")
			out.Result.State = "resource_limit"
			return out
		}
	}
	browserDisabled := r.HTTPOnly || (in.UseBrowser != nil && !*in.UseBrowser)
	if browserDisabled && (strings.Contains(in.Strategy, "browser") || in.LearnedMethod == "browser_rendering" || in.LearnedMethod == "learned_pattern_browser_rendering") {
		if err := admit(ctx, u); err != nil {
			return finishFailure(err)
		}
		out.Result = failed(u, "browser_required")
		out.Reason = "learned_or_explicit_browser_strategy"
		return out
	}
	if transport.IsChallengePage([]byte(in.HTML)) {
		return finishFailure(&transport.PublicChallengeError{})
	}
	if in.Strategy == "source_content" || (in.HTML != "" && in.Strategy != "" && in.Strategy != "auto" && in.Strategy != "pipeline" && in.Strategy != "browser_rendering" && in.Strategy != "browser_heuristics" && in.Strategy != "learned_pattern_browser_rendering" && in.Strategy != "learned_pattern") {
		out.Result = extractor.Extract(ctx, in)
		return out
	}
	if r.Fetch == nil {
		r.Fetch = transport.FetchPublic
	}
	if r.Render == nil {
		r.Render = transport.BrowserFetchPublic
	}
	var trace []string
	var transportLimit error
	var fetchFailure error
	var stopFailure error
	finalURLs := map[string]string{}
	options := transport.Options{MaxWireBytes: int64(in.MaxBytes), MaxDecodedBytes: int64(in.MaxBytes)}
	fetch := func(raw string) (string, error) {
		if stopFailure != nil {
			return "", stopFailure
		}
		if err := ctx.Err(); err != nil {
			return "", err
		}
		// Also enforce admission with injected transports, so alternatives cannot
		// cause private destinations even in a custom runner.
		if err := transport.ValidatePublicURL(raw); err != nil {
			return "", err
		}
		doc, err := r.Fetch(ctx, raw, options)
		if err != nil {
			fetchFailure = err
			if terminalFetchFailure(err) {
				stopFailure = err
			}
			if IsTransportLimit(err) {
				transportLimit = err
			}
			return "", err
		}
		if len(doc.HTML) > in.MaxBytes {
			transportLimit = extractor.ErrOutputLimit
			stopFailure = extractor.ErrOutputLimit
			return "", extractor.ErrOutputLimit
		}
		if transport.IsChallengePage(doc.HTML) {
			stopFailure = &transport.PublicChallengeError{}
			fetchFailure = stopFailure
			return "", stopFailure
		}
		if doc.URL != "" {
			if err := transport.ValidatePublicURL(doc.URL); err != nil {
				fetchFailure = err
				return "", err
			}
			finalURLs[raw] = doc.URL
		}
		return string(doc.HTML), nil
	}
	if redditURL(u) {
		jsonURL, _ := url.Parse(u)
		jsonURL.Path = strings.TrimRight(jsonURL.Path, "/") + ".json"
		jsonURL.RawQuery = "limit=10&raw_json=1"
		trace = append(trace, "http_reddit_json")
		if in.RedditJSON == "" {
			in.RedditJSON, _ = fetch(jsonURL.String())
		}
		if stopFailure != nil {
			return finishFailure(stopFailure)
		}
		linked := redditLinkedURL(in.RedditJSON)
		if linked != "" && in.LinkedHTML == "" {
			trace = append(trace, "http_reddit_linked_article")
			in.LinkedHTML, _ = fetch(linked)
		}
		if stopFailure != nil {
			return finishFailure(stopFailure)
		}
		if in.RedditJSON != "" {
			candidate := in
			candidate.Strategy = "reddit_json"
			candidate.Content = ""
			got := extractor.Extract(ctx, candidate)
			if got.State == "resource_limit" {
				stopFailure = extractor.ErrOutputLimit
			}
			if got.State == "extracted" {
				// Linked replacement keeps Reddit title/author/date, while linked
				// media and provenance use the actual redirected article base.
				if linked != "" && in.LinkedHTML != "" && got.SelectorUsed != "reddit_json" {
					base := linked
					if final := finalURLs[linked]; final != "" {
						base = final
					}
					linkedInput := in
					linkedInput.URL = base
					linkedInput.HTML = in.LinkedHTML
					linkedInput.RedditJSON = ""
					linkedInput.LinkedHTML = ""
					linkedInput.Content = ""
					linkedInput.Strategy = "enhanced_selectors"
					article := extractor.Extract(ctx, linkedInput)
					if article.State == "extracted" {
						got.ExtractedFrom = base
						got.Media = article.Media
						if got.ImageURL == "" {
							got.ImageURL = article.ImageURL
							for _, media := range article.Media {
								if got.ImageURL == "" && media.Type == "image" {
									got.ImageURL = media.URL
								}
							}
						}
					}
				}
				got.Trace = append(trace, got.Trace...)
				out.Result = got
				return out
			}
		}
	}
	// All parser strategies operate on a shared page response. The offline core
	// remains the authority for quality, metadata, media and final truncation.
	page := func(pageInput extractor.Input) extractor.Result {
		if err := transport.ValidatePublicURL(pageInput.URL); err != nil {
			fetchFailure = err
			return failed(pageInput.URL, "public_url_denied")
		}
		pageInput.Content = ""
		pageInput.RedditJSON = ""
		pageInput.LinkedHTML = ""
		pageInput.AlternativeHTML = nil
		if pageInput.HTML == "" {
			trace = append(trace, "http_primary")
			h, err := fetch(pageInput.URL)
			if err != nil {
				trace = append(trace, "http_primary_failed")
				if stopFailure != nil {
					return failed(pageInput.URL, classifyFailure(stopFailure))
				}
			} else {
				pageInput.HTML = h
				if final := finalURLs[pageInput.URL]; final != "" {
					pageInput.URL = final
				}
			}
		}
		browserURL := pageInput.URL
		var browserAttempted bool
		render := func() {
			if browserAttempted {
				return
			}
			browserAttempted = true
			if request.ChromeEndpoint == "" || browserDisabled {
				trace = append(trace, "browser_unavailable")
				return
			}
			trace = append(trace, "browser_fetch")
			doc, err := r.Render(ctx, request.ChromeEndpoint, pageInput.URL, transport.BrowserOptions{MaxWireBytes: int64(in.MaxBytes), MaxDOMBytes: int64(in.MaxBytes)})
			if err != nil {
				if IsTransportLimit(err) {
					transportLimit = err
				}
				trace = append(trace, "browser_fetch_failed")
				fetchFailure = err
				return
			}
			if len(doc.HTML) > in.MaxBytes {
				transportLimit = extractor.ErrOutputLimit
				trace = append(trace, "browser_byte_limit")
				return
			}
			pageInput.RenderedHTML = string(doc.HTML)
			if doc.URL != "" {
				if err := transport.ValidatePublicURL(doc.URL); err != nil {
					fetchFailure = err
					pageInput.RenderedHTML = ""
					return
				}
				browserURL = doc.URL
			}
		}
		attempt := func(method string) extractor.Result {
			candidate := pageInput
			candidate.Strategy = method
			if strings.Contains(method, "browser") {
				candidate.URL = browserURL
			}
			got := extractor.Extract(ctx, candidate)
			if got.State == "resource_limit" {
				stopFailure = extractor.ErrOutputLimit
			}
			trace = append(trace, got.Trace...)
			if method == "learned_pattern" && !strings.HasPrefix(got.Method, "learned_pattern_") {
				got.State = "failed"
				got.Content = ""
			}
			return got
		}
		var got extractor.Result
		if extractor.HasIncompleteTable(pageInput.HTML) {
			if browserDisabled {
				if err := admit(ctx, pageInput.URL); err != nil {
					stopFailure = err
					return failed(pageInput.URL, classifyFailure(err))
				}
				out.Reason = "incomplete_table"
				return failed(pageInput.URL, "browser_required")
			}
			render()
			if extractor.HasPopulatedTableCounterpart(pageInput.HTML, pageInput.RenderedHTML) {
				trace = append(trace, "rendered_table_ready")
				got = attempt("browser_rendering")
				if got.State == "extracted" {
					return got
				}
			}
		}
		if pageInput.LearnedMethod == "browser_rendering" || pageInput.LearnedMethod == "learned_pattern_browser_rendering" {
			render()
			got = attempt("learned_pattern_browser_rendering")
			if pageInput.RenderedHTML != "" {
				out.Observations = append(out.Observations, Observation{Selector: pageInput.LearnedSelector, Method: "browser_rendering", Success: got.State == "extracted", Reason: got.Error})
			}
			if got.State == "extracted" {
				return got
			}
		} else if pageInput.LearnedSelector != "" {
			got = attempt("learned_pattern")
			if pageInput.HTML != "" {
				out.Observations = append(out.Observations, Observation{Selector: pageInput.LearnedSelector, Method: pageInput.LearnedMethod, Success: got.State == "extracted", Reason: got.Error})
			}
			if got.State == "extracted" {
				return got
			}
		}
		for _, method := range []string{"enhanced_selectors", "readability"} {
			got = attempt(method)
			if got.State == "extracted" {
				return got
			}
			if ctx.Err() != nil {
				return got
			}
		}
		render()
		got = attempt("browser_rendering")
		if got.State == "extracted" {
			return got
		}
		if pageInput.HTML == "" && ctx.Err() == nil {
			trace = append(trace, "http_alternate")
			alternate := options
			alternate.AlternateHeaders = true
			doc, err := r.Fetch(ctx, pageInput.URL, alternate)
			if IsTransportLimit(err) {
				transportLimit = err
			}
			if err != nil {
				fetchFailure = err
				if terminalFetchFailure(err) {
					stopFailure = err
					return failed(pageInput.URL, classifyFailure(err))
				}
			}
			if transport.IsChallengePage(doc.HTML) {
				stopFailure = &transport.PublicChallengeError{}
				return failed(pageInput.URL, "browser_required")
			}
			if err == nil && len(doc.HTML) <= in.MaxBytes {
				pageInput.HTML = string(doc.HTML)
				if doc.URL != "" && transport.ValidatePublicURL(doc.URL) == nil {
					pageInput.URL = doc.URL
				}
			}
		}
		got = attempt("fallback_extraction")
		if got.State != "extracted" && out.StudyHTML == "" {
			out.StudyHTML = pageInput.HTML
		}
		return got
	}
	if in.Strategy != "" && in.Strategy != "auto" && in.Strategy != "pipeline" && in.Strategy != "learned_pattern" && in.Strategy != "reddit_json" && in.Strategy != "reddit_linked_article" {
		candidate := in
		candidate.Content = ""
		if in.Strategy == "browser_rendering" || in.Strategy == "browser_heuristics" || in.Strategy == "learned_pattern_browser_rendering" {
			if candidate.RenderedHTML == "" && request.ChromeEndpoint != "" && (in.UseBrowser == nil || *in.UseBrowser) {
				doc, err := r.Render(ctx, request.ChromeEndpoint, u, transport.BrowserOptions{MaxWireBytes: int64(in.MaxBytes), MaxDOMBytes: int64(in.MaxBytes)})
				if err != nil {
					out.Result = failed(u, classifyFailure(err))
					return out
				}
				candidate.RenderedHTML = string(doc.HTML)
				if doc.URL != "" {
					candidate.URL = doc.URL
				}
			}
		} else if candidate.HTML == "" {
			h, err := fetch(u)
			if err != nil {
				return finishFailure(err)
			}
			candidate.HTML = h
			if final := finalURLs[u]; final != "" {
				candidate.URL = final
			}
		}
		out.Result = extractor.Extract(ctx, candidate)
		if out.Result.URL != u {
			out.Result.ExtractedFrom = out.Result.URL
			out.Result.URL = u
		}
		return out
	}
	out.Result = page(in)
	if stopFailure != nil {
		return finishFailure(stopFailure)
	}
	if out.Result.State != "extracted" && out.Result.Error != "browser_required" && ctx.Err() == nil {
		// The first two candidates are intentional, including a canonical self
		// link. No recursive expansion or unbounded retry queue is permitted.
		base := out.Result.URL
		if base == "" {
			base = u
		}
		for _, alt := range alternativeURLs(base, out.StudyHTML) {
			if ctx.Err() != nil {
				break
			}
			trace = append(trace, "alternative_fetch")
			candidate := in
			candidate.URL = alt
			candidate.HTML = ""
			candidate.RenderedHTML = ""
			candidate.LearnedSelector = ""
			candidate.LearnedMethod = ""
			got := page(candidate)
			if stopFailure != nil {
				return finishFailure(stopFailure)
			}
			if got.State == "extracted" {
				if got.ExtractedFrom == "" {
					got.ExtractedFrom = got.URL
				}
				got.URL = u
				out.Result = got
				trace = append(trace, "alternative_url_success")
				break
			}
		}
	}
	if err := ctx.Err(); err != nil {
		out.Result = failed(u, err.Error())
		out.StudyHTML = ""
	} else if out.Result.State != "extracted" && transportLimit != nil {
		out.Result = failed(u, transportLimit.Error())
		out.Result.State = "resource_limit"
		out.StudyHTML = ""
	} else if out.Result.State != "extracted" && in.Content != "" && !r.HTTPOnly {
		in.Strategy = "source_content"
		out.Result = extractor.Extract(ctx, in)
		trace = append(trace, "source_content_fallback")
	} else if out.Result.State != "extracted" && out.StudyHTML == "" && fetchFailure != nil {
		out.Result = failed(u, classifyFailure(fetchFailure))
	}
	out.Result.Trace = trace
	if out.Result.URL != u {
		out.Result.ExtractedFrom = out.Result.URL
		out.Result.URL = u
	}
	out.NeedsSelectorStudy = out.Result.State != "extracted" && out.Result.Error != "browser_required" && out.StudyHTML != "" && ctx.Err() == nil
	if out.Result.State == "extracted" {
		out.StudyHTML = ""
		if out.Result.SelectorUsed != "" {
			out.Observations = append(out.Observations, Observation{Selector: out.Result.SelectorUsed, Method: out.Result.MethodUsed, Success: true})
		}
	}
	return out
}

func redditURL(raw string) bool {
	u, e := url.Parse(raw)
	return e == nil && (u.Hostname() == "reddit.com" || strings.HasSuffix(u.Hostname(), ".reddit.com"))
}
func redditLinkedURL(raw string) string {
	var listing []struct {
		Data struct {
			Children []struct {
				Data struct {
					URL    string `json:"url"`
					IsSelf bool   `json:"is_self"`
					Hint   string `json:"post_hint"`
				} `json:"data"`
			} `json:"children"`
		} `json:"data"`
	}
	if json.Unmarshal([]byte(raw), &listing) != nil || len(listing) == 0 || len(listing[0].Data.Children) == 0 {
		return ""
	}
	p := listing[0].Data.Children[0].Data
	if p.IsSelf || p.Hint == "image" || redditURL(p.URL) {
		return ""
	}
	return p.URL
}
func alternativeURLs(base, html string) []string {
	if len(base) > maxAlternativeURLBytes {
		return nil
	}
	d, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil
	}
	var out []string
	add := func(s *goquery.Selection) {
		if len(out) >= 2 {
			return
		}
		href := s.AttrOr("href", "")
		if href == "" || len(href) > maxAlternativeURLBytes {
			return
		}
		v, e := url.Parse(href)
		if e != nil {
			return
		}
		resolved := u.ResolveReference(v).String()
		if len(resolved) <= maxAlternativeURLBytes {
			out = append(out, resolved)
		}
	}
	for _, rel := range []string{"canonical", "amphtml"} {
		if len(out) >= 2 {
			break
		}
		d.Find("link[rel]").EachWithBreak(func(_ int, s *goquery.Selection) bool {
			for _, token := range strings.Fields(s.AttrOr("rel", "")) {
				if token == rel {
					add(s)
					return false
				}
			}
			return true
		})
	}
	if len(out) < 2 {
		d.Find("a[href]").EachWithBreak(func(_ int, s *goquery.Selection) bool {
			text := strings.ToLower(s.Text())
			for _, phrase := range []string{"read more", "full article", "continue reading", "read full", "view full", "more details"} {
				if strings.Contains(text, phrase) {
					add(s)
					return len(out) < 2
				}
			}
			return len(out) < 2
		})
	}
	return out
}

// IsTransportLimit permits the service boundary to classify bounded failures.
func IsTransportLimit(err error) bool {
	return errors.Is(err, extractor.ErrWireLimit) || errors.Is(err, extractor.ErrOutputLimit)
}

func classifyFailure(err error) string {
	if errors.Is(err, transport.ErrPublicAdmissionUnavailable) {
		return "admission_unavailable"
	}
	if errors.Is(err, transport.ErrUnsafePublicURL) {
		return "public_url_denied"
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	if IsTransportLimit(err) {
		return "resource_limit"
	}
	var status *transport.PublicHTTPStatusError
	if errors.As(err, &status) {
		if status.StatusCode == 403 {
			return "upstream_forbidden"
		}
		if status.StatusCode == 401 || status.StatusCode == 407 {
			return "upstream_auth_required"
		}
		if status.StatusCode == 429 {
			return "upstream_rate_limited"
		}
		if status.StatusCode >= 500 {
			return "upstream_unavailable"
		}
		return "upstream_http_rejected"
	}
	var challenge *transport.PublicChallengeError
	if errors.As(err, &challenge) {
		return "browser_required"
	}
	return "transport_failed"
}

func terminalFetchFailure(err error) bool {
	var status *transport.PublicHTTPStatusError
	var challenge *transport.PublicChallengeError
	return errors.As(err, &status) || errors.As(err, &challenge) || errors.Is(err, transport.ErrUnsafePublicURL) || errors.Is(err, transport.ErrPublicAdmissionUnavailable) || IsTransportLimit(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}
