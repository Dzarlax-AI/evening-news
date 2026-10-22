package pipeline

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"net/url"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

type PageMonitorRequest struct {
	Operation                                                    string
	URL                                                          string
	SourceName                                                   string
	Budget                                                       time.Duration
	Limit                                                        int
	MinTitleLength                                               int
	UseBrowser                                                   bool
	WaitForJS                                                    bool
	HTML                                                         string
	SnapshotAt                                                   time.Time
	Selectors                                                    extractor.PageMonitorSelectors
	LearnedContainers, LearnedTitles, LearnedLinks, LearnedDates []string
	LearnedGeneral                                               []extractor.PageMonitorGeneralSelector
}

type PageMonitorOutcome struct {
	OriginalURL        string                      `json:"original_url"`
	FinalURL           string                      `json:"final_url"`
	FetchMethod        string                      `json:"fetch_method"`
	SnapshotAt         time.Time                   `json:"snapshot_at"`
	ContentHash        string                      `json:"content_hash"`
	SelectorsUsed      []string                    `json:"selectors_used"`
	Items              []extractor.PageMonitorItem `json:"items"`
	NeedsSelectorStudy bool                        `json:"needs_selector_study"`
	StudyHTML          string                      `json:"study_html,omitempty"`
	Outcome            string                      `json:"outcome"`
	Error              string                      `json:"error,omitempty"`
	Retryable          bool                        `json:"retryable"`
}

type PageMonitorSource struct {
	Fetch  func(context.Context, string, transport.Options) (transport.Document, error)
	Admit  func(context.Context, string) error
	Render func(context.Context, string, transport.BrowserOptions, string, time.Duration) (transport.RenderedDocument, error)
	Now    func() time.Time
}

func (s PageMonitorSource) Extract(ctx context.Context, in PageMonitorRequest) PageMonitorOutcome {
	out := PageMonitorOutcome{OriginalURL: in.URL, FinalURL: in.URL, Items: []extractor.PageMonitorItem{}, SelectorsUsed: []string{}, Outcome: "failed"}
	if in.Budget <= 0 || in.Budget > 30*time.Second {
		out.Outcome = "rejected"
		out.Error = "invalid_input"
		return out
	}
	ctx, cancel := context.WithTimeout(ctx, in.Budget)
	defer cancel()
	if in.Limit == 0 {
		in.Limit = 20
	}
	if in.MinTitleLength == 0 {
		in.MinTitleLength = 10
	}
	if in.Limit < 1 || in.Limit > 100 || in.MinTitleLength < 1 {
		out.Outcome = "rejected"
		out.Error = "invalid_input"
		return out
	}
	if in.Operation != "fetch" && in.Operation != "parse" {
		out.Outcome = "rejected"
		out.Error = "invalid_input"
		return out
	}
	html, finalURL, method, snapshot := in.HTML, in.URL, "provided_html", in.SnapshotAt
	if in.Operation == "parse" {
		_, offset := snapshot.Zone()
		if snapshot.IsZero() || offset != 0 {
			out.Outcome = "rejected"
			out.Error = "invalid_input"
			return out
		}
		if !validPageMonitorURL(in.URL) {
			out.Outcome = "rejected"
			out.Error = "invalid_input"
			return out
		}
	} else {
		if html != "" || !snapshot.IsZero() {
			out.Outcome = "rejected"
			out.Error = "invalid_input"
			return out
		}
		if err := transport.ValidatePublicURL(in.URL); err != nil {
			out.Outcome = "rejected"
			out.Error = "public_url_denied"
			return out
		}
		if in.UseBrowser {
			if s.Render == nil {
				out.Error = "unavailable"
				out.Retryable = true
				return out
			}
			admit := s.Admit
			if admit == nil {
				admit = transport.AdmitPublicURL
			}
			if err := admit(ctx, in.URL); err != nil {
				return pageMonitorTransportFailure(out, err)
			}
			settle := time.Duration(0)
			if in.WaitForJS {
				settle = 2 * time.Second
			}
			doc, err := s.Render(ctx, in.URL, transport.BrowserOptions{Timeout: in.Budget, MaxWireBytes: transport.MaxPageBytes, MaxDOMBytes: transport.MaxPageBytes}, pageMonitorReadySelector, settle)
			if err != nil {
				return pageMonitorTransportFailure(out, err)
			}
			html = string(doc.HTML)
			finalURL = doc.URL
			method = "native_browser"
		} else {
			fetch := s.Fetch
			if fetch == nil {
				fetch = transport.FetchPublic
			}
			doc, err := fetch(ctx, in.URL, transport.Options{Timeout: in.Budget, MaxWireBytes: transport.MaxPageBytes, MaxDecodedBytes: transport.MaxPageBytes})
			if err != nil {
				return pageMonitorTransportFailure(out, err)
			}
			html = string(doc.HTML)
			finalURL = doc.URL
			method = "http"
		}
		now := time.Now
		if s.Now != nil {
			now = s.Now
		}
		snapshot = now().UTC()
	}
	if len(html) > transport.MaxPageBytes {
		out.Error = "resource_limit"
		return out
	}
	items, used, err := extractor.ParsePageMonitorList(extractor.PageMonitorOptions{Context: ctx, URL: in.URL, HTML: html, Now: snapshot, Limit: in.Limit, MinTitleLength: in.MinTitleLength, SourceName: in.SourceName, Selectors: in.Selectors, LearnedContainerSelectors: in.LearnedContainers, LearnedTitleSelectors: in.LearnedTitles, LearnedLinkSelectors: in.LearnedLinks, LearnedDateSelectors: in.LearnedDates, LearnedGeneral: in.LearnedGeneral})
	out.Items = items
	out.SelectorsUsed = used
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			out = pageMonitorTransportFailure(out, err)
		} else if errors.Is(err, extractor.ErrOutputLimit) {
			out.Error = "resource_limit"
		} else if errors.Is(err, extractor.ErrPageMonitorSelector) {
			out.Outcome = "rejected"
			out.Error = "unsupported_selector"
		} else {
			out.Error = "parse_failed"
		}
		if len(items) > 0 {
			out.Outcome = "partial"
			out.Retryable = errors.Is(err, context.DeadlineExceeded)
		}
		out.FinalURL = finalURL
		out.FetchMethod = method
		out.SnapshotAt = snapshot.UTC()
		sum := md5.Sum([]byte(html))
		out.ContentHash = hex.EncodeToString(sum[:])
		return out
	}
	out.Items = items
	out.SelectorsUsed = used
	out.FinalURL = finalURL
	out.FetchMethod = method
	out.SnapshotAt = snapshot.UTC()
	sum := md5.Sum([]byte(html))
	out.ContentHash = hex.EncodeToString(sum[:])
	out.Outcome = "success"
	if pageMonitorNeedsStudy(items, in.URL) {
		out.NeedsSelectorStudy = true
		out.StudyHTML = html
	}
	if err := ctx.Err(); err != nil {
		out.StudyHTML = ""
		out = pageMonitorTransportFailure(out, err)
		if len(out.Items) > 0 {
			out.Outcome = "partial"
		} else {
			out.Outcome = "failed"
		}
		return out
	}
	return out
}

const pageMonitorReadySelector = "body"

func validPageMonitorURL(raw string) bool {
	u, e := url.Parse(raw)
	return e == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil
}
func pageMonitorNeedsStudy(items []extractor.PageMonitorItem, raw string) bool {
	if len(items) == 0 {
		return false
	}
	base := strings.TrimRight(strings.Split(strings.Split(raw, "?")[0], "#")[0], "/")
	matches := 0
	unique := map[string]bool{}
	for _, item := range items {
		v := strings.TrimRight(strings.Split(strings.Split(item.Link, "?")[0], "#")[0], "/")
		if v == "" || v == base {
			matches++
		}
		if v != "" {
			unique[v] = true
		}
	}
	if float64(matches)/float64(len(items)) > .5 {
		return true
	}
	if len(unique) == 1 && len(items) > 1 {
		return true
	}
	return len(items) == 1 && (matches == 1 || items[0].Link == "")
}
func pageMonitorTransportFailure(out PageMonitorOutcome, err error) PageMonitorOutcome {
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		out.Error = "deadline_exceeded"
		out.Retryable = true
	case errors.Is(err, context.Canceled):
		out.Error = "cancelled"
	case errors.Is(err, transport.ErrBrowserEventOverflow):
		out.Error = "event_overflow"
		out.Retryable = true
	case errors.Is(err, transport.ErrNativeOwnershipLost), errors.Is(err, transport.ErrNativeNotReady), errors.Is(err, transport.ErrNativeBusy):
		out.Error = "unavailable"
		out.Retryable = true
	case errors.Is(err, transport.ErrUnsafePublicURL):
		out.Error = "public_url_denied"
	case errors.Is(err, extractor.ErrWireLimit), errors.Is(err, extractor.ErrOutputLimit):
		out.Error = "resource_limit"
	default:
		out.Error = "transport_failed"
		out.Retryable = true
	}
	return out
}
