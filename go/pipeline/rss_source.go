package pipeline

import (
	"context"
	"errors"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

const (
	RSSMaxFeedBytes  = transport.MaxPageBytes
	RSSMaxItems      = extractor.MaxRSSItems
	RSSSourceTimeout = 60 * time.Second
	RSSHTTPTimeout   = 60 * time.Second
)

type RSSOutcome struct {
	Articles          []extractor.RSSArticle `json:"articles"`
	FinalURL          string                 `json:"final_url"`
	Outcome           string                 `json:"outcome"`
	Error             string                 `json:"error,omitempty"`
	Retryable         bool                   `json:"retryable"`
	RetryAfterSeconds *int                   `json:"retry_after_seconds,omitempty"`
}

type RSSSource struct {
	Admit func(context.Context, string) error
	Fetch func(context.Context, string, transport.Options) (transport.Document, error)
	Now   func() time.Time
}

// FetchFeed retrieves one public RSS/Atom feed and parses it offline. It does
// not fetch article links, media, or external XML entities.
func (s RSSSource) FetchFeed(ctx context.Context, sourceURL, sourceName string, limit int) RSSOutcome {
	out := RSSOutcome{Articles: []extractor.RSSArticle{}, FinalURL: sourceURL, Outcome: "failed"}
	if ctx == nil || limit < 0 || limit > RSSMaxItems {
		return rssFailure(out, "invalid_input", false)
	}
	if err := transport.ValidatePublicURL(sourceURL); err != nil {
		return rssFailure(out, "public_url_denied", false)
	}
	ctx, cancel := context.WithTimeout(ctx, RSSSourceTimeout)
	defer cancel()

	admit := s.Admit
	if admit == nil {
		admit = transport.AdmitPublicURL
	}
	if err := admit(ctx, sourceURL); err != nil {
		return rssTransportFailure(out, err, ctx)
	}
	fetch := s.Fetch
	if fetch == nil {
		fetch = transport.FetchPublicRSS
	}
	doc, err := fetch(ctx, sourceURL, transport.Options{Timeout: RSSHTTPTimeout, MaxWireBytes: RSSMaxFeedBytes, MaxDecodedBytes: RSSMaxFeedBytes})
	if err != nil {
		return rssTransportFailure(out, err, ctx)
	}
	if len(doc.HTML) > RSSMaxFeedBytes {
		return rssFailure(out, "resource_limit", false)
	}
	finalURL := doc.URL
	if finalURL == "" {
		finalURL = sourceURL
	}
	if err := transport.ValidatePublicURL(finalURL); err != nil {
		return rssFailure(out, "public_url_denied", false)
	}
	out.FinalURL = finalURL
	now := s.Now
	if now == nil {
		now = time.Now
	}
	articles, err := extractor.ParseRSSSource(ctx, string(doc.HTML), finalURL, sourceName, limit, now())
	if err != nil {
		if errors.Is(err, extractor.ErrRSSResourceLimit) {
			return rssFailure(out, "resource_limit", false)
		}
		if errors.Is(err, extractor.ErrInvalidRSSInput) {
			return rssFailure(out, "invalid_input", false)
		}
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return rssFailure(out, rssContextCode(err), errors.Is(err, context.DeadlineExceeded))
		}
		return rssFailure(out, "invalid_feed", false)
	}
	if err := ctx.Err(); err != nil {
		return rssFailure(out, rssContextCode(err), errors.Is(err, context.DeadlineExceeded))
	}
	out.Articles = articles
	out.Outcome = "success"
	return out
}

func rssFailure(out RSSOutcome, code string, retryable bool) RSSOutcome {
	out.Outcome = "failed"
	out.Error = code
	out.Retryable = retryable
	return out
}

func rssContextCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "canceled"
}

func rssTransportFailure(out RSSOutcome, err error, ctx context.Context) RSSOutcome {
	if ctx.Err() != nil {
		return rssFailure(out, rssContextCode(ctx.Err()), errors.Is(ctx.Err(), context.DeadlineExceeded))
	}
	if errors.Is(err, transport.ErrUnsafePublicURL) {
		return rssFailure(out, "public_url_denied", false)
	}
	if errors.Is(err, extractor.ErrWireLimit) || errors.Is(err, extractor.ErrOutputLimit) {
		return rssFailure(out, "resource_limit", false)
	}
	var status *transport.PublicHTTPStatusError
	if errors.As(err, &status) {
		out = rssFailure(out, rssHTTPStatusCode(status.StatusCode), status.StatusCode == 408 || status.StatusCode == 429 || status.StatusCode >= 500)
		out.RetryAfterSeconds = status.RetryAfterSeconds
		return out
	}
	return rssFailure(out, "transport_error", true)
}

func rssHTTPStatusCode(status int) string {
	switch status {
	case 401, 403:
		return "upstream_auth_required"
	case 408:
		return "upstream_timeout"
	case 429:
		return "upstream_rate_limited"
	default:
		return "upstream_http_error"
	}
}
