package pipeline

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/transport"
)

func readRSSPipelineFixture(t *testing.T, name string) []byte {
	t.Helper()
	body, err := os.ReadFile("../../tests/fixtures/go-source-manager/" + name)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func TestRSSSourceFetchFeedUsesBoundedPublicTransportAndFixedNow(t *testing.T) {
	body := readRSSPipelineFixture(t, "rss-semantics.xml")
	now := time.Date(2026, 10, 23, 11, 12, 13, 0, time.FixedZone("+02", 2*60*60))
	var gotOptions transport.Options
	var admitted, fetched bool
	source := RSSSource{
		Admit: func(_ context.Context, raw string) error {
			admitted = raw == "https://publisher.example/feed.xml"
			return nil
		},
		Fetch: func(_ context.Context, raw string, opts transport.Options) (transport.Document, error) {
			fetched = raw == "https://publisher.example/feed.xml"
			gotOptions = opts
			return transport.Document{HTML: body, URL: "https://publisher.example/final.xml"}, nil
		},
		Now: func() time.Time { return now },
	}
	got := source.FetchFeed(context.Background(), "https://publisher.example/feed.xml", "fixture", 0)
	if got.Outcome != "success" || got.Error != "" || len(got.Articles) != 4 || got.FinalURL != "https://publisher.example/final.xml" {
		t.Fatalf("unexpected successful feed outcome: %+v", got)
	}
	if !admitted || !fetched || gotOptions.Timeout != 60*time.Second || gotOptions.MaxWireBytes != RSSMaxFeedBytes || gotOptions.MaxDecodedBytes != RSSMaxFeedBytes {
		t.Fatalf("RSS transport bounds/admission mismatch: admitted=%v fetched=%v options=%+v", admitted, fetched, gotOptions)
	}
	if got.Articles[2].PublishedAt != "2026-10-23T09:12:13Z" {
		t.Fatalf("missing-date fallback should use injected UTC now, got %q", got.Articles[2].PublishedAt)
	}
}

func TestRSSSourceRejectsUnsafeURLBeforeAdmissionOrFetch(t *testing.T) {
	calls := 0
	source := RSSSource{
		Admit: func(context.Context, string) error { calls++; return nil },
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			calls++
			return transport.Document{}, nil
		},
	}
	got := source.FetchFeed(context.Background(), "http://127.0.0.1/feed.xml", "fixture", 0)
	if got.Outcome != "failed" || got.Error != "public_url_denied" || calls != 0 {
		t.Fatalf("unsafe URL should be rejected before any callback: outcome=%+v calls=%d", got, calls)
	}
}

func TestRSSSourceSanitizesFailuresAndPreservesRetryAfter(t *testing.T) {
	retry := 30
	source := RSSSource{
		Admit: func(context.Context, string) error { return nil },
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{}, &transport.PublicHTTPStatusError{StatusCode: 429, RetryAfterSeconds: &retry}
		},
	}
	got := source.FetchFeed(context.Background(), "https://publisher.example/feed.xml", "fixture", 0)
	if got.Outcome != "failed" || got.Error != "upstream_rate_limited" || !got.Retryable || got.RetryAfterSeconds == nil || *got.RetryAfterSeconds != retry {
		t.Fatalf("429 should be a finite retryable outcome: %+v", got)
	}
	if got.Error == "" || got.Error == "public_http_status: 429" {
		t.Fatalf("unexpected error detail: %q", got.Error)
	}

	source.Fetch = func(context.Context, string, transport.Options) (transport.Document, error) {
		return transport.Document{}, errors.New("private response detail")
	}
	got = source.FetchFeed(context.Background(), "https://publisher.example/feed.xml", "fixture", 0)
	if got.Error != "transport_error" || !got.Retryable {
		t.Fatalf("transport errors should be sanitized: %+v", got)
	}
}

func TestRSSSourceRejectsMalformedFeedAndOversizedBodies(t *testing.T) {
	source := RSSSource{Admit: func(context.Context, string) error { return nil }}
	source.Fetch = func(context.Context, string, transport.Options) (transport.Document, error) {
		return transport.Document{HTML: []byte(`<rss version="2.0"><channel><item></channel></rss>`), URL: "https://publisher.example/feed.xml"}, nil
	}
	got := source.FetchFeed(context.Background(), "https://publisher.example/feed.xml", "fixture", 0)
	if got.Error != "invalid_feed" || got.Outcome != "failed" {
		t.Fatalf("malformed feed outcome: %+v", got)
	}
	source.Fetch = func(context.Context, string, transport.Options) (transport.Document, error) {
		return transport.Document{HTML: make([]byte, RSSMaxFeedBytes+1), URL: "https://publisher.example/feed.xml"}, nil
	}
	got = source.FetchFeed(context.Background(), "https://publisher.example/feed.xml", "fixture", 0)
	if got.Error != "resource_limit" || got.Outcome != "failed" {
		t.Fatalf("oversized body outcome: %+v", got)
	}
}

func TestRSSSourceRejectsUnsafeFinalURL(t *testing.T) {
	source := RSSSource{
		Admit: func(context.Context, string) error { return nil },
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{HTML: []byte(`<rss version="2.0"><channel></channel></rss>`), URL: "http://192.168.1.1/feed"}, nil
		},
	}
	got := source.FetchFeed(context.Background(), "https://publisher.example/feed.xml", "fixture", 0)
	if got.Error != "public_url_denied" || got.Outcome != "failed" {
		t.Fatalf("unsafe final URL outcome: %+v", got)
	}
}
