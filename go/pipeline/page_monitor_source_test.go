package pipeline

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

func TestPageMonitorParseDoesNotUseNetworkOrBrowser(t *testing.T) {
	calls := 0
	source := PageMonitorSource{
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			calls++
			return transport.Document{}, nil
		},
		Admit: func(context.Context, string) error { calls++; return nil },
		Render: func(context.Context, string, transport.BrowserOptions, string, time.Duration) (transport.RenderedDocument, error) {
			calls++
			return transport.RenderedDocument{}, nil
		},
	}
	provided := `<article><h2>Snapshot parsed offline</h2><p>Published yesterday</p></article>`
	snapshot := time.Date(2026, 10, 22, 8, 30, 0, 0, time.FixedZone("zero", 0))
	out := source.Extract(context.Background(), PageMonitorRequest{
		Operation: "parse", URL: "https://example.org/original", SourceName: "Example", Budget: time.Second,
		HTML: provided, SnapshotAt: snapshot, Selectors: extractor.PageMonitorSelectors{Article: []string{"article"}},
	})
	if calls != 0 {
		t.Fatalf("parse unexpectedly accessed network/browser: %d calls", calls)
	}
	if out.Outcome != "success" || out.FetchMethod != "provided_html" || len(out.Items) != 1 || out.Items[0].Link != "https://example.org/original" {
		t.Fatalf("unexpected parse result: %+v", out)
	}
	h := md5.Sum([]byte(provided))
	if out.ContentHash != hex.EncodeToString(h[:]) || !out.SnapshotAt.Equal(snapshot) {
		t.Fatalf("snapshot metadata mismatch: %+v", out)
	}
}

func TestPageMonitorCancelledParseReportsFiniteError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	out := (PageMonitorSource{}).Extract(ctx, PageMonitorRequest{
		Operation: "parse", URL: "https://example.org/", SourceName: "Example", Budget: time.Second,
		HTML:       `<article><h2>Cancellation should report clearly</h2></article>`,
		SnapshotAt: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC),
		Selectors:  extractor.PageMonitorSelectors{Article: []string{"article"}},
	})
	if out.Outcome != "failed" || out.Error != "cancelled" || out.Retryable {
		t.Fatalf("cancelled parse lost its finite failure: %+v", out)
	}
}

func TestPageMonitorTransportFailureClasses(t *testing.T) {
	tests := []struct {
		name      string
		err       error
		code      string
		retryable bool
	}{
		{"resource_limit", extractor.ErrOutputLimit, "resource_limit", false},
		{"event_overflow", transport.ErrBrowserEventOverflow, "event_overflow", true},
		{"unavailable", transport.ErrNativeOwnershipLost, "unavailable", true},
		{"public_url_denied", transport.ErrUnsafePublicURL, "public_url_denied", false},
		{"native_policy", transport.ErrNativePolicy, "native_browser_policy_denied", false},
		{"deadline", context.DeadlineExceeded, "deadline_exceeded", true},
		{"native admission busy", transport.ErrNativeAdmissionDeadline, "native_browser_busy", true},
		{"cancelled", context.Canceled, "cancelled", false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := pageMonitorTransportFailure(PageMonitorOutcome{}, tc.err)
			if got.Error != tc.code || got.Retryable != tc.retryable {
				t.Fatalf("failure=%+v, want code=%q retryable=%v", got, tc.code, tc.retryable)
			}
		})
	}
	t.Run("ownership_takes_precedence_over_joined_policy", func(t *testing.T) {
		got := pageMonitorTransportFailure(PageMonitorOutcome{}, errors.Join(transport.ErrNativeOwnershipLost, transport.ErrNativePolicy))
		if got.Error != "unavailable" || !got.Retryable {
			t.Fatalf("joined native failure=%+v", got)
		}
	})
}

func TestPageMonitorFetchUsesExactlyOneSelectedTransport(t *testing.T) {
	called := ""
	var renderSettle time.Duration
	source := PageMonitorSource{
		Admit: func(context.Context, string) error { called = "admit"; return nil },
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			called = "http"
			return transport.Document{URL: "https://example.org/final", HTML: []byte(`<article><h2>HTTP snapshot article</h2></article>`)}, nil
		},
		Render: func(_ context.Context, _ string, _ transport.BrowserOptions, _ string, settle time.Duration) (transport.RenderedDocument, error) {
			called = "browser"
			renderSettle = settle
			return transport.RenderedDocument{Document: transport.Document{URL: "https://example.org/final", HTML: []byte(`<article><h2>Browser snapshot article</h2></article>`)}}, nil
		},
		Now: func() time.Time { return time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC) },
	}
	for _, browser := range []bool{false, true} {
		called = ""
		out := source.Extract(context.Background(), PageMonitorRequest{
			Operation: "fetch", URL: "https://example.org/updates", SourceName: "Example", Budget: time.Second,
			UseBrowser: browser, WaitForJS: browser, Selectors: extractor.PageMonitorSelectors{Article: []string{"article"}},
		})
		want := "http"
		if browser {
			want = "browser"
		}
		if out.Outcome != "success" || called != want || len(out.Items) != 1 || out.FinalURL != "https://example.org/final" {
			t.Fatalf("browser=%v call=%q response=%+v", browser, called, out)
		}
		if browser && renderSettle != 2*time.Second || !browser && renderSettle != 0 {
			t.Fatalf("browser=%v render settle=%s", browser, renderSettle)
		}
	}
}
