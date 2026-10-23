package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

const testTelegramURL = "https://t.me/s/channel"

func telegramPage(content string) string {
	return `<div class="tgme_widget_message" data-post="channel/1"><a class="tgme_widget_message_date" href="/channel/1"><time datetime="2026-10-22T12:00:00+00:00"></time></a><div class="tgme_widget_message_text">` + content + `</div></div>`
}

func TestTelegram429DoesNotFallback(t *testing.T) {
	var renders int
	source := TelegramSource{
		Admit: func(context.Context, string) error { return nil },
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{}, &transport.PublicHTTPStatusError{StatusCode: 429}
		},
		Render: func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
			renders++
			return transport.RenderedDocument{}, nil
		},
	}
	got := source.FetchChannel(context.Background(), testTelegramURL, "News", 10)
	if got.Outcome != "failed" || got.Error != "upstream_rate_limited" || !got.Retryable || renders != 0 {
		t.Fatalf("429 fallback policy failed: outcome=%s error=%s retry=%v renders=%d", got.Outcome, got.Error, got.Retryable, renders)
	}
}

func TestTelegramPreservesAdmissionBusyWhenSourceBudgetExpires(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Millisecond)
	defer cancel()
	source := TelegramSource{
		Admit: func(context.Context, string) error { return nil },
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{}, errors.New("temporary network failure")
		},
		Render: func(ctx context.Context, _ string, _ transport.BrowserOptions, _ string) (transport.RenderedDocument, error) {
			<-ctx.Done()
			return transport.RenderedDocument{}, transport.ErrNativeAdmissionDeadline
		},
	}
	got := source.FetchChannel(ctx, testTelegramURL, "News", 10)
	if got.Outcome != "failed" || got.Error != "native_browser_busy" || !got.Retryable {
		t.Fatalf("queued native render timeout was reclassified: %+v", got)
	}
}

func TestTelegramOtherClientErrorsDoNotFallback(t *testing.T) {
	for _, code := range []int{400, 401, 403, 404, 407, 429} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			var renders int
			source := TelegramSource{
				Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
					return transport.Document{}, &transport.PublicHTTPStatusError{StatusCode: code}
				},
				Render: func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
					renders++
					return transport.RenderedDocument{}, nil
				},
			}
			got := source.FetchChannel(context.Background(), testTelegramURL, "News", 10)
			if renders != 0 || got.Outcome != "failed" {
				t.Fatalf("HTTP %d unexpectedly fell back: outcome=%s renders=%d", code, got.Outcome, renders)
			}
		})
	}
}

func TestTelegramServerErrorCanUseOneNativeFallback(t *testing.T) {
	var renders int
	source := TelegramSource{
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{}, &transport.PublicHTTPStatusError{StatusCode: 503}
		},
		Admit: func(context.Context, string) error { return nil },
		Now:   func() time.Time { return time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC) },
		Render: func(_ context.Context, _ string, _ transport.BrowserOptions, selector string) (transport.RenderedDocument, error) {
			renders++
			if selector != TelegramNativeReadySelector {
				t.Fatalf("selector=%q", selector)
			}
			return transport.RenderedDocument{Document: transport.Document{HTML: []byte(telegramPage("A sufficiently long Telegram post body.")), URL: testTelegramURL}}, nil
		},
	}
	got := source.FetchChannel(context.Background(), testTelegramURL, "News", 10)
	if renders != 1 || got.Outcome != "success" || got.AccessMethod != "native_browser" {
		t.Fatalf("eligible fallback failed: %+v renders=%d", got, renders)
	}
}

func TestTelegramEmptyHTTPUsesOneFixedNativeFallback(t *testing.T) {
	var renders int
	source := TelegramSource{
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{HTML: []byte(`<html><body>no posts</body></html>`), URL: testTelegramURL}, nil
		},
		Admit: func(context.Context, string) error { return nil },
		Now:   func() time.Time { return time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC) },
		Render: func(_ context.Context, _ string, opts transport.BrowserOptions, selector string) (transport.RenderedDocument, error) {
			renders++
			if selector != TelegramNativeReadySelector {
				t.Fatalf("selector=%q", selector)
			}
			if opts.MaxWireBytes != transport.MaxPageBytes || opts.MaxDOMBytes != transport.MaxPageBytes {
				t.Fatalf("unbounded browser options: %+v", opts)
			}
			return transport.RenderedDocument{Document: transport.Document{HTML: []byte(telegramPage("A sufficiently long Telegram post body.")), URL: testTelegramURL}}, nil
		},
	}
	got := source.FetchChannel(context.Background(), testTelegramURL, "News", 10)
	if got.Outcome != "success" || got.AccessMethod != "native_browser" || renders != 1 || len(got.Articles) != 1 {
		t.Fatalf("fallback failed: outcome=%s method=%s renders=%d articles=%d error=%s", got.Outcome, got.AccessMethod, renders, len(got.Articles), got.Error)
	}
}

func TestTelegramEnvelopeFinalURLIsCanonicalAndRawAccessURLIsActual(t *testing.T) {
	actual := "https://telegram.me/channel"
	source := TelegramSource{
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{HTML: []byte(telegramPage("A sufficiently long Telegram post body.")), URL: actual}, nil
		},
		Now: func() time.Time { return time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC) },
	}
	got := source.FetchChannel(context.Background(), testTelegramURL, "News", 10)
	if got.Outcome != "success" || got.FinalURL != testTelegramURL || len(got.Articles) != 1 {
		t.Fatalf("final URL was not canonicalized: outcome=%s final=%s", got.Outcome, got.FinalURL)
	}
	if got.Articles[0].RawData["access_method"] != actual {
		t.Fatalf("raw access URL lost actual redirect target: %#v", got.Articles[0].RawData["access_method"])
	}
}

func TestTelegramCancellationStopsBeforeFallback(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	var renders int
	source := TelegramSource{
		Fetch: func(ctx context.Context, _ string, _ transport.Options) (transport.Document, error) {
			<-ctx.Done()
			return transport.Document{}, ctx.Err()
		},
		Render: func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
			renders++
			return transport.RenderedDocument{}, nil
		},
	}
	cancel()
	got := source.FetchChannel(ctx, testTelegramURL, "News", 10)
	if got.Error != "cancelled" || renders != 0 {
		t.Fatalf("cancellation lost: error=%s renders=%d", got.Error, renders)
	}
}

func TestTelegramPostLimitReturnsSuccessfulPrefix(t *testing.T) {
	page := telegramPage("A sufficiently long first Telegram post.") + telegramPage(strings.Repeat("x", 100001))
	source := TelegramSource{
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{HTML: []byte(page), URL: testTelegramURL}, nil
		},
		Now: func() time.Time { return time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC) },
	}
	got := source.FetchChannel(context.Background(), testTelegramURL, "News", 10)
	if got.Outcome != "partial" || got.Error != "resource_limit" || len(got.Articles) != 1 {
		t.Fatalf("partial prefix lost: outcome=%s error=%s articles=%d", got.Outcome, got.Error, len(got.Articles))
	}
}

func TestTelegramResponseBudgetRejectsAmplifiedMetadata(t *testing.T) {
	got := enforceTelegramResponseBudget(TelegramOutcome{Outcome: "success", Articles: []extractor.TelegramArticle{{Title: "x", RawData: map[string]any{"media": strings.Repeat("x", 5*1024*1024)}}}})
	if got.Outcome != "failed" || got.Error != "response_too_large" || len(got.Articles) != 0 {
		t.Fatalf("oversize metadata survived: outcome=%s error=%s articles=%d", got.Outcome, got.Error, len(got.Articles))
	}
}

func TestTelegramNearPageLimitMetadataAmplificationIsBounded(t *testing.T) {
	urlValue := "https://example.org/" + strings.Repeat("x", transport.MaxPageBytes-330) + ".jpg"
	page := telegramPage(`<div class="tgme_widget_message_photo"><img src="` + urlValue + `"></div>`)
	if len(page) >= transport.MaxPageBytes {
		t.Fatalf("fixture must remain below page limit: %d", len(page))
	}
	source := TelegramSource{
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{HTML: []byte(page), URL: testTelegramURL}, nil
		},
		Now: func() time.Time { return time.Date(2026, 10, 22, 12, 0, 0, 0, time.UTC) },
	}
	got := source.FetchChannel(context.Background(), testTelegramURL, "News", 10)
	if got.Outcome != "failed" || got.Error != "response_too_large" || len(got.Articles) != 0 {
		t.Fatalf("amplified media metadata was returned or truncated: outcome=%s error=%s articles=%d", got.Outcome, got.Error, len(got.Articles))
	}
}

func TestTelegramArrayLimitReturnsPartialPrefixWithoutTruncation(t *testing.T) {
	article := extractor.TelegramArticle{Title: "ok", RawData: map[string]any{"external_links": []string{}, "hashtags": []string{}}}
	article.MediaFiles = make([]map[string]any, telegramArticleArrayLimit+1)
	for i := range article.MediaFiles {
		article.MediaFiles[i] = map[string]any{"type": "image", "url": "https://example.org/image"}
	}
	got := enforceTelegramResponseBudget(TelegramOutcome{Outcome: "success", Articles: []extractor.TelegramArticle{article}})
	if got.Outcome != "failed" || got.Error != "resource_limit" || len(got.Articles) != 0 {
		t.Fatalf("array cap was not explicit: outcome=%s error=%s count=%d", got.Outcome, got.Error, len(got.Articles))
	}
}

func TestTelegramBoundedPreflightCoversJSONEscaping(t *testing.T) {
	article := extractor.TelegramArticle{
		Title: "quotes \" slash \\ html <>& unicode \u2028 and invalid " + string([]byte{0xff}),
		URL:   "https://example.org/\x01", Content: "line\nfeed\tquote \"",
		SourceName: "источник", RawData: map[string]any{"metadata": []any{"<tag>", string([]byte{0xfe, 0xfd}), map[string]any{"control": "\x00"}}},
	}
	encoded, err := json.Marshal(article)
	if err != nil {
		t.Fatal(err)
	}
	estimated, ok := jsonSizeUpperBound(reflect.ValueOf(article), len(encoded), 0)
	if !ok || estimated < len(encoded) {
		t.Fatalf("preflight underestimates JSON: estimated=%d actual=%d ok=%v", estimated, len(encoded), ok)
	}
}

func TestTelegramOversizeArticlePreservesEarlierPrefix(t *testing.T) {
	first := extractor.TelegramArticle{Title: "first", Content: "first post", RawData: map[string]any{}}
	second := extractor.TelegramArticle{Title: "oversize", RawData: map[string]any{"metadata": strings.Repeat("<", telegramResponseArticleBudget)}}
	got := enforceTelegramResponseBudget(TelegramOutcome{Outcome: "success", Articles: []extractor.TelegramArticle{first, second}})
	if got.Outcome != "partial" || got.Error != "response_too_large" || len(got.Articles) != 1 || got.Articles[0].Title != "first" {
		t.Fatalf("successful prefix was lost or oversized article truncated: outcome=%s error=%s articles=%d", got.Outcome, got.Error, len(got.Articles))
	}
}

func TestTelegramResourceLimitNeverFallsBack(t *testing.T) {
	var renders int
	source := TelegramSource{
		Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
			return transport.Document{}, extractor.ErrOutputLimit
		},
		Render: func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error) {
			renders++
			return transport.RenderedDocument{}, errors.New("unexpected")
		},
	}
	got := source.FetchChannel(context.Background(), testTelegramURL, "News", 10)
	if got.Error != "resource_limit" || renders != 0 {
		t.Fatalf("resource limit retried: error=%s renders=%d", got.Error, renders)
	}
}

func TestTelegramNativeEventOverflowHasDistinctCode(t *testing.T) {
	err := errors.Join(transport.ErrBrowserEventOverflow, transport.ErrNativeOwnershipLost)
	code, _ := telegramNativeError(err)
	if code != "event_overflow" {
		t.Fatalf("event overflow collapsed into %q", code)
	}
}
