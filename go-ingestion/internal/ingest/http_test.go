package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-ingestion/internal/store"
)

func TestPageMonitorSameSnapshotReparseRequiresActualCollectorContract(t *testing.T) {
	fixture, err := os.ReadFile("testdata/page_monitor_actual_response.json")
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			RequestID string    `json:"request_id"`
			URL       string    `json:"url"`
			HTML      string    `json:"provided_html"`
			Operation string    `json:"operation"`
			Snapshot  time.Time `json:"snapshot_at"`
			Learned   []string  `json:"learned_title_selectors"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		if request.Operation != "parse" || request.HTML != "<html>same captured document</html>" || request.Snapshot.IsZero() || len(request.Learned) != 1 || request.Learned[0] != ".learned-title-unique" {
			t.Errorf("reparse did not use the captured snapshot and learned selector: %+v", request)
		}
		response := bytes.Replace(fixture, []byte(`"fixture-request-id"`), []byte(strconv.Quote(request.RequestID)), 1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(response)
	}))
	defer server.Close()

	handler := NewHandler(nil, Config{ExtractorURL: server.URL, ExtractorToken: "fixture"})
	snapshot := time.Date(2026, 10, 24, 12, 30, 0, 0, time.UTC)
	page := &pageMonitorResult{Outcome: "needs_selector_study", SnapshotAt: &snapshot, StudyHTML: "<html>same captured document</html>"}
	source := store.Source{ID: 7, Name: "Fixture monitor", Type: "custom", URL: "https://fixture.invalid/list"}
	descriptor := descriptor{SourceID: 7, Type: "custom", URL: source.URL, Limit: 5,
		PageMonitor: &sourceConfig{LearnedTitleSelectors: []string{".learned-title-unique"}}}
	got, err := handler.parsePageMonitor(context.Background(), source, descriptor, page)
	if err != nil || got == nil || len(got.Items) != 1 || got.Items[0].Title != "Learned title" || got.Items[0].ContentType != "news" ||
		got.Items[0].ID != "fixture-item-id" || got.Items[0].Source != "Fixture monitor" || got.Items[0].SourceURL != source.URL {
		t.Fatalf("actual complete collector response rejected or item metadata lost: got=%+v err=%v", got, err)
	}
	for _, flags := range []struct {
		needsStudy  bool
		htmlOmitted bool
	}{
		{needsStudy: true, htmlOmitted: false},
		{needsStudy: true, htmlOmitted: true},
		{needsStudy: false, htmlOmitted: false},
	} {
		t.Run(fmt.Sprintf("needs_study_%t_html_omitted_%t", flags.needsStudy, flags.htmlOmitted), func(t *testing.T) {
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(fixture, &wire); err != nil {
				t.Fatal(err)
			}
			wire["needs_selector_study"], _ = json.Marshal(flags.needsStudy)
			wire["study_html_omitted"], _ = json.Marshal(flags.htmlOmitted)
			if flags.needsStudy && !flags.htmlOmitted {
				wire["study_html"], _ = json.Marshal("<html>same captured document</html>")
			} else {
				delete(wire, "study_html")
			}
			payload, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			flagServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					RequestID string `json:"request_id"`
				}
				_ = json.NewDecoder(r.Body).Decode(&request)
				response := bytes.Replace(payload, []byte(`"fixture-request-id"`), []byte(strconv.Quote(request.RequestID)), 1)
				_, _ = w.Write(response)
			}))
			defer flagServer.Close()
			flagHandler := NewHandler(nil, Config{ExtractorURL: flagServer.URL, ExtractorToken: "fixture"})
			if _, err := flagHandler.parsePageMonitor(context.Background(), source, descriptor, page); err != nil {
				t.Fatalf("valid independent response flags were rejected: %v", err)
			}
		})
	}

	// These required fields are present in the service's serialized parse response.
	// Omitting either makes the response inconsistent with a provided-HTML reparse.
	for _, field := range []string{"fetch_method"} {
		t.Run("missing "+field, func(t *testing.T) {
			var wire map[string]json.RawMessage
			if err := json.Unmarshal(fixture, &wire); err != nil {
				t.Fatal(err)
			}
			delete(wire, field)
			modified, err := json.Marshal(wire)
			if err != nil {
				t.Fatal(err)
			}
			badServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var request struct {
					RequestID string `json:"request_id"`
				}
				_ = json.NewDecoder(r.Body).Decode(&request)
				response := bytes.Replace(modified, []byte(`"fixture-request-id"`), []byte(strconv.Quote(request.RequestID)), 1)
				_, _ = w.Write(response)
			}))
			defer badServer.Close()
			badHandler := NewHandler(nil, Config{ExtractorURL: badServer.URL, ExtractorToken: "fixture"})
			if _, err := badHandler.parsePageMonitor(context.Background(), source, descriptor, page); err == nil {
				t.Fatalf("response missing %s was accepted", field)
			}
		})
	}
}

func TestHTMLRequestEncodingHonorsReceiverByteLimits(t *testing.T) {
	markup := strings.Repeat("<p>news</p>", maxSelectorHTMLBytes/11)
	markup += strings.Repeat("x", maxSelectorHTMLBytes-len(markup))
	quotedMarkup := strings.Repeat(`"`, maxSelectorHTMLBytes)

	t.Run("selector callback accepts near-limit ordinary markup", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil || len(body) > maxSelectorCallbackBytes {
				t.Errorf("callback received invalid body length %d: %v", len(body), err)
			}
			var request selectorRequest
			if err := json.Unmarshal(body, &request); err != nil || len(request.StudyHTML) != maxSelectorHTMLBytes {
				t.Errorf("near-limit markup did not round trip: html bytes=%d err=%v", len(request.StudyHTML), err)
			}
			_ = json.NewEncoder(w).Encode(selectorResponse{Version: 1,
				LearnedContainerSelectors: []string{}, LearnedTitleSelectors: []string{},
				LearnedLinkSelectors: []string{}, LearnedDateSelectors: []string{}})
		}))
		defer server.Close()

		handler := NewHandler(nil, Config{SelectorURL: server.URL, SelectorToken: "fixture"})
		_, err := handler.selectorCall(context.Background(), store.Fence{DeadlineAt: time.Now().Add(time.Minute)},
			store.Source{ID: 1, Fingerprint: "fixture"}, "study", markup)
		if err != nil || requests.Load() != 1 {
			t.Fatalf("near-limit callback request rejected: requests=%d err=%v", requests.Load(), err)
		}
	})

	t.Run("selector callback rejects escaped over-budget markup before HTTP", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
		defer server.Close()
		handler := NewHandler(nil, Config{SelectorURL: server.URL, SelectorToken: "fixture"})
		_, err := handler.selectorCall(context.Background(), store.Fence{DeadlineAt: time.Now().Add(time.Minute)},
			store.Source{ID: 1, Fingerprint: "fixture"}, "study", quotedMarkup)
		if err == nil || requests.Load() != 0 {
			t.Fatalf("over-budget callback body was sent: requests=%d err=%v", requests.Load(), err)
		}
	})

	fixture, err := os.ReadFile("testdata/page_monitor_actual_response.json")
	if err != nil {
		t.Fatal(err)
	}
	snapshot := time.Date(2026, 10, 24, 12, 30, 0, 0, time.UTC)
	page := &pageMonitorResult{Outcome: "needs_selector_study", SnapshotAt: &snapshot, StudyHTML: markup}
	source := store.Source{ID: 7, Name: "Fixture monitor", Type: "custom", URL: "https://fixture.invalid/list"}
	descriptor := descriptor{SourceID: 7, Type: "custom", URL: source.URL, Limit: 5, PageMonitor: &sourceConfig{}}
	t.Run("collector accepts near-limit ordinary markup", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			body, err := io.ReadAll(r.Body)
			if err != nil || len(body) > maxExtractorBytes {
				t.Errorf("collector received invalid body length %d: %v", len(body), err)
			}
			var request struct {
				RequestID string `json:"request_id"`
				HTML      string `json:"provided_html"`
			}
			if err := json.Unmarshal(body, &request); err != nil || len(request.HTML) != maxSelectorHTMLBytes {
				t.Errorf("near-limit markup did not round trip: html bytes=%d err=%v", len(request.HTML), err)
			}
			response := bytes.Replace(fixture, []byte(`"fixture-request-id"`), []byte(strconv.Quote(request.RequestID)), 1)
			_, _ = w.Write(response)
		}))
		defer server.Close()
		handler := NewHandler(nil, Config{ExtractorURL: server.URL, ExtractorToken: "fixture"})
		if _, err := handler.parsePageMonitor(context.Background(), source, descriptor, page); err != nil || requests.Load() != 1 {
			t.Fatalf("near-limit collector request rejected: requests=%d err=%v", requests.Load(), err)
		}
	})

	t.Run("collector rejects escaped over-budget markup before HTTP", func(t *testing.T) {
		var requests atomic.Int32
		server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { requests.Add(1) }))
		defer server.Close()
		handler := NewHandler(nil, Config{ExtractorURL: server.URL, ExtractorToken: "fixture"})
		escapedPage := *page
		escapedPage.StudyHTML = quotedMarkup
		if _, err := handler.parsePageMonitor(context.Background(), source, descriptor, &escapedPage); err == nil || requests.Load() != 0 {
			t.Fatalf("over-budget collector body was sent: requests=%d err=%v", requests.Load(), err)
		}
	})
}

func ptrTime(value time.Time) *time.Time { return &value }

func TestFetchBatchAllowsPayloadFreeDeferredAlongsideSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			RequestID string `json:"request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"contract_version": "1", "request_id": request.RequestID,
			"results": []any{
				map[string]any{"request_id": request.RequestID, "source_id": 1, "type": "rss", "outcome": "deferred",
					"error": map[string]any{"code": "response_budget_exhausted", "retry_after_seconds": 1}},
				map[string]any{"request_id": request.RequestID, "source_id": 2, "type": "rss", "outcome": "success", "rss_articles": []any{}},
			},
		})
	}))
	defer server.Close()

	handler := NewHandler(nil, Config{ExtractorURL: server.URL, ExtractorToken: "fixture"})
	got, code, _ := handler.fetchBatch(context.Background(), []descriptor{
		{SourceID: 1, Type: "rss"}, {SourceID: 2, Type: "rss"},
	})
	if code != "" {
		t.Fatalf("valid mixed batch rejected: %q", code)
	}
	if deferred := got[1]; deferred.Outcome != "deferred" || hasAnyCollectedPayload(deferred) || deferred.Error == nil || deferred.Error.Code != "response_budget_exhausted" {
		t.Fatalf("payload-free deferred outcome was not preserved: %+v", deferred)
	}
	if success := got[2]; success.Outcome != "success" || success.RSSArticles == nil {
		t.Fatalf("neighboring success result was lost: %+v", success)
	}
}

func TestFetchBatchRejectsWrongTypedDeferredPayload(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			RequestID string `json:"request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"contract_version": "1", "request_id": request.RequestID,
			"results": []any{map[string]any{
				"request_id": request.RequestID, "source_id": 1, "type": "custom", "outcome": "deferred",
				"rss_articles": []any{},
			}},
		})
	}))
	defer server.Close()

	handler := NewHandler(nil, Config{ExtractorURL: server.URL, ExtractorToken: "fixture"})
	if _, code, _ := handler.fetchBatch(context.Background(), []descriptor{{SourceID: 1, Type: "custom"}}); code != "extractor_response_invalid" {
		t.Fatalf("wrong typed deferred payload accepted: code=%q", code)
	}
}

func TestMakeDescriptorMatchesPythonPageMonitorSelectorDefaults(t *testing.T) {
	expected := sourceConfig{
		ArticleSelectors: []string{"article", ".article", ".news-item", ".post", ".entry", ".changelog-item", ".update-item", ".release-note", ".content li", ".main li", "ul.updates li", "ul.news li", ".content > div", ".main > div", ".updates > div", `[class*="item"]`, `[class*="post"]`, `[class*="article"]`, "h2, h3, h4", ".title", ".headline", ".heading"},
		TitleSelectors:   []string{"h1", "h2", "h3", "h4", ".title", ".headline", ".heading", "a[href]", ".link", ".post-title", ".article-title"},
		DateSelectors:    []string{"time", "time[datetime]", "[datetime]", ".date", ".timestamp", ".published", ".created", ".updated", ".publish-date", ".published-date", ".creation-date", ".post-date", ".article-date", ".news-date", ".entry-date", ".blog-date", ".meta .date", ".meta time", ".metadata .date", ".info .date", ".post-meta .date", ".article-meta .date", ".entry-meta .date", ".commit-date", ".release-date", ".version-date", ".update-date", ".changelog-date", ".timeline .date", ".byline .date", ".author-date", ".publish-info .date", `[class*="date"]`, `[class*="time"]`, `[class*="publish"]`, `[id*="date"]`, `[id*="time"]`},
		LinkSelectors:    []string{"a[href]", ".link", ".read-more", ".permalink"},
	}

	falseValue := false
	missing := store.Source{ID: 1, Type: "custom", URL: "https://fixture.invalid/news", Config: map[string]any{
		"use_browser": false, "enable_ai_analysis": false,
	}}
	descriptor, code := makeDescriptor(missing, store.PageState{})
	if code != "" || descriptor.PageMonitor == nil {
		t.Fatalf("descriptor creation failed: descriptor=%+v code=%q", descriptor, code)
	}
	got := descriptor.PageMonitor
	if !reflect.DeepEqual(got.ArticleSelectors, expected.ArticleSelectors) || !reflect.DeepEqual(got.TitleSelectors, expected.TitleSelectors) ||
		!reflect.DeepEqual(got.DateSelectors, expected.DateSelectors) || !reflect.DeepEqual(got.LinkSelectors, expected.LinkSelectors) {
		t.Fatalf("omitted selectors do not match Python defaults: got=%+v", got)
	}
	if got.UseBrowser == nil || *got.UseBrowser != falseValue || missing.Config["enable_ai_analysis"] != false {
		t.Fatalf("false options were not preserved: page_monitor=%+v config=%v", got, missing.Config)
	}

	empty := store.Source{ID: 2, Type: "custom", URL: "https://fixture.invalid/empty", Config: map[string]any{
		"article_selectors": []any{}, "title_selectors": []any{}, "date_selectors": []any{}, "link_selectors": []any{},
	}}
	descriptor, code = makeDescriptor(empty, store.PageState{})
	if code != "" || descriptor.PageMonitor == nil {
		t.Fatalf("explicit empty descriptor creation failed: descriptor=%+v code=%q", descriptor, code)
	}
	got = descriptor.PageMonitor
	if got.ArticleSelectors == nil || got.TitleSelectors == nil || got.DateSelectors == nil || got.LinkSelectors == nil ||
		len(got.ArticleSelectors) != 0 || len(got.TitleSelectors) != 0 || len(got.DateSelectors) != 0 || len(got.LinkSelectors) != 0 {
		t.Fatalf("explicit empty selector groups were replaced by defaults: %+v", got)
	}
}

func TestPageMonitorAcknowledgementFinalizesOnlyAfterReturnedPrefix(t *testing.T) {
	var response collectResult
	if err := json.Unmarshal([]byte(`{"type":"custom","outcome":"success","page_monitor":{"outcome":"success","items":[{"title":"Fixture","link":"https://fixture.invalid/a","description":"body"}]}}`), &response); err != nil {
		t.Fatal(err)
	}
	state := pageState(store.PageState{ScanLimit: 5}, response.PageMonitor, map[string]any{"max_articles_per_check": float64(5)})
	if !state.ScanActive || state.CursorOffset != 0 {
		t.Fatalf("pageState finalized before article commit acknowledgement: %+v", state)
	}
	acknowledgePageItems(state, response.PageMonitor)
	if state.ScanActive || state.CursorOffset != 0 || len(state.ScanHashes) != 0 || len(state.AcknowledgedHashes) != 0 {
		t.Fatalf("fully acknowledged scan was not finalized: %+v", state)
	}
	if got, want := state.PreviousHashes["0"], pageArticleHash("Fixture", "https://fixture.invalid/a", "body"); got != want {
		t.Fatalf("committed hash missing from previous snapshot: got %q want %q", got, want)
	}
}

func TestRetryDeferredSourceRetainsTypedDeferredPrefix(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		var request struct {
			RequestID string `json:"request_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"contract_version": "1", "request_id": request.RequestID,
			"results": []any{map[string]any{
				"request_id": request.RequestID, "source_id": 17, "type": "custom", "outcome": "deferred",
				"page_monitor": map[string]any{"outcome": "deferred", "items": []any{map[string]any{
					"title": "fixture prefix", "link": "https://fixture.invalid/a", "description": "safe local fixture",
				}}},
			}},
		})
	}))
	defer server.Close()
	handler := NewHandler(nil, Config{ExtractorURL: server.URL, ExtractorToken: "fixture"})
	d := descriptor{SourceID: 17, Type: "custom"}
	result, code := handler.retryDeferredSource(context.Background(), d, 0)
	if code != "" || !hasCollectedPayload(result) || result.Outcome != "deferred" {
		t.Fatalf("deferred prefix was discarded: result=%+v code=%q", result, code)
	}
	if calls.Load() != 2 {
		t.Fatalf("expected busy retry followed by typed prefix, got %d calls", calls.Load())
	}
}

func TestRetryAfterHintsAreBoundedAndDoNotBeatBackoff(t *testing.T) {
	for _, tc := range []struct {
		name    string
		backoff time.Duration
		hint    time.Duration
		want    time.Duration
	}{
		{name: "backoff dominates shorter hint", backoff: 2 * time.Second, hint: time.Second, want: 2 * time.Second},
		{name: "hint raises backoff", backoff: 250 * time.Millisecond, hint: 3 * time.Second, want: 3 * time.Second},
		{name: "hint is capped", backoff: 250 * time.Millisecond, hint: 12 * time.Second, want: 5 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := boundedRetryDelay(tc.backoff, tc.hint); got != tc.want {
				t.Fatalf("boundedRetryDelay(%s,%s)=%s, want %s", tc.backoff, tc.hint, got, tc.want)
			}
		})
	}
	if got := boundedRetryAfterHeader("8"); got != 5*time.Second {
		t.Fatalf("HTTP Retry-After was not capped: %s", got)
	}
	if got := boundedRetryAfterHeader("invalid"); got != 0 {
		t.Fatalf("invalid HTTP Retry-After should be ignored: %s", got)
	}
	if got := retryDelayForAttempt(0, 250*time.Millisecond, 3*time.Second); got != 3*time.Second {
		t.Fatalf("initial HTTP hint must delay the first retry: %s", got)
	}
	if got := retryDelayForAttempt(0, 250*time.Millisecond, 0); got != 0 {
		t.Fatalf("initial request should not incur retry delay without a hint: %s", got)
	}
	seconds := 9
	failure := &struct {
		Code              string `json:"code"`
		RetryAfterSeconds *int   `json:"retry_after_seconds"`
	}{Code: "busy", RetryAfterSeconds: &seconds}
	if got := collectorRetryAfter(failure); got != 5*time.Second {
		t.Fatalf("collector retry_after_seconds was not capped: %s", got)
	}
}

func TestTelegramEnrichmentUsesExtractorAndPreservesRichFields(t *testing.T) {
	content := "Forwarded from @source\nНовая статья #Тест #news #Тест\n" + strings.Repeat("текст ", 12)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/extract" || r.Header.Get("Authorization") != "Bearer fixture" {
			t.Errorf("unexpected extractor request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		var request struct {
			RequestID string `json:"request_id"`
			URL       string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"contract_version": "1", "build_version": "fixture", "request_id": request.RequestID,
			"outcome": "success", "original_url": request.URL, "final_url": request.URL,
			"result":       map[string]any{"content": content, "title": "ignored by legacy policy", "media": []any{}},
			"observations": []any{},
		})
	}))
	defer server.Close()
	handler := NewHandler(nil, Config{ExtractorURL: server.URL, ExtractorToken: "fixture"})
	article := telegramArticle{Title: "Original title", URL: "https://t.me/c/1", Content: "short post",
		MediaFiles: json.RawMessage(`[{"type":"photo","url":"https://fixture.invalid/image"}]`),
		RawData:    map[string]any{"original_link": "https://fixture.invalid/article", "preserved": true}}
	if code := handler.enrichTelegramArticle(context.Background(), &article, article.RawData["original_link"].(string)); code != "" {
		t.Fatalf("unexpected extractor outcome: %q", code)
	}
	if article.Content != content || article.Title != "@source" {
		t.Fatalf("content/title do not match legacy enrichment behavior: title=%q content=%q", article.Title, article.Content)
	}
	if article.RawData["hashtags"] == nil || article.RawData["content_length"] != len([]rune(content)) || article.RawData["preserved"] != true {
		t.Fatalf("legacy metadata policy or arbitrary metadata was not preserved: %#v", article.RawData)
	}
	if string(article.MediaFiles) != `[{"type":"photo","url":"https://fixture.invalid/image"}]` {
		t.Fatalf("rich media field changed during enrichment: %s", article.MediaFiles)
	}
}

func TestTelegramEnrichmentBusyPreservesOriginalPost(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer server.Close()
	handler := NewHandler(nil, Config{ExtractorURL: server.URL, ExtractorToken: "fixture"})
	article := telegramArticle{Title: "Original", Content: "short", RawData: map[string]any{"original_link": "https://fixture.invalid/article"}}
	if code := handler.enrichTelegramArticle(context.Background(), &article, article.RawData["original_link"].(string)); code != "busy" {
		t.Fatalf("busy outcome lost: %q", code)
	}
	if article.Title != "Original" || article.Content != "short" {
		t.Fatalf("failed optional enrichment modified source post: %+v", article)
	}
}

func TestTelegramEnrichmentNativeBrowserRetryMatchesEligibility(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			RequestID  string `json:"request_id"`
			URL        string `json:"url"`
			UseBrowser bool   `json:"use_browser"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Error(err)
			return
		}
		attempt := calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		if attempt == 1 {
			if request.UseBrowser {
				t.Error("first extraction attempt unexpectedly enabled native browser")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"contract_version": "1", "request_id": request.RequestID, "original_url": request.URL,
				"outcome": "rejected", "result": map[string]any{},
				"error": map[string]any{"code": "browser_required", "retryable": true},
			})
			return
		}
		if !request.UseBrowser {
			t.Error("eligible retry did not enable native browser")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"contract_version": "1", "request_id": request.RequestID, "original_url": request.URL,
			"outcome": "success", "result": map[string]any{"content": strings.Repeat("long article ", 4)},
		})
	}))
	defer server.Close()
	handler := NewHandler(nil, Config{ExtractorURL: server.URL, ExtractorToken: "fixture", ExtractorNativeBrowser: true})
	article := telegramArticle{Title: "Original", Content: "short", RawData: map[string]any{}}
	if code := handler.enrichTelegramArticle(context.Background(), &article, "https://fixture.invalid/article"); code != "" {
		t.Fatalf("browser retry failed: %q", code)
	}
	if calls.Load() != 2 {
		t.Fatalf("eligible HTTPS article got %d attempts, want 2", calls.Load())
	}

	calls.Store(0)
	article = telegramArticle{Title: "Original", Content: "short", RawData: map[string]any{}}
	if code := handler.enrichTelegramArticle(context.Background(), &article, "http://fixture.invalid/article"); code != "browser_required" {
		t.Fatalf("ineligible URL lost first-attempt status: %q", code)
	}
	if calls.Load() != 1 {
		t.Fatalf("ineligible HTTP article got %d attempts, want 1", calls.Load())
	}
}

func TestPageMonitorPartialPrefixKeepsUndeliveredScanTail(t *testing.T) {
	var response collectResult
	if err := json.Unmarshal([]byte(`{"type":"custom","outcome":"partial","page_monitor":{"outcome":"partial","items":[{"title":"Fixture","link":"https://fixture.invalid/a","description":"body"}],"next_offset":2}}`), &response); err != nil {
		t.Fatal(err)
	}
	state := pageState(store.PageState{ScanLimit: 5}, response.PageMonitor, map[string]any{"max_articles_per_check": float64(5)})
	acknowledgePageItems(state, response.PageMonitor)
	if !state.ScanActive || state.CursorOffset != 2 || state.ScanHashes["0"] == "" || state.AcknowledgedHashes["0"] == "" {
		t.Fatalf("partial prefix lost its continuation state: %+v", state)
	}
}

func TestPageArticleHashMatchesPythonUnicodeCodepointPrefix(t *testing.T) {
	got := pageArticleHash("Заголовок 😀", "https://fixture.invalid/ё", strings.Repeat("a", 99)+"éTAIL")
	const want = "c430c5fc47a73cbc24a358545595cf59"
	if got != want {
		t.Fatalf("Python-compatible Unicode hash mismatch: got %s want %s", got, want)
	}
}
