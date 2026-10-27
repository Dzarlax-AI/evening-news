package service

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/pipeline"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

type sourceManagerTestRunner struct {
	RSS         func(context.Context, RSSRequest) RSSResponse
	Telegram    func(context.Context, TelegramRequest) TelegramResponse
	PageMonitor func(context.Context, PageMonitorRequest) PageMonitorResponse
}

func (r sourceManagerTestRunner) Run(context.Context, Request) Response {
	return Response{Outcome: "success"}
}
func (r sourceManagerTestRunner) RunRSS(ctx context.Context, req RSSRequest) RSSResponse {
	if r.RSS != nil {
		return r.RSS(ctx, req)
	}
	return RSSResponse{Outcome: "success", FinalURL: req.URL, Articles: []extractor.RSSArticle{}}
}
func (r sourceManagerTestRunner) RunTelegram(ctx context.Context, req TelegramRequest) TelegramResponse {
	if r.Telegram != nil {
		return r.Telegram(ctx, req)
	}
	return TelegramResponse{Outcome: "success", OriginalURL: req.ChannelURL, FinalURL: req.ChannelURL, Articles: []extractor.TelegramArticle{}}
}
func (r sourceManagerTestRunner) RunPageMonitor(ctx context.Context, req PageMonitorRequest) PageMonitorResponse {
	if r.PageMonitor != nil {
		return r.PageMonitor(ctx, req)
	}
	return PageMonitorResponse{Outcome: "success", OriginalURL: req.URL, FinalURL: req.URL, Items: []extractor.PageMonitorItem{}, SelectorsUsed: []string{}}
}

type deadlineRecorder struct {
	*httptest.ResponseRecorder
	deadline time.Time
}

func (w *deadlineRecorder) SetWriteDeadline(deadline time.Time) error {
	w.deadline = deadline
	return nil
}

func sourceBatchPost(s *Server, body string, writer http.ResponseWriter) {
	r := httptest.NewRequest(http.MethodPost, "/v1/sources/collect", strings.NewReader(body))
	r.Header.Set("Authorization", "Bearer 0123456789abcdef")
	s.ServeHTTP(writer, r)
}

func TestSourceBatchCollectPreservesOrderAndIndependentOutcomes(t *testing.T) {
	var called atomic.Int32
	runner := sourceManagerTestRunner{RSS: func(_ context.Context, req RSSRequest) RSSResponse {
		called.Add(1)
		if req.SourceName == "broken" {
			return RSSResponse{Outcome: "failed", Error: "upstream_timeout", Retryable: true}
		}
		return RSSResponse{Outcome: "success", FinalURL: req.URL, Articles: []extractor.RSSArticle{}}
	}}
	s, err := New(Config{Token: "0123456789abcdef", MaxConcurrent: 1}, runner)
	if err != nil {
		t.Fatal(err)
	}
	writer := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	body := `{"contract_version":"1","request_id":"batch-1","batch_budget_ms":1000,"sources":[{"source_id":9,"type":"rss","source_name":"ok","url":"https://example.org/feed"},{"source_id":3,"type":"rss","source_name":"broken","url":"https://example.org/other"},{"source_id":4,"type":"rss","source_name":"disabled","url":"https://example.org/off","enabled":false},{"source_id":5,"type":"unknown","source_name":"bad","url":"https://example.org/bad"}]}`
	sourceBatchPost(s, body, writer)
	if writer.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", writer.Code, writer.Body.String())
	}
	var got SourceCollectResponse
	if err := json.Unmarshal(writer.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Results) != 4 || got.Results[0].SourceID != 9 || got.Results[1].SourceID != 3 || got.Results[2].SourceID != 4 || got.Results[3].SourceID != 5 {
		t.Fatalf("result order/IDs = %+v", got.Results)
	}
	if got.Results[0].Outcome != "success" || got.Results[1].Outcome != "failed" || got.Results[1].Error.Code != "upstream_timeout" || got.Results[2].Outcome != "rejected" || got.Results[2].Error.Code != "source_disabled" || got.Results[3].Outcome != "rejected" {
		t.Fatalf("outcomes = %+v", got.Results)
	}
	if called.Load() != 2 {
		t.Fatalf("RSS runner called %d times, want 2", called.Load())
	}
	if writer.deadline.Before(time.Now().Add(5 * time.Second)) {
		t.Fatalf("write deadline was not extended: %s", writer.deadline)
	}
	article := httptest.NewRecorder()
	articleRequest := httptest.NewRequest(http.MethodPost, "/v1/extract", strings.NewReader(`{"request_id":"after-batch","url":"https://example.org/article","content":"hello"}`))
	articleRequest.Header.Set("Authorization", "Bearer 0123456789abcdef")
	s.ServeHTTP(article, articleRequest)
	if article.Code != http.StatusOK {
		t.Fatalf("article request after batch = %d %s", article.Code, article.Body.String())
	}
}

func TestSourceBatchPageMonitorDefaultsCollectSyntheticArticleWithoutAI(t *testing.T) {
	const fixtureURL = "https://pm.fixture.test/news"
	const html = `<html><main><article class="news-item"><h2><a href="/story">Ordinary synthetic article title</a></h2><p>Fixture body.</p></article></main></html>`
	var articleCount atomic.Int32
	runner := sourceManagerTestRunner{PageMonitor: func(ctx context.Context, req PageMonitorRequest) PageMonitorResponse {
		if req.UseBrowser == nil || *req.UseBrowser || req.WaitForJS == nil || *req.WaitForJS {
			t.Errorf("HTTP-only synthetic collector request changed: use_browser=%v wait_for_js=%v", req.UseBrowser, req.WaitForJS)
		}
		if len(req.ArticleSelectors) == 0 || req.ArticleSelectors[0] != "article" || len(req.TitleSelectors) == 0 || req.TitleSelectors[0] != "h1" {
			t.Errorf("Python default selector groups did not reach collector: article=%v title=%v", req.ArticleSelectors, req.TitleSelectors)
		}
		parser := pipeline.PageMonitorSource{
			Fetch: func(context.Context, string, transport.Options) (transport.Document, error) {
				return transport.Document{HTML: []byte(html), StatusCode: http.StatusOK, URL: fixtureURL}, nil
			},
			Now: func() time.Time { return time.Date(2026, 10, 24, 12, 0, 0, 0, time.UTC) },
		}
		out := parser.Extract(ctx, pipeline.PageMonitorRequest{
			Operation: "fetch", URL: req.URL, SourceName: req.SourceName, Budget: time.Duration(req.BudgetMS) * time.Millisecond,
			Limit: req.Limit, Offset: req.Offset, MinTitleLength: req.MinTitleLength,
			UseBrowser: false, WaitForJS: false,
			Selectors:         extractor.PageMonitorSelectors{Article: req.ArticleSelectors, Title: req.TitleSelectors, Date: req.DateSelectors, Link: req.LinkSelectors},
			LearnedContainers: req.LearnedContainerSelectors, LearnedTitles: req.LearnedTitleSelectors,
			LearnedLinks: req.LearnedLinkSelectors, LearnedDates: req.LearnedDateSelectors, LearnedGeneral: req.LearnedGeneralSelectors,
		})
		articleCount.Store(int32(len(out.Items)))
		return PageMonitorResponse{Outcome: out.Outcome, OriginalURL: out.OriginalURL, FinalURL: out.FinalURL,
			FetchMethod: out.FetchMethod, SnapshotAt: &out.SnapshotAt, ContentHash: out.ContentHash,
			SelectorsUsed: out.SelectorsUsed, Items: out.Items, NextOffset: out.NextOffset,
			NeedsSelectorStudy: out.NeedsSelectorStudy, StudyHTML: out.StudyHTML}
	}}
	server, err := New(Config{Token: "0123456789abcdef", BuildVersion: "fixture", MaxConcurrent: 1}, runner)
	if err != nil {
		t.Fatal(err)
	}
	useBrowser, waitForJS := false, false
	request := SourceCollectRequest{ContractVersion: "1", RequestID: "defaults-no-ai", Sources: []SourceDescriptor{{
		SourceID: 51, Type: "custom", SourceName: "Fixture PM", URL: fixtureURL,
		PageMonitor: &PageMonitorSourceConfig{
			UseBrowser: &useBrowser, WaitForJS: &waitForJS, Limit: 20, MinTitleLength: 10,
			ArticleSelectors: []string{"article", ".article", ".news-item", ".post", ".entry", ".changelog-item", ".update-item", ".release-note", ".content li", ".main li", "ul.updates li", "ul.news li", ".content > div", ".main > div", ".updates > div", `[class*="item"]`, `[class*="post"]`, `[class*="article"]`, "h2, h3, h4", ".title", ".headline", ".heading"},
			TitleSelectors:   []string{"h1", "h2", "h3", "h4", ".title", ".headline", ".heading", "a[href]", ".link", ".post-title", ".article-title"},
			DateSelectors:    []string{"time", "time[datetime]", "[datetime]", ".date", ".timestamp", ".published", ".created", ".updated", ".publish-date", ".published-date", ".creation-date", ".post-date", ".article-date", ".news-date", ".entry-date", ".blog-date", ".meta .date", ".meta time", ".metadata .date", ".info .date", ".post-meta .date", ".article-meta .date", ".entry-meta .date", ".commit-date", ".release-date", ".version-date", ".update-date", ".changelog-date", ".timeline .date", ".byline .date", ".author-date", ".publish-info .date", `[class*="date"]`, `[class*="time"]`, `[class*="publish"]`, `[id*="date"]`, `[id*="time"]`},
			LinkSelectors:    []string{"a[href]", ".link", ".read-more", ".permalink"},
		},
	}}}
	bodyBytes, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	writer := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	sourceBatchPost(server, string(bodyBytes), writer)
	if writer.Code != http.StatusOK {
		t.Fatalf("collector API status=%d body=%s", writer.Code, writer.Body.String())
	}
	var response SourceCollectResponse
	if err := json.Unmarshal(writer.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(response.Results) != 1 || response.Results[0].Outcome != "success" || response.Results[0].PageMonitor == nil ||
		len(response.Results[0].PageMonitor.Items) != 1 || response.Results[0].PageMonitor.Items[0].Title != "Ordinary synthetic article title" ||
		response.Results[0].PageMonitor.Items[0].Link != "https://pm.fixture.test/story" || articleCount.Load() != 1 {
		t.Fatalf("default-selector synthetic collection failed: results=%+v count=%d", response.Results, articleCount.Load())
	}
}

func TestConcurrentPageMonitorDiagnosticsStayScopedAndSanitized(t *testing.T) {
	var output bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previousOutput) })

	started := make(chan struct{}, 2)
	release := make(chan struct{})
	runner := sourceManagerTestRunner{PageMonitor: func(ctx context.Context, req PageMonitorRequest) PageMonitorResponse {
		started <- struct{}{}
		select {
		case <-release:
		case <-ctx.Done():
			t.Errorf("source %s context ended before release: %v", req.RequestID, ctx.Err())
		}
		// The first source carries attacker-shaped values; the second has a distinct
		// valid failure. Their concurrent log fields must remain request-scoped.
		if req.RequestID == "source-41" {
			transport.RecordPageMonitorDiagnostic(ctx, "navigate\nLEAK_SOURCE", "https://secret.example/path")
		} else {
			transport.RecordPageMonitorDiagnostic(ctx, "dom", "evaluation_failed")
		}
		return PageMonitorResponse{Outcome: "failed", OriginalURL: req.URL, FinalURL: req.URL,
			Items: []extractor.PageMonitorItem{}, SelectorsUsed: []string{},
			Error: &Failure{Code: "transport_failed\nLEAK_CODE", Retryable: true}}
	}}
	s, err := New(Config{Token: "0123456789abcdef", MaxConcurrent: 2, NativeBrowser: true}, runner)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"contract_version":"1","request_id":"batch-secret-id","batch_budget_ms":2000,"sources":[{"source_id":41,"type":"custom","source_name":"source-secret-one","url":"https://source-one-secret.test/path"},{"source_id":82,"type":"custom","source_name":"source-secret-two","url":"https://source-two-secret.test/path"}]}`
	writer := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	done := make(chan struct{})
	go func() { sourceBatchPost(s, body, writer); close(done) }()
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(2 * time.Second):
			close(release)
			t.Fatal("both custom source requests did not enter concurrently")
		}
	}
	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("source batch did not complete")
	}

	logs := output.String()
	if strings.Contains(logs, "batch-secret-id") || strings.Contains(logs, "source-secret") || strings.Contains(logs, "LEAK_") || strings.Contains(logs, "secret.example") {
		t.Fatalf("diagnostic log leaked request payload: %s", logs)
	}
	tokenHash := sha256.Sum256([]byte("batch-secret-id"))
	token := hex.EncodeToString(tokenHash[:])
	if strings.Count(logs, "event=page_monitor_failure") != 2 {
		t.Fatalf("failure log count = %d, want one per failed source: %s", strings.Count(logs, "event=page_monitor_failure"), logs)
	}
	for _, want := range []string{"request_token=" + token, "source_id=41", "source_id=82", "stage=other", "reason=operation_failed", "stage=dom", "reason=evaluation_failed", "outcome=failed", "error_code=other"} {
		if !strings.Contains(logs, want) {
			t.Errorf("diagnostics missing %q: %s", want, logs)
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(logs), "\n") {
		switch {
		case strings.Contains(line, "source_id=41"):
			if !strings.Contains(line, "stage=other") || !strings.Contains(line, "reason=operation_failed") {
				t.Errorf("source 41 received another request's diagnostic: %s", line)
			}
		case strings.Contains(line, "source_id=82"):
			if !strings.Contains(line, "stage=dom") || !strings.Contains(line, "reason=evaluation_failed") {
				t.Errorf("source 82 received another request's diagnostic: %s", line)
			}
		}
	}
}

func TestDirectPageMonitorDiagnosticUsesServerToken(t *testing.T) {
	var output bytes.Buffer
	previousOutput := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previousOutput) })
	runner := sourceManagerTestRunner{PageMonitor: func(ctx context.Context, req PageMonitorRequest) PageMonitorResponse {
		transport.RecordPageMonitorDiagnostic(ctx, "navigate", "navigation_dns")
		return PageMonitorResponse{Outcome: "failed", OriginalURL: req.URL, FinalURL: req.URL,
			Items: []extractor.PageMonitorItem{}, SelectorsUsed: []string{}, Error: &Failure{Code: "transport_failed", Retryable: true}}
	}}
	s, err := New(Config{Token: "0123456789abcdef", MaxConcurrent: 1}, runner)
	if err != nil {
		t.Fatal(err)
	}
	body := `{"contract_version":"1","request_id":"caller-request-secret","operation":"fetch","url":"https://origin-secret.test/feed","source_name":"source-secret","budget_ms":2000,"use_browser":false,"wait_for_js":false}`
	request := httptest.NewRequest(http.MethodPost, "/v1/page-monitor/extract", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer 0123456789abcdef")
	writer := httptest.NewRecorder()
	s.ServeHTTP(writer, request)
	if writer.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", writer.Code, writer.Body.String())
	}
	logs := output.String()
	for _, secret := range []string{"caller-request-secret", "origin-secret", "source-secret"} {
		if strings.Contains(logs, secret) {
			t.Fatalf("direct diagnostic leaked %q: %s", secret, logs)
		}
	}
	if strings.Count(logs, "event=page_monitor_failure") != 1 || !strings.Contains(logs, "stage=navigate") || !strings.Contains(logs, "reason=navigation_dns") {
		t.Fatalf("unexpected direct diagnostic: %s", logs)
	}
}

func TestNativeSourceBatchLetsTelegramHTTPJobsOverlap(t *testing.T) {
	for _, tc := range []struct {
		name, requestID string
		native          bool
		wantActive      int32
	}{
		{name: "native", requestID: "native-tg-parallel", native: true, wantActive: 5},
		{name: "legacy", requestID: "legacy-tg-serialized", wantActive: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var active, maxActive atomic.Int32
			started := make(chan struct{}, 5)
			release := make(chan struct{})
			runner := sourceManagerTestRunner{Telegram: func(ctx context.Context, req TelegramRequest) TelegramResponse {
				current := active.Add(1)
				defer active.Add(-1)
				for prior := maxActive.Load(); current > prior && !maxActive.CompareAndSwap(prior, current); prior = maxActive.Load() {
				}
				started <- struct{}{}
				select {
				case <-release:
				case <-ctx.Done():
					return TelegramResponse{Outcome: "failed", Error: &Failure{Code: "cancelled"}}
				}
				return TelegramResponse{Outcome: "success", OriginalURL: req.ChannelURL, FinalURL: req.ChannelURL, Articles: []extractor.TelegramArticle{}}
			}}
			s, err := New(Config{Token: "0123456789abcdef", MaxConcurrent: 1, NativeBrowser: tc.native}, runner)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan *deadlineRecorder, 1)
			go func() {
				writer := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
				var body strings.Builder
				fmt.Fprintf(&body, `{"contract_version":"1","request_id":%q,"batch_budget_ms":3000,"sources":[`, tc.requestID)
				for i := 1; i <= 5; i++ {
					if i > 1 {
						body.WriteByte(',')
					}
					fmt.Fprintf(&body, `{"source_id":%d,"type":"telegram","source_name":"channel-%d","url":"https://t.me/channel_%d"}`, i, i, i)
				}
				body.WriteString("]}")
				sourceBatchPost(s, body.String(), writer)
				done <- writer
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("no Telegram HTTP job started")
			}
			if tc.native {
				for i := 1; i < 5; i++ {
					select {
					case <-started:
					case <-time.After(time.Second):
						close(release)
						t.Fatalf("only %d Telegram HTTP jobs started before release", i)
					}
				}
			} else {
				select {
				case <-started:
					close(release)
					t.Fatal("legacy browser source lane allowed a sibling to start")
				case <-time.After(25 * time.Millisecond):
				}
			}
			close(release)
			select {
			case writer := <-done:
				if writer.Code != http.StatusOK {
					t.Fatalf("source batch status=%d body=%s", writer.Code, writer.Body.String())
				}
			case <-time.After(time.Second):
				t.Fatal("source batch did not finish after Telegram requests were released")
			}
			if maxActive.Load() != tc.wantActive {
				t.Fatalf("Telegram HTTP concurrency=%d, want %d", maxActive.Load(), tc.wantActive)
			}
		})
	}
}

func TestSourceBatchMalformedRSSFailureKeepsMixedBatchValid(t *testing.T) {
	runner := sourceManagerTestRunner{RSS: func(_ context.Context, req RSSRequest) RSSResponse {
		if req.SourceName == "malformed" {
			return RSSResponse{Outcome: "failed"}
		}
		return RSSResponse{Outcome: "success", FinalURL: req.URL, Articles: []extractor.RSSArticle{}}
	}}
	s, err := New(Config{Token: "0123456789abcdef"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	writer := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	body := `{"contract_version":"1","request_id":"mixed-failure","sources":[{"source_id":1,"type":"rss","source_name":"first","url":"https://example.org/first"},{"source_id":2,"type":"rss","source_name":"malformed","url":"https://example.org/malformed"},{"source_id":3,"type":"rss","source_name":"last","url":"https://example.org/last"}]}`
	sourceBatchPost(s, body, writer)
	if writer.Code != http.StatusOK {
		t.Fatalf("status = %d body=%s", writer.Code, writer.Body.String())
	}
	var got SourceCollectResponse
	if err := json.Unmarshal(writer.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Results) != 3 || got.Results[0].Outcome != "success" || got.Results[1].Outcome != "failed" || got.Results[1].Error == nil || got.Results[1].Error.Code != "invalid_runner_response" || got.Results[2].Outcome != "success" {
		t.Fatalf("mixed batch outcomes = %+v", got.Results)
	}
}

func TestSourceBatchRejectsDuplicateIDsAndFailsClosedWithoutDeadlineSupport(t *testing.T) {
	s, err := New(Config{Token: "0123456789abcdef"}, sourceManagerTestRunner{})
	if err != nil {
		t.Fatal(err)
	}
	body := `{"contract_version":"1","request_id":"dupe","sources":[{"source_id":1,"type":"rss","source_name":"a","url":"https://example.org/a"},{"source_id":1,"type":"rss","source_name":"b","url":"https://example.org/b"}]}`
	writer := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	sourceBatchPost(s, body, writer)
	if writer.Code != http.StatusBadRequest || !strings.Contains(writer.Body.String(), "invalid_request") && !strings.Contains(writer.Body.String(), "duplicate_source_id") {
		t.Fatalf("duplicate response = %d %s", writer.Code, writer.Body.String())
	}
	unsupported := httptest.NewRecorder()
	sourceBatchPost(s, `{"contract_version":"1","request_id":"no-deadline","sources":[]}`, unsupported)
	if unsupported.Code != http.StatusServiceUnavailable || !strings.Contains(unsupported.Body.String(), "write_deadline_unavailable") {
		t.Fatalf("unsupported deadline response = %d %s", unsupported.Code, unsupported.Body.String())
	}
}

func TestSourceBatchAggregateCapDefersWholeSourceAndIndividualCapRejects(t *testing.T) {
	large := strings.Repeat("x", 5*1024*1024)
	runner := sourceManagerTestRunner{RSS: func(_ context.Context, req RSSRequest) RSSResponse {
		if req.SourceName == "too-large" {
			individual := strings.Repeat("y", MaxSourceBatchBytes)
			return RSSResponse{Outcome: "success", FinalURL: req.URL, Articles: []extractor.RSSArticle{{Title: "too large", URL: "https://example.org/too-large", Content: &individual}}}
		}
		return RSSResponse{Outcome: "success", FinalURL: req.URL, Articles: []extractor.RSSArticle{{Title: req.SourceName, URL: req.URL, Content: &large}}}
	}}
	s, err := New(Config{Token: "0123456789abcdef"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	writer := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	body := `{"contract_version":"1","request_id":"large","sources":[{"source_id":1,"type":"rss","source_name":"first","url":"https://example.org/first"},{"source_id":2,"type":"rss","source_name":"second","url":"https://example.org/second"},{"source_id":3,"type":"rss","source_name":"too-large","url":"https://example.org/too-large"}]}`
	sourceBatchPost(s, body, writer)
	if writer.Code != http.StatusOK || int64(writer.Body.Len()) > MaxSourceBatchBytes {
		t.Fatalf("bounded status/body = %d/%d", writer.Code, writer.Body.Len())
	}
	var got SourceCollectResponse
	if err := json.Unmarshal(writer.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Results) != 3 || got.Results[0].SourceID != 1 || got.Results[1].SourceID != 2 || got.Results[2].SourceID != 3 || got.Results[2].Outcome != "failed" || got.Results[2].Error == nil || got.Results[2].Error.Code != "resource_limit" || got.Results[2].RSSArticles != nil {
		t.Fatalf("oversize prefix result = %+v", got.Results)
	}
	firstTwoSuccesses, firstTwoDeferred := 0, 0
	for _, result := range got.Results[:2] {
		switch result.Outcome {
		case "success":
			firstTwoSuccesses++
			if len(result.RSSArticles) != 1 || (result.RSSArticles[0].Title != "first" && result.RSSArticles[0].Title != "second") {
				t.Fatalf("successful source payload = %+v", result)
			}
		case "deferred":
			firstTwoDeferred++
			if result.Error == nil || result.Error.Code != "response_budget_exhausted" || result.Error.Retryable != true || result.RSSArticles != nil {
				t.Fatalf("deferred source payload = %+v", result)
			}
		default:
			t.Fatalf("unexpected aggregate outcome = %+v", result)
		}
	}
	if firstTwoSuccesses != 1 || firstTwoDeferred != 1 {
		t.Fatalf("aggregate outcomes success=%d deferred=%d: %+v", firstTwoSuccesses, firstTwoDeferred, got.Results[:2])
	}
}

func TestSourceBatchConcurrentLargeResultsReserveAggregateBeforeRetention(t *testing.T) {
	const contentBytes = 2 * 1024 * 1024
	var active, maxActive atomic.Int32
	runner := sourceManagerTestRunner{RSS: func(ctx context.Context, req RSSRequest) RSSResponse {
		current := active.Add(1)
		for prior := maxActive.Load(); current > prior && !maxActive.CompareAndSwap(prior, current); prior = maxActive.Load() {
		}
		defer active.Add(-1)
		select {
		case <-time.After(20 * time.Millisecond):
		case <-ctx.Done():
			return RSSResponse{Outcome: "failed", Error: "cancelled"}
		}
		content := strings.Repeat("x", contentBytes)
		article := extractor.RSSArticle{Title: req.SourceName, URL: req.URL, Content: &content}
		return RSSResponse{Outcome: "success", FinalURL: req.URL, Articles: []extractor.RSSArticle{article}}
	}}
	s, err := New(Config{Token: "0123456789abcdef"}, runner)
	if err != nil {
		t.Fatal(err)
	}
	var body strings.Builder
	body.WriteString(`{"contract_version":"1","request_id":"concurrent-large","sources":[`)
	for i := 1; i <= 8; i++ {
		if i > 1 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `{"source_id":%d,"type":"rss","source_name":"large-%d","url":"https://example.org/%d"}`, i, i, i)
	}
	body.WriteString(`]}`)
	writer := &deadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
	sourceBatchPost(s, body.String(), writer)
	if writer.Code != http.StatusOK || int64(writer.Body.Len()) > MaxSourceBatchBytes {
		t.Fatalf("bounded response = %d/%d", writer.Code, writer.Body.Len())
	}
	if maxActive.Load() < 2 {
		t.Fatalf("synthetic sources did not overlap: max active=%d", maxActive.Load())
	}
	var got SourceCollectResponse
	if err := json.Unmarshal(writer.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	var retained int64
	var successes, deferred int
	for i, result := range got.Results {
		if result.SourceID != int64(i+1) {
			t.Fatalf("result order at %d: %+v", i, result)
		}
		switch result.Outcome {
		case "success":
			successes++
			if len(result.RSSArticles) != 1 || result.RSSArticles[0].Content == nil {
				t.Fatalf("success %d lost its article", result.SourceID)
			}
			retained += int64(len(*result.RSSArticles[0].Content))
		case "deferred":
			deferred++
			if len(result.RSSArticles) != 0 || result.Error == nil || result.Error.Code != "response_budget_exhausted" {
				t.Fatalf("deferred source %d retained payload: %+v", result.SourceID, result)
			}
		default:
			t.Fatalf("unexpected outcome for source %d: %s", result.SourceID, result.Outcome)
		}
	}
	if successes == 0 || deferred == 0 || successes+deferred != 8 || retained > MaxSourceBatchBytes {
		t.Fatalf("retained aggregate outcomes success=%d deferred=%d bytes=%d", successes, deferred, retained)
	}
}

type pipeListener struct {
	conn net.Conn
	done chan struct{}
	used bool
}

func (l *pipeListener) Accept() (net.Conn, error) {
	if l.used {
		<-l.done
		return nil, net.ErrClosed
	}
	l.used = true
	return l.conn, nil
}
func (l *pipeListener) Close() error {
	select {
	case <-l.done:
	default:
		close(l.done)
	}
	return nil
}
func (l *pipeListener) Addr() net.Addr { return pipeAddr("source-batch-test") }

type pipeAddr string

func (a pipeAddr) Network() string { return "pipe" }
func (a pipeAddr) String() string  { return string(a) }

func TestSourceBatchOverridesShortServerWriteTimeout(t *testing.T) {
	s, err := New(Config{Token: "0123456789abcdef"}, sourceManagerTestRunner{RSS: func(ctx context.Context, req RSSRequest) RSSResponse {
		select {
		case <-time.After(60 * time.Millisecond):
		case <-ctx.Done():
			return RSSResponse{Outcome: "failed", Error: "cancelled"}
		}
		return RSSResponse{Outcome: "success", FinalURL: req.URL, Articles: []extractor.RSSArticle{}}
	}})
	if err != nil {
		t.Fatal(err)
	}
	serverConn, clientConn := net.Pipe()
	listener := &pipeListener{conn: serverConn, done: make(chan struct{})}
	server := &http.Server{Handler: s, WriteTimeout: 10 * time.Millisecond}
	go func() { _ = server.Serve(listener) }()
	defer func() { _ = server.Close(); _ = clientConn.Close() }()
	body := `{"contract_version":"1","request_id":"pipe-deadline","batch_budget_ms":1000,"sources":[{"source_id":1,"type":"rss","source_name":"slow","url":"https://example.org/feed"}]}`
	if _, err := fmt.Fprintf(clientConn, "POST /v1/sources/collect HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer 0123456789abcdef\r\nContent-Type: application/json\r\nContent-Length: %d\r\nConnection: close\r\n\r\n%s", len(body), body); err != nil {
		t.Fatal(err)
	}
	if err := clientConn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	response, err := http.ReadResponse(bufio.NewReader(clientConn), &http.Request{Method: http.MethodPost})
	if err != nil {
		t.Fatalf("read response despite short initial server deadline: %v", err)
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusOK || !strings.Contains(string(payload), `"source_id":1`) {
		t.Fatalf("response = %d %s", response.StatusCode, payload)
	}
}
