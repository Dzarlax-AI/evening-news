package service

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
)

type sourceManagerTestRunner struct {
	RSS      func(context.Context, RSSRequest) RSSResponse
	Telegram func(context.Context, TelegramRequest) TelegramResponse
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
func (r sourceManagerTestRunner) RunPageMonitor(_ context.Context, req PageMonitorRequest) PageMonitorResponse {
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
