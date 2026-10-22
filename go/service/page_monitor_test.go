package service

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
)

type pageMonitorTestRunner struct {
	seen     PageMonitorRequest
	response PageMonitorResponse
	called   int
	onRun    func(context.Context, PageMonitorRequest) PageMonitorResponse
}

func (r *pageMonitorTestRunner) Run(context.Context, Request) Response {
	return Response{Outcome: "success"}
}
func (r *pageMonitorTestRunner) RunPageMonitor(ctx context.Context, req PageMonitorRequest) PageMonitorResponse {
	r.called++
	r.seen = req
	if r.onRun != nil {
		return r.onRun(ctx, req)
	}
	return r.response
}

func pageMonitorPost(s *Server, body string, authorized bool) *httptest.ResponseRecorder {
	r := httptest.NewRequest("POST", "/v1/page-monitor/extract", strings.NewReader(body))
	if authorized {
		r.Header.Set("Authorization", "Bearer "+testToken)
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	return w
}

func TestPageMonitorWireValidationAndDefaults(t *testing.T) {
	runner := &pageMonitorTestRunner{response: PageMonitorResponse{Outcome: "success", Items: []extractor.PageMonitorItem{}}}
	s, err := New(Config{Token: testToken, MaxConcurrent: 1, BrowserAvailable: func() bool { return true }}, runner)
	if err != nil {
		t.Fatal(err)
	}
	valid := `{"contract_version":"1","request_id":"pm","operation":"fetch","url":"https://example.org/updates","source_name":"Example","budget_ms":30000,"article_selectors":["article"]}`
	if w := pageMonitorPost(s, valid, true); w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	if runner.called != 1 || runner.seen.UseBrowser == nil || !*runner.seen.UseBrowser || runner.seen.WaitForJS == nil || !*runner.seen.WaitForJS || runner.seen.Limit != 20 || runner.seen.Offset != 0 || runner.seen.MinTitleLength != 10 {
		t.Fatalf("defaults not preserved: %+v", runner.seen)
	}
	tests := []struct {
		name, body string
		status     int
	}{{"wrong_mode_html", strings.Replace(valid, `"operation":"fetch"`, `"operation":"fetch","provided_html":"<p>x</p>"`, 1), 400}, {"missing_parse_snapshot", `{"contract_version":"1","request_id":"pm","operation":"parse","url":"https://example.org/","source_name":"Example","budget_ms":1000,"provided_html":""}`, 400}, {"parse_browser_options", `{"contract_version":"1","request_id":"pm","operation":"parse","url":"https://example.org/","source_name":"Example","budget_ms":1000,"provided_html":"","snapshot_at":"2026-10-22T00:00:00Z","use_browser":false}`, 400}, {"large_selector", strings.Replace(valid, `"article"`, `"`+strings.Repeat("x", 2049)+`"`, 1), 400}, {"offset_at_window_end", strings.Replace(valid, `"budget_ms":30000`, `"budget_ms":30000,"offset":20`, 1), 400}}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			w := pageMonitorPost(s, tc.body, true)
			if w.Code != tc.status {
				t.Fatalf("status=%d want=%d body=%s", w.Code, tc.status, w.Body.String())
			}
		})
	}
	if w := pageMonitorPost(s, valid, false); w.Code != 401 {
		t.Fatalf("unauthorized status=%d", w.Code)
	}
	withOffset := strings.Replace(valid, `"budget_ms":30000`, `"budget_ms":30000,"limit":5,"offset":3`, 1)
	if w := pageMonitorPost(s, withOffset, true); w.Code != 200 || runner.seen.Offset != 3 || runner.seen.Limit != 5 {
		t.Fatalf("explicit offset was not passed through: %s, request=%+v", w.Body.String(), runner.seen)
	}
}

func TestPageMonitorParseSnapshotMustBeUTCAndHasNoBrowserOptions(t *testing.T) {
	runner := &pageMonitorTestRunner{response: PageMonitorResponse{Outcome: "success", Items: []extractor.PageMonitorItem{}}}
	s, _ := New(Config{Token: testToken}, runner)
	body := `{"contract_version":"1","request_id":"parse","operation":"parse","url":"https://example.org/","source_name":"Example","budget_ms":1000,"provided_html":"","snapshot_at":"2026-10-22T00:00:00Z"}`
	if w := pageMonitorPost(s, body, true); w.Code != 200 {
		t.Fatalf("parse failed: %s", w.Body.String())
	}
	if runner.seen.ProvidedHTML == nil || runner.seen.SnapshotAt == nil || runner.seen.UseBrowser != nil {
		t.Fatalf("parse contract changed: %+v", runner.seen)
	}
	bad := strings.Replace(body, "00:00:00Z", "02:00:00+02:00", 1)
	if w := pageMonitorPost(s, bad, true); w.Code != 400 {
		t.Fatalf("offset timestamp accepted: %s", w.Body.String())
	}
	zeroOffset := strings.Replace(body, "2026-10-22T00:00:00Z", "2026-10-22T00:00:00+00:00", 1)
	if w := pageMonitorPost(s, zeroOffset, true); w.Code != 200 || runner.seen.SnapshotAt == nil || runner.seen.SnapshotAt.Location() != time.UTC {
		t.Fatalf("zero-offset timestamp should normalize to UTC: %s, %+v", w.Body.String(), runner.seen.SnapshotAt)
	}
}

func TestPageMonitorAllowsLongSelectedTitleMinimum(t *testing.T) {
	runner := &pageMonitorTestRunner{response: PageMonitorResponse{Outcome: "success", Items: []extractor.PageMonitorItem{}}}
	s, _ := New(Config{Token: testToken}, runner)
	body := `{"contract_version":"1","request_id":"long","operation":"parse","url":"https://example.org/","source_name":"Example","budget_ms":1000,"min_title_length":220,"provided_html":"","snapshot_at":"2026-10-22T00:00:00Z"}`
	if w := pageMonitorPost(s, body, true); w.Code != 200 || runner.seen.MinTitleLength != 220 {
		t.Fatalf("long selected-title minimum rejected: %s", w.Body.String())
	}
}

func TestPageMonitorCancellationRetainsRunnerPrefix(t *testing.T) {
	item := extractor.PageMonitorItem{Title: "Accepted prefix", Published: time.Date(2026, 10, 22, 0, 0, 0, 0, time.UTC)}
	runner := &pageMonitorTestRunner{onRun: func(ctx context.Context, _ PageMonitorRequest) PageMonitorResponse {
		<-ctx.Done()
		return PageMonitorResponse{Outcome: "partial", Items: []extractor.PageMonitorItem{item}, NeedsSelectorStudy: true, StudyHTML: "large captured html"}
	}}
	s, _ := New(Config{Token: testToken}, runner)
	body := `{"contract_version":"1","request_id":"cancel","operation":"fetch","url":"https://example.org/","source_name":"Example","budget_ms":1,"use_browser":false}`
	w := pageMonitorPost(s, body, true)
	var got PageMonitorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Items) != 1 || got.Outcome != "partial" || got.Error == nil || got.Error.Code != "deadline_exceeded" || got.StudyHTML != "" || got.NextOffset == nil || *got.NextOffset != 1 {
		t.Fatalf("prefix not preserved across deadline: %+v", got)
	}
}

func TestPageMonitorResponseEscapedSizeAndPrefix(t *testing.T) {
	items := []extractor.PageMonitorItem{
		{Title: "Новости <one>"},
		{Title: "Second item", Description: strings.Repeat("é", MaxJSONBytes)},
	}
	resp := boundPageMonitorResponse(PageMonitorResponse{
		ContractVersion: "1", BuildVersion: "test", RequestID: "size", Outcome: "success", OriginalURL: "https://example.org/", FinalURL: "https://example.org/",
		Items: items, SelectorsUsed: []string{},
	}, 0, 20)
	encoded, err := json.Marshal(resp)
	if err != nil || len(encoded) > MaxJSONBytes {
		t.Fatalf("bounded response exceeds wire limit: %d, %v", len(encoded), err)
	}
	if resp.Outcome != "partial" || len(resp.Items) != 1 || resp.Items[0].Title != items[0].Title {
		t.Fatalf("response did not retain accepted prefix: %+v", resp)
	}
	if resp.NextOffset == nil || *resp.NextOffset != 1 {
		t.Fatalf("response truncation missing continuation: %+v", resp.NextOffset)
	}
}

func TestPageMonitorResponseContinuationDeliversAllItemsAcrossPages(t *testing.T) {
	const count = 100
	items := make([]extractor.PageMonitorItem, count)
	for i := range items {
		items[i] = extractor.PageMonitorItem{Title: fmt.Sprintf("item-%03d-%s", i, strings.Repeat("x", 100000))}
	}
	var delivered []string
	offset := 0
	for offset < count {
		resp := boundPageMonitorResponse(PageMonitorResponse{
			ContractVersion: "1", BuildVersion: "test", RequestID: "pages", Outcome: "success", OriginalURL: "https://example.org/", FinalURL: "https://example.org/",
			Items: items[offset:], SelectorsUsed: []string{},
		}, offset, count)
		for _, item := range resp.Items {
			delivered = append(delivered, item.Title[:10])
		}
		if resp.NextOffset == nil {
			if offset+len(resp.Items) != count {
				t.Fatalf("page at offset %d omitted tail without cursor: items=%d", offset, len(resp.Items))
			}
			offset += len(resp.Items)
			break
		}
		if *resp.NextOffset <= offset || *resp.NextOffset > count || *resp.NextOffset != offset+len(resp.Items) {
			t.Fatalf("invalid continuation at offset %d: kept=%d next=%d", offset, len(resp.Items), *resp.NextOffset)
		}
		offset = *resp.NextOffset
	}
	if len(delivered) != count {
		t.Fatalf("delivered %d/%d items", len(delivered), count)
	}
	for i, title := range delivered {
		if want := fmt.Sprintf("item-%03d-", i); title[:len(want)] != want {
			t.Fatalf("item %d=%q, want prefix %q", i, title, want)
		}
	}
}

func TestPageMonitorResponseContinuationSkipsOneUnrepresentableItem(t *testing.T) {
	resp := boundPageMonitorResponse(PageMonitorResponse{
		ContractVersion: "1", BuildVersion: "test", RequestID: "oversized", Outcome: "success", OriginalURL: "https://example.org/", FinalURL: "https://example.org/",
		Items: []extractor.PageMonitorItem{{Title: strings.Repeat("x", MaxJSONBytes+1)}}, SelectorsUsed: []string{},
	}, 4, 5)
	if resp.Outcome != "partial" || len(resp.Items) != 0 || resp.NextOffset == nil || *resp.NextOffset != 5 {
		t.Fatalf("unrepresentable item did not advance finite cursor: %+v", resp)
	}
}
