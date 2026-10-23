package service

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/internal/jsonsingle"
)

func (s *Server) extractPageMonitor(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !s.authorized(r) {
		s.pageMonitorHTTPFailure(w, http.StatusUnauthorized, "unauthorized", "")
		return
	}
	if !s.ready() {
		s.pageMonitorHTTPFailure(w, http.StatusServiceUnavailable, "unavailable", "")
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		s.pageMonitorHTTPFailure(w, http.StatusTooManyRequests, "busy", "")
		return
	}
	var req PageMonitorRequest
	tooLarge, err := jsonsingle.Decode(http.MaxBytesReader(w, r.Body, MaxJSONBytes), MaxJSONBytes, &req)
	if tooLarge {
		s.pageMonitorHTTPFailure(w, http.StatusRequestEntityTooLarge, "request_too_large", "")
		return
	}
	if err != nil {
		var large *http.MaxBytesError
		if errors.As(err, &large) {
			s.pageMonitorHTTPFailure(w, http.StatusRequestEntityTooLarge, "request_too_large", "")
		} else {
			s.pageMonitorHTTPFailure(w, http.StatusBadRequest, "invalid_request", "")
		}
		return
	}
	if code := validatePageMonitorRequest(&req); code != "" {
		status := http.StatusBadRequest
		if code == "request_too_large" {
			status = http.StatusRequestEntityTooLarge
		}
		s.pageMonitorHTTPFailure(w, status, code, req.RequestID)
		return
	}
	runner, ok := s.runner.(PageMonitorRunner)
	if !ok {
		s.pageMonitorHTTPFailure(w, http.StatusServiceUnavailable, "unavailable", req.RequestID)
		return
	}
	if req.Operation == "fetch" && boolValue(req.UseBrowser, true) && !s.browser() {
		s.pageMonitorHTTPFailure(w, http.StatusServiceUnavailable, "unavailable", req.RequestID)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(req.BudgetMS)*time.Millisecond)
	defer cancel()
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		s.pageMonitorHTTPFailure(w, http.StatusServiceUnavailable, "unavailable", req.RequestID)
		return
	}
	s.next++
	id := s.next
	s.active[id] = cancel
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.active, id); s.mu.Unlock() }()
	started := time.Now()
	resp := runner.RunPageMonitor(ctx, req)
	if ctx.Err() != nil {
		code := "cancelled"
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			code = "deadline_exceeded"
			if s.cfg.NativeBrowser && req.Operation == "fetch" && boolValue(req.UseBrowser, true) && validPageMonitorAdmissionBusy(resp) {
				code = "native_browser_busy"
			}
		}
		resp.StudyHTML = ""
		resp.StudyHTMLOmitted = resp.NeedsSelectorStudy
		if len(resp.Items) > 0 {
			resp.Outcome = "partial"
			if next := req.Offset + len(resp.Items); next < req.Limit {
				resp.NextOffset = &next
			}
		} else {
			resp.Outcome = "failed"
		}
		resp.Error = &Failure{Code: code, Retryable: code == "deadline_exceeded" || code == "native_browser_busy"}
	}
	resp.ContractVersion = ContractVersion
	resp.BuildVersion = s.cfg.BuildVersion
	resp.RequestID = req.RequestID
	resp.OriginalURL = req.URL
	if resp.FinalURL == "" {
		resp.FinalURL = req.URL
	}
	if resp.Items == nil {
		resp.Items = []extractor.PageMonitorItem{}
	}
	if resp.SelectorsUsed == nil {
		resp.SelectorsUsed = []string{}
	}
	if resp.Outcome != "success" && resp.Outcome != "partial" && resp.Outcome != "failed" && resp.Outcome != "rejected" {
		resp.Outcome = "failed"
		resp.Error = &Failure{Code: "invalid_runner_response"}
	}
	resp = boundPageMonitorResponse(resp, req.Offset, req.Limit)
	s.mu.Lock()
	s.completed++
	s.latency += time.Since(started)
	s.outcomes[boundedOutcome(resp.Outcome)]++
	s.methods["page_monitor_"+req.Operation]++
	s.mu.Unlock()
	s.writePageMonitor(w, http.StatusOK, resp)
}

func validPageMonitorAdmissionBusy(resp PageMonitorResponse) bool {
	return resp.Outcome == "failed" && len(resp.Items) == 0 && resp.Error != nil &&
		resp.Error.Code == "native_browser_busy" && resp.Error.Retryable
}

func boolValue(value *bool, defaultValue bool) bool {
	if value == nil {
		return defaultValue
	}
	return *value
}

func validatePageMonitorRequest(r *PageMonitorRequest) string {
	if r.ContractVersion != ContractVersion || r.RequestID == "" || len(r.RequestID) > 128 || len(r.URL) > 8192 || r.SourceName == "" || len(r.SourceName) > 256 || r.BudgetMS < 1 || r.BudgetMS > 30000 {
		return "invalid_request"
	}
	u, err := url.Parse(r.URL)
	if err != nil || u.Scheme != "http" && u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Opaque != "" {
		return "invalid_request"
	}
	if r.Limit == 0 {
		r.Limit = 20
	}
	if r.Limit < 1 || r.Limit > 100 {
		return "invalid_request"
	}
	if r.Offset < 0 || r.Offset >= r.Limit {
		return "invalid_request"
	}
	if r.MinTitleLength == 0 {
		r.MinTitleLength = 10
	}
	if r.MinTitleLength < 1 || r.MinTitleLength > 100000 {
		return "invalid_request"
	}
	if r.Operation != "fetch" && r.Operation != "parse" {
		return "invalid_request"
	}
	if r.Operation == "fetch" {
		if r.ProvidedHTML != nil || r.SnapshotAt != nil {
			return "invalid_request"
		}
		if r.UseBrowser == nil {
			v := true
			r.UseBrowser = &v
		}
		if r.WaitForJS == nil {
			v := true
			r.WaitForJS = &v
		}
	}
	if r.Operation == "parse" {
		if r.ProvidedHTML == nil || r.SnapshotAt == nil || r.UseBrowser != nil || r.WaitForJS != nil {
			return "invalid_request"
		}
		if len(*r.ProvidedHTML) > MaxBytes {
			return "request_too_large"
		}
		_, offset := r.SnapshotAt.Zone()
		if offset != 0 {
			return "invalid_request"
		}
		r.SnapshotAt = ptrTime(r.SnapshotAt.UTC())
	}
	groups := [][]string{r.ArticleSelectors, r.TitleSelectors, r.DateSelectors, r.LinkSelectors, r.LearnedContainerSelectors, r.LearnedTitleSelectors, r.LearnedLinkSelectors, r.LearnedDateSelectors}
	if len(r.LearnedGeneralSelectors) > 64 {
		return "invalid_request"
	}
	count := 0
	for _, g := range r.LearnedGeneralSelectors {
		if !finiteConfidence(g.Confidence) || strings.TrimSpace(g.Selector) == "" {
			return "invalid_request"
		}
		groups = append(groups, []string{g.Selector})
	}
	for _, g := range groups {
		if len(g) > 64 {
			return "invalid_request"
		}
		count += len(g)
		for _, selector := range g {
			if selector == "" || len(selector) > 2048 || !utf8.ValidString(selector) {
				return "invalid_request"
			}
		}
	}
	if count > 256 {
		return "invalid_request"
	}
	return ""
}
func finiteConfidence(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func (s *Server) pageMonitorHTTPFailure(w http.ResponseWriter, status int, code, requestID string) {
	s.mu.Lock()
	s.rejected[status]++
	s.mu.Unlock()
	s.writePageMonitor(w, status, PageMonitorResponse{ContractVersion: ContractVersion, BuildVersion: s.cfg.BuildVersion, RequestID: requestID, Outcome: "rejected", Items: []extractor.PageMonitorItem{}, SelectorsUsed: []string{}, Error: &Failure{Code: code, Retryable: status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable}})
}

// Bound response content using JSON's actual string escaping rules before encoding.
func boundPageMonitorResponse(resp PageMonitorResponse, offset, limit int) PageMonitorResponse {
	const budget = MaxJSONBytes - 256 // reserve room for final status and error metadata
	originalItems := resp.Items
	resp.Items = []extractor.PageMonitorItem{}
	if resp.NeedsSelectorStudy && resp.StudyHTML != "" {
		resp.Items = nil
		if pageMonitorResponseSize(resp) > budget {
			resp.StudyHTML = ""
			resp.StudyHTMLOmitted = true
			resp.Outcome = "partial"
			resp.Error = &Failure{Code: "resource_limit", Retryable: false}
		}
	}
	baseSize := pageMonitorResponseSize(resp)
	kept := make([]extractor.PageMonitorItem, 0, len(originalItems))
	used := baseSize - 2 // replace empty [] with item array content
	for _, item := range originalItems {
		estimate := pageMonitorItemSize(item)
		if used+estimate+2 > budget {
			resp.Outcome = "partial"
			resp.Error = &Failure{Code: "resource_limit", Retryable: false}
			break
		}
		used += estimate + 1 // comma separator (overestimates the first entry safely)
		kept = append(kept, item)
	}
	resp.Items = kept
	if len(kept) < len(originalItems) {
		next := offset + len(kept)
		if len(kept) == 0 {
			next = offset + 1
		}
		if (len(kept) == 0 && next <= limit) || (len(kept) > 0 && next < limit) {
			resp.NextOffset = &next
		}
	} else if resp.NextOffset != nil && (*resp.NextOffset <= offset || *resp.NextOffset > limit) {
		resp.NextOffset = nil
	}
	return resp
}

func pageMonitorResponseSize(resp PageMonitorResponse) int {
	resp.Items = []extractor.PageMonitorItem{}
	study := resp.StudyHTML
	resp.StudyHTML = ""
	b, err := json.Marshal(resp)
	if err != nil {
		return MaxJSONBytes + 1
	}
	if study != "" {
		return len(b) + jsonStringSize(study) + len(`,"study_html":`)
	}
	return len(b)
}

func pageMonitorItemSize(item extractor.PageMonitorItem) int {
	return 160 + jsonStringSize(item.Title) + jsonStringSize(item.Link) + jsonStringSize(item.Description) + jsonStringSize(item.ContentType) + jsonStringSize(item.ID) + jsonStringSize(item.Source) + jsonStringSize(item.SourceURL) + 48
}

func jsonStringSize(v string) int {
	used := 0
	for len(v) > 0 {
		r, size := utf8.DecodeRuneInString(v)
		if r == utf8.RuneError && size == 1 {
			used += 6
		} else {
			switch {
			case r == '"' || r == '\\' || r == '\b' || r == '\f' || r == '\n' || r == '\r' || r == '\t':
				used += 2
			case r < 0x20 || r == '<' || r == '>' || r == '&' || r == '\u2028' || r == '\u2029':
				used += 6
			default:
				used += size
			}
		}
		v = v[size:]
	}
	return used + 2
}

func ptrTime(v time.Time) *time.Time { return &v }

func (s *Server) writePageMonitor(w http.ResponseWriter, status int, resp PageMonitorResponse) {
	b, err := json.Marshal(resp)
	if err != nil || len(b) > MaxJSONBytes {
		status = http.StatusInternalServerError
		b, _ = json.Marshal(PageMonitorResponse{ContractVersion: ContractVersion, BuildVersion: s.cfg.BuildVersion, RequestID: resp.RequestID, OriginalURL: resp.OriginalURL, FinalURL: resp.FinalURL, Outcome: "failed", Items: []extractor.PageMonitorItem{}, SelectorsUsed: []string{}, Error: &Failure{Code: "response_too_large"}})
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}
