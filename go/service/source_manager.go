package service

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/internal/jsonsingle"
	"github.com/Dzarlax-AI/evening-news/go/pipeline"
)

const (
	MaxSourceBatchBytes = 8 * 1024 * 1024
	MaxSourceBatchSize  = 8
	MaxSourceBudget     = 120 * time.Second
	DefaultSourceBudget = 120 * time.Second
	DefaultBatchBudget  = 120 * time.Second
)

type SourceCollectRequest struct {
	ContractVersion string             `json:"contract_version"`
	RequestID       string             `json:"request_id"`
	BatchBudgetMS   int64              `json:"batch_budget_ms,omitempty"`
	Sources         []SourceDescriptor `json:"sources"`
}

type SourceDescriptor struct {
	SourceID          int64                    `json:"source_id"`
	Type              string                   `json:"type"`
	SourceName        string                   `json:"source_name"`
	URL               string                   `json:"url"`
	Enabled           *bool                    `json:"enabled,omitempty"`
	Limit             int                      `json:"limit,omitempty"`
	PerSourceBudgetMS int64                    `json:"per_source_budget_ms,omitempty"`
	Telegram          *TelegramSourceConfig    `json:"telegram,omitempty"`
	PageMonitor       *PageMonitorSourceConfig `json:"page_monitor,omitempty"`
}

type TelegramSourceConfig struct {
	Limit int `json:"limit,omitempty"`
}

// PageMonitorSourceConfig mirrors the fetch-related portion of
// PageMonitorRequest. operation, URL, source name, and budget are manager-owned.
type PageMonitorSourceConfig struct {
	UseBrowser                *bool                                  `json:"use_browser,omitempty"`
	WaitForJS                 *bool                                  `json:"wait_for_js,omitempty"`
	Limit                     int                                    `json:"limit,omitempty"`
	Offset                    int                                    `json:"offset,omitempty"`
	MinTitleLength            int                                    `json:"min_title_length,omitempty"`
	ArticleSelectors          []string                               `json:"article_selectors,omitempty"`
	TitleSelectors            []string                               `json:"title_selectors,omitempty"`
	DateSelectors             []string                               `json:"date_selectors,omitempty"`
	LinkSelectors             []string                               `json:"link_selectors,omitempty"`
	LearnedContainerSelectors []string                               `json:"learned_container_selectors,omitempty"`
	LearnedTitleSelectors     []string                               `json:"learned_title_selectors,omitempty"`
	LearnedLinkSelectors      []string                               `json:"learned_link_selectors,omitempty"`
	LearnedDateSelectors      []string                               `json:"learned_date_selectors,omitempty"`
	LearnedGeneralSelectors   []extractor.PageMonitorGeneralSelector `json:"learned_general_selectors,omitempty"`
}

type SourceCollectResponse struct {
	ContractVersion string                `json:"contract_version"`
	BuildVersion    string                `json:"build_version"`
	RequestID       string                `json:"request_id"`
	Results         []SourceCollectResult `json:"results"`
	Error           *Failure              `json:"error,omitempty"`
}

type SourceCollectResult struct {
	SourceID    int64                  `json:"source_id"`
	Type        string                 `json:"type"`
	Outcome     string                 `json:"outcome"`
	FinalURL    string                 `json:"final_url,omitempty"`
	FetchedAt   *time.Time             `json:"fetched_at,omitempty"`
	Error       *Failure               `json:"error,omitempty"`
	RSSArticles []extractor.RSSArticle `json:"rss_articles"`
	Telegram    *TelegramResponse      `json:"telegram,omitempty"`
	PageMonitor *PageMonitorResponse   `json:"page_monitor,omitempty"`
}

func (s *Server) collectSources(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if !s.authorized(r) {
		s.sourceBatchHTTPFailure(w, http.StatusUnauthorized, "unauthorized", "")
		return
	}
	if !s.ready() {
		s.sourceBatchHTTPFailure(w, http.StatusServiceUnavailable, "unavailable", "")
		return
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		w.Header().Set("Retry-After", "1")
		s.sourceBatchHTTPFailure(w, http.StatusTooManyRequests, "busy", "")
		return
	}

	var req SourceCollectRequest
	tooLarge, err := jsonsingle.Decode(http.MaxBytesReader(w, r.Body, MaxSourceBatchBytes), MaxSourceBatchBytes, &req)
	if tooLarge {
		s.sourceBatchHTTPFailure(w, http.StatusRequestEntityTooLarge, "request_too_large", "")
		return
	}
	if err != nil {
		var maxErr *http.MaxBytesError
		if errors.As(err, &maxErr) {
			s.sourceBatchHTTPFailure(w, http.StatusRequestEntityTooLarge, "request_too_large", "")
		} else {
			s.sourceBatchHTTPFailure(w, http.StatusBadRequest, "invalid_request", "")
		}
		return
	}
	if req.ContractVersion != ContractVersion || !validRequestID(req.RequestID) || len(req.Sources) > MaxSourceBatchSize {
		s.sourceBatchHTTPFailure(w, http.StatusBadRequest, "invalid_request", req.RequestID)
		return
	}
	batchBudget := DefaultBatchBudget
	if req.BatchBudgetMS != 0 {
		if req.BatchBudgetMS < 1 || req.BatchBudgetMS > MaxSourceBudget.Milliseconds() {
			s.sourceBatchHTTPFailure(w, http.StatusBadRequest, "invalid_request", req.RequestID)
			return
		}
		batchBudget = time.Duration(req.BatchBudgetMS) * time.Millisecond
	}
	seen := make(map[int64]struct{}, len(req.Sources))
	for _, source := range req.Sources {
		if source.SourceID <= 0 {
			s.sourceBatchHTTPFailure(w, http.StatusBadRequest, "invalid_request", req.RequestID)
			return
		}
		if _, ok := seen[source.SourceID]; ok {
			s.sourceBatchHTTPFailure(w, http.StatusBadRequest, "duplicate_source_id", req.RequestID)
			return
		}
		seen[source.SourceID] = struct{}{}
	}
	if s.runner == nil {
		s.sourceBatchHTTPFailure(w, http.StatusServiceUnavailable, "unavailable", req.RequestID)
		return
	}
	if err := setSourceBatchWriteDeadline(w, batchBudget+5*time.Second); err != nil {
		s.sourceBatchHTTPFailure(w, http.StatusServiceUnavailable, "write_deadline_unavailable", req.RequestID)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), batchBudget)
	defer cancel()
	workReserve := batchBudget / 10
	if workReserve > time.Second {
		workReserve = time.Second
	}
	if workReserve < time.Millisecond {
		workReserve = time.Millisecond
	}
	if workReserve >= batchBudget {
		workReserve = batchBudget / 2
	}
	workCtx, stopWork := context.WithTimeout(ctx, batchBudget-workReserve)
	defer stopWork()
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		s.sourceBatchHTTPFailure(w, http.StatusServiceUnavailable, "unavailable", req.RequestID)
		return
	}
	s.next++
	activeID := s.next
	s.active[activeID] = cancel
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.active, activeID); s.mu.Unlock() }()

	resp := s.runSourceBatch(workCtx, req)
	resp.ContractVersion = ContractVersion
	resp.BuildVersion = s.cfg.BuildVersion
	resp.RequestID = req.RequestID
	s.mu.Lock()
	s.completed++
	s.outcomes["source_batch"]++
	s.methods["source_batch"]++
	s.mu.Unlock()
	s.writeSourceBatch(w, http.StatusOK, resp)
}

func (s *Server) runSourceBatch(ctx context.Context, req SourceCollectRequest) SourceCollectResponse {
	resp := SourceCollectResponse{ContractVersion: ContractVersion, BuildVersion: s.cfg.BuildVersion, RequestID: req.RequestID, Results: make([]SourceCollectResult, len(req.Sources))}
	jobs := make([]pipeline.SourceJob, 0, len(req.Sources))
	jobResultIndexes := make([]int, 0, len(req.Sources))
	for i, desc := range req.Sources {
		result := SourceCollectResult{SourceID: desc.SourceID, Type: safeSourceType(desc.Type), Outcome: "rejected"}
		if code := validateSourceDescriptor(&desc); code != "" {
			result.Error = &Failure{Code: code, Retryable: false}
			resp.Results[i] = result
			continue
		}
		if desc.Enabled != nil && !*desc.Enabled {
			result.Error = &Failure{Code: "source_disabled", Retryable: false}
			resp.Results[i] = result
			continue
		}
		budget := DefaultSourceBudget
		if desc.PerSourceBudgetMS != 0 {
			budget = time.Duration(desc.PerSourceBudgetMS) * time.Millisecond
		}
		resp.Results[i] = deferredSourceResult(result)
		job := pipeline.SourceJob{SourceID: desc.SourceID, Budget: budget, Browser: sourceUsesBrowser(desc), Run: s.sourceRunner(desc, result)}
		jobs = append(jobs, job)
		jobResultIndexes = append(jobResultIndexes, i)
	}
	baseBytes, ok := estimateJSONSize(resp)
	if !ok || baseBytes > MaxSourceBatchBytes {
		baseBytes = MaxSourceBatchBytes
	}
	remaining := int64(MaxSourceBatchBytes) - baseBytes
	var reserveMu sync.Mutex
	for i := range jobs {
		jobs[i].RetainResult = func(completed pipeline.SourceJobResult) pipeline.SourceJobResult {
			value, ok := completed.Value.(SourceCollectResult)
			if !ok {
				return completed
			}
			size, sizeOK := estimateJSONSize(value)
			placeholderSize, placeholderOK := estimateJSONSize(deferredSourceResult(value))
			if !sizeOK || size > MaxSourceBatchBytes-1024 {
				value = minimalSourceResult(value, "resource_limit")
				size, _ = estimateJSONSize(value)
			}
			if !placeholderOK {
				placeholderSize = 0
			}
			extra := size - placeholderSize
			if extra < 0 {
				extra = 0
			}
			reserveMu.Lock()
			if extra > remaining {
				if sourceResultHasPayload(value) {
					value = deferredSourceResult(value)
					completed.Outcome = "deferred"
				} else {
					value = minimalSourceResult(value, "resource_limit")
					completed.Outcome = "failed"
				}
			} else {
				remaining -= extra
			}
			reserveMu.Unlock()
			completed.Value = value
			completed.Error = failureCode(value.Error)
			completed.Retryable = failureRetryable(value.Error)
			return completed
		}
	}
	completed := (pipeline.SourceManager{}).Collect(ctx, jobs)
	for j, done := range completed {
		i := jobResultIndexes[j]
		if value, ok := done.Value.(SourceCollectResult); ok {
			value.SourceID = done.SourceID
			if done.Outcome == "deferred" || done.Outcome == "failed" && done.Error != "" {
				value.Outcome = done.Outcome
				if value.Error == nil {
					value.Error = &Failure{Code: done.Error, Retryable: done.Retryable}
				}
			}
			resp.Results[i] = value
			continue
		}
		resp.Results[i] = SourceCollectResult{SourceID: done.SourceID, Type: req.Sources[i].Type, Outcome: done.Outcome, Error: &Failure{Code: fallbackSourceError(done.Error), Retryable: done.Retryable}}
	}
	return boundSourceBatchResponse(resp)
}

func (s *Server) sourceRunner(desc SourceDescriptor, base SourceCollectResult) func(context.Context) pipeline.SourceJobResult {
	return func(ctx context.Context) pipeline.SourceJobResult {
		result := base
		switch desc.Type {
		case "rss":
			runner, ok := s.runner.(RSSRunner)
			if !ok {
				result.Outcome = "failed"
				result.Error = &Failure{Code: "rss_unavailable", Retryable: true}
				break
			}
			out := runner.RunRSS(ctx, RSSRequest{URL: desc.URL, SourceName: desc.SourceName, Limit: desc.Limit})
			result.Outcome, result.FinalURL = normalizedSourceOutcome(out.Outcome), out.FinalURL
			if result.Outcome == "failed" && out.Error == "" {
				result.Error = &Failure{Code: "invalid_runner_response", Retryable: false}
			}
			result.RSSArticles = out.Articles
			if result.RSSArticles == nil {
				result.RSSArticles = []extractor.RSSArticle{}
			}
			now := time.Now().UTC()
			result.FetchedAt = &now
			if out.Error != "" {
				result.Error = &Failure{Code: out.Error, Retryable: out.Retryable, RetryAfterSeconds: out.RetryAfterSeconds}
			}
		case "telegram":
			runner, ok := s.runner.(TelegramRunner)
			if !ok {
				result.Outcome = "failed"
				result.Error = &Failure{Code: "telegram_unavailable", Retryable: true}
				break
			}
			limit := desc.Limit
			if desc.Telegram != nil && desc.Telegram.Limit != 0 {
				limit = desc.Telegram.Limit
			}
			if limit == 0 {
				limit = 100
			}
			canonical, _ := normalizeTelegramChannel(desc.URL)
			budget := budgetMS(ctx)
			resp := runner.RunTelegram(ctx, TelegramRequest{ContractVersion: ContractVersion, RequestID: sourceRequestID(desc.SourceID), ChannelURL: canonical, SourceName: desc.SourceName, Limit: limit, BudgetMS: budget})
			resp.ContractVersion, resp.BuildVersion = ContractVersion, s.cfg.BuildVersion
			resp.RequestID, resp.OriginalURL = sourceRequestID(desc.SourceID), desc.URL
			originalOutcome := resp.Outcome
			resp.Outcome = normalizedSourceOutcome(resp.Outcome)
			if resp.Outcome == "failed" && originalOutcome != "failed" && resp.Error == nil {
				resp.Error = &Failure{Code: "invalid_runner_response", Retryable: false}
			}
			if resp.Articles == nil {
				resp.Articles = []extractor.TelegramArticle{}
			}
			result.Outcome, result.FinalURL, result.Telegram = resp.Outcome, resp.FinalURL, &resp
			now := time.Now().UTC()
			result.FetchedAt = &now
			if resp.Error != nil {
				result.Error = resp.Error
			}
		case "custom":
			runner, ok := s.runner.(PageMonitorRunner)
			if !ok {
				result.Outcome = "failed"
				result.Error = &Failure{Code: "page_monitor_unavailable", Retryable: true}
				break
			}
			pm := desc.PageMonitor
			request := PageMonitorRequest{ContractVersion: ContractVersion, RequestID: sourceRequestID(desc.SourceID), Operation: "fetch", URL: desc.URL, SourceName: desc.SourceName, BudgetMS: budgetMS(ctx)}
			if pm != nil {
				request.UseBrowser, request.WaitForJS = pm.UseBrowser, pm.WaitForJS
				request.Limit, request.Offset, request.MinTitleLength = pm.Limit, pm.Offset, pm.MinTitleLength
				request.ArticleSelectors, request.TitleSelectors = pm.ArticleSelectors, pm.TitleSelectors
				request.DateSelectors, request.LinkSelectors = pm.DateSelectors, pm.LinkSelectors
				request.LearnedContainerSelectors = pm.LearnedContainerSelectors
				request.LearnedTitleSelectors, request.LearnedLinkSelectors = pm.LearnedTitleSelectors, pm.LearnedLinkSelectors
				request.LearnedDateSelectors, request.LearnedGeneralSelectors = pm.LearnedDateSelectors, pm.LearnedGeneralSelectors
			}
			if request.Limit == 0 && desc.Limit != 0 {
				request.Limit = desc.Limit
			}
			if code := validatePageMonitorRequest(&request); code != "" {
				result.Outcome = "rejected"
				result.Error = &Failure{Code: code, Retryable: false}
				break
			}
			resp := runner.RunPageMonitor(ctx, request)
			resp.ContractVersion, resp.BuildVersion = ContractVersion, s.cfg.BuildVersion
			resp.RequestID = request.RequestID
			originalOutcome := resp.Outcome
			resp.Outcome = normalizedSourceOutcome(resp.Outcome)
			if resp.Outcome == "failed" && originalOutcome != "failed" && resp.Error == nil {
				resp.Error = &Failure{Code: "invalid_runner_response", Retryable: false}
			}
			if resp.Items == nil {
				resp.Items = []extractor.PageMonitorItem{}
			}
			if resp.SelectorsUsed == nil {
				resp.SelectorsUsed = []string{}
			}
			result.Outcome, result.FinalURL, result.PageMonitor = resp.Outcome, resp.FinalURL, &resp
			now := time.Now().UTC()
			result.FetchedAt = &now
			if resp.Error != nil {
				result.Error = resp.Error
			}
		}
		if estimate, ok := estimateJSONSize(result); !ok || estimate > MaxSourceBatchBytes-1024 {
			result = minimalSourceResult(result, "resource_limit")
		}
		return pipeline.SourceJobResult{SourceID: desc.SourceID, Outcome: result.Outcome, Error: failureCode(result.Error), Retryable: failureRetryable(result.Error), Value: result}
	}
}

type RSSRequest struct {
	URL        string
	SourceName string
	Limit      int
}

type RSSResponse struct {
	Articles          []extractor.RSSArticle
	FinalURL          string
	Outcome           string
	Error             string
	Retryable         bool
	RetryAfterSeconds *int
}

type RSSRunner interface {
	RunRSS(context.Context, RSSRequest) RSSResponse
}

func validateSourceDescriptor(d *SourceDescriptor) string {
	if d.SourceID <= 0 || !utf8.ValidString(d.SourceName) || strings.TrimSpace(d.SourceName) == "" || len(d.SourceName) > 256 || len(d.URL) > 8192 {
		return "invalid_source"
	}
	u, err := url.Parse(d.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.Opaque != "" || u.Fragment != "" {
		return "invalid_source"
	}
	if d.PerSourceBudgetMS < 0 || d.PerSourceBudgetMS > MaxSourceBudget.Milliseconds() {
		return "invalid_source"
	}
	switch d.Type {
	case "rss":
		if d.Telegram != nil || d.PageMonitor != nil || d.Limit < 0 || d.Limit > extractor.MaxRSSItems {
			return "invalid_source"
		}
	case "telegram":
		if d.PageMonitor != nil || d.Telegram != nil && d.Telegram.Limit != 0 && (d.Telegram.Limit < 1 || d.Telegram.Limit > 100) {
			return "invalid_source"
		}
		limit := d.Limit
		if d.Telegram != nil && d.Telegram.Limit != 0 {
			limit = d.Telegram.Limit
		}
		if limit < 0 || limit > 100 {
			return "invalid_source"
		}
		if _, err := normalizeTelegramChannel(d.URL); err != nil {
			return "invalid_source"
		}
	case "custom":
		if d.Telegram != nil {
			return "invalid_source"
		}
		if d.PageMonitor != nil {
			r := pageMonitorRequestFromDescriptor(*d, DefaultSourceBudget)
			if code := validatePageMonitorRequest(&r); code != "" {
				return code
			}
		}
	default:
		return "unsupported_source_type"
	}
	return ""
}

func safeSourceType(sourceType string) string {
	switch sourceType {
	case "rss", "telegram", "custom":
		return sourceType
	default:
		return "unknown"
	}
}

func normalizedSourceOutcome(outcome string) string {
	switch outcome {
	case "success", "partial", "failed", "rejected":
		return outcome
	default:
		return "failed"
	}
}

func pageMonitorRequestFromDescriptor(d SourceDescriptor, budget time.Duration) PageMonitorRequest {
	pageBudget := budget
	if pageBudget > 30*time.Second {
		pageBudget = 30 * time.Second
	}
	r := PageMonitorRequest{ContractVersion: ContractVersion, RequestID: sourceRequestID(d.SourceID), Operation: "fetch", URL: d.URL, SourceName: d.SourceName, BudgetMS: pageBudget.Milliseconds()}
	if p := d.PageMonitor; p != nil {
		r.UseBrowser, r.WaitForJS = p.UseBrowser, p.WaitForJS
		r.Limit, r.Offset, r.MinTitleLength = p.Limit, p.Offset, p.MinTitleLength
		r.ArticleSelectors, r.TitleSelectors, r.DateSelectors, r.LinkSelectors = p.ArticleSelectors, p.TitleSelectors, p.DateSelectors, p.LinkSelectors
		r.LearnedContainerSelectors, r.LearnedTitleSelectors = p.LearnedContainerSelectors, p.LearnedTitleSelectors
		r.LearnedLinkSelectors, r.LearnedDateSelectors = p.LearnedLinkSelectors, p.LearnedDateSelectors
		r.LearnedGeneralSelectors = p.LearnedGeneralSelectors
	}
	if r.Limit == 0 && d.Limit != 0 {
		r.Limit = d.Limit
	}
	return r
}

func sourceUsesBrowser(d SourceDescriptor) bool {
	switch d.Type {
	case "telegram":
		return true
	case "custom":
		if d.PageMonitor == nil || d.PageMonitor.UseBrowser == nil {
			return true
		}
		return *d.PageMonitor.UseBrowser
	default:
		return false
	}
}

func budgetMS(ctx context.Context) int64 {
	if deadline, ok := ctx.Deadline(); ok {
		left := time.Until(deadline).Milliseconds()
		if left < 1 {
			return 1
		}
		if left > (30 * time.Second).Milliseconds() {
			return (30 * time.Second).Milliseconds()
		}
		return left
	}
	return DefaultSourceBudget.Milliseconds()
}

func sourceRequestID(id int64) string { return "source-" + strconv.FormatInt(id, 10) }

func validRequestID(id string) bool { return id != "" && len(id) <= 128 && utf8.ValidString(id) }

func failureCode(f *Failure) string {
	if f == nil {
		return ""
	}
	return f.Code
}

func failureRetryable(f *Failure) bool { return f != nil && f.Retryable }

func fallbackSourceError(code string) string {
	if code == "" {
		return "source_failed"
	}
	return code
}

func boundSourceBatchResponse(resp SourceCollectResponse) SourceCollectResponse {
	base := SourceCollectResponse{ContractVersion: resp.ContractVersion, BuildVersion: resp.BuildVersion, RequestID: resp.RequestID, Results: []SourceCollectResult{}}
	baseBytes, ok := estimateJSONSize(base)
	if !ok || baseBytes > MaxSourceBatchBytes {
		return SourceCollectResponse{ContractVersion: ContractVersion, BuildVersion: resp.BuildVersion, RequestID: resp.RequestID, Results: []SourceCollectResult{}}
	}
	minimum := make([]int64, len(resp.Results))
	for i, result := range resp.Results {
		if sourceResultHasPayload(result) {
			result = deferredSourceResult(result)
		}
		size, ok := estimateJSONSize(result)
		if !ok {
			result = minimalSourceResult(result, "resource_limit")
			size, _ = estimateJSONSize(result)
			resp.Results[i] = result
		}
		minimum[i] = size + 1
	}
	totalMinimum := baseBytes
	for _, size := range minimum {
		totalMinimum += size
	}
	if totalMinimum > MaxSourceBatchBytes {
		for i := range resp.Results {
			resp.Results[i] = minimalSourceResult(resp.Results[i], "resource_limit")
		}
		return resp
	}
	available := int64(MaxSourceBatchBytes) - totalMinimum
	keptPayloadPrefix := true
	for i := range resp.Results {
		result := resp.Results[i]
		if !sourceResultHasPayload(result) {
			continue
		}
		size, ok := estimateJSONSize(result)
		if !ok || size > MaxSourceBatchBytes-1024 {
			resp.Results[i] = minimalSourceResult(result, "resource_limit")
			continue
		}
		extra := size + 1 - minimum[i]
		if !keptPayloadPrefix || extra > available {
			resp.Results[i] = deferredSourceResult(result)
			keptPayloadPrefix = false
			continue
		}
		available -= extra
	}
	return resp
}

func sourceResultHasPayload(result SourceCollectResult) bool {
	return len(result.RSSArticles) > 0 || result.Telegram != nil || result.PageMonitor != nil
}

func deferredSourceResult(result SourceCollectResult) SourceCollectResult {
	result.Outcome = "deferred"
	result.FinalURL, result.FetchedAt = "", nil
	result.RSSArticles, result.Telegram, result.PageMonitor = nil, nil, nil
	seconds := 1
	result.Error = &Failure{Code: "response_budget_exhausted", Retryable: true, RetryAfterSeconds: &seconds}
	return result
}

func minimalSourceResult(result SourceCollectResult, code string) SourceCollectResult {
	result.Outcome = "failed"
	result.Error = &Failure{Code: code, Retryable: false}
	result.RSSArticles, result.Telegram, result.PageMonitor = nil, nil, nil
	return result
}

func estimateJSONSize(value any) (int64, bool) {
	return estimateJSONValue(reflect.ValueOf(value))
}

func estimateJSONValue(v reflect.Value) (int64, bool) {
	if !v.IsValid() {
		return 4, true
	}
	for v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
		if v.IsNil() {
			return 4, true
		}
		v = v.Elem()
	}
	switch v.Kind() {
	case reflect.String:
		return estimateJSONStringSize(v.String()), true
	case reflect.Bool:
		if v.Bool() {
			return 4, true
		}
		return 5, true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return int64(len(strconv.FormatInt(v.Int(), 10))), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return int64(len(strconv.FormatUint(v.Uint(), 10))), true
	case reflect.Float32, reflect.Float64:
		return 32, true
	case reflect.Slice, reflect.Array:
		if v.Type().Elem().Kind() == reflect.Uint8 {
			return 2 + 4*((int64(v.Len())+2)/3), true
		}
		size := int64(2)
		for i := 0; i < v.Len(); i++ {
			item, ok := estimateJSONValue(v.Index(i))
			if !ok {
				return 0, false
			}
			if i > 0 {
				size++
			}
			size += item
		}
		return size, true
	case reflect.Map:
		if v.IsNil() {
			return 4, true
		}
		if v.Type().Key().Kind() != reflect.String {
			return 0, false
		}
		size := int64(2)
		iter := v.MapRange()
		count := 0
		for iter.Next() {
			keySize := estimateJSONStringSize(iter.Key().String())
			valueSize, ok := estimateJSONValue(iter.Value())
			if !ok {
				return 0, false
			}
			if count > 0 {
				size++
			}
			size += keySize + 1 + valueSize
			count++
		}
		return size, true
	case reflect.Struct:
		if v.Type().PkgPath() == "time" && v.Type().Name() == "Time" {
			return 64, true
		}
		size := int64(2)
		count := 0
		for i := 0; i < v.NumField(); i++ {
			field := v.Type().Field(i)
			if field.PkgPath != "" {
				continue
			}
			tag := field.Tag.Get("json")
			if tag == "-" {
				continue
			}
			parts := strings.Split(tag, ",")
			name := field.Name
			if parts[0] != "" {
				name = parts[0]
			}
			omitEmpty := false
			for _, option := range parts[1:] {
				if option == "omitempty" {
					omitEmpty = true
					break
				}
			}
			if omitEmpty && v.Field(i).IsZero() {
				continue
			}
			fieldSize, ok := estimateJSONValue(v.Field(i))
			if !ok {
				return 0, false
			}
			if count > 0 {
				size++
			}
			size += estimateJSONStringSize(name) + 1 + fieldSize
			count++
		}
		return size, true
	default:
		return 0, false
	}
}

func estimateJSONStringSize(value string) int64 {
	size := int64(2)
	if !utf8.ValidString(value) {
		return size + int64(len(value))*6
	}
	for i := 0; i < len(value); i++ {
		b := value[i]
		switch {
		case b == '"' || b == '\\' || b == '\b' || b == '\f' || b == '\n' || b == '\r' || b == '\t':
			size += 2
		case b < 0x20:
			size += 6
		case b == '<' || b == '>' || b == '&':
			size += 6 // encoding/json HTML-escapes these characters
		default:
			if b == 0xe2 && i+2 < len(value) && value[i+1] == 0x80 && (value[i+2] == 0xa8 || value[i+2] == 0xa9) {
				size += 6
				i += 2
			} else {
				size++
			}
		}
	}
	return size
}

func (s *Server) sourceBatchHTTPFailure(w http.ResponseWriter, status int, code, requestID string) {
	s.mu.Lock()
	s.rejected[status]++
	s.mu.Unlock()
	s.writeSourceBatch(w, status, SourceCollectResponse{ContractVersion: ContractVersion, BuildVersion: s.cfg.BuildVersion, RequestID: requestID, Results: []SourceCollectResult{}, Error: &Failure{Code: code, Retryable: status == http.StatusTooManyRequests || status == http.StatusServiceUnavailable}})
}

func (s *Server) writeSourceBatch(w http.ResponseWriter, status int, resp SourceCollectResponse) {
	b, err := json.Marshal(resp)
	if err != nil || len(b) > MaxSourceBatchBytes {
		status = http.StatusInternalServerError
		fallback := SourceCollectResponse{ContractVersion: ContractVersion, BuildVersion: s.cfg.BuildVersion, RequestID: resp.RequestID, Results: make([]SourceCollectResult, len(resp.Results))}
		for i, result := range resp.Results {
			fallback.Results[i] = minimalSourceResult(result, "resource_limit")
		}
		b, err = json.Marshal(fallback)
		if err != nil || len(b) > MaxSourceBatchBytes {
			fallback.BuildVersion = ""
			b, _ = json.Marshal(fallback)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func setSourceBatchWriteDeadline(w http.ResponseWriter, duration time.Duration) error {
	if duration <= 0 {
		return errors.New("invalid deadline")
	}
	return http.NewResponseController(w).SetWriteDeadline(time.Now().Add(duration))
}
