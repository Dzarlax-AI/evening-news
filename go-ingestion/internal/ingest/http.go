package ingest

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf16"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go-ingestion/internal/store"
	"github.com/jackc/pgx/v5"
)

const maxEnvelopeBytes = 8192
const maxExtractorBytes = 8 * 1024 * 1024

type Config struct {
	Token                  string
	ExtractorURL           string
	ExtractorToken         string
	ExtractorNativeBrowser bool
	SelectorURL            string
	SelectorToken          string
}

type Handler struct {
	store    *store.Store
	cfg      Config
	http     *http.Client
	mu       sync.Mutex
	runs     map[string]context.CancelFunc
	reserved map[string]bool
}

type Envelope struct {
	Version           int       `json:"version"`
	JobID             string    `json:"job_id"`
	AttemptGeneration int32     `json:"attempt_generation"`
	OwnerID           string    `json:"owner_id"`
	Generation        int32     `json:"generation"`
	BackendPID        int32     `json:"backend_pid"`
	ExecutorInstance  string    `json:"executor_instance_id"`
	DeadlineAt        time.Time `json:"deadline_at"`
}

type sourceConfig struct {
	Limit                     int               `json:"limit,omitempty"`
	Offset                    int               `json:"offset,omitempty"`
	MinTitleLength            int               `json:"min_title_length,omitempty"`
	UseBrowser                *bool             `json:"use_browser,omitempty"`
	WaitForJS                 *bool             `json:"wait_for_js,omitempty"`
	ArticleSelectors          []string          `json:"article_selectors,omitempty"`
	TitleSelectors            []string          `json:"title_selectors,omitempty"`
	DateSelectors             []string          `json:"date_selectors,omitempty"`
	LinkSelectors             []string          `json:"link_selectors,omitempty"`
	LearnedContainerSelectors []string          `json:"learned_container_selectors,omitempty"`
	LearnedTitleSelectors     []string          `json:"learned_title_selectors,omitempty"`
	LearnedLinkSelectors      []string          `json:"learned_link_selectors,omitempty"`
	LearnedDateSelectors      []string          `json:"learned_date_selectors,omitempty"`
	LearnedGeneralSelectors   []generalSelector `json:"learned_general_selectors,omitempty"`
}

type generalSelector struct {
	Selector   string  `json:"selector"`
	Confidence float64 `json:"confidence"`
}

type descriptor struct {
	SourceID          int32  `json:"source_id"`
	Type              string `json:"type"`
	SourceName        string `json:"source_name"`
	URL               string `json:"url"`
	Limit             int    `json:"limit,omitempty"`
	PerSourceBudgetMS int64  `json:"per_source_budget_ms,omitempty"`
	Telegram          *struct {
		Limit int `json:"limit,omitempty"`
	} `json:"telegram,omitempty"`
	PageMonitor *sourceConfig `json:"page_monitor,omitempty"`
}

type collectResponse struct {
	ContractVersion string          `json:"contract_version"`
	RequestID       string          `json:"request_id"`
	Results         []collectResult `json:"results"`
}
type collectResult struct {
	RequestID string     `json:"request_id"`
	SourceID  int32      `json:"source_id"`
	Type      string     `json:"type"`
	Outcome   string     `json:"outcome"`
	FetchedAt *time.Time `json:"fetched_at"`
	Error     *struct {
		Code              string `json:"code"`
		RetryAfterSeconds *int   `json:"retry_after_seconds"`
	} `json:"error"`
	RSSArticles []store.Article `json:"rss_articles"`
	Telegram    *struct {
		Articles []telegramArticle `json:"articles"`
	} `json:"telegram"`
	PageMonitor *pageMonitorResult `json:"page_monitor"`
}
type pageMonitorItem struct {
	Title       string     `json:"title"`
	Link        string     `json:"link"`
	Description string     `json:"description"`
	Published   *time.Time `json:"published"`
	ContentType string     `json:"content_type"`
	ID          string     `json:"id"`
	Source      string     `json:"source"`
	SourceURL   string     `json:"source_url"`
}
type pageMonitorResult struct {
	Outcome            string            `json:"outcome"`
	SnapshotAt         *time.Time        `json:"snapshot_at"`
	ContentHash        string            `json:"content_hash"`
	Items              []pageMonitorItem `json:"items"`
	NextOffset         *int              `json:"next_offset"`
	NeedsSelectorStudy bool              `json:"needs_selector_study"`
	StudyHTML          string            `json:"study_html"`
}
type telegramArticle struct {
	Title       string          `json:"title"`
	URL         string          `json:"url"`
	Content     string          `json:"content"`
	Summary     *string         `json:"summary"`
	PublishedAt *time.Time      `json:"published_at"`
	SourceType  string          `json:"source_type"`
	SourceName  string          `json:"source_name"`
	ImageURL    *string         `json:"image_url"`
	MediaFiles  json.RawMessage `json:"media_files"`
	RawData     map[string]any  `json:"raw_data"`
}

type selectorRequest struct {
	Envelope
	Operation         string `json:"operation"`
	SourceID          int32  `json:"source_id"`
	SourceFingerprint string `json:"source_fingerprint"`
	StudyHTML         string `json:"study_html,omitempty"`
}

type selectorResponse struct {
	Version                   int      `json:"version"`
	LearnedContainerSelectors []string `json:"learned_container_selectors"`
	LearnedTitleSelectors     []string `json:"learned_title_selectors"`
	LearnedLinkSelectors      []string `json:"learned_link_selectors"`
	LearnedDateSelectors      []string `json:"learned_date_selectors"`
}

func NewHandler(db *store.Store, cfg Config) *Handler {
	return &Handler{store: db, cfg: cfg, http: &http.Client{Timeout: 125 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, runs: map[string]context.CancelFunc{}, reserved: map[string]bool{}}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("POST /internal/v1/ingestion/runs", h.createRun)
	mux.HandleFunc("GET /internal/v1/ingestion/runs/{run_id}", h.getRun)
	mux.HandleFunc("POST /internal/v1/ingestion/runs/{run_id}/cancel", h.cancelRun)
	return mux
}

func (h *Handler) authorized(r *http.Request) bool {
	header := r.Header.Get("Authorization")
	if !strings.HasPrefix(header, "Bearer ") {
		return false
	}
	got := strings.TrimPrefix(header, "Bearer ")
	return h.cfg.Token != "" && len(got) == len(h.cfg.Token) && subtle.ConstantTimeCompare([]byte(got), []byte(h.cfg.Token)) == 1
}

func (h *Handler) createRun(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		writeJSON(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxEnvelopeBytes)
	data, readErr := io.ReadAll(r.Body)
	if readErr != nil || rejectDuplicateKeys(data) != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var e Envelope
	if err := dec.Decode(&e); err != nil || dec.Decode(new(any)) != io.EOF || !validEnvelope(e) {
		writeJSON(w, 400, map[string]string{"error": "invalid_request"})
		return
	}
	canonical, _ := json.Marshal(e)
	digestBytes := sha256.Sum256(canonical)
	f := store.Fence{RunID: store.RunID(e.JobID, e.AttemptGeneration), JobID: e.JobID, AttemptGeneration: e.AttemptGeneration, EnvelopeSHA256: hex.EncodeToString(digestBytes[:]), OwnerID: e.OwnerID, WorkerGeneration: e.Generation, BackendPID: e.BackendPID, ExecutorInstance: e.ExecutorInstance, DeadlineAt: e.DeadlineAt.UTC()}
	if id, digest, found, err := h.store.ExistingRun(r.Context(), f); err != nil {
		writeJSON(w, 500, map[string]string{"error": "run_lookup_failed"})
		return
	} else if found {
		if digest != f.EnvelopeSHA256 {
			writeJSON(w, 409, map[string]string{"error": "envelope_conflict"})
			return
		}
		status, err := h.store.GetRun(r.Context(), id)
		if err != nil {
			writeJSON(w, 500, map[string]string{"error": "run_lookup_failed"})
			return
		}
		if status["state"] == "running" {
			if !h.launch(f) {
				w.Header().Set("Retry-After", "1")
				writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "busy"})
				return
			}
		}
		writeJSON(w, 200, status)
		return
	}
	if !h.reserveRun(f.RunID) {
		w.Header().Set("Retry-After", "1")
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "busy"})
		return
	}
	reserved := true
	defer func() {
		if reserved {
			h.releaseRunReservation(f.RunID)
		}
	}()
	tx, err := h.store.Pool.Begin(r.Context())
	if err != nil {
		writeJSON(w, 503, map[string]string{"error": "database_unavailable"})
		return
	}
	defer tx.Rollback(context.Background())
	if _, _, err = h.store.FenceTx(r.Context(), tx, f); err != nil {
		writeJSON(w, 409, map[string]string{"error": "stale_fence"})
		return
	}
	if err = tx.Commit(r.Context()); err != nil {
		writeJSON(w, 503, map[string]string{"error": "database_unavailable"})
		return
	}
	if !h.launch(f) {
		writeJSON(w, http.StatusTooManyRequests, map[string]string{"error": "busy"})
		return
	}
	reserved = false
	status, err := h.store.GetRun(r.Context(), f.RunID)
	if err != nil {
		writeJSON(w, 202, map[string]string{"run_id": f.RunID, "state": "running"})
		return
	}
	writeJSON(w, 202, status)
}

func rejectDuplicateKeys(data []byte) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	var value func() error
	value = func() error {
		tok, err := dec.Token()
		if err != nil {
			return err
		}
		delim, ok := tok.(json.Delim)
		if !ok {
			return nil
		}
		switch delim {
		case '{':
			seen := map[string]bool{}
			for dec.More() {
				keyToken, err := dec.Token()
				if err != nil {
					return err
				}
				key, ok := keyToken.(string)
				if !ok || seen[key] {
					return errors.New("duplicate or invalid object key")
				}
				seen[key] = true
				if err := value(); err != nil {
					return err
				}
			}
			end, err := dec.Token()
			if err != nil || end != json.Delim('}') {
				return errors.New("invalid JSON object")
			}
		case '[':
			for dec.More() {
				if err := value(); err != nil {
					return err
				}
			}
			end, err := dec.Token()
			if err != nil || end != json.Delim(']') {
				return errors.New("invalid JSON array")
			}
		default:
			return errors.New("unexpected JSON delimiter")
		}
		return nil
	}
	if err := value(); err != nil {
		return err
	}
	if _, err := dec.Token(); err != io.EOF {
		return errors.New("trailing JSON data")
	}
	return nil
}

func (h *Handler) getRun(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		writeJSON(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	v, err := h.store.GetRun(r.Context(), r.PathValue("run_id"))
	if errors.Is(err, pgx.ErrNoRows) {
		writeJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	if err != nil {
		writeJSON(w, 500, map[string]string{"error": "run_lookup_failed"})
		return
	}
	if v["state"] == "running" {
		if fence, fenceErr := h.store.LoadFence(r.Context(), r.PathValue("run_id")); fenceErr == nil {
			h.launch(fence)
		}
	}
	writeJSON(w, 200, v)
}
func (h *Handler) cancelRun(w http.ResponseWriter, r *http.Request) {
	if !h.authorized(r) {
		writeJSON(w, 401, map[string]string{"error": "unauthorized"})
		return
	}
	id := r.PathValue("run_id")
	if err := h.store.Cancel(r.Context(), id); err != nil {
		writeJSON(w, 500, map[string]string{"error": "cancel_failed"})
		return
	}
	h.mu.Lock()
	if cancel := h.runs[id]; cancel != nil {
		cancel()
	}
	h.mu.Unlock()
	v, err := h.store.GetRun(r.Context(), id)
	if err != nil {
		writeJSON(w, 404, map[string]string{"error": "not_found"})
		return
	}
	writeJSON(w, 200, v)
}

func validSelectorResponse(r selectorResponse) bool {
	if r.Version != 1 {
		return false
	}
	for _, group := range [][]string{r.LearnedContainerSelectors, r.LearnedTitleSelectors, r.LearnedLinkSelectors, r.LearnedDateSelectors} {
		if len(group) > 64 {
			return false
		}
		for _, selector := range group {
			if strings.TrimSpace(selector) == "" || len(selector) > 2048 || !utf8.ValidString(selector) {
				return false
			}
		}
	}
	return true
}

func validEnvelope(e Envelope) bool {
	if e.Version != 1 || !canonicalUUID(e.JobID) || !canonicalUUID(e.OwnerID) || !canonicalUUID(e.ExecutorInstance) || e.AttemptGeneration < 1 || e.Generation < 1 || e.BackendPID < 1 {
		return false
	}
	return !e.DeadlineAt.IsZero() && e.DeadlineAt.Location() == time.UTC && e.DeadlineAt.After(time.Now().UTC()) && e.DeadlineAt.Sub(time.Now()) <= 24*time.Hour
}
func canonicalUUID(v string) bool {
	if len(v) != 36 {
		return false
	}
	for i, c := range v {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
func (h *Handler) reserveRun(runID string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.runs[runID] != nil || h.reserved[runID] {
		return true
	}
	if len(h.runs)+len(h.reserved) >= 1 {
		return false
	}
	h.reserved[runID] = true
	return true
}

func (h *Handler) releaseRunReservation(runID string) {
	h.mu.Lock()
	delete(h.reserved, runID)
	h.mu.Unlock()
}

func (h *Handler) launch(f store.Fence) bool {
	h.mu.Lock()
	if h.runs[f.RunID] != nil {
		h.mu.Unlock()
		return true
	}
	if h.reserved[f.RunID] {
		delete(h.reserved, f.RunID)
	} else if len(h.runs)+len(h.reserved) >= 1 {
		h.mu.Unlock()
		return false
	}
	ctx, cancel := context.WithDeadline(context.Background(), f.DeadlineAt)
	h.runs[f.RunID] = cancel
	h.mu.Unlock()
	go func() {
		defer func() { cancel(); h.mu.Lock(); delete(h.runs, f.RunID); h.mu.Unlock() }()
		release, acquired, err := h.store.AcquireRunLock(ctx, f.RunID)
		if err != nil || !acquired {
			return
		}
		defer release()
		result, code := h.collect(ctx, f)
		state := "succeeded"
		if code != "" {
			state = "failed"
		}
		if err := h.store.UpdateRun(context.Background(), f, state, result, code); err != nil {
			log.Printf("ingestion terminal run update failed run_id=%s", f.RunID)
		}
	}()
	return true
}

func (h *Handler) collect(ctx context.Context, f store.Fence) (map[string]any, string) {
	// Keep the collection pass bounded independently of the parent job's
	// deadline, which remains the immutable SQL fence deadline.
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	sources, err := h.store.EnabledSources(ctx, f)
	if err != nil {
		return map[string]any{"status": "warning"}, "source_read_failed"
	}
	result := map[string]any{"status": "success", "sources_total": len(sources), "sources_completed": 0, "articles_fetched": 0}
	issues := map[string]int{}
	completed, failed := 0, 0
	for start := 0; start < len(sources); start += 8 {
		end := start + 8
		if end > len(sources) {
			end = len(sources)
		}
		batchSources := make([]store.Source, 0, end-start)
		batchPages := make(map[int32]store.PageState, end-start)
		batchDescriptors := make(map[int32]descriptor, end-start)
		descriptors := make([]descriptor, 0, end-start)
		for _, src := range sources[start:end] {
			if ctx.Err() != nil {
				return result, "deadline_or_cancelled"
			}
			page, ok, e := h.store.BeginSource(ctx, f, src)
			if e != nil {
				return result, "source_progress_failed"
			}
			if !ok {
				continue
			}
			if src.Type == "custom" {
				if learned, callbackErr := h.selectorCall(ctx, f, src, "load", ""); callbackErr == nil {
					applyLearnedSelectors(src.Config, learned)
				}
			}
			d, descriptorErr := makeDescriptor(src, page)
			if descriptorErr != "" {
				if _, e := h.store.CommitSource(ctx, f, src, nil, "failed", descriptorErr, nil); e != nil {
					return result, "source_result_persist_failed"
				}
				issues[descriptorErr]++
				completed++
				failed++
				continue
			}
			batchSources = append(batchSources, src)
			batchPages[src.ID] = page
			batchDescriptors[src.ID] = d
			descriptors = append(descriptors, d)
		}
		if len(descriptors) == 0 {
			continue
		}
		collectedBatch, batchCode, batchRetryAfter := h.fetchBatch(ctx, descriptors)
		for _, src := range batchSources {
			if ctx.Err() != nil {
				return result, "deadline_or_cancelled"
			}
			collected, code := collectedBatch[src.ID], batchCode
			selectorStudySHA256 := ""
			if code == "busy" || (code == "" && !hasCollectedPayload(collected) &&
				(collected.Outcome == "busy" || collected.Outcome == "deferred" ||
					(collected.Error != nil && (collected.Error.Code == "busy" || collected.Error.Code == "deferred")))) {
				collected, code = h.retryDeferredSource(ctx, batchDescriptors[src.ID], batchRetryAfter)
			}
			if code == "" && src.Type == "custom" && collected.Outcome == "success" &&
				collected.PageMonitor != nil && collected.PageMonitor.NeedsSelectorStudy &&
				collected.PageMonitor.StudyHTML != "" && batchPages[src.ID].CursorOffset == 0 &&
				src.Config["enable_ai_analysis"] != false {
				studyHTML := collected.PageMonitor.StudyHTML
				if learned, studyErr := h.selectorCall(ctx, f, src, "study", studyHTML); studyErr == nil {
					applyLearnedSelectors(src.Config, learned)
					updatedDescriptor, descriptorErr := makeDescriptor(src, batchPages[src.ID])
					if descriptorErr == "" {
						batchDescriptors[src.ID] = updatedDescriptor
					}
					if descriptorErr == "" {
						if reparsed, parseErr := h.parsePageMonitor(ctx, src, batchDescriptors[src.ID], collected.PageMonitor); parseErr == nil {
							collected.PageMonitor = reparsed
							collected.Outcome = reparsed.Outcome
							collected.Error = nil
							digest := sha256.Sum256([]byte(studyHTML))
							selectorStudySHA256 = hex.EncodeToString(digest[:])
						}
					}
				}
			}
			if code == "" {
				code = errorCode(collected)
			}
			// The collector can return a valid bounded prefix together with a
			// typed terminal/deferred outcome. Persist and acknowledge only that
			// prefix in the same transaction as its cursor; a response without a
			// usable payload remains an ordinary failed/deferred source.
			partialPrefix := code != "" && (collected.Outcome == "partial" || collected.Outcome == "deferred") && hasCollectedPayload(collected)
			if code != "" && !partialPrefix {
				outcome := code
				if outcome != "busy" && outcome != "deferred" && outcome != "resource_limit" && outcome != "unavailable" {
					outcome = "failed"
				}
				if _, e := h.store.CommitSource(ctx, f, src, nil, outcome, code, nil); e != nil {
					return result, "source_result_persist_failed"
				}
				issues[code]++
				completed++
				failed++
				continue
			}
			if src.Type == "telegram" && collected.Telegram != nil {
				h.enrichTelegram(ctx, collected.Telegram.Articles)
			}
			var state *store.PageState
			if src.Type == "custom" && collected.PageMonitor != nil {
				state = pageState(batchPages[src.ID], collected.PageMonitor, src.Config)
				acknowledgePageItems(state, collected.PageMonitor)
			}
			articles := normalize(collected, src, state)
			var counts store.CommitCounts
			var e error
			if selectorStudySHA256 != "" {
				counts, e = h.store.CommitSourceWithSelectorStudy(ctx, f, src, articles, collected.Outcome, code, state, selectorStudySHA256)
			} else {
				counts, e = h.store.CommitSource(ctx, f, src, articles, collected.Outcome, code, state)
			}
			if e != nil {
				return result, "source_commit_failed"
			}
			result["articles_fetched"] = result["articles_fetched"].(int) + counts.Saved + counts.Updated
			completed++
			if code != "" {
				issues[code]++
				failed++
			}
			// A deferred typed prefix advances durable Page Monitor cursor state.
			// Resume from that committed cursor within this pass budget instead of
			// treating the delivered prefix as a complete source attempt.
			if collected.Outcome == "deferred" && hasCollectedPayload(collected) {
				for retry := 0; retry < 8 && ctx.Err() == nil; retry++ {
					delay := time.NewTimer(min(250*time.Millisecond*time.Duration(retry+1), 2*time.Second))
					select {
					case <-ctx.Done():
						delay.Stop()
						return result, "deadline_or_cancelled"
					case <-delay.C:
					}
					nextPage, ok, beginErr := h.store.BeginSource(ctx, f, src)
					if beginErr != nil {
						return result, "source_resume_failed"
					}
					if !ok {
						break
					}
					nextDescriptor, descriptorCode := makeDescriptor(src, nextPage)
					if descriptorCode != "" {
						return result, "source_resume_config_invalid"
					}
					next, nextCode := h.retryDeferredSource(ctx, nextDescriptor, 0)
					if nextCode == "deadline_or_cancelled" {
						return result, nextCode
					}
					if nextCode == "" {
						nextCode = errorCode(next)
					}
					nextPrefix := nextCode != "" && (next.Outcome == "partial" || next.Outcome == "deferred") && hasCollectedPayload(next)
					if nextCode != "" && !nextPrefix {
						nextOutcome := nextCode
						if nextOutcome != "busy" && nextOutcome != "deferred" && nextOutcome != "resource_limit" && nextOutcome != "unavailable" {
							nextOutcome = "failed"
						}
						if _, err := h.store.CommitSource(ctx, f, src, nil, nextOutcome, nextCode, nil); err != nil {
							return result, "source_resume_result_failed"
						}
						break
					}
					var nextState *store.PageState
					if src.Type == "custom" && next.PageMonitor != nil {
						nextState = pageState(nextPage, next.PageMonitor, src.Config)
						acknowledgePageItems(nextState, next.PageMonitor)
					}
					nextArticles := normalize(next, src, nextState)
					nextCounts, err := h.store.CommitSource(ctx, f, src, nextArticles, next.Outcome, nextCode, nextState)
					if err != nil {
						return result, "source_resume_commit_failed"
					}
					result["articles_fetched"] = result["articles_fetched"].(int) + nextCounts.Saved + nextCounts.Updated
					if next.Outcome != "deferred" || !hasCollectedPayload(next) {
						break
					}
					collected = next
				}
			}
		}
	}
	durableCompleted, durableFetched, durableIssues, durableFailed, totalsErr := h.store.SourceTotals(ctx, f)
	if totalsErr != nil {
		return result, "source_totals_failed"
	}
	completed, issues = durableCompleted, durableIssues
	failed = durableFailed
	warningCount := 0
	for _, count := range issues {
		warningCount += count
	}
	result["sources_completed"] = completed
	result["articles_fetched"] = durableFetched
	result["errors_count"] = warningCount
	if len(issues) > 0 {
		result["status"] = "warning"
		result["source_outcomes"] = issues
	}
	if len(sources) > 0 && completed == len(sources) && failed == len(sources) {
		return result, "all_sources_failed"
	}
	return result, ""
}

func (h *Handler) enrichTelegram(ctx context.Context, articles []telegramArticle) {
	// Linked-article enrichment is optional and shares the existing bounded
	// ingestion pass. Each extractor attempt is limited to 30 seconds.
	for i := range articles {
		if ctx.Err() != nil {
			break
		}
		article := &articles[i]
		if article.Content == "" || len([]rune(article.Content)) >= 200 || article.RawData == nil {
			continue
		}
		link, ok := article.RawData["original_link"].(string)
		if !ok || link == "" {
			continue
		}
		if code := h.enrichTelegramArticle(ctx, article, link); code != "" {
			log.Printf("telegram enrichment skipped code=%s", code)
		}
	}
}

type telegramEnrichmentResponse struct {
	ContractVersion string `json:"contract_version"`
	RequestID       string `json:"request_id"`
	OriginalURL     string `json:"original_url"`
	Outcome         string `json:"outcome"`
	Result          struct {
		Content string `json:"content"`
	} `json:"result"`
	Error *struct {
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
	} `json:"error"`
}

func (h *Handler) enrichTelegramArticle(ctx context.Context, article *telegramArticle, link string) string {
	first, code := h.requestTelegramEnrichment(ctx, link, false)
	if code != "" {
		return code
	}
	if first.Outcome == "success" {
		return applyTelegramContent(article, first.Result.Content)
	}
	failureCode := telegramExtractorError(first)
	if h.cfg.ExtractorNativeBrowser && telegramBrowserEligible(link) && nativeBrowserRetryAllowed(failureCode) {
		browserResult, browserCode := h.requestTelegramEnrichment(ctx, link, true)
		if browserCode != "" {
			return browserCode
		}
		if browserResult.Outcome == "success" {
			return applyTelegramContent(article, browserResult.Result.Content)
		}
		return telegramExtractorError(browserResult)
	}
	return failureCode
}

func (h *Handler) requestTelegramEnrichment(ctx context.Context, link string, useBrowser bool) (telegramEnrichmentResponse, string) {
	requestIDBytes := make([]byte, 16)
	if _, err := rand.Read(requestIDBytes); err != nil {
		return telegramEnrichmentResponse{}, "unavailable"
	}
	requestIDBytes[6] = (requestIDBytes[6] & 0x0f) | 0x40
	requestIDBytes[8] = (requestIDBytes[8] & 0x3f) | 0x80
	requestID := fmt.Sprintf("%x-%x-%x-%x-%x", requestIDBytes[:4], requestIDBytes[4:6], requestIDBytes[6:8], requestIDBytes[8:10], requestIDBytes[10:])
	payload := map[string]any{"request_id": requestID, "url": link, "strategy": "auto",
		"source_type": "article", "budget_ms": 30000, "max_bytes": 5 * 1024 * 1024,
		"max_chars": 100000, "use_browser": useBrowser}
	body, err := json.Marshal(payload)
	if err != nil {
		return telegramEnrichmentResponse{}, "unavailable"
	}
	reqCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost,
		strings.TrimRight(h.cfg.ExtractorURL, "/")+"/v1/extract", bytes.NewReader(body))
	if err != nil {
		return telegramEnrichmentResponse{}, "unavailable"
	}
	req.Header.Set("Authorization", "Bearer "+h.cfg.ExtractorToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	resp, err := h.http.Do(req)
	if err != nil {
		if errors.Is(reqCtx.Err(), context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return telegramEnrichmentResponse{}, "deferred"
		}
		return telegramEnrichmentResponse{}, "unavailable"
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusTooManyRequests {
		return telegramEnrichmentResponse{}, "busy"
	}
	if resp.StatusCode == http.StatusServiceUnavailable || resp.StatusCode == http.StatusBadGateway || resp.StatusCode == http.StatusGatewayTimeout {
		return telegramEnrichmentResponse{}, "unavailable"
	}
	encoding := strings.ToLower(resp.Header.Get("Content-Encoding"))
	if resp.StatusCode != http.StatusOK || (encoding != "" && encoding != "identity") {
		return telegramEnrichmentResponse{}, "rejected"
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxExtractorBytes+1))
	if err != nil || len(raw) > maxExtractorBytes || rejectDuplicateKeys(raw) != nil {
		return telegramEnrichmentResponse{}, "resource_limit"
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	var result telegramEnrichmentResponse
	if dec.Decode(&result) != nil || dec.Decode(new(any)) != io.EOF || result.ContractVersion != "1" ||
		result.RequestID != requestID || result.OriginalURL != link {
		return telegramEnrichmentResponse{}, "unavailable"
	}
	return result, ""
}

func applyTelegramContent(article *telegramArticle, content string) string {
	if utf8.RuneCountInString(content) > 100000 {
		return "resource_limit"
	}
	if content == "" || utf8.RuneCountInString(content) <= 2*utf8.RuneCountInString(article.Content) {
		return "no_content"
	}
	applyTelegramEnrichment(article, content)
	return ""
}

func telegramExtractorError(response telegramEnrichmentResponse) string {
	if response.Error == nil {
		if response.Outcome == "needs_selector_study" {
			return "needs_selector_study"
		}
		if response.Outcome == "rejected" {
			return "no_content"
		}
		return "unavailable"
	}
	switch response.Error.Code {
	case "busy", "resource_limit", "unavailable", "deferred", "no_content", "browser_required",
		"needs_selector_study", "upstream_forbidden", "challenge", "invalid_content":
		return response.Error.Code
	default:
		return "rejected"
	}
}

func nativeBrowserRetryAllowed(code string) bool {
	switch code {
	case "browser_required", "no_content", "needs_selector_study", "upstream_forbidden", "challenge", "invalid_content":
		return true
	default:
		return false
	}
}

func telegramBrowserEligible(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	if err != nil || strings.ToLower(parsed.Scheme) != "https" || parsed.Hostname() == "" || parsed.User != nil {
		return false
	}
	port := parsed.Port()
	return port == "" || port == "443"
}

var (
	telegramTitlePrefix = regexp.MustCompile(`(?i)^(Forwarded from|Reply to|@\w+:?\s*)`)
	telegramHashtags    = regexp.MustCompile(`(?m)(?:^|\s)#([\pL\pN_]+)`)
	telegramTagCleanup  = regexp.MustCompile(`[^\w_]+`)
)

func applyTelegramEnrichment(article *telegramArticle, content string) {
	article.Content = content
	lines := strings.FieldsFunc(content, func(r rune) bool { return r == '\n' || r == '\r' })
	title := "Telegram Post"
	for _, line := range lines {
		if line = strings.TrimSpace(line); line != "" {
			title = line
			break
		}
	}
	title = strings.TrimSpace(telegramTitlePrefix.ReplaceAllString(title, ""))
	if runes := []rune(title); len(runes) > 120 {
		title = strings.TrimRight(string(runes[:120]), " \t\r\n")
	}
	article.Title = title
	tags := make([]string, 0, 20)
	seen := map[string]bool{}
	for _, match := range telegramHashtags.FindAllStringSubmatch(content, -1) {
		tag := strings.ToLower(telegramTagCleanup.ReplaceAllString(match[1], ""))
		if tag != "" && !seen[tag] {
			seen[tag] = true
			tags = append(tags, tag)
			if len(tags) == 20 {
				break
			}
		}
	}
	if article.RawData == nil {
		article.RawData = map[string]any{}
	}
	article.RawData["hashtags"] = tags
	article.RawData["content_length"] = utf8.RuneCountInString(content)
}

func hasCollectedPayload(r collectResult) bool {
	switch r.Type {
	case "rss":
		return r.RSSArticles != nil
	case "telegram":
		return r.Telegram != nil
	case "custom":
		return r.PageMonitor != nil
	default:
		return false
	}
}

func makeDescriptor(src store.Source, p store.PageState) (descriptor, string) {
	d := descriptor{SourceID: src.ID, Type: src.Type, SourceName: src.Name, URL: src.URL, PerSourceBudgetMS: 120000}
	if src.Type == "rss" {
		if limit, ok := positiveConfig(src.Config, "limit", 1000); ok {
			d.Limit = limit
		}
		return d, ""
	}
	if src.Type == "telegram" {
		limit, ok := positiveConfig(src.Config, "limit", 100)
		if !ok {
			return d, "invalid_source_config"
		}
		d.Telegram = &struct {
			Limit int `json:"limit,omitempty"`
		}{limit}
		return d, ""
	}
	c := sourceConfig{}
	if err := decodeMap(src.Config, &c); err != nil {
		return d, "invalid_source_config"
	}
	if c.Limit, _ = positiveConfig(src.Config, "max_articles_per_check", 100); c.Limit == 0 {
		c.Limit = 20
	}
	d.Limit = c.Limit
	c.Limit = 0
	c.Offset = p.CursorOffset
	d.PageMonitor = &c
	return d, ""
}
func (h *Handler) fetchBatch(ctx context.Context, descriptors []descriptor) (map[int32]collectResult, string, time.Duration) {
	requestID := fmt.Sprintf("ing-%d", time.Now().UnixNano())
	body, _ := json.Marshal(map[string]any{"contract_version": "1", "request_id": requestID, "batch_budget_ms": 120000, "sources": descriptors})
	u := strings.TrimRight(h.cfg.ExtractorURL, "/") + "/v1/sources/collect"
	req, err := http.NewRequestWithContext(ctx, "POST", u, bytes.NewReader(body))
	if err != nil {
		return nil, "extractor_unavailable", 0
	}
	req.Header.Set("Authorization", "Bearer "+h.cfg.ExtractorToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.http.Do(req)
	if err != nil {
		return nil, "extractor_unavailable", 0
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxExtractorBytes+1))
	if err != nil || len(raw) > maxExtractorBytes {
		return nil, "extractor_response_invalid", 0
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		code := "extractor_unavailable"
		if resp.StatusCode == http.StatusTooManyRequests {
			code = "busy"
		}
		return nil, code, boundedRetryAfterHeader(resp.Header.Get("Retry-After"))
	}
	var wire collectResponse
	if json.Unmarshal(raw, &wire) != nil || wire.ContractVersion != "1" || wire.RequestID != requestID || len(wire.Results) != len(descriptors) {
		return nil, "extractor_response_invalid", 0
	}
	wanted := make(map[int32]string, len(descriptors))
	for _, d := range descriptors {
		wanted[d.SourceID] = d.Type
	}
	results := make(map[int32]collectResult, len(descriptors))
	for _, r := range wire.Results {
		typ, ok := wanted[r.SourceID]
		if !ok || typ != r.Type {
			return nil, "extractor_response_invalid", 0
		}
		if _, duplicate := results[r.SourceID]; duplicate {
			return nil, "extractor_response_invalid", 0
		}
		if r.Outcome == "success" || r.Outcome == "partial" || r.Outcome == "deferred" {
			switch typ {
			case "rss":
				if r.RSSArticles == nil || r.Telegram != nil || r.PageMonitor != nil {
					return nil, "extractor_response_invalid", 0
				}
			case "telegram":
				if r.Telegram == nil || r.PageMonitor != nil || r.RSSArticles != nil {
					return nil, "extractor_response_invalid", 0
				}
			case "custom":
				if r.PageMonitor == nil || r.Telegram != nil || r.RSSArticles != nil {
					return nil, "extractor_response_invalid", 0
				}
			}
		}
		results[r.SourceID] = r
	}
	if len(results) != len(wanted) {
		return nil, "extractor_response_invalid", 0
	}
	return results, "", 0
}

// retryDeferredSource waits briefly for transient collector admission pressure
// within the bounded collection pass. It reuses the same descriptor and does
// not consume article-processing attempts.
func (h *Handler) retryDeferredSource(ctx context.Context, d descriptor, initialRetryAfter time.Duration) (collectResult, string) {
	delay := 250 * time.Millisecond
	retryAfter := initialRetryAfter
	for attempt := 0; attempt < 8; attempt++ {
		if wait := retryDelayForAttempt(attempt, delay, retryAfter); wait > 0 {
			timer := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				timer.Stop()
				return collectResult{}, "deadline_or_cancelled"
			case <-timer.C:
			}
			delay = min(delay*3/2, 2*time.Second)
		}
		results, code, responseRetryAfter := h.fetchBatch(ctx, []descriptor{d})
		if code != "" {
			if code == "busy" || code == "extractor_unavailable" {
				retryAfter = responseRetryAfter
				continue
			}
			return collectResult{}, code
		}
		result := results[d.SourceID]
		retryAfter = collectorRetryAfter(result.Error)
		if hasCollectedPayload(result) || (result.Outcome != "busy" && result.Outcome != "deferred" &&
			!(result.Error != nil && (result.Error.Code == "busy" || result.Error.Code == "deferred"))) {
			return result, ""
		}
	}
	return collectResult{Outcome: "deferred", Error: &struct {
		Code              string `json:"code"`
		RetryAfterSeconds *int   `json:"retry_after_seconds"`
	}{Code: "deferred"}}, ""
}

func collectorRetryAfter(failure *struct {
	Code              string `json:"code"`
	RetryAfterSeconds *int   `json:"retry_after_seconds"`
}) time.Duration {
	if failure == nil || failure.RetryAfterSeconds == nil || *failure.RetryAfterSeconds <= 0 {
		return 0
	}
	return time.Duration(min(*failure.RetryAfterSeconds, 5)) * time.Second
}

func boundedRetryAfterHeader(value string) time.Duration {
	seconds, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil || seconds <= 0 {
		return 0
	}
	return time.Duration(min(seconds, 5)) * time.Second
}

func boundedRetryDelay(backoff, retryAfter time.Duration) time.Duration {
	if retryAfter > 5*time.Second {
		retryAfter = 5 * time.Second
	}
	return max(backoff, retryAfter)
}

func retryDelayForAttempt(attempt int, backoff, retryAfter time.Duration) time.Duration {
	if attempt == 0 && retryAfter == 0 {
		return 0
	}
	return boundedRetryDelay(backoff, retryAfter)
}

func (h *Handler) selectorCall(ctx context.Context, f store.Fence, src store.Source, operation, html string) (selectorResponse, error) {
	if len(html) > 5*1024*1024 {
		return selectorResponse{}, errors.New("selector payload exceeds limit")
	}
	reqBody, err := json.Marshal(selectorRequest{
		Envelope: Envelope{Version: 1, JobID: f.JobID, AttemptGeneration: f.AttemptGeneration,
			OwnerID: f.OwnerID, Generation: f.WorkerGeneration, BackendPID: f.BackendPID,
			ExecutorInstance: f.ExecutorInstance, DeadlineAt: f.DeadlineAt.UTC()},
		Operation: operation, SourceID: src.ID, SourceFingerprint: src.Fingerprint, StudyHTML: html,
	})
	if err != nil {
		return selectorResponse{}, errors.New("selector request encoding failed")
	}
	u := strings.TrimRight(h.cfg.SelectorURL, "/") + "/internal/v1/ingestion/selectors"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(reqBody))
	if err != nil {
		return selectorResponse{}, errors.New("selector request unavailable")
	}
	req.Header.Set("Authorization", "Bearer "+h.cfg.SelectorToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.http.Do(req)
	if err != nil {
		return selectorResponse{}, errors.New("selector callback unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return selectorResponse{}, errors.New("selector callback rejected")
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 8193))
	if err != nil || len(raw) > 8192 || rejectDuplicateKeys(raw) != nil {
		return selectorResponse{}, errors.New("selector callback response invalid")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var result selectorResponse
	if dec.Decode(&result) != nil || dec.Decode(new(any)) != io.EOF || !validSelectorResponse(result) {
		return selectorResponse{}, errors.New("selector callback response invalid")
	}
	return result, nil
}

func applyLearnedSelectors(config map[string]any, result selectorResponse) {
	config["learned_container_selectors"] = result.LearnedContainerSelectors
	config["learned_title_selectors"] = result.LearnedTitleSelectors
	config["learned_link_selectors"] = result.LearnedLinkSelectors
	config["learned_date_selectors"] = result.LearnedDateSelectors
}

func (h *Handler) parsePageMonitor(ctx context.Context, src store.Source, d descriptor, captured *pageMonitorResult) (*pageMonitorResult, error) {
	if captured == nil || captured.SnapshotAt == nil || captured.StudyHTML == "" || d.PageMonitor == nil || len(captured.StudyHTML) > 5*1024*1024 {
		return nil, errors.New("captured page is invalid")
	}
	type parseRequest struct {
		ContractVersion           string            `json:"contract_version"`
		RequestID                 string            `json:"request_id"`
		Operation                 string            `json:"operation"`
		URL                       string            `json:"url"`
		SourceName                string            `json:"source_name"`
		BudgetMS                  int64             `json:"budget_ms"`
		Limit                     int               `json:"limit"`
		Offset                    int               `json:"offset"`
		MinTitleLength            int               `json:"min_title_length"`
		ProvidedHTML              string            `json:"provided_html"`
		SnapshotAt                time.Time         `json:"snapshot_at"`
		ArticleSelectors          []string          `json:"article_selectors,omitempty"`
		TitleSelectors            []string          `json:"title_selectors,omitempty"`
		DateSelectors             []string          `json:"date_selectors,omitempty"`
		LinkSelectors             []string          `json:"link_selectors,omitempty"`
		LearnedContainerSelectors []string          `json:"learned_container_selectors,omitempty"`
		LearnedTitleSelectors     []string          `json:"learned_title_selectors,omitempty"`
		LearnedLinkSelectors      []string          `json:"learned_link_selectors,omitempty"`
		LearnedDateSelectors      []string          `json:"learned_date_selectors,omitempty"`
		LearnedGeneralSelectors   []generalSelector `json:"learned_general_selectors,omitempty"`
	}
	requestID := fmt.Sprintf("study-%d", time.Now().UnixNano())
	cfg := d.PageMonitor
	body, _ := json.Marshal(parseRequest{ContractVersion: "1", RequestID: requestID, Operation: "parse",
		URL: src.URL, SourceName: src.Name, BudgetMS: 30000, Limit: d.Limit, Offset: 0,
		MinTitleLength: cfg.MinTitleLength, ProvidedHTML: captured.StudyHTML, SnapshotAt: captured.SnapshotAt.UTC(),
		ArticleSelectors: cfg.ArticleSelectors, TitleSelectors: cfg.TitleSelectors, DateSelectors: cfg.DateSelectors,
		LinkSelectors: cfg.LinkSelectors, LearnedContainerSelectors: cfg.LearnedContainerSelectors,
		LearnedTitleSelectors: cfg.LearnedTitleSelectors, LearnedLinkSelectors: cfg.LearnedLinkSelectors,
		LearnedDateSelectors: cfg.LearnedDateSelectors, LearnedGeneralSelectors: cfg.LearnedGeneralSelectors})
	u := strings.TrimRight(h.cfg.ExtractorURL, "/") + "/v1/page-monitor/extract"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.cfg.ExtractorToken)
	req.Header.Set("Content-Type", "application/json")
	resp, err := h.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxExtractorBytes+1))
	if err != nil || len(raw) > maxExtractorBytes || rejectDuplicateKeys(raw) != nil || resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, errors.New("same-snapshot reparse failed")
	}
	var wire struct {
		ContractVersion    string            `json:"contract_version"`
		BuildVersion       string            `json:"build_version"`
		RequestID          string            `json:"request_id"`
		Outcome            string            `json:"outcome"`
		OriginalURL        string            `json:"original_url"`
		FinalURL           string            `json:"final_url"`
		FetchMethod        string            `json:"fetch_method"`
		SelectorsUsed      []string          `json:"selectors_used"`
		SnapshotAt         *time.Time        `json:"snapshot_at"`
		ContentHash        string            `json:"content_hash"`
		Items              []pageMonitorItem `json:"items"`
		NextOffset         *int              `json:"next_offset"`
		NeedsSelectorStudy bool              `json:"needs_selector_study"`
		StudyHTML          string            `json:"study_html"`
		StudyHTMLOmitted   bool              `json:"study_html_omitted"`
		Error              *struct {
			Code              string `json:"code"`
			Retryable         bool   `json:"retryable"`
			UpstreamStatus    int    `json:"upstream_status"`
			RetryAfterSeconds *int   `json:"retry_after_seconds"`
			Reason            string `json:"reason"`
		} `json:"error"`
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&wire) != nil || dec.Decode(new(any)) != io.EOF || wire.ContractVersion != "1" || wire.RequestID != requestID ||
		wire.BuildVersion == "" || wire.OriginalURL != src.URL || !validHTTPURL(wire.FinalURL) || wire.SelectorsUsed == nil ||
		wire.FetchMethod != "provided_html" ||
		(wire.Outcome != "success" && wire.Outcome != "partial") || wire.SnapshotAt == nil || len(wire.Items) > d.Limit {
		return nil, errors.New("same-snapshot reparse response invalid")
	}
	return &pageMonitorResult{Outcome: wire.Outcome, SnapshotAt: wire.SnapshotAt,
		ContentHash: wire.ContentHash, Items: wire.Items, NextOffset: wire.NextOffset,
		NeedsSelectorStudy: wire.NeedsSelectorStudy, StudyHTML: ""}, nil
}

func validHTTPURL(raw string) bool {
	parsed, err := url.Parse(raw)
	return err == nil && (strings.EqualFold(parsed.Scheme, "http") || strings.EqualFold(parsed.Scheme, "https")) && parsed.Hostname() != "" && parsed.User == nil
}

func normalize(r collectResult, src store.Source, page *store.PageState) []store.Article {
	if r.RSSArticles != nil {
		out := append([]store.Article(nil), r.RSSArticles...)
		for i := range out {
			out[i].SourceName, out[i].SourceType = src.Name, src.Type
		}
		return out
	}
	if r.Telegram != nil {
		out := make([]store.Article, 0, len(r.Telegram.Articles))
		for _, a := range r.Telegram.Articles {
			c := a.Content
			out = append(out, store.Article{Title: a.Title, URL: a.URL, Content: &c, Summary: a.Summary, ImageURL: a.ImageURL, MediaFiles: a.MediaFiles, PublishedAt: a.PublishedAt, SourceType: a.SourceType, SourceName: src.Name, RawData: a.RawData})
		}
		return out
	}
	if r.PageMonitor != nil {
		out := make([]store.Article, 0, len(r.PageMonitor.Items))
		include := map[int]bool{}
		if page != nil {
			for _, index := range page.CandidateIndexes {
				include[index] = true
			}
		}
		for index, a := range r.PageMonitor.Items {
			if page != nil && !include[index] {
				continue
			}
			c := a.Description
			out = append(out, store.Article{Title: a.Title, URL: a.Link, Content: &c, PublishedAt: a.Published, SourceType: r.Type, SourceName: src.Name, MediaFiles: json.RawMessage(`[]`), RawData: map[string]any{}})
		}
		return out
	}
	return []store.Article{}
}
func pageState(old store.PageState, r *pageMonitorResult, cfg map[string]any) *store.PageState {
	limit, ok := positiveConfig(cfg, "max_articles_per_check", 100)
	if !ok || limit == 0 {
		limit = 20
	}
	state := old
	if state.ScanLimit < 1 || state.ScanLimit > 100 {
		state.ScanLimit = limit
	}
	if state.PreviousHashes == nil {
		state.PreviousHashes = map[string]string{}
	}
	if state.ScanHashes == nil || !state.ScanActive {
		state.ScanHashes = map[string]string{}
		state.AcknowledgedHashes = map[string]string{}
		state.CursorOffset = 0
		state.ScanActive = true
	}
	if state.AcknowledgedHashes == nil {
		state.AcknowledgedHashes = map[string]string{}
	}
	state.SegmentOffset = state.CursorOffset
	if r.SnapshotAt != nil {
		t := r.SnapshotAt.UTC()
		state.SnapshotAt = &t
	}
	state.ContentSHA256 = strings.ToLower(r.ContentHash)
	baseAck := map[string]bool{}
	for _, hash := range state.PreviousHashes {
		baseAck[hash] = true
	}
	for position, hash := range state.ScanHashes {
		if state.AcknowledgedHashes[position] == hash {
			baseAck[hash] = true
		}
	}
	seen := map[string]bool{}
	for index, item := range r.Items {
		position := state.SegmentOffset + index
		hash := pageArticleHash(item.Title, item.Link, item.Description)
		key := strconv.Itoa(position)
		state.ScanHashes[key] = hash
		if previous, exists := state.PreviousHashes[key]; exists && previous == hash {
			state.AcknowledgedHashes[key] = hash
			baseAck[hash] = true
		}
		if !baseAck[hash] && !seen[hash] {
			state.CandidateIndexes = append(state.CandidateIndexes, index)
			seen[hash] = true
		}
	}
	if r.NextOffset != nil {
		state.CursorOffset = *r.NextOffset
		state.ScanActive = true
	} else {
		state.ScanActive = true
	}
	return &state
}

func pageArticleHash(title, link, description string) string {
	if runes := []rune(description); len(runes) > 100 {
		description = string(runes[:100])
	}
	canonical := []byte(`{"description": ` + pythonJSONQuote(description) + `, "link": ` + pythonJSONQuote(link) + `, "title": ` + pythonJSONQuote(title) + `}`)
	sum := md5.Sum(canonical)
	return hex.EncodeToString(sum[:16])
}

func pythonJSONQuote(value string) string {
	var out strings.Builder
	out.WriteByte('"')
	for _, r := range value {
		switch r {
		case '"':
			out.WriteString(`\"`)
		case '\\':
			out.WriteString(`\\`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		default:
			if r < 0x20 {
				fmt.Fprintf(&out, `\u%04x`, r)
			} else if r > 0x7e {
				if r <= 0xffff {
					fmt.Fprintf(&out, `\u%04x`, r)
				} else {
					hi, lo := utf16.EncodeRune(r)
					fmt.Fprintf(&out, `\u%04x\u%04x`, hi, lo)
				}
			} else {
				out.WriteRune(r)
			}
		}
	}
	out.WriteByte('"')
	return out.String()
}

func acknowledgePageItems(state *store.PageState, response *pageMonitorResult) {
	for index, item := range response.Items {
		position := strconv.Itoa(state.SegmentOffset + index)
		state.AcknowledgedHashes[position] = pageArticleHash(item.Title, item.Link, item.Description)
	}
	complete := response.Outcome == "success" && response.NextOffset == nil
	if response.NextOffset != nil && *response.NextOffset >= state.ScanLimit {
		complete = true
	}
	if !complete {
		return
	}
	current := map[string]string{}
	for position, hash := range state.ScanHashes {
		if state.AcknowledgedHashes[position] == hash || state.PreviousHashes[position] == hash {
			current[position] = hash
		}
	}
	state.PreviousHashes = current
	state.ScanHashes = map[string]string{}
	state.AcknowledgedHashes = map[string]string{}
	state.CursorOffset = 0
	state.ScanActive = false
}
func errorCode(r collectResult) string {
	if r.Error != nil && r.Error.Code != "" {
		return safeCode(r.Error.Code)
	}
	if r.Outcome == "success" || r.Outcome == "partial" {
		return ""
	}
	if r.Outcome == "resource_limit" || r.Outcome == "unavailable" || r.Outcome == "busy" || r.Outcome == "deferred" {
		return r.Outcome
	}
	return "source_failed"
}
func safeCode(s string) string {
	if len(s) > 40 {
		return "source_failed"
	}
	for _, r := range s {
		if !(r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '_' || r == '-') {
			return "source_failed"
		}
	}
	return s
}
func decodeMap(m map[string]any, v any) error {
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	return json.Unmarshal(b, v)
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func positiveConfig(m map[string]any, key string, max int) (int, bool) {
	v, exists := m[key]
	if !exists || v == nil {
		return 0, true
	}
	n, ok := v.(float64)
	if !ok || n != float64(int(n)) || n <= 0 {
		return 0, false
	}
	if int(n) > max {
		return 0, false
	}
	return int(n), true
}
