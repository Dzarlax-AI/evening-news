package httpui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/jobs"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/management"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/repositories"
)

type ManagementConfig struct {
	Service                   *management.Service
	Auth                      *auth.Service
	PublicAdmin               func(*http.Request) bool
	ScheduledJobsToken        string
	WorkerMode                bool
	DispatchMode              string
	ScheduledJobsReceiptLimit int
	EditorialDigestStatus     func(context.Context, time.Time, string) (repositories.EditorialDigestStatus, error)
}
type managementHandler struct{ cfg ManagementConfig }

// RegisterManagement registers only completed command/query routes. Heavy provider-backed
// analysis, backup, telegram settings and other unowned APIs remain unregistered.
func RegisterManagement(mux *http.ServeMux, cfg ManagementConfig) {
	h := &managementHandler{cfg: cfg}
	admin := func(pattern string, fn http.HandlerFunc) { mux.Handle(pattern, RequireAPIAdmin(cfg.Auth, fn)) }
	admin("GET /api/v1/sources", h.sources)
	admin("POST /api/v1/sources", h.createSource)
	admin("PUT /api/v1/sources/{sourceID}", h.updateSource)
	admin("PATCH /api/v1/sources/{sourceID}/toggle", h.toggleSource)
	admin("DELETE /api/v1/sources/{sourceID}", h.deleteSource)
	admin("POST /api/v1/sources/{sourceID}/test", h.testSource)
	admin("GET /api/v1/categories/", h.categories)
	admin("POST /api/v1/categories", h.createCategory)
	admin("POST /api/v1/categories/", h.createCategory)
	admin("PUT /api/v1/categories/{categoryID}", h.updateCategory)
	admin("GET /api/v1/categories/mappings", h.mappings)
	admin("POST /api/v1/categories/mappings", h.upsertMapping)
	admin("PUT /api/v1/categories/mappings/{mappingID}", h.updateMapping)
	admin("DELETE /api/v1/categories/mappings/{mappingID}", h.deleteMapping)
	admin("POST /api/v1/categories/mappings/{mappingID}/toggle", h.toggleMapping)
	admin("GET /api/v1/categories/mappings/fixed-categories", h.fixedCategories)
	admin("GET /api/v1/categories/mappings/unmapped", h.unmapped)
	admin("GET /api/v1/category-mappings", h.mappings)
	admin("POST /api/v1/category-mappings", h.upsertMapping)
	admin("PUT /api/v1/category-mappings/{mappingID}", h.updateMapping)
	admin("DELETE /api/v1/category-mappings/{mappingID}", h.deleteMapping)
	admin("POST /api/v1/category-mappings/{mappingID}/toggle", h.toggleMapping)
	admin("GET /api/v1/category-mappings/fixed-categories", h.fixedCategoryMappings)
	admin("GET /api/v1/category-mappings/unmapped", h.unmapped)
	admin("GET /api/v1/articles/{articleID}", h.article)
	admin("POST /api/v1/articles/{articleID}/reprocess", h.reprocessArticle)
	admin("POST /api/v1/process/run", h.processRun)
	admin("GET /api/v1/process/status", h.processStatus)
	admin("POST /api/v1/process/retry", h.processRetry)
	admin("GET /api/v1/summaries/daily", h.dailySummaries)
	admin("GET /api/v1/summaries/editorial-status", h.editorialDigestStatus)
	admin("GET /api/v1/summaries/categories", h.summaryCategories)
	admin("GET /api/v1/summaries/{summaryID}", h.summary)
	admin("POST /api/v1/summaries/generate", h.generateSummaries)
	admin("GET /api/v1/jobs/{jobID}", h.job)
	admin("POST /api/v1/backup", h.createBackup)
	admin("POST /api/v1/backup/sync", h.createBackup)
	admin("POST /api/v1/telegram/send-digest", h.sendDigest)
	admin("GET /api/v1/telegram/settings", h.telegramSettings)
	admin("POST /api/v1/telegram/settings", h.updateTelegramSettings)
	admin("GET /api/v1/stats/dashboard", h.dashboardStats)
	admin("GET /api/v1/stats/queue", h.statsQueue)
	admin("GET /api/v1/stats/ai-usage", h.aiUsageStats)
	admin("GET /api/v1/stats/processing", h.processingStats)
	admin("GET /api/v1/stats/extractor", h.extractorStats)
	admin("POST /api/v1/categories/ai-category-mapping/approve", h.approveAIMapping)
	admin("GET /api/v1/system/health/db", h.databaseHealth)
	admin("GET /api/v1/system/info", h.systemInfo)
	mux.Handle("POST /api/public/article/{articleID}/reprocess", auth.RequirePublicAdmin(
		cfg.PublicAdmin, http.HandlerFunc(h.publicReprocess),
	))
	mux.HandleFunc("POST /api/v1/internal/scheduled-jobs", h.acceptScheduledJob)
}

func (h *managementHandler) dashboardStats(w http.ResponseWriter, r *http.Request) {
	days, ok := queryInt(w, r, "days", 7, 1, 30)
	if !ok {
		return
	}
	h.run(w, r, 200, func(ctx context.Context) (any, error) { return h.cfg.Service.DashboardStats(ctx, days) })
}
func (h *managementHandler) aiUsageStats(w http.ResponseWriter, r *http.Request) {
	days, ok := queryInt(w, r, "days", 30, 1, 90)
	if !ok {
		return
	}
	h.run(w, r, 200, func(ctx context.Context) (any, error) { return h.cfg.Service.AIUsageStats(ctx, days) })
}
func (h *managementHandler) processingStats(w http.ResponseWriter, r *http.Request) {
	days, ok := queryInt(w, r, "days", 30, 1, 365)
	if !ok {
		return
	}
	h.run(w, r, 200, func(ctx context.Context) (any, error) { return h.cfg.Service.ProcessingStats(ctx, days) })
}
func (h *managementHandler) extractorStats(w http.ResponseWriter, r *http.Request) {
	h.run(w, r, 200, func(ctx context.Context) (any, error) { return h.cfg.Service.ExtractorStats(ctx) })
}
func (h *managementHandler) statsQueue(w http.ResponseWriter, r *http.Request) {
	h.run(w, r, 200, func(ctx context.Context) (any, error) { return h.cfg.Service.ProcessStatus(ctx) })
}
func queryInt(w http.ResponseWriter, r *http.Request, key string, def, min, max int) (int, bool) {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return def, true
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		writeInputError(w, "query", key, "int_parsing", "Input should be a valid integer", raw)
		return 0, false
	}
	if v < min || v > max {
		typ, msg := "greater_than_equal", fmt.Sprintf("Input should be greater than or equal to %d", min)
		if v > max {
			typ, msg = "less_than_equal", fmt.Sprintf("Input should be less than or equal to %d", max)
		}
		writeInputError(w, "query", key, typ, msg, v)
		return 0, false
	}
	return v, true
}
func (h *managementHandler) telegramSettings(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	body, err := h.cfg.Service.TelegramSettings(ctx)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}
func (h *managementHandler) updateTelegramSettings(w http.ResponseWriter, r *http.Request) {
	var in management.TelegramSettings
	if !decodeBody(w, r, &in) {
		return
	}
	h.run(w, r, 200, func(ctx context.Context) (any, error) {
		return h.cfg.Service.UpdateTelegramSettings(ctx, in)
	})
}
func (h *managementHandler) approveAIMapping(w http.ResponseWriter, r *http.Request) {
	var request struct {
		AICategory            string   `json:"ai_category"`
		ApprovedFixedCategory string   `json:"approved_fixed_category"`
		ConfidenceThreshold   *float64 `json:"confidence_threshold"`
		Description           *string  `json:"description"`
	}
	if !decodeBodyRequired(w, r, &request, "ai_category", "approved_fixed_category") {
		return
	}
	in := management.MappingRequest{AICategory: request.AICategory, FixedCategory: request.ApprovedFixedCategory, ConfidenceThreshold: request.ConfidenceThreshold, Description: request.Description}
	h.run(w, r, 200, func(ctx context.Context) (any, error) { return h.cfg.Service.ApproveAIMapping(ctx, in) })
}
func (h *managementHandler) createBackup(w http.ResponseWriter, r *http.Request) {
	h.enqueue(w, r, func(ctx context.Context, key string) (any, error) {
		return h.cfg.Service.EnqueueJob(ctx, "backup", map[string]any{}, key)
	})
}
func (h *managementHandler) sendDigest(w http.ResponseWriter, r *http.Request) {
	h.enqueue(w, r, func(ctx context.Context, key string) (any, error) {
		return h.cfg.Service.EnqueueJob(ctx, "telegram_digest", map[string]any{}, key)
	})
}
func (h *managementHandler) databaseHealth(w http.ResponseWriter, r *http.Request) {
	h.run(w, r, 200, func(ctx context.Context) (any, error) { return h.cfg.Service.DatabaseHealth(ctx) })
}
func (h *managementHandler) systemInfo(w http.ResponseWriter, r *http.Request) {
	var fs syscall.Statfs_t
	var disk any
	if err := syscall.Statfs(".", &fs); err == nil {
		total := fs.Blocks * uint64(fs.Bsize)
		free := fs.Bavail * uint64(fs.Bsize)
		used := total - free
		percent := 0.0
		if total > 0 {
			percent = float64(used) * 100 / float64(total)
		}
		disk = map[string]any{"total": total, "used": used, "free": free, "usage_percent": percent}
	}
	commit := "unknown"
	if b, ok := debug.ReadBuildInfo(); ok {
		for _, s := range b.Settings {
			if s.Key == "vcs.revision" && len(s.Value) >= 7 {
				commit = s.Value[:7]
			}
		}
	}
	writeJSON(w, 200, map[string]any{"go_version": runtime.Version(), "git_commit": commit, "disk_usage": disk, "timestamp": managementTimestamp()})
}

func managementTimestamp() string {
	now := time.Now().UTC().Truncate(time.Microsecond)
	if now.Nanosecond() == 0 {
		return now.Format("2006-01-02T15:04:05")
	}
	return now.Format("2006-01-02T15:04:05.000000")
}
func (h *managementHandler) sources(w http.ResponseWriter, r *http.Request) {
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.Sources(ctx) })
}
func (h *managementHandler) createSource(w http.ResponseWriter, r *http.Request) {
	var in management.SourceCreate
	if !decodeBodyRequired(w, r, &in, "name", "source_type", "url") {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.CreateSource(ctx, in) })
}
func (h *managementHandler) updateSource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "sourceID", "source_id")
	if !ok {
		return
	}
	var in management.SourceUpdate
	if !decodeBody(w, r, &in) {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.UpdateSource(ctx, id, in) })
}
func (h *managementHandler) toggleSource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "sourceID", "source_id")
	if !ok {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.ToggleSource(ctx, id) })
}
func (h *managementHandler) deleteSource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "sourceID", "source_id")
	if !ok {
		return
	}
	purge := false
	if raw := r.URL.Query().Get("delete_articles"); raw != "" {
		v, e := strconv.ParseBool(raw)
		if e != nil {
			writeInputError(w, "query", "delete_articles", "bool_parsing", "Input should be a valid boolean", raw)
			return
		}
		purge = v
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.DeleteSource(ctx, id, purge) })
}
func (h *managementHandler) testSource(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "sourceID", "source_id")
	if !ok {
		return
	}
	h.enqueue(w, r, func(ctx context.Context, key string) (any, error) { return h.cfg.Service.TestSource(ctx, id, key) })
}
func (h *managementHandler) categories(w http.ResponseWriter, r *http.Request) {
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.Categories(ctx) })
}
func (h *managementHandler) createCategory(w http.ResponseWriter, r *http.Request) {
	var in management.CategoryCreate
	if !decodeBodyRequired(w, r, &in, "name", "display_name") {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.CreateCategory(ctx, in) })
}
func (h *managementHandler) updateCategory(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "categoryID", "category_id")
	if !ok {
		return
	}
	var in management.CategoryUpdate
	if !decodeBodyRequired(w, r, &in, "display_name") {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.UpdateCategory(ctx, id, in) })
}
func (h *managementHandler) mappings(w http.ResponseWriter, r *http.Request) {
	active := true
	if raw := r.URL.Query().Get("active_only"); raw != "" {
		v, e := strconv.ParseBool(raw)
		if e != nil {
			writeInputError(w, "query", "active_only", "bool_parsing", "Input should be a valid boolean", raw)
			return
		}
		active = v
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.Mappings(ctx, active) })
}
func (h *managementHandler) upsertMapping(w http.ResponseWriter, r *http.Request) {
	var in management.MappingRequest
	if !decodeBodyRequired(w, r, &in, "ai_category", "fixed_category") {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.UpsertMapping(ctx, in) })
}
func (h *managementHandler) updateMapping(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "mappingID", "mapping_id")
	if !ok {
		return
	}
	var in management.MappingRequest
	if !decodeBodyRequired(w, r, &in, "ai_category", "fixed_category") {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.UpdateMapping(ctx, id, in) })
}
func (h *managementHandler) deleteMapping(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "mappingID", "mapping_id")
	if !ok {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.DeleteMapping(ctx, id) })
}
func (h *managementHandler) toggleMapping(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "mappingID", "mapping_id")
	if !ok {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.ToggleMapping(ctx, id) })
}
func (h *managementHandler) fixedCategories(w http.ResponseWriter, r *http.Request) {
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.FixedCategories(ctx) })
}
func (h *managementHandler) fixedCategoryMappings(w http.ResponseWriter, r *http.Request) {
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.FixedCategoryMappings(ctx) })
}
func (h *managementHandler) unmapped(w http.ResponseWriter, r *http.Request) {
	limit := 100
	if raw := r.URL.Query().Get("limit"); raw != "" {
		v, e := strconv.Atoi(raw)
		if e != nil {
			writeInputError(w, "query", "limit", "int_parsing", "Input should be a valid integer", raw)
			return
		}
		limit = v
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.UnmappedCategories(ctx, limit) })
}
func (h *managementHandler) article(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "articleID", "article_id")
	if !ok {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.Article(ctx, id) })
}
func (h *managementHandler) reprocessArticle(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "articleID", "article_id")
	if !ok {
		return
	}
	in := management.ReprocessRequest{}
	if r.ContentLength != 0 {
		if !decodeBody(w, r, &in) {
			return
		}
	}
	h.enqueue(w, r, func(ctx context.Context, key string) (any, error) {
		return h.cfg.Service.ReprocessArticle(ctx, id, in, key)
	})
}
func (h *managementHandler) publicReprocess(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "articleID", "article_id")
	if !ok {
		return
	}
	in := management.ReprocessRequest{}
	if !decodeBody(w, r, &in) {
		return
	}
	h.enqueue(w, r, func(ctx context.Context, key string) (any, error) {
		return h.cfg.Service.ReprocessArticle(ctx, id, in, key)
	})
}
func (h *managementHandler) processRun(w http.ResponseWriter, r *http.Request) {
	h.enqueue(w, r, func(ctx context.Context, key string) (any, error) { return h.cfg.Service.ProcessRun(ctx, key) })
}
func (h *managementHandler) processStatus(w http.ResponseWriter, r *http.Request) {
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.ProcessStatus(ctx) })
}
func (h *managementHandler) processRetry(w http.ResponseWriter, r *http.Request) {
	var in management.RetryRequest
	if !decodeBodyRequired(w, r, &in, "article_ids") {
		return
	}
	if len(in.ArticleIDs) < 1 || len(in.ArticleIDs) > 100 {
		writeInputError(w, "body", "article_ids", "too_short", "List should have at least 1 item", nil)
		return
	}
	h.enqueue(w, r, func(ctx context.Context, key string) (any, error) { return h.cfg.Service.ProcessRetry(ctx, in, key) })
}
func (h *managementHandler) dailySummaries(w http.ResponseWriter, r *http.Request) {
	limit := 10
	offset := 0
	if raw := r.URL.Query().Get("limit"); raw != "" {
		v, e := strconv.Atoi(raw)
		if e != nil {
			writeInputError(w, "query", "limit", "int_parsing", "Input should be a valid integer", raw)
			return
		}
		limit = v
	}
	if raw := r.URL.Query().Get("offset"); raw != "" {
		v, e := strconv.Atoi(raw)
		if e != nil {
			writeInputError(w, "query", "offset", "int_parsing", "Input should be a valid integer", raw)
			return
		}
		offset = v
	}
	q := management.SummaryQuery{Limit: limit, Offset: offset, Category: r.URL.Query().Get("category")}
	if d := r.URL.Query().Get("date"); d != "" {
		q.Date = &d
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.DailySummaries(ctx, q) })
}

func (h *managementHandler) editorialDigestStatus(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	dateValues, dateSupplied := query["date"]
	if !dateSupplied || len(dateValues) != 1 {
		writeInputError(w, "query", "date", "date_parsing", "Input should be a valid date in YYYY-MM-DD format", nil)
		return
	}
	date, err := time.Parse("2006-01-02", dateValues[0])
	if err != nil || date.Format("2006-01-02") != dateValues[0] {
		writeInputError(w, "query", "date", "date_parsing", "Input should be a valid date in YYYY-MM-DD format", dateValues[0])
		return
	}
	locale := "ru"
	if localeValues, supplied := query["locale"]; supplied {
		if len(localeValues) != 1 || (localeValues[0] != "ru" && localeValues[0] != "sr-Latn") {
			writeInputError(w, "query", "locale", "value_error", "Unsupported locale", nil)
			return
		}
		locale = localeValues[0]
	}
	if h.cfg.EditorialDigestStatus == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Editorial digest status unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	status, err := h.cfg.EditorialDigestStatus(ctx, date, locale)
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Editorial digest status unavailable")
		return
	}
	if status.Ready {
		if status.Date != date.Format("2006-01-02") || status.Locale != locale || !validUUID(status.EditionID) {
			writeProblem(w, http.StatusServiceUnavailable, "Editorial digest status unavailable")
			return
		}
		status.PreviewURL = fmt.Sprintf("/digest/%s?locale=%s&edition=%s", status.Date, status.Locale, status.EditionID)
	} else {
		status.Date = date.Format("2006-01-02")
		status.Locale = locale
		status.EditionID = ""
		status.PreviewURL = ""
		status.MainCount = 0
		status.BriefCount = 0
		status.StoryCount = 0
		status.ArticleCount = 0
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *managementHandler) summaryCategories(w http.ResponseWriter, r *http.Request) {
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.SummaryCategories(ctx) })
}
func (h *managementHandler) summary(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "summaryID", "summary_id")
	if !ok {
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.Summary(ctx, id) })
}
func (h *managementHandler) generateSummaries(w http.ResponseWriter, r *http.Request) {
	h.enqueue(w, r, func(ctx context.Context, key string) (any, error) { return h.cfg.Service.GenerateSummaries(ctx, key) })
}
func (h *managementHandler) job(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("jobID")
	if !validUUID(id) {
		writeInputError(w, "path", "job_id", "uuid_parsing", "Input should be a valid UUID", id)
		return
	}
	h.run(w, r, http.StatusOK, func(ctx context.Context) (any, error) { return h.cfg.Service.GetJob(ctx, id) })
}

type enqueueCall func(context.Context, string) (any, error)

func (h *managementHandler) enqueue(w http.ResponseWriter, r *http.Request, fn enqueueCall) {
	h.run(w, r, http.StatusAccepted, func(ctx context.Context) (any, error) { return fn(ctx, r.Header.Get("Idempotency-Key")) })
}

type serviceCall func(context.Context) (any, error)

func (h *managementHandler) run(w http.ResponseWriter, r *http.Request, success int, fn serviceCall) {
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	out, err := fn(ctx)
	if err != nil {
		writeManagementError(w, err)
		return
	}
	writeJSON(w, success, out)
}
func pathID(w http.ResponseWriter, r *http.Request, path, key string) (int64, bool) {
	v, err := strconv.ParseInt(r.PathValue(path), 10, 64)
	if err != nil {
		writeInputError(w, "path", key, "int_parsing", "Input should be a valid integer", r.PathValue(path))
		return 0, false
	}
	return v, true
}
func decodeBody(w http.ResponseWriter, r *http.Request, out any) bool {
	return decodeBodyRequired(w, r, out)
}
func decodeBodyRequired(w http.ResponseWriter, r *http.Request, out any, required ...string) bool {
	body, err := io.ReadAll(io.LimitReader(r.Body, (1<<20)+1))
	if err != nil || len(body) > 1<<20 {
		writeInputError(w, "body", "body", "json_invalid", "JSON body is invalid or too large", nil)
		return false
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		writeInputError(w, "body", "body", "missing", "Field required", nil)
		return false
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(body, &fields); err != nil || fields == nil {
		writeInputError(w, "body", "body", "json_invalid", "JSON body is invalid", nil)
		return false
	}
	for _, key := range required {
		if _, ok := fields[key]; !ok {
			writeInputError(w, "body", key, "missing", "Field required", nil)
			return false
		}
	}
	if err = json.Unmarshal(body, out); err != nil {
		writeInputError(w, "body", "body", "json_invalid", "JSON body is invalid", nil)
		return false
	}
	return true
}
func writeInputError(w http.ResponseWriter, source, field, typ, msg string, input any) {
	detail := map[string]any{"type": typ, "loc": []string{source, field}, "msg": msg}
	if typ != "missing" && input != nil {
		detail["input"] = input
	}
	writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"detail": []map[string]any{detail}})
}
func writeManagementError(w http.ResponseWriter, err error) {
	var api *management.APIError
	if errors.As(err, &api) {
		if api.Status == 422 {
			parts := strings.SplitN(api.Detail, ": ", 2)
			field, msg := "body", api.Detail
			if len(parts) == 2 {
				field, msg = parts[0], parts[1]
			}
			writeInputError(w, "body", field, "value_error", msg, nil)
			return
		}
		writeProblem(w, api.Status, api.Detail)
		return
	}
	switch {
	case errors.Is(err, jobs.ErrQueueFull):
		w.Header().Set("Retry-After", "10")
		writeProblem(w, http.StatusServiceUnavailable, "Background queue is full")
	case errors.Is(err, jobs.ErrIdempotencyConflict):
		writeProblem(w, http.StatusConflict, "Idempotency key conflicts with an existing job")
	case errors.Is(err, jobs.ErrWorkerUnavailable):
		writeProblem(w, http.StatusServiceUnavailable, "Background worker unavailable")
	case errors.Is(err, management.ErrConflict):
		writeProblem(w, http.StatusConflict, "Record conflicts with existing data")
	case errors.Is(err, jobs.ErrNotFound):
		writeProblem(w, http.StatusNotFound, "Job not found")
	case errors.Is(err, management.ErrAdapterUnavailable):
		writeProblem(w, http.StatusServiceUnavailable, "Required management adapter unavailable")
	default:
		writeProblem(w, http.StatusServiceUnavailable, "Database unavailable")
	}
}
func validUUID(s string) bool {
	if len(s) != 36 {
		return false
	}
	for i, c := range s {
		if i == 8 || i == 13 || i == 18 || i == 23 {
			if c != '-' {
				return false
			}
			continue
		}
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// ScheduledJob is the strict JSON envelope accepted from the private scheduler.
type scheduledJobWire struct {
	Version          int            `json:"version"`
	OccurrenceID     string         `json:"occurrence_id"`
	ScheduleKey      string         `json:"schedule_key"`
	ScheduleRevision int            `json:"schedule_revision"`
	ScheduledFor     string         `json:"scheduled_for"`
	Operation        string         `json:"operation"`
	Payload          map[string]any `json:"payload"`
}

func (h *managementHandler) acceptScheduledJob(w http.ResponseWriter, r *http.Request) {
	if h.cfg.ScheduledJobsToken == "" {
		writeProblem(w, http.StatusServiceUnavailable, "scheduled job receiver unavailable")
		return
	}
	authz := r.Header.Get("Authorization")
	if !strings.HasPrefix(authz, "Bearer ") || !secureEqual(strings.TrimPrefix(authz, "Bearer "), h.cfg.ScheduledJobsToken) {
		writeProblem(w, http.StatusUnauthorized, "unauthorized")
		return
	}
	if mt := strings.TrimSpace(strings.Split(r.Header.Get("Content-Type"), ";")[0]); !strings.EqualFold(mt, "application/json") {
		writeProblem(w, http.StatusUnsupportedMediaType, "application/json required")
		return
	}
	if enc := r.Header.Get("Content-Encoding"); enc != "" && !strings.EqualFold(enc, "identity") {
		writeProblem(w, http.StatusUnsupportedMediaType, "content encoding is not supported")
		return
	}
	if r.ContentLength > 8192 {
		writeProblem(w, http.StatusRequestEntityTooLarge, "scheduled job envelope too large")
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 8193))
	if err != nil || len(raw) > 8192 {
		writeProblem(w, http.StatusRequestEntityTooLarge, "scheduled job envelope too large")
		return
	}
	if hasDuplicateJSONKeys(raw) {
		writeProblem(w, http.StatusUnprocessableEntity, "invalid scheduled job envelope")
		return
	}
	var wire scheduledJobWire
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	dec.DisallowUnknownFields()
	if err = dec.Decode(&wire); err != nil {
		writeProblem(w, http.StatusUnprocessableEntity, "invalid scheduled job envelope")
		return
	}
	var extra any
	if dec.Decode(&extra) != io.EOF {
		writeProblem(w, http.StatusUnprocessableEntity, "invalid scheduled job envelope")
		return
	}
	if wire.Payload == nil {
		writeProblem(w, http.StatusUnprocessableEntity, "invalid scheduled job envelope")
		return
	}
	payload := normalizeJSONNumbers(wire.Payload).(map[string]any)
	cmd := jobs.OccurrenceCommand{Version: wire.Version, OccurrenceID: wire.OccurrenceID, ScheduleKey: wire.ScheduleKey, ScheduleRevision: wire.ScheduleRevision, ScheduledFor: wire.ScheduledFor, Operation: wire.Operation, Payload: payload, IdempotencyKey: r.Header.Get("Idempotency-Key"), WorkerMode: h.cfg.WorkerMode, DispatchMode: h.cfg.DispatchMode, ReceiptLimit: h.cfg.ScheduledJobsReceiptLimit}
	ctx, cancel := context.WithTimeout(r.Context(), 8*time.Second)
	defer cancel()
	accepted, err := h.cfg.Service.AcceptScheduledJob(ctx, cmd)
	if err != nil {
		if errors.Is(err, jobs.ErrIdempotencyConflict) {
			writeProblem(w, http.StatusConflict, "occurrence content conflicts")
			return
		}
		if errors.Is(err, jobs.ErrWorkerUnavailable) {
			writeProblem(w, http.StatusServiceUnavailable, "background worker unavailable")
			return
		}
		if errors.Is(err, jobs.ErrQueueFull) {
			w.Header().Set("Retry-After", "10")
			writeProblem(w, http.StatusServiceUnavailable, "background queue is full")
			return
		}
		var api *management.APIError
		if errors.As(err, &api) {
			writeProblem(w, api.Status, api.Detail)
			return
		}
		writeProblem(w, http.StatusServiceUnavailable, "scheduled job receiver unavailable")
		return
	}
	status := http.StatusAccepted
	if !accepted.Created {
		status = http.StatusOK
	}
	writeJSON(w, status, map[string]any{"version": 1, "occurrence_id": accepted.OccurrenceID, "job_id": accepted.JobID, "status": "accepted"})
}
func secureEqual(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	var diff byte
	for i := range a {
		diff |= a[i] ^ b[i]
	}
	return diff == 0
}
func hasDuplicateJSONKeys(raw []byte) bool {
	var value any
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	v, err := decodeUniqueValue(dec)
	if err != nil {
		return true
	}
	if _, err = dec.Token(); err != io.EOF {
		return true
	}
	_ = value
	_ = v
	return false
}
func decodeUniqueValue(dec *json.Decoder) (any, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		if t == '{' {
			m := map[string]any{}
			for dec.More() {
				kTok, e := dec.Token()
				if e != nil {
					return nil, e
				}
				k, ok := kTok.(string)
				if !ok {
					return nil, fmt.Errorf("invalid key")
				}
				if _, exists := m[k]; exists {
					return nil, fmt.Errorf("duplicate key")
				}
				v, e := decodeUniqueValue(dec)
				if e != nil {
					return nil, e
				}
				m[k] = v
			}
			_, err = dec.Token()
			return m, err
		}
		if t == '[' {
			a := []any{}
			for dec.More() {
				v, e := decodeUniqueValue(dec)
				if e != nil {
					return nil, e
				}
				a = append(a, v)
			}
			_, err = dec.Token()
			return a, err
		}
		return nil, fmt.Errorf("invalid delimiter")
	}
	return tok, nil
}
func normalizeJSONNumbers(v any) any {
	switch x := v.(type) {
	case json.Number:
		if n, e := x.Int64(); e == nil {
			return n
		}
		f, _ := x.Float64()
		return f
	case map[string]any:
		for k, val := range x {
			x[k] = normalizeJSONNumbers(val)
		}
		return x
	case []any:
		for i, val := range x {
			x[i] = normalizeJSONNumbers(val)
		}
		return x
	}
	return v
}
