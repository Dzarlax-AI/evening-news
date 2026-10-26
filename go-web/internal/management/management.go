// Package management contains typed command/query contracts for admin API operations.
package management

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/jobs"
)

var (
	ErrNotFound            = errors.New("record not found")
	ErrConflict            = errors.New("record conflicts with existing data")
	ErrQueueFull           = jobs.ErrQueueFull
	ErrIdempotencyConflict = jobs.ErrIdempotencyConflict
	ErrWorkerUnavailable   = jobs.ErrWorkerUnavailable
	ErrAdapterUnavailable  = errors.New("required backend adapter unavailable")
)

type APIError struct {
	Status int
	Detail string
}

func (e *APIError) Error() string { return e.Detail }

type SourceCreate struct {
	Name           string `json:"name"`
	SourceType     string `json:"source_type"`
	URL            string `json:"url"`
	IsActive       *bool  `json:"is_active"`
	UpdateInterval int    `json:"update_interval"`
}
type SourceUpdate struct {
	Name           *string `json:"name"`
	SourceType     *string `json:"source_type"`
	URL            *string `json:"url"`
	IsActive       *bool   `json:"is_active"`
	UpdateInterval *int    `json:"update_interval"`
}
type CategoryCreate struct {
	Name        string  `json:"name"`
	DisplayName string  `json:"display_name"`
	Description *string `json:"description"`
	Color       *string `json:"color"`
}
type CategoryUpdate struct {
	DisplayName string  `json:"display_name"`
	Color       *string `json:"color"`
	Description *string `json:"description"`
	IsLocal     *bool   `json:"is_local"`
}
type MappingRequest struct {
	AICategory          string   `json:"ai_category"`
	FixedCategory       string   `json:"fixed_category"`
	ConfidenceThreshold *float64 `json:"confidence_threshold"`
	Description         *string  `json:"description"`
}
type SummaryQuery struct {
	Limit, Offset int
	Category      string
	Date          *string
}
type RetryRequest struct {
	ArticleIDs []int64 `json:"article_ids"`
}
type ReprocessRequest struct {
	Force          *bool `json:"force"`
	ReextractMedia bool  `json:"reextract_media"`
}
type TelegramSettings struct {
	ChatID        *string `json:"chat_id"`
	ServiceChatID *string `json:"service_chat_id"`
}
type TelegramSettingsNotifier interface{ TelegramSettingsUpdated(context.Context) error }
type TelegramConfigurationReader interface {
	TelegramConfiguration(context.Context) (json.RawMessage, error)
}
type MappingCacheInvalidator interface{ InvalidateCategoryCache(context.Context) error }
type Repository interface {
	Sources(context.Context) (map[string]any, error)
	CreateSource(context.Context, SourceCreate) (map[string]any, error)
	UpdateSource(context.Context, int64, SourceUpdate) (map[string]any, error)
	ToggleSource(context.Context, int64) (map[string]any, error)
	DeleteSource(context.Context, int64, bool) (map[string]any, error)
	Categories(context.Context) (map[string]any, error)
	CreateCategory(context.Context, CategoryCreate) (map[string]any, error)
	UpdateCategory(context.Context, int64, CategoryUpdate) (map[string]any, error)
	Mappings(context.Context, bool) ([]map[string]any, error)
	UpsertMapping(context.Context, MappingRequest) (map[string]any, error)
	UpdateMapping(context.Context, int64, MappingRequest) (map[string]any, error)
	DeleteMapping(context.Context, int64) (map[string]any, error)
	ToggleMapping(context.Context, int64) (map[string]any, error)
	FixedCategories(context.Context) (map[string]any, error)
	FixedCategoryMappings(context.Context) (map[string]any, error)
	UnmappedCategories(context.Context, int) (map[string]any, error)
	Article(context.Context, int64) (map[string]any, error)
	ArticleExists(context.Context, int64) (bool, error)
	SourceExists(context.Context, int64) (bool, error)
	DailySummaries(context.Context, SummaryQuery) (map[string]any, error)
	SummaryCategories(context.Context) (map[string]any, error)
	Summary(context.Context, int64) (map[string]any, error)
	ProcessingStatus(context.Context) (map[string]any, error)
	DashboardStats(context.Context, int) (map[string]any, error)
	AIUsageStats(context.Context, int) (map[string]any, error)
	ProcessingStats(context.Context, int) (map[string]any, error)
	ExtractorStats(context.Context) (map[string]any, error)
	UpdateTelegramSettings(context.Context, TelegramSettings) error
	ApproveAIMapping(context.Context, MappingRequest) (map[string]any, error)
	DatabaseHealth(context.Context) (map[string]any, error)
}
type MappingCacheNotifier interface{ NotifyCategoryMappingsChanged(context.Context) error }
type Service struct {
	repo                  Repository
	jobs                  *jobs.Service
	notifier              MappingCacheNotifier
	telegramNotifier      TelegramSettingsNotifier
	telegramConfiguration TelegramConfigurationReader
	mappingInvalidator    MappingCacheInvalidator
}

func New(repo Repository, jobService *jobs.Service, notifier MappingCacheNotifier) *Service {
	service := &Service{repo: repo, jobs: jobService, notifier: notifier}
	if n, ok := notifier.(TelegramSettingsNotifier); ok {
		service.telegramNotifier = n
	}
	if n, ok := notifier.(TelegramConfigurationReader); ok {
		service.telegramConfiguration = n
	}
	if n, ok := notifier.(MappingCacheInvalidator); ok {
		service.mappingInvalidator = n
	}
	return service
}

func (s *Service) SetTelegramSettingsNotifier(n TelegramSettingsNotifier) { s.telegramNotifier = n }
func (s *Service) TelegramSettings(ctx context.Context) (json.RawMessage, error) {
	if s.telegramConfiguration == nil {
		return nil, ErrAdapterUnavailable
	}
	return s.telegramConfiguration.TelegramConfiguration(ctx)
}
func (s *Service) SetMappingCacheInvalidator(n MappingCacheInvalidator) { s.mappingInvalidator = n }

func (s *Service) Sources(ctx context.Context) (map[string]any, error) { return s.repo.Sources(ctx) }
func (s *Service) CreateSource(ctx context.Context, in SourceCreate) (map[string]any, error) {
	if !validSourceType(in.SourceType) {
		return nil, &APIError{Status: 400, Detail: "unsupported source type"}
	}
	return s.repo.CreateSource(ctx, in)
}
func (s *Service) UpdateSource(ctx context.Context, id int64, in SourceUpdate) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Source not found")
	}
	return s.repo.UpdateSource(ctx, id, in)
}
func (s *Service) TestSource(ctx context.Context, id int64, key string) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Source not found")
	}
	ok, err := s.repo.SourceExists(ctx, id)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, notFound("Source not found")
	}
	return s.enqueue(ctx, "source_test", map[string]any{"source_id": id}, key)
}
func (s *Service) ToggleSource(ctx context.Context, id int64) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Source not found")
	}
	return s.repo.ToggleSource(ctx, id)
}
func (s *Service) DeleteSource(ctx context.Context, id int64, purge bool) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Source not found")
	}
	return s.repo.DeleteSource(ctx, id, purge)
}
func (s *Service) Categories(ctx context.Context) (map[string]any, error) {
	return s.repo.Categories(ctx)
}
func (s *Service) CreateCategory(ctx context.Context, in CategoryCreate) (map[string]any, error) {
	if in.Color == nil {
		v := "#6c757d"
		in.Color = &v
	}
	return s.repo.CreateCategory(ctx, in)
}
func (s *Service) UpdateCategory(ctx context.Context, id int64, in CategoryUpdate) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Category not found")
	}
	return s.repo.UpdateCategory(ctx, id, in)
}
func (s *Service) Mappings(ctx context.Context, active bool) ([]map[string]any, error) {
	return s.repo.Mappings(ctx, active)
}
func (s *Service) UpsertMapping(ctx context.Context, in MappingRequest) (map[string]any, error) {
	if err := validateMapping(&in); err != nil {
		return nil, err
	}
	if s.notifier == nil && s.mappingInvalidator == nil {
		return nil, fmt.Errorf("%w: category mapping cache notifier", ErrAdapterUnavailable)
	}
	out, err := s.repo.UpsertMapping(ctx, in)
	if err == nil {
		err = s.notifyMappingChanged(ctx)
	}
	return out, err
}
func (s *Service) UpdateMapping(ctx context.Context, id int64, in MappingRequest) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Mapping not found")
	}
	if err := validateMapping(&in); err != nil {
		return nil, err
	}
	if s.notifier == nil && s.mappingInvalidator == nil {
		return nil, fmt.Errorf("%w: category mapping cache notifier", ErrAdapterUnavailable)
	}
	out, err := s.repo.UpdateMapping(ctx, id, in)
	if err == nil {
		err = s.notifyMappingChanged(ctx)
	}
	return out, err
}
func (s *Service) DeleteMapping(ctx context.Context, id int64) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Mapping not found")
	}
	if s.notifier == nil && s.mappingInvalidator == nil {
		return nil, fmt.Errorf("%w: category mapping cache notifier", ErrAdapterUnavailable)
	}
	out, err := s.repo.DeleteMapping(ctx, id)
	if err == nil {
		err = s.notifyMappingChanged(ctx)
	}
	return out, err
}
func (s *Service) ToggleMapping(ctx context.Context, id int64) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Mapping not found")
	}
	if s.notifier == nil && s.mappingInvalidator == nil {
		return nil, fmt.Errorf("%w: category mapping cache notifier", ErrAdapterUnavailable)
	}
	out, err := s.repo.ToggleMapping(ctx, id)
	if err == nil {
		err = s.notifyMappingChanged(ctx)
	}
	return out, err
}
func (s *Service) FixedCategories(ctx context.Context) (map[string]any, error) {
	return s.repo.FixedCategories(ctx)
}
func (s *Service) FixedCategoryMappings(ctx context.Context) (map[string]any, error) {
	return s.repo.FixedCategoryMappings(ctx)
}
func (s *Service) UnmappedCategories(ctx context.Context, limit int) (map[string]any, error) {
	return s.repo.UnmappedCategories(ctx, limit)
}
func (s *Service) notifyMappingChanged(ctx context.Context) error {
	if s.mappingInvalidator != nil {
		if err := s.mappingInvalidator.InvalidateCategoryCache(ctx); err != nil {
			return &APIError{Status: 502, Detail: "Mapping was saved but private category cache invalidation failed; do not retry automatically"}
		}
		return nil
	}
	if s.notifier == nil {
		return fmt.Errorf("%w: category mapping cache notifier", ErrAdapterUnavailable)
	}
	if err := s.notifier.NotifyCategoryMappingsChanged(ctx); err != nil {
		return &APIError{Status: 502, Detail: "Mapping was saved but private category cache invalidation failed; do not retry automatically"}
	}
	return nil
}
func validateMapping(in *MappingRequest) error {
	if in.ConfidenceThreshold == nil {
		v := 0.7
		in.ConfidenceThreshold = &v
	}
	return nil
}
func (s *Service) Article(ctx context.Context, id int64) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Article not found")
	}
	return s.repo.Article(ctx, id)
}
func (s *Service) ReprocessArticle(ctx context.Context, id int64, in ReprocessRequest, key string) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Article not found")
	}
	exists, err := s.repo.ArticleExists(ctx, id)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, notFound("Article not found")
	}
	return s.enqueue(ctx, "article_reprocess", map[string]any{"article_ids": []int64{id}, "force": forceValue(in.Force), "reextract_media": in.ReextractMedia}, key)
}
func (s *Service) ProcessRun(ctx context.Context, key string) (map[string]any, error) {
	return s.enqueue(ctx, "news_processing", map[string]any{}, key)
}
func (s *Service) EnqueueJob(ctx context.Context, kind string, payload map[string]any, key string) (map[string]any, error) {
	return s.enqueue(ctx, kind, payload, key)
}
func (s *Service) ProcessRetry(ctx context.Context, in RetryRequest, key string) (map[string]any, error) {
	return s.enqueue(ctx, "article_reprocess", map[string]any{"article_ids": in.ArticleIDs, "force": false, "reextract_media": false}, key)
}
func (s *Service) ProcessStatus(ctx context.Context) (map[string]any, error) {
	return s.repo.ProcessingStatus(ctx)
}
func (s *Service) DashboardStats(ctx context.Context, days int) (map[string]any, error) {
	return s.repo.DashboardStats(ctx, days)
}
func (s *Service) AIUsageStats(ctx context.Context, days int) (map[string]any, error) {
	return s.repo.AIUsageStats(ctx, days)
}
func (s *Service) ProcessingStats(ctx context.Context, days int) (map[string]any, error) {
	return s.repo.ProcessingStats(ctx, days)
}
func (s *Service) ExtractorStats(ctx context.Context) (map[string]any, error) {
	return s.repo.ExtractorStats(ctx)
}
func (s *Service) UpdateTelegramSettings(ctx context.Context, in TelegramSettings) (map[string]any, error) {
	if s.telegramNotifier == nil {
		return nil, ErrAdapterUnavailable
	}
	if err := s.repo.UpdateTelegramSettings(ctx, in); err != nil {
		return nil, err
	}
	if err := s.telegramNotifier.TelegramSettingsUpdated(ctx); err != nil {
		return map[string]any{"success": true, "message": "Настройки сохранены", "warning": "The saved settings may not be active in the private service yet; the update was not retried."}, nil
	}
	return map[string]any{"success": true, "message": "Настройки сохранены"}, nil
}
func (s *Service) ApproveAIMapping(ctx context.Context, in MappingRequest) (map[string]any, error) {
	if err := validateMapping(&in); err != nil {
		return nil, err
	}
	if s.notifier == nil && s.mappingInvalidator == nil {
		return nil, ErrAdapterUnavailable
	}
	out, err := s.repo.ApproveAIMapping(ctx, in)
	if err == nil {
		err = s.notifyMappingChanged(ctx)
	}
	return out, err
}
func (s *Service) DatabaseHealth(ctx context.Context) (map[string]any, error) {
	return s.repo.DatabaseHealth(ctx)
}
func (s *Service) DailySummaries(ctx context.Context, q SummaryQuery) (map[string]any, error) {
	if q.Limit < 1 || q.Limit > 100 {
		return nil, bad("limit", "ensure this value is between 1 and 100")
	}
	if q.Offset < 0 {
		return nil, bad("offset", "ensure this value is greater than or equal to 0")
	}
	return s.repo.DailySummaries(ctx, q)
}
func (s *Service) SummaryCategories(ctx context.Context) (map[string]any, error) {
	return s.repo.SummaryCategories(ctx)
}
func (s *Service) Summary(ctx context.Context, id int64) (map[string]any, error) {
	if id < 1 {
		return nil, notFound("Summary not found")
	}
	return s.repo.Summary(ctx, id)
}
func (s *Service) GenerateSummaries(ctx context.Context, key string) (map[string]any, error) {
	return s.enqueue(ctx, "summaries_generate", map[string]any{}, key)
}
func (s *Service) GetJob(ctx context.Context, id string) (map[string]any, error) {
	if s.jobs == nil {
		return nil, ErrAdapterUnavailable
	}
	j, err := s.jobs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return map[string]any{"success": true, "job_id": j.ID, "kind": j.Kind, "status": j.Status, "created_at": j.CreatedAt, "started_at": j.StartedAt, "finished_at": j.FinishedAt, "result": j.Result, "error_code": j.ErrorCode, "worker_available": j.WorkerAvailable}, nil
}
func (s *Service) enqueue(ctx context.Context, kind string, payload map[string]any, key string) (map[string]any, error) {
	if s.jobs == nil {
		return nil, ErrAdapterUnavailable
	}
	var err error
	if key == "" {
		key, err = randomIdempotencyKey()
		if err != nil {
			return nil, err
		}
	}
	if _, err := jobs.ValidatePayload(kind, payload); err != nil {
		return nil, &APIError{Status: 422, Detail: err.Error()}
	}
	if !safeIdempotencyKey(key) {
		return nil, &APIError{Status: 422, Detail: "idempotency key must be a safe 1-200 character token"}
	}
	result, err := s.jobs.Submit(ctx, jobs.SubmitCommand{Kind: kind, Payload: payload, IdempotencyKey: key})
	if err != nil {
		return nil, err
	}
	return map[string]any{"success": true, "status": "queued", "job_id": result.Job.ID, "status_url": "/api/v1/jobs/" + result.Job.ID}, nil
}
func safeIdempotencyKey(s string) bool {
	if len(s) < 1 || len(s) > 200 {
		return false
	}
	for i, c := range s {
		if !(c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || i > 0 && strings.ContainsRune("._:+-", c)) {
			return false
		}
	}
	return true
}
func validSourceType(s string) bool { return s == "rss" || s == "telegram" || s == "custom" }
func forceValue(v *bool) bool {
	if v == nil {
		return true
	}
	return *v
}
func randomIdempotencyKey() (string, error) {
	var b [24]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(b[:]), nil
}
func bad(field, detail string) error {
	return &APIError{Status: 422, Detail: fmt.Sprintf("%s: %s", field, detail)}
}
func notFound(detail string) error { return &APIError{Status: 404, Detail: detail} }

func (s *Service) AcceptScheduledJob(ctx context.Context, cmd jobs.OccurrenceCommand) (jobs.AcceptedOccurrence, error) {
	if s.jobs == nil {
		return jobs.AcceptedOccurrence{}, ErrAdapterUnavailable
	}
	if err := jobs.ValidateOccurrence(cmd); err != nil {
		return jobs.AcceptedOccurrence{}, &APIError{Status: 422, Detail: "invalid scheduled job envelope"}
	}
	return s.jobs.AcceptOccurrence(ctx, cmd)
}
