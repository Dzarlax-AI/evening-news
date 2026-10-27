// Package orchestration translates durable job kinds into bounded Go service
// calls and finite Python domain operations.
package orchestration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-execution"
)

type Config struct {
	Domain     DomainConfig
	Ingestion  ServiceConfig
	Processing ServiceConfig
	HTTPClient *http.Client
}

type Pipeline struct {
	domain     *DomainClient
	ingestion  *serviceRunClient
	processing *serviceRunClient
}

func NewPipeline(cfg Config) (*Pipeline, error) {
	if cfg.HTTPClient != nil {
		if cfg.Domain.HTTPClient == nil {
			cfg.Domain.HTTPClient = cfg.HTTPClient
		}
		if cfg.Ingestion.HTTPClient == nil {
			cfg.Ingestion.HTTPClient = cfg.HTTPClient
		}
		if cfg.Processing.HTTPClient == nil {
			cfg.Processing.HTTPClient = cfg.HTTPClient
		}
	}
	domain, err := NewDomainClient(cfg.Domain)
	if err != nil {
		return nil, err
	}
	ingest, err := newServiceRunClient(cfg.Ingestion, "/internal/v1/ingestion/runs")
	if err != nil {
		return nil, err
	}
	processing, err := newServiceRunClient(cfg.Processing, "/internal/v1/processing/runs")
	if err != nil {
		return nil, err
	}
	return &Pipeline{domain: domain, ingestion: ingest, processing: processing}, nil
}

// Ready performs the authenticated Python domain check used by the embedded
// execution status and new-job admission paths. A transient failure blocks new
// admissions without stopping the runner or its worker-generation recovery.
func (p *Pipeline) Ready(ctx context.Context) error {
	return p.domain.Ready(ctx)
}

func (p *Pipeline) Execute(ctx context.Context, f execution.ExecutionContext, kind string, payload json.RawMessage) (execution.Result, error) {
	var values map[string]json.RawMessage
	if len(payload) == 0 {
		payload = []byte(`{}`)
	}
	if err := json.Unmarshal(payload, &values); err != nil || values == nil {
		return execution.Result{}, errors.New("job payload is invalid")
	}
	var input map[string]any
	if err := json.Unmarshal(payload, &input); err != nil {
		return execution.Result{}, errors.New("job payload is invalid")
	}
	status, err := p.domain.Status(ctx)
	if err != nil {
		return execution.Result{}, err
	}
	call := func(op, id string, in any) (map[string]any, error) {
		raw, e := p.domain.ExecutePinned(ctx, f, status.DomainInstanceID, op, id, in)
		if e != nil {
			return nil, e
		}
		var m map[string]any
		if json.Unmarshal(raw, &m) != nil || m == nil {
			return nil, errors.New("domain operation result is invalid")
		}
		return m, nil
	}
	var result map[string]any
	switch kind {
	case "news_processing":
		result, err = p.fullCycle(ctx, f, call)
	case "news_digest":
		result = map[string]any{}
		if boolDefault(input, "run_processing", true) {
			result, err = p.fullCycle(ctx, f, call)
		}
		if err == nil && boolDefault(input, "send_telegram", true) {
			parts, e := p.digest(ctx, f, call)
			if e != nil {
				err = e
			} else {
				result["parts_sent"] = parts
			}
		}
	case "telegram_digest":
		var parts int
		parts, err = p.digest(ctx, f, call)
		result = map[string]any{"parts_sent": parts}
	case "summaries_generate":
		result, err = call("summaries_generate", "summaries_generate", map[string]any{})
	case "backup":
		result, err = call("backup", "backup", map[string]any{})
	case "source_test":
		sourceID, ok := positiveInt(input["source_id"])
		if !ok {
			err = errors.New("source test payload is invalid")
		} else {
			result, err = call("source_test", fmt.Sprintf("source_test:%d", sourceID), map[string]any{"source_id": sourceID})
			if err == nil {
				success, ok := result["success"].(bool)
				if !ok {
					err = errors.New("source test result is invalid")
				} else {
					result = map[string]any{"success": success, "connection_ok": success}
					if !success {
						result["status"] = "warning"
					}
				}
			}
		}
	case "article_reprocess":
		ids, ok := articleIDs(input["article_ids"])
		if !ok {
			err = errors.New("article reprocess payload is invalid")
			break
		}
		if boolDefault(input, "reextract_media", false) {
			for _, id := range ids {
				if _, e := call("media_refresh", fmt.Sprintf("media_refresh:%d", id), map[string]any{"article_id": id}); e != nil && errors.Is(e, ErrStaleFence) {
					err = e
					break
				}
			}
		}
		if err == nil {
			result, err = p.processing.execute(ctx, f)
		}
	case "reprocess_failed":
		limit := 20
		if n, ok := positiveInt(input["limit"]); ok {
			limit = n
		}
		dryRun := boolDefault(input, "dry_run", false)
		_ = limit
		_ = dryRun // Processing dispatch reads the admitted persisted job payload.
		result, err = p.processing.execute(ctx, f)
	default:
		err = fmt.Errorf("unsupported job kind %q", kind)
	}
	if err != nil {
		return execution.Result{}, err
	}
	if result == nil {
		result = map[string]any{}
	}
	clean := boundedProjection(kind, result)
	out, e := json.Marshal(clean)
	if e != nil || len(out) > execution.MaxResultBytes {
		return execution.Result{}, errors.New("job result exceeded its limit")
	}
	return execution.Result{Value: out}, nil
}

func (p *Pipeline) fullCycle(ctx context.Context, f execution.ExecutionContext, call func(string, string, any) (map[string]any, error)) (map[string]any, error) {
	started := time.Now()
	ingest, err := p.ingestion.execute(ctx, f)
	if err != nil {
		return nil, errors.New("article ingestion failed")
	}
	result := map[string]any{"sources_synced": metric(ingest["sources_completed"]), "articles_fetched": metric(ingest["articles_fetched"]), "errors_count": metric(ingest["errors_count"])}
	processed, err := p.processing.execute(ctx, f)
	if err != nil {
		return nil, errors.New("article processing failed")
	}
	status, _ := processed["status"].(string)
	if status == "success" || status == "warning" {
		result["status"] = status
	}
	for _, key := range []string{"articles_processed", "articles_summarized", "articles_categorized", "articles_requeued", "articles_rejected", "articles_retrying", "articles_exhausted", "total_candidates", "found_candidates", "processed", "improved", "duration_seconds", "requeued_count"} {
		if v, ok := safeMetric(processed[key]); ok {
			result[key] = v
		}
	}
	result["errors_count"] = math.Min(1e9, metric(result["errors_count"])+metric(processed["errors_count"]))
	// Persist at millisecond precision. This keeps the shared Go/Python JSON
	// fingerprint stable and matches the core receipt duration precision.
	result["duration_seconds"] = durationSeconds(time.Since(started))
	// Python's full-cycle path treats dashboard-stat persistence as best effort.
	// Keep the explicit leaf call, but do not turn a stats write failure into a
	// failed news run.
	stats := map[string]any{
		"articles_fetched":   metric(result["articles_fetched"]),
		"articles_processed": metric(result["articles_processed"]),
		"api_calls_made":     0,
		"errors_count":       metric(result["errors_count"]),
		"duration_seconds":   metric(result["duration_seconds"]),
	}
	_, _ = call("stats_publish", "stats_publish", map[string]any{"stats": stats})
	return result, nil
}

func (p *Pipeline) digest(ctx context.Context, f execution.ExecutionContext, call func(string, string, any) (map[string]any, error)) (int, error) {
	status, err := call("summary_status", "summary_status", map[string]any{})
	if err != nil {
		return 0, errors.New("summary status failed")
	}
	count, ok := nonnegativeInt(status["summaries_count"])
	if !ok {
		return 0, errors.New("summary status result is invalid")
	}
	if count == 0 {
		if _, err = call("summaries_generate", "summaries_generate", map[string]any{}); err != nil {
			return 0, errors.New("daily summaries generation failed")
		}
	}
	prepared, err := call("digest_prepare", "digest_prepare", map[string]any{})
	if err != nil {
		return 0, errors.New("digest preparation failed")
	}
	token, ok := prepared["prepared_token"].(string)
	if !ok || !canonicalUUID(token) {
		return 0, errors.New("digest preparation result is invalid")
	}
	published, err := call("digest_publish", "digest_publish", map[string]any{"prepared_token": token})
	if err != nil {
		return 0, errors.New("digest publication outcome is unknown")
	}
	if success, ok := published["success"].(bool); !ok || !success {
		return 0, errors.New("digest delivery failed")
	}
	parts, ok := nonnegativeInt(published["parts_sent"])
	total, totalOK := nonnegativeInt(published["parts_total"])
	_, richOK := published["rich_message"].(bool)
	if !ok || !totalOK || total < parts || !richOK {
		return 0, errors.New("digest publication result is invalid")
	}
	return parts, nil
}

func boundedProjection(kind string, v map[string]any) map[string]any {
	out := map[string]any{"status": "success"}
	statuses := map[string]bool{"success": true, "warning": true, "deferred": true, "dry_run": true, "completed": true, "rejected": true, "exhausted": true, "retry_wait": true, "incomplete": true, "settled": true, "busy": true}
	if s, ok := v["status"].(string); ok && statuses[s] {
		out["status"] = s
	}
	for _, k := range []string{"success", "settled", "retryable", "connection_ok"} {
		if b, ok := v[k].(bool); ok {
			out[k] = b
		}
	}
	for _, k := range []string{"articles_fetched", "articles_processed", "articles_summarized", "articles_categorized", "articles_requeued", "articles_rejected", "articles_retrying", "articles_exhausted", "total_candidates", "found_candidates", "processed", "improved", "summaries_generated", "parts_sent", "errors_count", "duration_seconds", "backup_bytes", "requeued_count"} {
		if n, ok := safeMetric(v[k]); ok {
			out[k] = n
		}
	}
	if kind == "article_reprocess" {
		for _, k := range []string{"settled", "retryable"} {
			if b, ok := v[k].(bool); ok {
				out[k] = b
			}
		}
		outcomes := projectArticleOutcomes(v["article_outcomes"])
		if outcomes != nil {
			out["article_outcomes"] = outcomes
			validTotal := len(outcomes)
			if raw, ok := v["article_outcomes"].([]any); ok {
				validTotal = 0
				for _, item := range raw {
					if validArticleOutcome(item) {
						validTotal++
					}
				}
			}
			total := validTotal
			if n, ok := nonnegativeInt(v["article_outcomes_total"]); ok {
				total = n
				if total < validTotal {
					total = validTotal
				}
				if total > 100 {
					total = 100
				}
			}
			out["article_outcomes_total"] = total
			out["article_outcomes_omitted"] = max(0, total-len(outcomes))
			if hasAnyNonCompleted(v["article_outcomes"]) && out["status"] == "success" {
				out["status"] = "warning"
			}
		}
	}
	if kind == "reprocess_failed" {
		out["dry_run"] = out["status"] == "dry_run"
	}
	if kind == "source_test" {
		ok, _ := out["connection_ok"].(bool)
		if !ok {
			out["status"] = "warning"
		}
	}
	if v["success"] == false || metric(v["errors_count"]) > 0 {
		if out["status"] == "success" {
			out["status"] = "warning"
		}
	}
	return out
}

func projectArticleOutcomes(value any) []map[string]any {
	raw, ok := value.([]any)
	if !ok {
		return nil
	}
	valid := make([]map[string]any, 0, len(raw))
	safeReasons := map[string]bool{"advertisement": true, "resource_limit": true, "category_mapping_failed": true, "missing_summary": true, "smart_filter_rejected": true, "extraction_challenge": true, "provider_timeout": true}
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		id, ok := positiveInt(m["article_id"])
		if !ok {
			continue
		}
		state, ok := m["state"].(string)
		if !ok || !(state == "completed" || state == "rejected" || state == "exhausted" || state == "pending" || state == "retry_wait" || state == "processing") {
			continue
		}
		reason, _ := m["reason"].(string)
		if !safeReasons[reason] {
			reason = "other"
		}
		valid = append(valid, map[string]any{"article_id": id, "state": state, "reason": reason})
	}
	limit := 20
	if len(valid) == 1 {
		limit = 1
	}
	if len(valid) > limit {
		valid = valid[:limit]
	}
	return valid
}
func validArticleOutcome(v any) bool {
	m, ok := v.(map[string]any)
	if !ok {
		return false
	}
	if _, ok := positiveInt(m["article_id"]); !ok {
		return false
	}
	state, ok := m["state"].(string)
	return ok && (state == "completed" || state == "rejected" || state == "exhausted" || state == "pending" || state == "retry_wait" || state == "processing")
}
func hasNonCompleted(v []map[string]any) bool {
	for _, item := range v {
		if item["state"] != "completed" {
			return true
		}
	}
	return false
}
func hasAnyNonCompleted(v any) bool {
	raw, ok := v.([]any)
	if !ok {
		return false
	}
	for _, item := range raw {
		if validArticleOutcome(item) {
			m := item.(map[string]any)
			if m["state"] != "completed" {
				return true
			}
		}
	}
	return false
}
func safeMetric(v any) (float64, bool) {
	n, ok := v.(float64)
	if !ok {
		return 0, false
	}
	if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 {
		return 0, false
	}
	return math.Min(1e9, n), true
}
func metric(v any) float64 { n, _ := safeMetric(v); return n }

func durationSeconds(d time.Duration) float64 {
	seconds := math.Min(86400, math.Max(0, d.Seconds()))
	return math.Min(86400, math.Round(seconds*1000)/1000)
}
func nonnegativeInt(v any) (int, bool) {
	n, ok := v.(float64)
	if !ok || n < 0 || n > 1e9 || math.Trunc(n) != n {
		return 0, false
	}
	return int(n), true
}
func positiveInt(v any) (int, bool) { n, ok := nonnegativeInt(v); return n, ok && n > 0 }
func boolDefault(v map[string]any, k string, def bool) bool {
	b, ok := v[k].(bool)
	if !ok {
		return def
	}
	return b
}
func articleIDs(v any) ([]int, bool) {
	a, ok := v.([]any)
	if !ok || len(a) == 0 || len(a) > 100 {
		return nil, false
	}
	ids := make([]int, 0, len(a))
	seen := map[int]bool{}
	for _, item := range a {
		id, ok := positiveInt(item)
		if !ok || seen[id] {
			return nil, false
		}
		seen[id] = true
		ids = append(ids, id)
	}
	sort.Ints(ids)
	return ids, true
}
