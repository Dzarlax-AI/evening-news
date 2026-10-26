package repositories

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/management"
	"github.com/jackc/pgx/v5"
)

func (r *ManagementRepository) DashboardStats(ctx context.Context, days int) (map[string]any, error) {
	var total, today, recent, processed, processedToday, ads, totalSources, activeSources, categories, apiCalls, errorsToday, processedCount int64
	var processingSeconds float64
	var topCategory *string
	queries := []struct {
		sql string
		dst any
	}{
		{`SELECT count(*) FROM articles`, &total},
		{`SELECT count(*) FROM articles WHERE fetched_at >= date_trunc('day',timezone('UTC',now()))`, &today},
		{`SELECT count(*) FROM articles WHERE fetched_at >= timezone('UTC',now())-($1 * interval '1 day')`, &recent},
		{`SELECT count(*) FROM articles WHERE processed IS TRUE`, &processed},
		{`SELECT count(*) FROM articles WHERE processed IS TRUE AND fetched_at >= date_trunc('day',timezone('UTC',now()))`, &processedToday},
		{`SELECT count(*) FROM articles WHERE is_advertisement IS TRUE`, &ads},
		{`SELECT count(*) FROM sources`, &totalSources},
		{`SELECT count(*) FROM sources WHERE enabled IS TRUE`, &activeSources},
		{`SELECT count(*) FROM categories`, &categories},
	}
	for i, q := range queries {
		var err error
		if i == 2 {
			err = r.pool.QueryRow(ctx, q.sql, days).Scan(q.dst)
		} else {
			err = r.pool.QueryRow(ctx, q.sql).Scan(q.dst)
		}
		if err != nil {
			return nil, err
		}
	}
	if err := r.pool.QueryRow(ctx, `SELECT c.display_name FROM article_categories ac JOIN categories c ON c.id=ac.category_id GROUP BY c.id,c.display_name ORDER BY count(*) DESC LIMIT 1`).Scan(&topCategory); err != nil && err != pgx.ErrNoRows {
		return nil, err
	}
	if err := r.pool.QueryRow(ctx, `SELECT coalesce(sum(api_calls_made),0),coalesce(sum(errors_count),0),coalesce(sum(articles_processed),0),coalesce(sum(processing_time_seconds),0) FROM processing_stats WHERE date >= (timezone('UTC',now())::date)`).Scan(&apiCalls, &errorsToday, &processedCount, &processingSeconds); err != nil {
		return nil, err
	}
	apiRate, avgMS := 0, 0
	if apiCalls > 0 {
		apiRate = int(math.RoundToEven(float64(apiCalls-errorsToday) * 100 / float64(apiCalls)))
	}
	if processedCount > 0 && processingSeconds > 0 {
		avgMS = int(processingSeconds / float64(processedCount) * 1000)
	}
	var queued, active int64
	var worker bool
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state IN ('queued','interrupted')),count(*) FILTER(WHERE state='running') FROM background_jobs`).Scan(&queued, &active); err != nil {
		return nil, err
	}
	err := r.pool.QueryRow(ctx, `SELECT status='running' AND heartbeat_at >= timezone('UTC',now())-interval '20 seconds' FROM background_worker_status WHERE id=1`).Scan(&worker)
	if err != nil && err.Error() != "no rows in result set" {
		return nil, err
	}
	processingRate, adRate := 0.0, 0.0
	if total > 0 {
		processingRate = float64(processed) / float64(total) * 100
		adRate = float64(ads) / float64(total) * 100
	}
	return map[string]any{"active_sources": activeSources, "total_sources": totalSources, "disabled_sources": totalSources - activeSources, "today_articles": today, "processed_today": processedToday, "categories_count": categories, "top_category": valueOr(topCategory, "N/A"), "api_calls_today": apiCalls, "api_success_rate": apiRate, "errors_today": errorsToday, "avg_processing_time": avgMS, "articles_per_hour": int(math.RoundToEven(float64(today) / 24)), "total_articles": total, "recent_articles": recent, "processed_articles": processed, "processing_rate": processingRate, "advertisements": ads, "ad_rate": adRate, "queue_stats": map[string]any{"worker_available": worker, "queued_jobs": queued, "active_jobs": active}, "period_days": days, "timestamp": managementTimestamp()}, nil
}

func managementTimestamp() string {
	now := time.Now().UTC().Truncate(time.Microsecond)
	if now.Nanosecond() == 0 {
		return now.Format("2006-01-02T15:04:05")
	}
	return now.Format("2006-01-02T15:04:05.000000")
}

func (r *ManagementRepository) AIUsageStats(ctx context.Context, days int) (map[string]any, error) {
	since := time.Now().UTC().AddDate(0, 0, -days)
	today := time.Now().UTC().Format("2006-01-02")
	var req, tokens, discovered, successful, requestsToday, tokensToday int64
	var cost, costToday float64
	err := r.pool.QueryRow(ctx, `SELECT count(*),coalesce(sum(tokens_used),0),coalesce(sum(credits_cost),0),coalesce(sum(patterns_discovered),0),coalesce(sum(patterns_successful),0) FROM ai_usage_tracking WHERE created_at >= $1 OR created_at IS NULL`, since).Scan(&req, &tokens, &cost, &discovered, &successful)
	if err != nil {
		return nil, err
	}
	if err = r.pool.QueryRow(ctx, `SELECT count(*),coalesce(sum(tokens_used),0),coalesce(sum(credits_cost),0) FROM ai_usage_tracking WHERE created_at >= $1::date`, today).Scan(&requestsToday, &tokensToday, &costToday); err != nil {
		return nil, err
	}
	byType := []map[string]any{}
	rows, err := r.pool.Query(ctx, `SELECT analysis_type,count(*),coalesce(sum(tokens_used),0),coalesce(sum(credits_cost),0) FROM ai_usage_tracking WHERE created_at >= $1 OR created_at IS NULL GROUP BY analysis_type ORDER BY sum(tokens_used) DESC`, since)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var typ string
		var n, t int64
		var c float64
		if err = rows.Scan(&typ, &n, &t, &c); err != nil {
			rows.Close()
			return nil, err
		}
		byType = append(byType, map[string]any{"type": typ, "requests": n, "tokens": t, "cost": c})
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	daily := []map[string]any{}
	rows, err = r.pool.Query(ctx, `SELECT created_at::date,count(*),coalesce(sum(tokens_used),0),coalesce(sum(credits_cost),0) FROM ai_usage_tracking WHERE created_at >= $1 GROUP BY created_at::date ORDER BY created_at::date`, since)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d time.Time
		var n, t int64
		var c float64
		if err = rows.Scan(&d, &n, &t, &c); err != nil {
			rows.Close()
			return nil, err
		}
		daily = append(daily, map[string]any{"date": d.Format("2006-01-02"), "requests": n, "tokens": t, "cost": c})
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	top := []map[string]any{}
	rows, err = r.pool.Query(ctx, `SELECT domain,count(*),coalesce(sum(tokens_used),0),coalesce(sum(credits_cost),0) FROM ai_usage_tracking WHERE created_at >= $1 OR created_at IS NULL GROUP BY domain ORDER BY sum(tokens_used) DESC LIMIT 10`, since)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var d string
		var n, t int64
		var c float64
		if err = rows.Scan(&d, &n, &t, &c); err != nil {
			rows.Close()
			return nil, err
		}
		top = append(top, map[string]any{"domain": d, "requests": n, "tokens": t, "cost": c})
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"period_days": days, "total": map[string]any{"requests": req, "tokens": tokens, "cost": cost, "patterns_discovered": discovered, "patterns_successful": successful}, "today": map[string]any{"requests": requestsToday, "tokens": tokensToday, "cost": costToday}, "by_type": byType, "daily": daily, "top_domains": top, "timestamp": managementTimestamp()}, nil
}

func (r *ManagementRepository) ProcessingStats(ctx context.Context, days int) (map[string]any, error) {
	since := time.Now().UTC().AddDate(0, 0, -days)
	rows, err := r.pool.Query(ctx, `SELECT date,articles_fetched,articles_processed,api_calls_made,errors_count,processing_time_seconds FROM processing_stats WHERE date >= $1::date ORDER BY date`, since)
	if err != nil {
		return nil, err
	}
	daily := []map[string]any{}
	totals := map[string]int64{"articles_fetched": 0, "articles_processed": 0, "api_calls_made": 0, "errors_count": 0, "total_processing_time": 0}
	for rows.Next() {
		var d time.Time
		var fetched, processed, calls, errs, duration int64
		if err = rows.Scan(&d, &fetched, &processed, &calls, &errs, &duration); err != nil {
			rows.Close()
			return nil, err
		}
		daily = append(daily, map[string]any{"date": d.Format("2006-01-02"), "articles_fetched": fetched, "articles_processed": processed, "api_calls_made": calls, "errors_count": errs, "processing_time_seconds": duration})
		totals["articles_fetched"] += fetched
		totals["articles_processed"] += processed
		totals["api_calls_made"] += calls
		totals["errors_count"] += errs
		totals["total_processing_time"] += duration
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"period_days": days, "daily_stats": daily, "totals": totals, "timestamp": managementTimestamp()}, nil
}

func (r *ManagementRepository) ExtractorStats(ctx context.Context) (map[string]any, error) {
	var total, success int64
	var successNullable *int64
	var avgNullable *float64
	if err := r.pool.QueryRow(ctx, `SELECT count(*),sum(CASE WHEN content IS NOT NULL AND length(content)>100 THEN 1 ELSE 0 END),avg(CASE WHEN content IS NOT NULL THEN length(content) ELSE 0 END) FROM articles WHERE fetched_at >= now()-interval '7 days'`).Scan(&total, &successNullable, &avgNullable); err != nil {
		return nil, err
	}
	var successValue, avgValue any
	if successNullable != nil {
		success = *successNullable
		successValue = success
	}
	if avgNullable != nil {
		avgValue = *avgNullable
	}
	rate := 0.0
	if total > 0 {
		rate = float64(success) * 100 / float64(total)
	}
	since := time.Now().UTC().AddDate(0, 0, -7)
	rows, err := r.pool.Query(ctx, `SELECT domain,count(*),count(*) FILTER(WHERE success),coalesce(avg(extraction_time_ms),0),coalesce(avg(quality_score),0) FROM extraction_attempts WHERE created_at >= $1 GROUP BY domain ORDER BY count(*) DESC LIMIT 20`, since)
	if err != nil {
		return nil, err
	}
	domains := []map[string]any{}
	for rows.Next() {
		var d string
		var attempts, ok int64
		var avgTime, avgQuality, rate float64
		if err = rows.Scan(&d, &attempts, &ok, &avgTime, &avgQuality); err != nil {
			rows.Close()
			return nil, err
		}
		if attempts > 0 {
			rate = math.RoundToEven(float64(ok)/float64(attempts)*10000) / 100
		}
		domains = append(domains, map[string]any{"domain": d, "total_attempts": attempts, "successful_attempts": ok, "success_rate": rate, "avg_extraction_time_ms": avgTime, "avg_quality_score": avgQuality})
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"recent_extractions": map[string]any{"total": total, "successful": successValue, "success_rate": rate, "avg_content_length": avgValue}, "domain_stats": domains, "timestamp": managementTimestamp()}, nil
}

func (r *ManagementRepository) UpdateTelegramSettings(ctx context.Context, in management.TelegramSettings) error {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	for _, x := range []struct {
		k string
		v *string
	}{{"telegram_chat_id", in.ChatID}, {"telegram_service_chat_id", in.ServiceChatID}} {
		value := ""
		if x.v != nil {
			value = strings.TrimSpace(*x.v)
		}
		if value == "" {
			if _, err = tx.Exec(ctx, `DELETE FROM settings WHERE key=$1`, x.k); err != nil {
				return err
			}
		} else if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value,description,updated_at) VALUES($1,to_jsonb($2::text),$3,timezone('UTC',now())) ON CONFLICT(key) DO UPDATE SET value=excluded.value,description=excluded.description,updated_at=timezone('UTC',now())`, x.k, value, "Telegram "+x.k+" (UI override)"); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (r *ManagementRepository) ApproveAIMapping(ctx context.Context, in management.MappingRequest) (map[string]any, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var exists bool
	var active bool
	var id int64
	err = tx.QueryRow(ctx, `SELECT id,is_active FROM category_mapping WHERE ai_category=$1 FOR UPDATE`, in.AICategory).Scan(&id, &active)
	if err == nil {
		exists = true
	}
	if err == nil && active {
		return map[string]any{"success": false, "message": fmt.Sprintf("Active mapping already exists for '%s'", in.AICategory)}, nil
	}
	if err != nil && err != pgx.ErrNoRows {
		return nil, err
	}
	if in.ConfidenceThreshold == nil {
		v := 0.7
		in.ConfidenceThreshold = &v
	}
	desc := fmt.Sprintf("AI-suggested mapping approved via admin panel")
	if in.Description != nil && *in.Description != "" {
		desc = *in.Description
	}
	if exists {
		_, err = tx.Exec(ctx, `UPDATE category_mapping SET fixed_category=$2,confidence_threshold=$3,description=$4,is_active=true,updated_at=timezone('UTC',now()) WHERE id=$1`, id, in.FixedCategory, *in.ConfidenceThreshold, desc)
	} else {
		_, err = tx.Exec(ctx, `INSERT INTO category_mapping(ai_category,fixed_category,confidence_threshold,description,created_by,is_active,usage_count,created_at,updated_at) VALUES($1,$2,$3,$4,'admin_ai_analysis',true,0,timezone('UTC',now()),timezone('UTC',now()))`, in.AICategory, in.FixedCategory, *in.ConfidenceThreshold, desc)
	}
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	var affected int64
	if err = r.pool.QueryRow(ctx, `SELECT count(article_id) FROM article_categories WHERE ai_category=$1`, in.AICategory).Scan(&affected); err != nil {
		return nil, err
	}
	if exists {
		return map[string]any{"success": true, "message": fmt.Sprintf("Reactivated mapping: '%s' → '%s'", in.AICategory, in.FixedCategory)}, nil
	}
	return map[string]any{"success": true, "message": fmt.Sprintf("Created mapping: '%s' → '%s'", in.AICategory, in.FixedCategory), "affected_articles": affected, "note": "Mapping will be applied to new articles automatically. Existing articles can be updated via reprocessing."}, nil
}

func (r *ManagementRepository) DatabaseHealth(ctx context.Context) (map[string]any, error) {
	var one int
	err := r.pool.QueryRow(ctx, `SELECT 1`).Scan(&one)
	if err != nil {
		return map[string]any{"ok": false, "pool": map[string]any{"status": "unavailable"}, "error": err.Error(), "timestamp": managementTimestamp()}, nil
	}
	stat := r.pool.Stat()
	return map[string]any{"ok": true, "pool": map[string]any{"status": "open", "size": stat.TotalConns(), "checked_in": stat.IdleConns(), "checked_out": stat.AcquiredConns(), "overflow": nil, "invalid": nil}, "test_query": one == 1, "timestamp": managementTimestamp()}, nil
}
