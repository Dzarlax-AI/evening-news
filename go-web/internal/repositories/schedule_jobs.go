package repositories

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/schedules"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ScheduleJobsSnapshot reads scheduler-visible execution metadata from durable receipts.
type ScheduleJobsSnapshot struct{ pool *pgxpool.Pool }

func NewScheduleJobsSnapshot(pool *pgxpool.Pool) *ScheduleJobsSnapshot {
	return &ScheduleJobsSnapshot{pool: pool}
}

func (r *ScheduleJobsSnapshot) Snapshot(ctx context.Context, keys []string) (schedules.JobsSnapshot, error) {
	out := schedules.JobsSnapshot{Latest: map[string]schedules.JobStatus{}, Running: map[string]bool{}, LastSuccess: map[string]schedules.JobStatus{}}
	if len(keys) == 0 {
		return out, nil
	}
	rows, err := r.pool.Query(ctx, `WITH ranked AS (
 SELECT schedule_key,state,created_at,finished_at,result,error_code,
 row_number() OVER (PARTITION BY schedule_key ORDER BY created_at DESC,id DESC) AS rank
 FROM background_jobs WHERE schedule_key=ANY($1::text[])
), successful AS (
 SELECT schedule_key,state,created_at,finished_at,result,error_code,
 row_number() OVER (PARTITION BY schedule_key ORDER BY finished_at DESC,id DESC) AS rank
 FROM background_jobs WHERE schedule_key=ANY($1::text[]) AND state='succeeded'
 AND COALESCE(result->>'status','') NOT IN ('warning','deferred')
)
SELECT 'latest' AS group_name,schedule_key,state,created_at,finished_at,result,error_code FROM ranked WHERE rank=1
UNION ALL
SELECT 'success',schedule_key,state,created_at,finished_at,result,error_code FROM successful WHERE rank=1
UNION ALL
SELECT DISTINCT 'running',schedule_key,'running',NULL::timestamp,NULL::timestamp,NULL::jsonb,NULL::text FROM background_jobs WHERE schedule_key=ANY($1::text[]) AND state='running'`, keys)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var group, key, state string
		var created, finished *time.Time
		var resultRaw []byte
		var errorCode *string
		if err := rows.Scan(&group, &key, &state, &created, &finished, &resultRaw, &errorCode); err != nil {
			return out, err
		}
		result := map[string]any{}
		if len(resultRaw) != 0 {
			_ = json.Unmarshal(resultRaw, &result)
		}
		status := schedules.JobStatus{CreatedAt: created, FinishedAt: finished, State: state, Result: result, ErrorCode: errorCode}
		switch group {
		case "latest":
			out.Latest[key] = status
		case "success":
			out.LastSuccess[key] = status
		case "running":
			out.Running[key] = true
		}
	}
	return out, rows.Err()
}
