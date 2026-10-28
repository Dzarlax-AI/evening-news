package schedulerbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

func decodeObject(b []byte) Object {
	var v Object
	if json.Unmarshal(b, &v) != nil || v == nil {
		return Object{}
	}
	return v
}

// Export captures a drained loopback News snapshot into a new private file.
func Export(ctx context.Context, dsn, path string) error {
	db, err := openDB(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close(context.Background())
	rows, err := db.Query(ctx, `SELECT id,task_name,enabled,schedule_type,hour,minute,weekdays,timezone,task_config,next_run,last_run,last_finished_at,last_success_at,last_status,last_duration_seconds::float8 FROM schedule_settings ORDER BY id`)
	if err != nil {
		return fail("snapshot_read_failed")
	}
	items := []Object{}
	history := []Object{}
	for rows.Next() {
		var id int64
		var task string
		var enabled *bool
		var typ, tz *string
		var hour, minute *int
		var weekdays, config []byte
		var next, last, finished, success *time.Time
		var status *string
		var duration *float64
		if err = rows.Scan(&id, &task, &enabled, &typ, &hour, &minute, &weekdays, &tz, &config, &next, &last, &finished, &success, &status, &duration); err != nil {
			rows.Close()
			return fail("snapshot_read_failed")
		}
		if !operations[task] || typ == nil {
			rows.Close()
			return fail("unsupported_legacy_schedule")
		}
		raw := decodeObject(config)
		safe, e := payload(task, raw, false)
		if e != nil {
			rows.Close()
			return e
		}
		cal := Object{"type": *typ, "timezone": tz}
		var days []any
		_ = json.Unmarshal(weekdays, &days)
		switch *typ {
		case "daily", "weekly":
			cal["hour"] = hour
			cal["minute"] = minute
			if *typ == "weekly" || len(days) > 0 {
				if days == nil {
					days = []any{}
				}
				cal["weekdays"] = days
			}
		case "hourly":
			cal["minute"] = minute
			if len(days) > 0 {
				cal["weekdays"] = days
			}
		case "interval":
			v, ok := raw["interval_minutes"]
			if !ok {
				v = 30
			}
			cal["interval_minutes"] = v
		default:
			rows.Close()
			return fail("unsupported_legacy_calendar")
		}
		key := fmt.Sprintf("news:%d", id)
		active := enabled != nil && *enabled
		items = append(items, Object{"schedule_key": key, "enabled": active, "calendar": cal, "target_alias": "news-worker", "operation": task, "payload": safe, "next_due_at": utc(next)})
		var lastStatus any
		if status != nil && map[string]bool{"success": true, "warning": true, "deferred": true, "failed": true, "interrupted": true, "delivery_unknown": true}[*status] {
			lastStatus = *status
		}
		history = append(history, Object{"schedule_key": key, "task_name": task, "last_run": utc(last), "last_finished_at": utc(finished), "last_success_at": utc(success), "last_status": lastStatus, "last_duration_seconds": duration})
		if len(items) > 100 {
			rows.Close()
			return fail("snapshot_exceeds_100_schedules")
		}
	}
	rows.Close()
	if rows.Err() != nil {
		return fail("snapshot_read_failed")
	}
	var active bool
	err = db.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM background_jobs WHERE kind=ANY($1) AND state=ANY($2))`, []string{"news_processing", "news_digest", "telegram_digest", "backup", "reprocess_failed"}, []string{"queued", "running", "retrying", "interrupted", "delivery_unknown"}).Scan(&active)
	if err != nil {
		return fail("snapshot_read_failed")
	}
	if active {
		return fail("active_work_remains")
	}
	rows, err = db.Query(ctx, `SELECT id::text,kind,state,schedule_key,occurrence_id::text,scheduled_for,created_at,finished_at FROM background_jobs WHERE kind=ANY($1) ORDER BY created_at,id`, []string{"news_processing", "news_digest", "telegram_digest", "backup", "reprocess_failed"})
	if err != nil {
		return fail("snapshot_read_failed")
	}
	inventory := []Object{}
	for rows.Next() {
		var id, kind, state string
		var key, occurrence *string
		var scheduled, created, finished *time.Time
		if err = rows.Scan(&id, &kind, &state, &key, &occurrence, &scheduled, &created, &finished); err != nil {
			rows.Close()
			return fail("snapshot_read_failed")
		}
		inventory = append(inventory, Object{"job_id": id, "kind": kind, "state": state, "schedule_key": key, "occurrence_id": occurrence, "scheduled_for": utc(scheduled), "created_at": utc(created), "finished_at": utc(finished)})
	}
	rows.Close()
	if rows.Err() != nil {
		return fail("snapshot_read_failed")
	}
	now := time.Now().UTC()
	return writeArtifact(path, Object{"format": "news-scheduler-snapshot-v1", "exported_at": utc(&now), "schedules": items, "legacy_run_history": history, "news_job_inventory": inventory})
}
