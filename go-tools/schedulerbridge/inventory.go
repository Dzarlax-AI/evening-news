package schedulerbridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"
)

func paused(v any) (Object, int64, int64, error) {
	m, ok := object(v)
	if ok {
		for key, value := range m {
			if key != "paused" && key != "control_revision" && key != "pending_outbox" && key != "last_tick_at" && key != "last_dispatch_at" {
				return nil, 0, 0, fail("invalid_scheduler_status")
			}
			if (key == "last_tick_at" || key == "last_dispatch_at") && value != nil && !timestamp(value) {
				return nil, 0, 0, fail("invalid_scheduler_status")
			}
		}
	}
	if !ok || m["paused"] != true {
		return nil, 0, 0, fail("scheduler_not_paused")
	}
	rev, ok := integer(m["control_revision"])
	if !ok || rev < 0 {
		return nil, 0, 0, fail("scheduler_not_paused")
	}
	pending, ok := integer(m["pending_outbox"])
	if !ok || pending < 0 {
		return nil, 0, 0, fail("scheduler_not_paused")
	}
	return m, rev, pending, nil
}
func occurrence(m Object) error {
	if !exact(m, "occurrence_id", "schedule_key", "schedule_revision", "scheduled_for", "status", "attempts", "created_at", "updated_at", "job_id", "target_alias", "lease_expires_at", "lease_active") {
		return fail("invalid_occurrence_fields")
	}
	id, ok := m["occurrence_id"].(string)
	if !ok || !uuidRE.MatchString(id) {
		return fail("invalid_occurrence")
	}
	if m["job_id"] != nil {
		v, ok := m["job_id"].(string)
		if !ok || !uuidRE.MatchString(v) {
			return fail("invalid_occurrence")
		}
	}
	key, ok := m["schedule_key"].(string)
	if !ok || !schedulerKey.MatchString(key) {
		return fail("invalid_occurrence")
	}
	rev, ok := integer(m["schedule_revision"])
	if !ok || rev < 1 {
		return fail("invalid_occurrence")
	}
	attempts, ok := integer(m["attempts"])
	if !ok || attempts < 0 {
		return fail("invalid_occurrence")
	}
	state, ok := m["status"].(string)
	if !ok || !map[string]bool{"pending": true, "leased": true, "blocked": true, "accepted": true}[state] {
		return fail("invalid_occurrence")
	}
	alias, ok := m["target_alias"].(string)
	if !ok || alias == "" || len([]rune(alias)) > 128 {
		return fail("invalid_occurrence")
	}
	lease, ok := m["lease_active"].(bool)
	if !ok {
		return fail("invalid_occurrence")
	}
	for _, key := range []string{"scheduled_for", "created_at", "updated_at"} {
		if !timestamp(m[key]) {
			return fail("invalid_occurrence")
		}
	}
	if m["lease_expires_at"] != nil && !timestamp(m["lease_expires_at"]) {
		return fail("invalid_occurrence")
	}
	if lease && (state != "leased" || m["lease_expires_at"] == nil) {
		return fail("invalid_occurrence")
	}
	return nil
}
func ownedSchedule(v any) bool {
	m, ok := object(v)
	if !ok || m["target_alias"] != "news-worker" {
		return false
	}
	op, ok := m["operation"].(string)
	key, k := m["schedule_key"].(string)
	if !ok || !k || !operations[op] {
		return false
	}
	if newsKey.MatchString(key) {
		return true
	}
	return strings.HasPrefix(key, "news:") && operations[strings.TrimPrefix(key, "news:")]
}
func pageAll(ctx context.Context, c ServiceConfig, h *http.Client, base, path, field string, limit, max int) ([]any, error) {
	result := []any{}
	cursor := ""
	seen := map[string]bool{}
	for {
		params := url.Values{"limit": []string{strconv.Itoa(limit)}}
		if cursor != "" {
			params.Set("cursor", cursor)
		}
		raw, e := request(ctx, h, "GET", base+path+"?"+params.Encode(), c.Token, nil)
		if e != nil {
			return nil, e
		}
		m, ok := object(raw)
		if !ok || !exact(m, field, "next_cursor") {
			return nil, fail("invalid_inventory_page")
		}
		rows, ok := m[field].([]any)
		if !ok || len(rows) > limit {
			return nil, fail("invalid_inventory_page")
		}
		for _, v := range rows {
			if field == "occurrences" {
				row, ok := object(v)
				if !ok {
					return nil, fail("invalid_occurrence_fields")
				}
				if e = occurrence(row); e != nil {
					return nil, e
				}
			}
		}
		result = append(result, rows...)
		if len(result) > max {
			return nil, fail("inventory_too_large")
		}
		if m["next_cursor"] == nil {
			break
		}
		next, ok := m["next_cursor"].(string)
		if !ok {
			return nil, fail("invalid_inventory_page")
		}
		if next == "" {
			break
		}
		if len(next) > 4096 || len(seen) >= 10000 {
			return nil, fail("inventory_cursor_invalid")
		}
		if seen[next] {
			return nil, fail("inventory_cursor_repeated")
		}
		seen[next] = true
		cursor = next
	}
	return result, nil
}
func rollbackRecord(resource Object, latest Object) (Object, error) {
	op, ok := resource["operation"].(string)
	if !ok || !operations[op] || resource["target_alias"] != "news-worker" {
		return nil, fail("invalid_schedule")
	}
	p, e := payload(op, resource["payload"], false)
	if e != nil {
		return nil, e
	}
	cal, ok := object(resource["calendar"])
	if !ok {
		return nil, fail("invalid_schedule_calendar")
	}
	item := Object{"schedule_key": resource["schedule_key"], "enabled": resource["enabled"], "calendar": cal, "target_alias": "news-worker", "operation": op, "payload": p, "next_due_at": resource["next_due_at"]}
	if e = validateItem(item); e != nil {
		return nil, e
	}
	key := resource["schedule_key"].(string)
	var id any = key
	if n, err := strconv.ParseInt(strings.TrimPrefix(key, "news:"), 10, 64); err == nil {
		id = n
	}
	config := Object{}
	for k, v := range p {
		config[k] = v
	}
	if cal["type"] == "interval" {
		v, ok := cal["interval_minutes"]
		if !ok {
			return nil, fail("invalid_schedule_calendar")
		}
		config["interval_minutes"] = v
	}
	var status, run, finished, success any
	running := false
	if latest != nil {
		status = latest["result_status"]
		if latest["state"] != "succeeded" {
			status = latest["state"]
		} else if status == "completed" || status == "success" {
			status = "success"
		}
		run = latest["created_at"]
		finished = latest["finished_at"]
		if status == "success" {
			success = finished
		}
		running = latest["state"] == "running"
	}
	hour := cal["hour"]
	if hour == nil {
		hour = 0
	}
	minute := cal["minute"]
	if minute == nil {
		minute = 0
	}
	weekdays := cal["weekdays"]
	if weekdays == nil {
		weekdays = []any{}
	}
	return Object{"id": id, "task_name": op, "enabled": item["enabled"], "schedule_type": cal["type"], "hour": hour, "minute": minute, "weekdays": weekdays, "timezone": cal["timezone"], "task_config": config, "last_run": run, "last_finished_at": finished, "last_success_at": success, "last_status": status, "last_error": nil, "last_duration_seconds": nil, "next_run": item["next_due_at"], "is_running": running, "created_at": resource["created_at"], "updated_at": resource["updated_at"], "schedule_key": key, "schedule_revision": resource["revision"]}, nil
}

// RollbackInventory records a stable paused scheduler and its News job history.
func RollbackInventory(ctx context.Context, c ServiceConfig, dsn, path string) error {
	h, base, e := c.client()
	if e != nil {
		return e
	}
	defer h.CloseIdleConnections()
	raw, e := request(ctx, h, "GET", base+"/v1/status", c.Token, nil)
	if e != nil {
		return e
	}
	_, rev, pending, e := paused(raw)
	if e != nil {
		return e
	}
	allSchedules, e := pageAll(ctx, c, h, base, "/v1/schedules", "schedules", 2, 10000)
	if e != nil {
		return e
	}
	schedules := []Object{}
	keys := map[string]bool{}
	for _, v := range allSchedules {
		if ownedSchedule(v) {
			m, _ := object(v)
			schedules = append(schedules, m)
			keys[m["schedule_key"].(string)] = true
		}
	}
	allOccurrences, e := pageAll(ctx, c, h, base, "/v1/occurrences", "occurrences", 100, 1000000)
	if e != nil {
		return e
	}
	occurrences := []Object{}
	orphans := map[string]bool{}
	for _, v := range allOccurrences {
		m, _ := object(v)
		if m["target_alias"] != "news-worker" {
			continue
		}
		if m["lease_active"] == true {
			return fail("inflight_leased_dispatch")
		}
		occurrences = append(occurrences, m)
		key := m["schedule_key"].(string)
		if !keys[key] {
			orphans[key] = true
		}
	}
	raw, e = request(ctx, h, "GET", base+"/v1/status", c.Token, nil)
	if e != nil {
		return e
	}
	status, finalRev, finalPending, e := paused(raw)
	if e != nil {
		return e
	}
	if rev != finalRev || pending != finalPending {
		return fail("scheduler_state_changed")
	}
	for k := range orphans {
		keys[k] = true
	}
	keyList := make([]string, 0, len(keys))
	for k := range keys {
		keyList = append(keyList, k)
	}
	sort.Strings(keyList)
	jobs := []Object{}
	if len(keyList) > 0 {
		db, err := openDB(ctx, dsn)
		if err != nil {
			return err
		}
		defer db.Close(context.Background())
		rows, err := db.Query(ctx, `SELECT id::text,schedule_key,occurrence_id::text,state,result::text,created_at,finished_at FROM background_jobs WHERE schedule_key=ANY($1) ORDER BY created_at,id LIMIT 100001`, keyList)
		if err != nil {
			return fail("snapshot_read_failed")
		}
		for rows.Next() {
			var id, state string
			var key, occ *string
			var result []byte
			var created, finished *time.Time
			if err = rows.Scan(&id, &key, &occ, &state, &result, &created, &finished); err != nil {
				rows.Close()
				return fail("snapshot_read_failed")
			}
			var resultStatus any
			var object Object
			if json.Unmarshal(result, &object) == nil && object != nil {
				if status, ok := object["status"].(string); ok && map[string]bool{"success": true, "completed": true, "warning": true, "deferred": true, "dry_run": true, "failed": true, "interrupted": true, "delivery_unknown": true}[status] {
					resultStatus = status
				}
			}
			jobs = append(jobs, Object{"job_id": id, "schedule_key": key, "occurrence_id": occ, "state": state, "result_status": resultStatus, "created_at": utc(created), "finished_at": utc(finished)})
			if len(jobs) > 100000 {
				rows.Close()
				return fail("job_inventory_too_large")
			}
		}
		rows.Close()
		if rows.Err() != nil {
			return fail("snapshot_read_failed")
		}
	}
	latest := map[string]Object{}
	for _, j := range jobs {
		key := *j["schedule_key"].(*string)
		old := latest[key]
		created, _ := j["created_at"].(string)
		oldCreated := ""
		if old != nil {
			oldCreated, _ = old["created_at"].(string)
		}
		if old == nil || created > oldCreated {
			latest[key] = j
		}
	}
	rollback := []Object{}
	for _, m := range schedules {
		v, err := rollbackRecord(m, latest[m["schedule_key"].(string)])
		if err != nil {
			return err
		}
		rollback = append(rollback, v)
	}
	orphanList := make([]string, 0, len(orphans))
	for k := range orphans {
		orphanList = append(orphanList, k)
	}
	sort.Strings(orphanList)
	now := time.Now().UTC()
	return writeArtifact(path, Object{"format": "news-scheduler-rollback-inventory-v1", "captured_at": utc(&now), "scheduler_status": status, "legacy_schedule_snapshot": Object{"format": "news-scheduler-rollback-config-v1", "schedules": rollback}, "orphaned_news_schedule_keys": orphanList, "news_execution_inventory": jobs, "scheduler_occurrences": occurrences, "note": "Inventory only. It preserves current configuration, successful News execution history, and pending/blocked/accepted occurrences. It does not delete, replay, or enable Python scheduling."})
}
