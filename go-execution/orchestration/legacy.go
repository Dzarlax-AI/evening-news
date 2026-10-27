package orchestration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-execution"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// LegacyAdapter owns the schedule side effects formerly applied by Python's
// executor. The supplied getters must return the live execution guard identity.
type LegacyAdapter struct {
	pool       *pgxpool.Pool
	domain     *DomainClient
	guardPID   func(context.Context) (int32, error)
	instanceID func() string
}

func NewLegacyAdapter(pool *pgxpool.Pool, domain *DomainClient, guardPID func(context.Context) (int32, error), instanceID func() string) (*LegacyAdapter, error) {
	if pool == nil || domain == nil || guardPID == nil || instanceID == nil {
		return nil, errors.New("legacy adapter configuration is invalid")
	}
	return &LegacyAdapter{pool, domain, guardPID, instanceID}, nil
}

func (a *LegacyAdapter) Finalize(ctx context.Context, f execution.ExecutionContext) (json.RawMessage, error) {
	pid, err := a.guardPID(ctx)
	if err != nil || pid < 1 {
		return nil, execution.ErrNotReady
	}
	instance := a.instanceID()
	if !canonicalUUID(instance) {
		return nil, execution.ErrNotReady
	}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, errors.New("finalization transaction unavailable")
	}
	defer tx.Rollback(context.Background())
	owner, gen, workerPID, err := lockCurrentWorker(ctx, tx)
	if err != nil {
		return nil, err
	}
	if err = checkExecutionLock(ctx, tx, pid); err != nil {
		return nil, err
	}
	var jobState string
	var attempt int64
	var payload, result []byte
	var errorCode *string
	var jobScheduleKey *string
	var jobOwnerGeneration *int64
	var finished *time.Time
	var created time.Time
	err = tx.QueryRow(ctx, `SELECT state,attempt_generation,payload::text,result::text,error_code,finished_at,created_at,schedule_key,owner_generation FROM background_jobs WHERE id=$1::uuid FOR UPDATE`, f.JobID).Scan(&jobState, &attempt, &payload, &result, &errorCode, &finished, &created, &jobScheduleKey, &jobOwnerGeneration)
	if err != nil || attempt != f.AttemptGeneration || (jobState != "succeeded" && jobState != "failed" && jobState != "delivery_unknown") {
		return nil, execution.ErrStaleAttempt
	}
	var existingState string
	var applied *time.Time
	var alertState *string
	err = tx.QueryRow(ctx, `SELECT state,legacy_completion_applied_at,legacy_alert_state FROM background_job_executions WHERE job_id=$1::uuid AND attempt_generation=$2 FOR UPDATE`, f.JobID, attempt).Scan(&existingState, &applied, &alertState)
	if errors.Is(err, pgx.ErrNoRows) {
		sum := sha256.Sum256([]byte(fmt.Sprintf("legacy-recovery:%s:%d", f.JobID, attempt)))
		_, err = tx.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,worker_generation,state,result,error_code) VALUES($1::uuid,$2,$3,$4::uuid,$5,$6,$7::jsonb,$8)`, f.JobID, attempt, hex.EncodeToString(sum[:]), instance, jobOwnerGeneration, jobState, jsonOrObject(result), errorCode)
		if err != nil {
			return nil, errors.New("legacy receipt creation failed")
		}
		existingState = jobState
	} else if err != nil {
		return nil, errors.New("legacy receipt lookup failed")
	}
	if existingState == "accepted" || existingState == "running" {
		_, err = tx.Exec(ctx, `UPDATE background_job_executions SET state=$3,result=$4::jsonb,error_code=$5,finished_at=$6 WHERE job_id=$1::uuid AND attempt_generation=$2`, f.JobID, attempt, jobState, jsonOrObject(result), errorCode, finished)
		if err != nil {
			return nil, errors.New("legacy receipt update failed")
		}
		existingState = jobState
	}
	first := applied == nil
	var scheduleID *int64
	var jobPayload map[string]any
	_ = json.Unmarshal(payload, &jobPayload)
	if n, ok := positiveInt(jobPayload["schedule_id"]); ok {
		x := int64(n)
		scheduleID = &x
	}
	if first && scheduleID != nil {
		if err = applyScheduleCompletion(ctx, tx, *scheduleID, created, jobState, result); err != nil {
			return nil, err
		}
	}
	newAlert := "not_needed"
	if scheduleID != nil || jobScheduleKey != nil {
		newAlert = "pending"
	}
	if first {
		_, err = tx.Exec(ctx, `UPDATE background_job_executions SET legacy_completion_applied_at=clock_timestamp() AT TIME ZONE 'UTC',legacy_alert_state=COALESCE(legacy_alert_state,$3) WHERE job_id=$1::uuid AND attempt_generation=$2`, f.JobID, attempt, newAlert)
		if err != nil {
			return nil, errors.New("legacy completion marker failed")
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, errors.New("legacy completion commit failed")
	}
	finalAlertState := newAlert
	if alertState != nil {
		finalAlertState = *alertState
	} else if !first {
		finalAlertState = newAlert
	}
	if first && newAlert == "pending" {
		body := map[string]any{"version": 1, "job_id": f.JobID, "attempt_generation": attempt, "owner_id": owner, "generation": gen, "backend_pid": workerPID, "executor_instance_id": instance, "executor_backend_pid": pid}
		responseAlertState, notifyErr := a.notifyFinalized(ctx, f.JobID, attempt, body)
		if notifyErr != nil { // delivery is unknown; intentionally do not retry
			a.markAlertUnknown(ctx, f, pid)
		} else if responseAlertState == "delivered" {
			// The Python leaf owns alert-state persistence. A response claiming
			// delivery without the durable receipt update is ambiguous.
			persisted, readErr := a.readAlertState(ctx, f, pid)
			if readErr != nil || persisted != "delivered" {
				a.markAlertUnknown(ctx, f, pid)
			}
		}
		finalAlertState, err = a.readAlertState(ctx, f, pid)
		if err != nil { // delivery is unknown; intentionally do not retry
			a.markAlertUnknown(ctx, f, pid)
			finalAlertState = "unknown"
		}
	}
	response, _ := json.Marshal(map[string]any{"job_id": f.JobID, "attempt_generation": attempt, "legacy_completion_applied": true, "alert_state": finalAlertState})
	return response, nil
}

func (a *LegacyAdapter) notifyFinalized(ctx context.Context, jobID string, attempt int64, body any) (string, error) {
	data, err := json.Marshal(body)
	if err != nil || len(data) > 4096 {
		return "unknown", errors.New("terminal event request invalid")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.domain.cfg.BaseURL+"/internal/v1/domain-events/finalized", strings.NewReader(string(data)))
	if err != nil {
		return "unknown", err
	}
	req.Header.Set("Authorization", "Bearer "+a.domain.cfg.Token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.domain.http.Do(req)
	if err != nil {
		return "unknown", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "unknown", fmt.Errorf("terminal event returned HTTP %d", resp.StatusCode)
	}
	data, err = io.ReadAll(io.LimitReader(resp.Body, maxDomainBody+1))
	if err != nil || len(data) > maxDomainBody {
		return "unknown", errors.New("terminal event response invalid")
	}
	var result struct {
		JobID                   string `json:"job_id"`
		AttemptGeneration       int64  `json:"attempt_generation"`
		LegacyCompletionApplied *bool  `json:"legacy_completion_applied"`
		AlertState              string `json:"alert_state"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(data)))
	decoder.DisallowUnknownFields()
	if decoder.Decode(&result) != nil || decoder.Decode(new(any)) != io.EOF || result.JobID != jobID || result.AttemptGeneration != attempt || result.LegacyCompletionApplied == nil || !*result.LegacyCompletionApplied || !validAlertState(result.AlertState) {
		return "unknown", errors.New("terminal event response invalid")
	}
	return result.AlertState, nil
}

func validAlertState(s string) bool {
	return s == "pending" || s == "attempting" || s == "delivered" || s == "unknown" || s == "not_needed"
}

func (a *LegacyAdapter) markAlertUnknown(ctx context.Context, f execution.ExecutionContext, pid int32) {
	cleanup, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tx, err := a.pool.BeginTx(cleanup, pgx.TxOptions{})
	if err != nil {
		return
	}
	defer tx.Rollback(context.Background())
	if _, _, _, err = lockCurrentWorker(cleanup, tx); err != nil {
		return
	}
	if checkExecutionLock(cleanup, tx, pid) != nil {
		return
	}
	_, err = tx.Exec(cleanup, `UPDATE background_job_executions SET legacy_alert_state='unknown' WHERE job_id=$1::uuid AND attempt_generation=$2 AND legacy_completion_applied_at IS NOT NULL AND legacy_alert_state IN ('pending','attempting')`, f.JobID, f.AttemptGeneration)
	if err != nil {
		return
	}
	_ = tx.Commit(cleanup)
}

// readAlertState observes the Python leaf's durable alert outcome under the
// same worker and executor fences used for writes.
func (a *LegacyAdapter) readAlertState(ctx context.Context, f execution.ExecutionContext, pid int32) (string, error) {
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return "unknown", errors.New("terminal alert state unavailable")
	}
	defer tx.Rollback(context.Background())
	if _, _, _, err = lockCurrentWorker(ctx, tx); err != nil {
		return "unknown", err
	}
	if err = checkExecutionLock(ctx, tx, pid); err != nil {
		return "unknown", err
	}
	var state string
	err = tx.QueryRow(ctx, `SELECT legacy_alert_state FROM background_job_executions WHERE job_id=$1::uuid AND attempt_generation=$2 AND legacy_completion_applied_at IS NOT NULL`, f.JobID, f.AttemptGeneration).Scan(&state)
	if err != nil || !validAlertState(state) {
		return "unknown", errors.New("terminal alert state invalid")
	}
	if err = tx.Commit(ctx); err != nil {
		return "unknown", errors.New("terminal alert state read failed")
	}
	return state, nil
}

func (a *LegacyAdapter) Recover(ctx context.Context, owner string, generation int64, backendPID int32) (json.RawMessage, error) {
	pid, err := a.guardPID(ctx)
	if err != nil || pid < 1 {
		return nil, execution.ErrNotReady
	}
	tx, err := a.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return nil, errors.New("recovery transaction unavailable")
	}
	defer tx.Rollback(context.Background())
	currentOwner, currentGen, currentPID, err := lockCurrentWorker(ctx, tx)
	if err != nil {
		return nil, err
	}
	if currentOwner != owner || currentGen != generation || currentPID != backendPID {
		return nil, execution.ErrLeaseLost
	}
	if err = checkExecutionLock(ctx, tx, pid); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT id,task_name,enabled,schedule_type,hour,minute,weekdays::text,timezone,task_config::text,next_run FROM schedule_settings WHERE is_running=true FOR UPDATE`)
	if err != nil {
		return nil, errors.New("schedule recovery query failed")
	}
	defer rows.Close()
	type sched struct {
		id               int64
		name             string
		enabled          bool
		typ              string
		hour, minute     int
		weekdays, config []byte
		tz               string
		next             *time.Time
	}
	all := []sched{}
	for rows.Next() {
		var s sched
		if err = rows.Scan(&s.id, &s.name, &s.enabled, &s.typ, &s.hour, &s.minute, &s.weekdays, &s.tz, &s.config, &s.next); err != nil {
			return nil, errors.New("schedule recovery row invalid")
		}
		all = append(all, s)
	}
	if rows.Err() != nil {
		return nil, errors.New("schedule recovery query failed")
	}
	var reconciled int
	now := time.Now().UTC()
	for _, s := range all {
		var active bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM background_jobs WHERE state IN ('queued','running','interrupted') AND (payload->>'schedule_id')=$1)`, fmt.Sprint(s.id)).Scan(&active); err != nil {
			return nil, errors.New("active schedule check failed")
		}
		if active {
			continue
		}
		var cfg map[string]any
		_ = json.Unmarshal(s.config, &cfg)
		delivery := s.name == "telegram_digest" || (s.name == "news_digest" && boolDefault(cfg, "send_telegram", true))
		state, msg := "interrupted", "Service restarted while task was marked running"
		var next *time.Time
		if delivery {
			state, msg = "delivery_unknown", "External delivery outcome is unknown after service restart"
			if s.enabled {
				if s.next == nil || !s.next.After(now) {
					n := nextOccurrence(s.typ, s.hour, s.minute, s.weekdays, s.tz, cfg, now)
					next = &n
				}
			} else {
				x := time.Time{}
				next = &x
			}
		} else if !s.enabled {
			x := time.Time{}
			next = &x
		} else if s.next == nil {
			n := nextOccurrence(s.typ, s.hour, s.minute, s.weekdays, s.tz, cfg, now)
			next = &n
		}
		if _, err = tx.Exec(ctx, `UPDATE schedule_settings SET is_running=false,last_finished_at=$2,last_status=$3,last_error=$4,next_run=CASE WHEN NOT $6 THEN NULL WHEN $5::timestamp IS NOT NULL THEN $5 ELSE next_run END WHERE id=$1`, s.id, now, state, msg, next, s.enabled); err != nil {
			return nil, errors.New("schedule recovery update failed")
		}
		reconciled++
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, errors.New("schedule recovery commit failed")
	}
	out, _ := json.Marshal(map[string]any{"event": "recovered", "schedules_reconciled": reconciled})
	return out, nil
}

func lockCurrentWorker(ctx context.Context, tx pgx.Tx) (string, int64, int32, error) {
	var owner *string
	var gen int64
	var pid *int32
	var heartbeat *time.Time
	var status string
	if err := tx.QueryRow(ctx, `SELECT owner_id::text,generation,lock_backend_pid,heartbeat_at,status FROM background_worker_status WHERE id=1 FOR UPDATE`).Scan(&owner, &gen, &pid, &heartbeat, &status); err != nil || owner == nil || pid == nil || heartbeat == nil || status != "running" {
		return "", 0, 0, execution.ErrLeaseLost
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp() AT TIME ZONE 'UTC'`).Scan(&now); err != nil || heartbeat.After(now) || now.Sub(*heartbeat) > 20*time.Second {
		return "", 0, 0, execution.ErrLeaseLost
	}
	var held bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=(((hashtext(current_schema())::bigint+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, *pid).Scan(&held); err != nil || !held {
		return "", 0, 0, execution.ErrLeaseLost
	}
	return *owner, gen, *pid, nil
}
func checkExecutionLock(ctx context.Context, tx pgx.Tx, pid int32) error {
	var held bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=((((hashtext(current_schema())::bigint # $2)+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, pid, execution.ExecutionGuardXOR).Scan(&held); err != nil || !held {
		return execution.ErrNotReady
	}
	return nil
}

func applyScheduleCompletion(ctx context.Context, tx pgx.Tx, id int64, created time.Time, state string, result []byte) error {
	var running, enabled bool
	var updated *time.Time
	err := tx.QueryRow(ctx, `SELECT is_running,enabled,updated_at FROM schedule_settings WHERE id=$1 FOR UPDATE`, id).Scan(&running, &enabled, &updated)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !running {
		return nil
	}
	if err != nil {
		return errors.New("schedule completion lookup failed")
	}
	if updated != nil && updated.After(created) {
		return nil
	}
	var m map[string]any
	_ = json.Unmarshal(result, &m)
	status, _ := m["status"].(string)
	lastStatus := "failed"
	if state == "delivery_unknown" {
		lastStatus = "delivery_unknown"
	} else if state == "succeeded" {
		if status != "warning" && status != "deferred" {
			lastStatus = "success"
		} else {
			lastStatus = status
		}
	}
	var duration any
	if n, ok := safeMetric(m["duration_seconds"]); ok {
		duration = n
	}
	lastErr := any("Background job failed")
	if state == "succeeded" {
		lastErr = nil
	}
	var next any = nil
	if enabled {
		if lastStatus == "deferred" {
			next = time.Now().UTC().Add(5 * time.Minute)
		} else if lastStatus == "failed" {
			next = time.Now().UTC().Add(10 * time.Minute)
		}
	}
	_, err = tx.Exec(ctx, `UPDATE schedule_settings SET is_running=false,last_finished_at=clock_timestamp() AT TIME ZONE 'UTC',last_status=$2,last_error=$3,last_duration_seconds=$4,last_success_at=CASE WHEN $2='success' THEN clock_timestamp() AT TIME ZONE 'UTC' ELSE last_success_at END,next_run=CASE WHEN NOT enabled THEN NULL WHEN $5::timestamp IS NOT NULL THEN $5 ELSE next_run END WHERE id=$1`, id, lastStatus, lastErr, duration, next)
	if err != nil {
		return errors.New("schedule completion update failed")
	}
	return nil
}
func jsonOrObject(b []byte) string {
	if len(b) == 0 || !json.Valid(b) {
		return `{}`
	}
	return string(b)
}

var tzToken = regexp.MustCompile(`^[A-Za-z0-9_+./-]{1,128}$`)

func nextOccurrence(typ string, hour, minute int, weekdaysJSON []byte, tz string, cfg map[string]any, now time.Time) time.Time {
	loc, err := time.LoadLocation(tz)
	if err != nil || !tzToken.MatchString(tz) {
		return now.Add(30 * time.Minute)
	}
	interval := 30
	if n, ok := positiveInt(cfg["interval_minutes"]); ok {
		interval = n
	}
	if interval > 1440 {
		interval = 1440
	}
	switch typ {
	case "interval":
		return now.Add(time.Duration(interval) * time.Minute)
	case "daily", "hourly":
	default:
		return now.Add(30 * time.Minute)
	}
	if hour < 0 || hour > 23 || minute < 0 || minute > 59 {
		return now.Add(30 * time.Minute)
	}
	var weekdays []int
	_ = json.Unmarshal(weekdaysJSON, &weekdays)
	local := now.In(loc)
	candidate := time.Date(local.Year(), local.Month(), local.Day(), hour, minute, 0, 0, loc)
	if typ == "hourly" {
		candidate = time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), minute, 0, 0, loc)
		if !candidate.After(local) {
			candidate = candidate.Add(time.Hour)
		}
	} else if !candidate.After(local) {
		candidate = candidate.AddDate(0, 0, 1)
	}
	if len(weekdays) > 0 {
		for i := 0; i < 8*7; i++ {
			wd := int(candidate.Weekday())
			if wd == 0 {
				wd = 7
			}
			valid := false
			for _, d := range weekdays {
				if d == wd {
					valid = true
					break
				}
			}
			if valid {
				break
			}
			candidate = candidate.AddDate(0, 0, 1)
			if typ == "hourly" {
				candidate = time.Date(candidate.Year(), candidate.Month(), candidate.Day(), 0, minute, 0, 0, loc)
			}
		}
	}
	return candidate.UTC()
}
