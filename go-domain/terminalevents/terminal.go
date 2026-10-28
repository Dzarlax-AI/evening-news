package terminalevents

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/privateapi"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var eventUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

type Event struct {
	Version     int    `json:"version"`
	JobID       string `json:"job_id"`
	Attempt     int    `json:"attempt_generation"`
	OwnerID     string `json:"owner_id"`
	Generation  int    `json:"generation"`
	WorkerPID   int    `json:"backend_pid"`
	ExecutorID  string `json:"executor_instance_id"`
	ExecutorPID int    `json:"executor_backend_pid"`
}
type Guard interface{ Check(context.Context) error }
type InstanceReader interface {
	Instance(context.Context) (string, error)
}
type Server struct {
	Token     string
	Pool      *pgxpool.Pool
	Guard     Guard
	Authority InstanceReader
	Alerts    *Alerts
	mu        sync.Mutex
	active    map[string]int
	locks     map[string]*eventLock
}

func New(token string, pool *pgxpool.Pool, guard Guard, authority InstanceReader, send AlertSender) *Server {
	return &Server{Token: token, Pool: pool, Guard: guard, Authority: authority, Alerts: NewAlerts(send), active: map[string]int{}, locks: map[string]*eventLock{}}
}
func respond(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	r = r.WithContext(ctx)
	expected := "Bearer " + s.Token
	got := r.Header.Get("Authorization")
	if s.Token == "" {
		respond(w, 503, map[string]string{"detail": "Executor authentication is unavailable"})
		return
	}
	if len(expected) != len(got) || subtle.ConstantTimeCompare([]byte(expected), []byte(got)) != 1 {
		respond(w, 401, map[string]string{"detail": "Unauthorized"})
		return
	}
	if s.Guard == nil || s.Guard.Check(r.Context()) != nil {
		respond(w, 503, map[string]string{"detail": "Executor instance is not the database owner"})
		return
	}
	raw, err := io.ReadAll(io.LimitReader(r.Body, 4097))
	if err != nil || len(raw) > 4096 {
		respond(w, 413, map[string]string{"detail": "Request body exceeds limit"})
		return
	}
	var fields map[string]json.RawMessage
	var event Event
	if json.Unmarshal(raw, &fields) != nil || len(fields) != 8 || privateapi.DecodeJSON(raw, &event) != nil || event.Version != 1 || !eventUUID.MatchString(event.JobID) || !eventUUID.MatchString(event.OwnerID) || !eventUUID.MatchString(event.ExecutorID) || event.Attempt < 0 || event.Attempt > 2147483647 || event.Generation <= 0 || event.Generation > 2147483647 || event.WorkerPID <= 0 || event.WorkerPID > 2147483647 || event.ExecutorPID <= 0 || event.ExecutorPID > 2147483647 {
		respond(w, 409, map[string]string{"detail": "Terminal event is not current"})
		return
	}
	for _, value := range fields {
		if string(value) == "null" {
			respond(w, 409, map[string]string{"detail": "Terminal event is not current"})
			return
		}
	}
	instance, err := s.Authority.Instance(r.Context())
	if err != nil || instance != event.ExecutorID {
		respond(w, 409, map[string]string{"detail": "Terminal event is not current"})
		return
	}
	release, err := s.acquire(r.Context(), event.JobID+":"+fmt.Sprint(event.Attempt))
	if err != nil {
		respond(w, 503, map[string]string{"detail": "Executor runtime is unavailable"})
		return
	}
	defer release()
	state, err := s.process(r.Context(), event)
	if err != nil {
		if errors.Is(err, authority.ErrStale) {
			respond(w, 409, map[string]string{"detail": "Terminal event authority is stale"})
		} else {
			respond(w, 500, map[string]string{"detail": "Terminal event failed"})
		}
		return
	}
	respond(w, 200, map[string]any{"job_id": event.JobID, "attempt_generation": event.Attempt, "legacy_completion_applied": true, "alert_state": state})
}

type terminalJob struct {
	Kind, State     string
	Payload, Result []byte
	ScheduleKey     *string
	QueuePosition   int64
	AlertState      string
}

func validateTerminal(ctx context.Context, tx pgx.Tx, e Event) (terminalJob, error) {
	var j terminalJob
	var valid bool
	err := tx.QueryRow(ctx, `SELECT owner_id=$1::uuid AND generation=$2 AND status='running' AND lock_backend_pid=$3 AND heartbeat_at IS NOT NULL AND heartbeat_at <= (clock_timestamp() AT TIME ZONE 'UTC') AND heartbeat_at >= (clock_timestamp() AT TIME ZONE 'UTC')-interval '20 seconds' FROM background_worker_status WHERE id=1 FOR UPDATE`, e.OwnerID, e.Generation, e.WorkerPID).Scan(&valid)
	if err != nil || !valid {
		return j, authority.ErrStale
	}
	for _, v := range []struct{ pid, xor int }{{e.WorkerPID, 0}, {e.ExecutorPID, 0x454E4732}} {
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=((((hashtext(current_schema()) # $2::integer)::bigint+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, v.pid, v.xor).Scan(&valid)
		if err != nil || !valid {
			return j, authority.ErrStale
		}
	}
	err = tx.QueryRow(ctx, `SELECT kind,state,payload::text,result::text,schedule_key,queue_position FROM background_jobs WHERE id=$1::uuid AND attempt_generation=$2 FOR UPDATE`, e.JobID, e.Attempt).Scan(&j.Kind, &j.State, &j.Payload, &j.Result, &j.ScheduleKey, &j.QueuePosition)
	if err != nil || !oneOf(j.State, "succeeded", "failed", "interrupted", "delivery_unknown") {
		return j, authority.ErrStale
	}
	err = tx.QueryRow(ctx, `SELECT legacy_alert_state FROM background_job_executions WHERE job_id=$1::uuid AND attempt_generation=$2 AND state=$3 AND legacy_completion_applied_at IS NOT NULL FOR UPDATE`, e.JobID, e.Attempt, j.State).Scan(&j.AlertState)
	if err != nil || !oneOf(j.AlertState, "pending", "attempting", "delivered", "unknown", "not_needed") {
		return j, authority.ErrStale
	}
	return j, nil
}

type eventLock struct {
	ch   chan struct{}
	refs int
}

func (s *Server) acquire(ctx context.Context, key string) (func(), error) {
	s.mu.Lock()
	l := s.locks[key]
	if l == nil {
		l = &eventLock{ch: make(chan struct{}, 1)}
		l.ch <- struct{}{}
		s.locks[key] = l
	}
	l.refs++
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		s.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(s.locks, key)
		}
		s.mu.Unlock()
		return nil, ctx.Err()
	case <-l.ch:
	}
	return func() {
		s.mu.Lock()
		l.ch <- struct{}{}
		l.refs--
		if l.refs == 0 {
			delete(s.locks, key)
		}
		s.mu.Unlock()
	}, nil
}
func (s *Server) activeJobs() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	v := map[string]bool{}
	for key := range s.active {
		v[key] = true
	}
	return v
}
func (s *Server) setActive(job string, active bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if active {
		s.active[job]++
	} else if s.active[job] <= 1 {
		delete(s.active, job)
	} else {
		s.active[job]--
	}
}
func alertKey(j terminalJob) string {
	if j.ScheduleKey != nil && *j.ScheduleKey != "" {
		return "scheduler-warning-" + *j.ScheduleKey
	}
	var m map[string]any
	_ = json.Unmarshal(j.Payload, &m)
	if v, ok := m["schedule_id"]; ok {
		return "scheduler-warning-" + fmt.Sprint(v)
	}
	return "scheduler-warning-None"
}
func eventState(ctx context.Context, tx pgx.Tx, e Event, state string) error {
	_, err := tx.Exec(ctx, `UPDATE background_job_executions SET legacy_alert_state=$3 WHERE job_id=$1::uuid AND attempt_generation=$2`, e.JobID, e.Attempt, state)
	return err
}
func (s *Server) process(ctx context.Context, e Event) (string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(context.Background())
	job, err := validateTerminal(ctx, tx, e)
	if err != nil {
		return "", err
	}
	identity := scheduleIdentity(job)
	if job.AlertState == "attempting" {
		if err = eventState(ctx, tx, e, "unknown"); err != nil {
			return "", err
		}
		if identity != "" {
			v, ok, er := loadLedger(ctx, tx, identity, false)
			if er != nil {
				return "", er
			}
			if ok {
				abandonLedger(&v, e.JobID)
				if er = saveLedger(ctx, tx, identity, v); er != nil {
					return "", er
				}
			}
		}
		if err = tx.Commit(ctx); err != nil {
			return "", err
		}
		return "unknown", nil
	}
	if job.AlertState != "pending" {
		return job.AlertState, nil
	}
	key := alertKey(job)
	validObservation := false
	healthyObservation := false
	var notice *sourceNotice
	var sourceEvents []sourceEvent
	if identity != "" && job.Kind == "news_processing" {
		// Savepoint isolates malformed or unavailable source policy, matching the
		// legacy fallback to the ordinary scheduler warning.
		nested, er := tx.Begin(ctx)
		if er == nil {
			var obs *observation
			if job.State == "succeeded" {
				obs, er = loadObservation(ctx, nested, job, e)
			}
			if er == nil {
				var ledger sourceLedger
				var ok bool
				ledger, ok, er = loadLedger(ctx, nested, identity, obs != nil)
				if er == nil && ok {
					active := s.activeJobs()
					if obs == nil {
						breakSourceStreaks(&ledger, job.QueuePosition, active)
					} else {
						var prev *int64
						prev, er = previousPosition(ctx, nested, job, identity)
						if er == nil {
							notice, validObservation = prepareSourceNotice(&ledger, job.QueuePosition, e.JobID, obs.Sources, prev, active)
							healthyObservation = obs.Warning == nil
						}
					}
					if er == nil {
						er = saveLedger(ctx, nested, identity, ledger)
					}
				}
			}
			if er != nil {
				_ = nested.Rollback(ctx)
				notice = nil
				validObservation = false
			} else if er = nested.Commit(ctx); er != nil {
				notice = nil
				validObservation = false
			}
		}
	}
	kind := ""
	if validObservation {
		if notice == nil && healthyObservation && s.Alerts.Active(key) {
			kind = "recovery"
		}
	} else {
		var result map[string]any
		_ = json.Unmarshal(job.Result, &result)
		failure := job.State != "succeeded" || result["status"] == "warning" || result["status"] == "deferred"
		if failure {
			kind = "failure"
		} else if s.Alerts.Active(key) {
			kind = "recovery"
		}
	}
	if notice == nil && kind == "" {
		if err = eventState(ctx, tx, e, "not_needed"); err != nil {
			return "", err
		}
		if err = tx.Commit(ctx); err != nil {
			return "", err
		}
		return "not_needed", nil
	}
	if notice != nil {
		sourceEvents = notice.Events
	}
	if err = eventState(ctx, tx, e, "attempting"); err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	s.setActive(e.JobID, true)
	sent := false
	if notice != nil {
		sent = s.Alerts.safeSend(ctx, notice.Title, notice.Message)
	} else if kind == "failure" {
		sent = s.Alerts.Failure(ctx, key, "Scheduled task "+job.Kind+" needs attention", "The scheduled task failed, was deferred, or completed with warnings.")
	} else {
		sent = s.Alerts.Recovery(ctx, key, "Scheduled task "+job.Kind+" recovered", "The latest scheduled task completed without warnings.")
	}
	s.setActive(e.JobID, false)
	if s.Guard.Check(ctx) != nil {
		return "unknown", nil
	}
	instance, err := s.Authority.Instance(ctx)
	if err != nil || instance != e.ExecutorID {
		return "unknown", nil
	}
	tx2, err := s.Pool.Begin(ctx)
	if err != nil {
		return "unknown", nil
	}
	defer tx2.Rollback(context.Background())
	current, err := validateTerminal(ctx, tx2, e)
	if err != nil {
		return "unknown", nil
	}
	if current.AlertState != "attempting" {
		return current.AlertState, nil
	}
	state := "unknown"
	if sent {
		state = "delivered"
	}
	if err = eventState(ctx, tx2, e, state); err != nil {
		return "unknown", nil
	}
	if notice != nil && identity != "" {
		ledger, ok, er := loadLedger(ctx, tx2, identity, false)
		if er != nil {
			return "unknown", nil
		}
		if ok {
			finishSourceNotice(&ledger, sourceEvents, e.JobID, sent)
			if er = saveLedger(ctx, tx2, identity, ledger); er != nil {
				return "unknown", nil
			}
		}
	}
	if err = tx2.Commit(ctx); err != nil {
		return "unknown", nil
	}
	return state, nil
}
