package execution

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type statusResponse struct {
	ExecutorInstanceID string `json:"executor_instance_id"`
	ActiveExecutions   int    `json:"active_executions"`
	Ready              bool   `json:"ready"`
}

func (s *Server) status(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	s.mu.Lock()
	ready := s.ready
	active := len(s.active)
	s.mu.Unlock()
	if !ready {
		writeErr(w, 503, "Executor is not ready")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	if s.pool == nil || s.pool.Ping(ctx) != nil || s.checkExecutionGuardPool(ctx) != nil {
		writeErr(w, 503, "Executor database is unavailable")
		return
	}
	if err := s.pipelineReady(ctx); err != nil {
		writeErr(w, 503, "Execution pipeline is unavailable")
		return
	}
	writeJSON(w, 200, statusResponse{ExecutorInstanceID: s.instance.String(), ActiveExecutions: active, Ready: true})
}

func (s *Server) auth(w http.ResponseWriter, r *http.Request) bool {
	if s.cfg.Token == "" {
		writeErr(w, 503, "Executor authentication is unavailable")
		return false
	}
	values := r.Header.Values("Authorization")
	expected := "Bearer " + s.cfg.Token
	if len(values) != 1 || subtle.ConstantTimeCompare([]byte(values[0]), []byte(expected)) != 1 {
		writeErr(w, 401, "Unauthorized")
		return false
	}
	return true
}
func (s *Server) ensureReady(w http.ResponseWriter) bool {
	s.mu.Lock()
	ok := s.ready && !s.closed
	s.mu.Unlock()
	if !ok {
		writeErr(w, 503, "Executor is not ready")
		return false
	}
	return true
}

func (s *Server) pipelineReady(parent context.Context) error {
	s.mu.Lock()
	ready := s.handlers.Ready
	s.mu.Unlock()
	if ready == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(parent, s.cfg.QueryTimeout)
	defer cancel()
	return ready(ctx)
}

func (s *Server) accept(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeErr(w, 405, "Method not allowed")
		return
	}
	if !s.auth(w, r) {
		return
	}
	if !s.ensureReady(w) {
		return
	}
	if s.handlers.Pipeline == nil {
		writeErr(w, 503, "Execution pipeline is unavailable")
		return
	}
	raw, e := readBounded(r, MaxEnvelopeBytes)
	if e != nil {
		writeErr(w, 422, "request body is invalid")
		return
	}
	env, digest, e := parseEnvelope(raw)
	if e != nil {
		writeErr(w, 422, "request body is invalid")
		return
	}
	s.admission.Lock()
	defer s.admission.Unlock()
	if !s.ensureReady(w) {
		return
	}
	// Resolve an existing attempt before global capacity so conflicting digests
	// always remain 409 and duplicates stay idempotent while another runs.
	prior, pe := s.loadReceipt(r.Context(), attemptKey{env.JobID, env.Attempt})
	if pe == nil {
		if prior.Digest != digest {
			writeErr(w, 409, "Execution attempt conflicts with its receipt")
			return
		}
		writeJSON(w, 202, map[string]any{"job_id": prior.JobID, "attempt_generation": prior.Attempt, "executor_instance_id": prior.Instance, "state": prior.State, "accepted_at": prior.AcceptedAt, "duplicate": true})
		return
	}
	if !errors.Is(pe, pgx.ErrNoRows) {
		writeErr(w, 503, "Execution receipt unavailable")
		return
	}
	s.mu.Lock()
	busy := len(s.active) > 0
	s.mu.Unlock()
	if busy {
		writeErr(w, 503, "Executor is busy")
		return
	}
	if err := s.pipelineReady(r.Context()); err != nil {
		writeErr(w, 503, "Execution pipeline is unavailable")
		return
	}
	receipt, accepted, e := s.acceptAttempt(r.Context(), env, digest)
	if e != nil {
		writeError(w, e)
		return
	}
	duplicate := !accepted
	if accepted {
		s.mu.Lock()
		if len(s.active) > 0 {
			s.mu.Unlock() // Receipt is durable; never execute twice, but surface saturated admission.
			writeErr(w, 503, "Executor is busy")
			return
		}
		parent := s.root
		if parent == nil {
			parent = r.Context()
		}
		runCtx, cancel := context.WithCancel(parent)
		key := attemptKey{env.JobID, env.Attempt}
		s.active[key] = cancel
		s.wg.Add(1)
		s.mu.Unlock()
		go s.runAttempt(runCtx, key, env, receipt.Kind, receipt.Payload)
	}
	writeJSON(w, 202, map[string]any{"job_id": env.JobID.String(), "attempt_generation": env.Attempt, "executor_instance_id": receipt.Instance, "state": receipt.State, "accepted_at": receipt.AcceptedAt, "duplicate": duplicate})
}

func (s *Server) executions(w http.ResponseWriter, r *http.Request) {
	if !s.auth(w, r) {
		return
	}
	if !s.ensureReady(w) {
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/internal/v1/job-executions/")
	parts := strings.Split(rest, "/")
	if len(parts) == 2 && parts[1] == "policy" && r.Method == "GET" {
		s.policy(w, r, parts[0])
		return
	}
	if len(parts) < 2 {
		writeErr(w, 404, "Execution receipt not found")
		return
	}
	id, e := canonicalUUID(parts[0])
	attempt, ae := strconv.ParseInt(parts[1], 10, 64)
	if e != nil || ae != nil || attempt < 0 || attempt > 2147483647 {
		writeErr(w, 404, "Execution receipt not found")
		return
	}
	if attempt == 0 && !(len(parts) == 3 && parts[2] == "events" && r.Method == "POST") {
		writeErr(w, 404, "Execution receipt not found")
		return
	}
	if len(parts) == 3 && parts[2] == "context" && r.Method == "GET" {
		s.contextEndpoint(w, r, attemptKey{id, attempt})
		return
	}
	if len(parts) == 3 && parts[2] == "cancel" && r.Method == "POST" {
		s.cancel(w, r, attemptKey{id, attempt})
		return
	}
	if len(parts) == 3 && parts[2] == "events" && r.Method == "POST" {
		s.finalize(w, r, attemptKey{id, attempt})
		return
	}
	if len(parts) != 2 || r.Method != "GET" {
		writeErr(w, 404, "Execution receipt not found")
		return
	}
	rec, e := s.loadReceipt(r.Context(), attemptKey{id, attempt})
	if errors.Is(e, pgx.ErrNoRows) {
		writeErr(w, 404, "Execution receipt not found")
		return
	}
	if e != nil {
		writeErr(w, 503, "Execution receipt unavailable")
		return
	}
	writeJSON(w, 200, rec.Map())
}

// contextEndpoint gives the trusted Python leaf adapter current execution
// authority without adding backend PID to the worker's six-field envelope.
func (s *Server) contextEndpoint(w http.ResponseWriter, r *http.Request, key attemptKey) {
	s.mu.Lock()
	_, active := s.active[key]
	ready := s.ready
	s.mu.Unlock()
	if !ready {
		writeErr(w, 503, "Executor is not ready")
		return
	}
	if !active {
		logExecutionFailure("context_endpoint", "attempt_not_active", nil)
		writeErr(w, 409, "Execution attempt is not active")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	var rec Receipt
	if err := s.loadReceiptPool(ctx, key, &rec); err != nil {
		logExecutionFailure("context_endpoint", "receipt_read_failed", err)
		if errors.Is(err, pgx.ErrNoRows) {
			writeErr(w, 404, "Execution receipt not found")
		} else {
			writeErr(w, 503, "Execution receipt unavailable")
		}
		return
	}
	if rec.Instance != s.instance.String() || rec.State != "running" {
		logExecutionFailure("context_endpoint", "receipt_not_running", nil)
		writeErr(w, 409, "Execution attempt is not current")
		return
	}
	var kind string
	var payload []byte
	var owner *string
	var gen int64
	var pid *int32
	var hb *time.Time
	var workerStatus string
	var state string
	var ownerGen *int64
	var attempt int64
	err := s.pool.QueryRow(ctx, `SELECT j.kind,j.payload::text,j.state,j.owner_generation,j.attempt_generation,w.owner_id::text,w.generation,w.lock_backend_pid,w.heartbeat_at,w.status FROM background_jobs j CROSS JOIN background_worker_status w WHERE j.id=$1 AND w.id=1`, key.job).Scan(&kind, &payload, &state, &ownerGen, &attempt, &owner, &gen, &pid, &hb, &workerStatus)
	if err != nil {
		logExecutionFailure("context_endpoint", "job_fence_read_failed", err)
		writeErr(w, 503, "Execution authority unavailable")
		return
	}
	if state != "running" || ownerGen == nil || rec.Generation == nil || *ownerGen != *rec.Generation || attempt != key.attempt || owner == nil || rec.OwnerID == nil || *owner != *rec.OwnerID || gen != *rec.Generation || pid == nil || hb == nil || workerStatus != "running" {
		logExecutionFailure("context_endpoint", "worker_or_attempt_mismatch", nil)
		writeErr(w, 409, "Execution attempt is not current")
		return
	}
	if err = s.checkWorkerLeasePool(ctx, *owner, gen, *pid); err != nil {
		logExecutionFailure("context_endpoint", "worker_lease_not_current", err)
		writeErr(w, 409, "Worker lease is not current")
		return
	}
	guardPID, guardErr := s.GuardBackendPID(ctx)
	if guardErr != nil {
		logExecutionFailure("context_endpoint", "execution_guard_unavailable", guardErr)
		writeErr(w, 503, "Executor guard is unavailable")
		return
	}
	validated, err := validatePayload(kind, json.RawMessage(payload))
	if err != nil {
		writeErr(w, 422, "Stored job is invalid")
		return
	}
	if rec.DeadlineAt == nil {
		writeErr(w, 409, "Execution deadline is not current")
		return
	}
	x := ExecutionContext{Version: 1, JobID: key.job.String(), AttemptGeneration: key.attempt, OwnerID: *owner, Generation: gen, BackendPID: *pid, ExecutorInstanceID: s.instance.String(), DeadlineAt: *rec.DeadlineAt, ExecutorBackendPID: guardPID}
	guardBackendPID, guardErr := s.GuardBackendPID(ctx)
	if guardErr != nil || guardBackendPID <= 0 {
		writeErr(w, 503, "Executor guard is unavailable")
		return
	}
	x.ExecutorBackendPID = guardBackendPID
	deadline, parseErr := time.Parse(time.RFC3339Nano, *rec.DeadlineAt)
	var dbNow time.Time
	if err = s.pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&dbNow); err != nil || parseErr != nil || !deadline.After(dbNow) {
		logExecutionFailure("context_endpoint", "deadline_not_current", err)
		writeErr(w, 409, "Execution deadline is no longer current")
		return
	}
	x.DeadlineAt = canonicalDeadline(deadline)
	var guardHeld bool
	if err = s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=((((hashtext(current_schema())::bigint # $2)+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, guardPID, ExecutionGuardXOR).Scan(&guardHeld); err != nil || !guardHeld {
		logExecutionFailure("context_endpoint", "execution_guard_not_held", err)
		writeErr(w, 409, "Executor guard is no longer current")
		return
	}
	writeJSON(w, 200, map[string]any{"version": 1, "envelope": x, "executor_backend_pid": guardPID, "kind": kind, "payload": validated})
}

func (s *Server) policy(w http.ResponseWriter, r *http.Request, rawID string) {
	id, e := canonicalUUID(rawID)
	if e != nil {
		writeErr(w, 404, "Job not found")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer cancel()
	var kind string
	var payload []byte
	var scheduleKey *string
	var scheduleID *int64
	e = s.pool.QueryRow(ctx, `SELECT kind,payload::text,schedule_key,(payload->>'schedule_id')::bigint FROM background_jobs WHERE id=$1`, id).Scan(&kind, &payload, &scheduleKey, &scheduleID)
	if errors.Is(e, pgx.ErrNoRows) {
		writeErr(w, 404, "Job not found")
		return
	}
	if e != nil {
		writeErr(w, 503, "Execution policy unavailable")
		return
	}
	timeout := defaultTimeout(kind)
	var p map[string]json.RawMessage
	_ = json.Unmarshal(payload, &p)
	if scheduleKey != nil && p["timeout_seconds"] != nil {
		if n, ok := rawInt(p["timeout_seconds"]); ok && n > 0 && n <= 86400 {
			timeout = int(n)
		}
	} else if scheduleID != nil {
		var config []byte
		e = s.pool.QueryRow(ctx, `SELECT task_config::text FROM schedule_settings WHERE id=$1`, *scheduleID).Scan(&config)
		if e == nil {
			var v map[string]any
			_ = json.Unmarshal(config, &v)
			if n, ok := v["timeout_seconds"]; ok {
				switch x := n.(type) {
				case float64:
					if x > 0 && x <= 86400 {
						timeout = int(x)
					}
				case bool:
					if x {
						timeout = 1
					}
				case string:
					if parsed, err := strconv.Atoi(x); err == nil && parsed > 0 {
						timeout = min(parsed, 86400)
					}
				}
			}
		}
	}
	writeJSON(w, 200, map[string]any{"timeout_seconds": timeout})
}

func defaultTimeout(kind string) int {
	switch kind {
	case "telegram_digest":
		return 600
	case "backup", "reprocess_failed":
		return 1800
	case "source_test":
		return 300
	default:
		return 3600
	}
}

func (s *Server) cancel(w http.ResponseWriter, r *http.Request, key attemptKey) {
	s.mu.Lock()
	stop, ok := s.active[key]
	if ok {
		stop()
	}
	s.mu.Unlock()
	if ok {
		// Revoke database-backed execution authority immediately. A handler that
		// ignores context cancellation can no longer publish or commit late work.
		_ = s.markCanceled(key)
	}
	if ok {
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			s.mu.Lock()
			_, still := s.active[key]
			s.mu.Unlock()
			if !still {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	rec, e := s.loadReceipt(r.Context(), key)
	if errors.Is(e, pgx.ErrNoRows) {
		writeErr(w, 404, "Execution receipt not found")
		return
	}
	if e != nil {
		writeErr(w, 503, "Execution receipt unavailable")
		return
	}
	m := rec.Map()
	s.mu.Lock()
	_, active := s.active[key]
	s.mu.Unlock()
	m["execution_stopped"] = !active
	m["current_executor_instance_id"] = s.instance.String()
	writeJSON(w, 200, m)
}

func (s *Server) markCanceled(key attemptKey) error {
	x, err := s.contextForReceipt(context.Background(), key)
	if err != nil {
		return err
	}
	qctx, cancel := context.WithTimeout(context.Background(), s.cfg.QueryTimeout)
	defer cancel()
	var kind string
	var payload []byte
	var jobState string
	if err = s.pool.QueryRow(qctx, `SELECT kind,payload::text,state FROM background_jobs WHERE id=$1`, key.job).Scan(&kind, &payload, &jobState); err != nil || jobState != "running" {
		return ErrStaleAttempt
	}
	state, code := "interrupted", "worker_interrupted"
	var receiptState string
	if err = s.pool.QueryRow(qctx, `SELECT state FROM background_job_executions WHERE job_id=$1 AND attempt_generation=$2 AND executor_instance_id=$3`, key.job, key.attempt, s.instance).Scan(&receiptState); err != nil {
		return ErrStaleAttempt
	}
	if receiptState == "running" && mayDeliver(kind, json.RawMessage(payload)) {
		state, code = "delivery_unknown", "external_outcome_unknown"
	}
	tx, err := s.pool.BeginTx(qctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(context.Background())
	if err = s.fenceTx(qctx, tx, key, x, false); err != nil {
		return err
	}
	_, err = tx.Exec(qctx, `UPDATE background_job_executions SET state=$3,result='{"status":"warning"}'::jsonb,error_code=$4,finished_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE job_id=$1 AND attempt_generation=$2 AND executor_instance_id=$5 AND state IN ('accepted','running')`, key.job, key.attempt, state, code, s.instance)
	if err != nil {
		return err
	}
	return tx.Commit(qctx)
}

func (s *Server) finalize(w http.ResponseWriter, r *http.Request, key attemptKey) {
	if s.handlers.Finalizer == nil {
		writeErr(w, 503, "Finalization is unavailable")
		return
	}
	raw, e := readBounded(r, MaxEnvelopeBytes)
	if e != nil || !isExactEvent(raw, "finalized") {
		writeErr(w, 422, "Unsupported job event")
		return
	}
	var owner string
	var generation int64
	var pid int32
	if err := s.pool.QueryRow(r.Context(), `SELECT owner_id::text,generation,lock_backend_pid FROM background_worker_status WHERE id=1 AND status='running'`).Scan(&owner, &generation, &pid); err != nil {
		writeErr(w, 409, "Worker lease is not current")
		return
	}
	if err := s.checkWorkerLeasePool(r.Context(), owner, generation, pid); err != nil {
		writeErr(w, 409, "Worker lease is not current")
		return
	}
	rec, receiptErr := s.loadReceipt(r.Context(), key)
	if receiptErr != nil && !(key.attempt == 0 && errors.Is(receiptErr, pgx.ErrNoRows)) {
		writeErr(w, 404, "Execution receipt not found")
		return
	}
	deadline := ""
	if receiptErr == nil && rec.DeadlineAt != nil {
		deadline = *rec.DeadlineAt
	}
	x := ExecutionContext{Version: 1, JobID: key.job.String(), AttemptGeneration: key.attempt, OwnerID: owner, Generation: generation, BackendPID: pid, ExecutorInstanceID: s.instance.String(), DeadlineAt: deadline, ExecutorBackendPID: s.guardPID}
	x.ExecutorBackendPID, e = s.GuardBackendPID(r.Context())
	if e != nil {
		writeErr(w, 503, "Executor guard is unavailable")
		return
	}
	out, e := s.handlers.Finalizer.Finalize(r.Context(), x)
	if e != nil {
		writeErr(w, 409, "Finalization could not be applied")
		return
	}
	if len(out) == 0 {
		out = json.RawMessage(`{}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(out)
}

func (s *Server) workerEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		writeErr(w, 405, "Method not allowed")
		return
	}
	if !s.auth(w, r) {
		return
	}
	if !s.ensureReady(w) {
		return
	}
	if s.handlers.Recovery == nil {
		writeErr(w, 503, "Recovery is unavailable")
		return
	}
	raw, e := readBounded(r, MaxEnvelopeBytes)
	if e != nil {
		writeErr(w, 422, "Worker event body is invalid")
		return
	}
	if rejectDuplicateKeys(raw) != nil {
		writeErr(w, 422, "Worker event body is invalid")
		return
	}
	var m map[string]any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&m) != nil || len(m) != 3 || m["event"] != "recovered" {
		writeErr(w, 422, "Worker event body is invalid")
		return
	}
	owner, e := canonicalUUIDValue(m["owner_id"])
	gen, e2 := positiveInt(m["generation"])
	if e != nil || e2 != nil {
		writeErr(w, 422, "Worker event body is invalid")
		return
	}
	var pid int32
	q, c := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	defer c()
	err := s.pool.QueryRow(q, `SELECT lock_backend_pid FROM background_worker_status WHERE id=1 AND owner_id=$1 AND generation=$2 AND status='running'`, owner, gen).Scan(&pid)
	if err != nil {
		writeErr(w, 409, "Worker lease is not current")
		return
	}
	if err = s.checkWorkerLeasePool(q, owner.String(), gen, pid); err != nil {
		writeErr(w, 409, "Worker lease is not current")
		return
	}
	out, err := s.handlers.Recovery.Recover(r.Context(), owner.String(), gen, pid)
	if err != nil {
		writeErr(w, 409, "Recovery could not be applied")
		return
	}
	if len(out) == 0 {
		out = json.RawMessage(`{"event":"recovered","schedules_reconciled":0}`)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)
	_, _ = w.Write(out)
}
func (s *Server) checkWorkerLeasePool(ctx context.Context, owner string, gen int64, pid int32) error {
	var held bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=(((hashtext(current_schema())::bigint+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, pid).Scan(&held)
	if err != nil || !held {
		return ErrLeaseLost
	}
	var currentOwner *string
	var currentGeneration int64
	var currentPID *int32
	var heartbeat *time.Time
	var status string
	if err = s.pool.QueryRow(ctx, `SELECT owner_id::text,generation,lock_backend_pid,heartbeat_at,status FROM background_worker_status WHERE id=1`).Scan(&currentOwner, &currentGeneration, &currentPID, &heartbeat, &status); err != nil || currentOwner == nil || currentPID == nil || heartbeat == nil || *currentOwner != owner || currentGeneration != gen || *currentPID != pid || status != "running" {
		return ErrLeaseLost
	}
	var now time.Time
	if err = s.pool.QueryRow(ctx, `SELECT clock_timestamp() AT TIME ZONE 'UTC'`).Scan(&now); err != nil || heartbeat.After(now) || now.Sub(*heartbeat) > HeartbeatStale {
		return ErrLeaseLost
	}
	return nil
}

func writeError(w http.ResponseWriter, e error) {
	switch {
	case errors.Is(e, ErrConflict):
		writeErr(w, 409, "Execution attempt conflicts with its receipt")
	case errors.Is(e, ErrLeaseLost), errors.Is(e, ErrStaleAttempt):
		writeErr(w, 409, "Worker lease or job attempt is not current")
	case errors.Is(e, ErrDeadline):
		writeErr(w, 422, "deadline_at is outside the allowed window")
	case errors.Is(e, ErrInvalidPayload):
		writeErr(w, 422, "Stored job is invalid")
	case errors.Is(e, ErrBusy):
		writeErr(w, 503, "Executor is busy")
	default:
		var pg *pgconn.PgError
		if errors.As(e, &pg) {
			writeErr(w, 503, "Execution database operation failed")
		} else {
			writeErr(w, 503, "Execution database unavailable")
		}
	}
}
func writeErr(w http.ResponseWriter, status int, detail string) {
	writeJSON(w, status, map[string]string{"detail": detail})
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func readBounded(r *http.Request, limit int64) ([]byte, error) {
	if r.ContentLength > limit {
		return nil, ErrInvalidPayload
	}
	b, e := io.ReadAll(io.LimitReader(r.Body, limit+1))
	if e != nil || int64(len(b)) > limit {
		return nil, ErrInvalidPayload
	}
	return b, nil
}
func canonicalUUID(s string) (uuid.UUID, error) {
	u, e := uuid.Parse(s)
	if e != nil || u.String() != s {
		return uuid.Nil, ErrInvalidPayload
	}
	return u, nil
}
func isExactEvent(raw []byte, name string) bool {
	if rejectDuplicateKeys(raw) != nil {
		return false
	}
	var m map[string]any
	return json.Unmarshal(raw, &m) == nil && len(m) == 1 && m["event"] == name
}
func closeConn(c *pgx.Conn) {
	if c != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = c.Close(ctx)
	}
}
