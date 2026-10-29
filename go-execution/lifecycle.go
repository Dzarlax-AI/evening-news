package execution

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"math"
	"time"
)

func (s *Server) runAttempt(ctx context.Context, key attemptKey, e Envelope, kind string, payload json.RawMessage) {
	startedAt := time.Now()
	defer s.wg.Done()
	defer func() { s.mu.Lock(); delete(s.active, key); s.mu.Unlock() }()
	if s.handlers.Pipeline == nil {
		s.finish(context.Background(), key, "failed", "worker_error", json.RawMessage(`{"status":"warning"}`))
		return
	}
	x, err := s.executionContext(ctx, key, e)
	if err != nil {
		// Any terminal write revalidates the worker lease, job attempt, and
		// execution guard in its own transaction. This closes the accepted-receipt
		// gap for transient startup/read errors without writing after authority is
		// lost.
		s.finish(context.Background(), key, "failed", "worker_error", json.RawMessage(`{"status":"warning"}`))
		return
	}
	if s.beforeSetRunning != nil {
		s.beforeSetRunning()
	}
	if err = s.setRunning(ctx, key, x); err != nil {
		logExecutionFailure("attempt_start_fence", "authority_fence_failed", err)
		if errors.Is(err, ErrDeadline) {
			state, code := "failed", "worker_error"
			if mayDeliver(kind, payload) {
				state, code = "delivery_unknown", "external_outcome_unknown"
			}
			seconds := math.Round(math.Max(0, math.Min(24*60*60, time.Since(startedAt).Seconds()))*1000) / 1000
			warning, _ := json.Marshal(map[string]any{"status": "warning", "duration_seconds": seconds})
			// Persist only after finish revalidates the worker lease and execution
			// guard in its terminal transaction. No pipeline work has started.
			s.finish(context.Background(), key, state, code, warning)
		} else {
			// setRunning did not commit pipeline state, so no external side effect
			// could have begun. finish() persists only if current authority still
			// passes the transaction fence.
			s.finish(context.Background(), key, "failed", "worker_error", json.RawMessage(`{"status":"warning"}`))
		}
		return
	}
	if kind == "article_reprocess" {
		if err = s.initializeArticleReprocess(ctx, key, x, payload); err != nil {
			if errors.Is(err, ErrLeaseLost) || errors.Is(err, ErrStaleAttempt) {
				return
			}
			s.finish(context.Background(), key, "failed", "worker_error", json.RawMessage(`{"status":"warning"}`))
			return
		}
	}
	execCtx, cancel := context.WithDeadline(ctx, e.Deadline)
	defer cancel()
	result, runErr := s.handlers.Pipeline.Execute(execCtx, x, kind, payload)
	state, code := "succeeded", ""
	if runErr != nil || execCtx.Err() != nil {
		// Keep diagnostics useful without surfacing a handler error that may
		// include provider, request, or database details.
		if runErr != nil {
			logExecutionFailure("pipeline_execute", SafeExecutionFailureReason(runErr), nil)
		} else if execCtx.Err() != nil {
			logExecutionFailure("pipeline_execute", "context_ended", nil)
		}
		state, code = pipelineFailureDisposition(kind, payload, runErr, execCtx.Err())
		result = Result{Value: json.RawMessage(`{"status":"warning"}`)}
	}
	bounded, berr := safeResult(result.Value)
	if berr != nil {
		logExecutionFailure("result_projection", "result_rejected", nil)
		if mayDeliver(kind, payload) {
			state, code = "delivery_unknown", "external_outcome_unknown"
		} else {
			state, code = "failed", "worker_error"
		}
		bounded = json.RawMessage(`{"status":"warning"}`)
	}
	var projection map[string]json.RawMessage
	if json.Unmarshal(bounded, &projection) == nil && projection != nil {
		seconds := math.Round(math.Max(0, math.Min(24*60*60, time.Since(startedAt).Seconds()))*1000) / 1000
		projection["duration_seconds"], _ = json.Marshal(seconds)
		bounded, _ = json.Marshal(projection)
	}
	if kind == "source_test" && state == "succeeded" {
		var projected map[string]json.RawMessage
		_ = json.Unmarshal(bounded, &projected)
		var connectionOK bool
		if projected == nil || (json.Unmarshal(projected["connection_ok"], &connectionOK) != nil && json.Unmarshal(projected["success"], &connectionOK) != nil) {
			state, code = "failed", "worker_error"
			bounded = json.RawMessage(`{"status":"warning","connection_ok":false}`)
		} else {
			projected["connection_ok"], _ = json.Marshal(connectionOK)
			if !connectionOK {
				projected["status"] = json.RawMessage(`"warning"`)
			}
			bounded, _ = json.Marshal(projected)
		}
		if !connectionOK {
			state, code = "failed", "worker_error"
		}
	}
	// The task context may be canceled at timeout/cancel. Receipt persistence
	// uses a fresh bounded DB context while revalidating the current lease.
	s.finish(context.Background(), key, state, code, bounded)
}

func pipelineFailureDisposition(kind string, payload []byte, runErr, contextErr error) (string, string) {
	if DeliveryOutcomeUnknown(kind, payload, runErr) {
		return "delivery_unknown", "external_outcome_unknown"
	}
	if errors.Is(contextErr, context.Canceled) || errors.Is(runErr, context.Canceled) {
		return "interrupted", "worker_interrupted"
	}
	return "failed", "worker_error"
}

func (s *Server) executionContext(ctx context.Context, key attemptKey, e Envelope) (ExecutionContext, error) {
	var pid int32
	var owner string
	var gen int64
	var deadline time.Time
	q, c := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer c()
	err := s.pool.QueryRow(q, `SELECT w.owner_id::text,w.generation,w.lock_backend_pid,r.deadline_at FROM background_worker_status w JOIN background_job_executions r ON r.job_id=$1 AND r.attempt_generation=$2 WHERE w.id=1`, key.job, key.attempt).Scan(&owner, &gen, &pid, &deadline)
	if err != nil {
		logExecutionFailure("attempt_context_read", "database_error", err)
		return ExecutionContext{}, ErrLeaseLost
	}
	if owner != e.OwnerID.String() || gen != e.Generation {
		logExecutionFailure("attempt_context_read", "worker_identity_mismatch", nil)
		return ExecutionContext{}, ErrLeaseLost
	}
	guardPID, err := s.GuardBackendPID(ctx)
	if err != nil {
		logExecutionFailure("attempt_context_read", "execution_guard_unavailable", err)
		return ExecutionContext{}, err
	}
	return ExecutionContext{Version: 1, JobID: key.job.String(), AttemptGeneration: key.attempt, OwnerID: owner, Generation: gen, BackendPID: pid, ExecutorInstanceID: s.instance.String(), DeadlineAt: canonicalDeadline(deadline), ExecutorBackendPID: guardPID}, nil
}

func canonicalDeadline(deadline time.Time) string {
	return deadline.UTC().Truncate(time.Microsecond).Format("2006-01-02T15:04:05.000000Z")
}
func (s *Server) setRunning(ctx context.Context, key attemptKey, x ExecutionContext) error {
	return s.fencedUpdate(ctx, key, x, `UPDATE background_job_executions SET state='running',started_at=COALESCE(started_at,clock_timestamp() AT TIME ZONE 'UTC') WHERE job_id=$1 AND attempt_generation=$2 AND executor_instance_id=$3 AND state='accepted'`)
}

// initializeArticleReprocess atomically resets selected rows once per durable
// job, preserving completed stages on subsequent attempts/retries.
func (s *Server) initializeArticleReprocess(ctx context.Context, key attemptKey, x ExecutionContext, payload json.RawMessage) error {
	var input struct {
		ArticleIDs []int64 `json:"article_ids"`
		Force      *bool   `json:"force"`
	}
	if err := json.Unmarshal(payload, &input); err != nil || len(input.ArticleIDs) == 0 {
		return ErrInvalidPayload
	}
	force := true
	if input.Force != nil {
		force = *input.Force
	}
	q, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()
	tx, err := s.pool.BeginTx(q, pgx.TxOptions{})
	if err != nil {
		logExecutionFailure("article_reprocess_initialization", "transaction_begin_failed", err)
		return errors.New("article reprocess initialization unavailable")
	}
	defer tx.Rollback(context.Background())
	if err = s.fenceTx(q, tx, key, x, true); err != nil {
		logExecutionFailure("article_reprocess_initialization", "authority_fence_failed", err)
		return err
	}
	var initialized *time.Time
	if err = tx.QueryRow(q, `SELECT initialized_at FROM background_jobs WHERE id=$1 FOR UPDATE`, key.job).Scan(&initialized); err != nil {
		logExecutionFailure("article_reprocess_initialization", "job_initialization_read_failed", err)
		return ErrStaleAttempt
	}
	if initialized != nil {
		return tx.Commit(q)
	}
	if force {
		_, err = tx.Exec(q, `UPDATE articles SET processing_state='pending',processing_attempts=0,next_attempt_at=NULL,last_processing_error=NULL,processed=false,summary_processed=false,category_processed=false,ad_processed=false WHERE id=ANY($1::bigint[])`, input.ArticleIDs)
	} else {
		_, err = tx.Exec(q, `UPDATE articles SET processing_state='pending',processing_attempts=0,next_attempt_at=NULL,last_processing_error=NULL,processed=false WHERE id=ANY($1::bigint[])`, input.ArticleIDs)
	}
	if err != nil {
		logExecutionFailure("article_reprocess_initialization", "article_reset_failed", err)
		return errors.New("article reprocess initialization failed")
	}
	if _, err = tx.Exec(q, `UPDATE background_jobs SET initialized_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=$1 AND initialized_at IS NULL`, key.job); err != nil {
		logExecutionFailure("article_reprocess_initialization", "job_initialization_write_failed", err)
		return errors.New("article reprocess initialization failed")
	}
	if err = tx.Commit(q); err != nil {
		logExecutionFailure("article_reprocess_initialization", "transaction_commit_failed", err)
		return errors.New("article reprocess initialization failed")
	}
	return nil
}
func (s *Server) finish(ctx context.Context, key attemptKey, state, code string, result json.RawMessage) {
	x, err := s.contextForReceipt(ctx, key)
	if err != nil {
		return
	}
	q, c := context.WithTimeout(context.Background(), s.cfg.QueryTimeout)
	defer c()
	tx, err := s.pool.BeginTx(q, pgx.TxOptions{})
	if err != nil {
		return
	}
	defer tx.Rollback(context.Background())
	if s.fenceTx(q, tx, key, x, false) != nil {
		return
	}
	_, err = tx.Exec(q, `UPDATE background_job_executions SET state=$3,result=$4::jsonb,error_code=NULLIF($5,''),finished_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE job_id=$1 AND attempt_generation=$2 AND executor_instance_id=$6 AND state IN ('accepted','running')`, key.job, key.attempt, state, string(result), code, s.instance)
	if err == nil {
		_ = tx.Commit(q)
	}
}
func (s *Server) contextForReceipt(ctx context.Context, key attemptKey) (ExecutionContext, error) {
	var owner string
	var gen int64
	var pid int32
	var deadline time.Time
	q, c := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer c()
	err := s.pool.QueryRow(q, `SELECT owner_id::text,worker_generation,(SELECT lock_backend_pid FROM background_worker_status WHERE id=1),deadline_at FROM background_job_executions WHERE job_id=$1 AND attempt_generation=$2`, key.job, key.attempt).Scan(&owner, &gen, &pid, &deadline)
	if err != nil {
		return ExecutionContext{}, err
	}
	return ExecutionContext{Version: 1, JobID: key.job.String(), AttemptGeneration: key.attempt, OwnerID: owner, Generation: gen, BackendPID: pid, ExecutorInstanceID: s.instance.String(), DeadlineAt: deadline.UTC().Format(time.RFC3339Nano), ExecutorBackendPID: s.guardPID}, nil
}
func (s *Server) fencedUpdate(ctx context.Context, key attemptKey, x ExecutionContext, sql string) error {
	q, c := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer c()
	tx, e := s.pool.BeginTx(q, pgx.TxOptions{})
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	if e = s.fenceTx(q, tx, key, x, true); e != nil {
		return e
	}
	_, e = tx.Exec(q, sql, key.job, key.attempt, s.instance)
	if e == nil {
		e = tx.Commit(q)
	}
	return e
}
func (s *Server) fenceTx(ctx context.Context, tx pgx.Tx, key attemptKey, x ExecutionContext, enforceDeadline bool) error {
	if x.ExecutorInstanceID != s.instance.String() || x.ExecutorBackendPID != s.guardPID {
		return ErrNotReady
	}
	if err := s.checkExecutionGuardTx(ctx, tx); err != nil {
		return err
	}
	if e := s.checkWorkerLeaseTx(ctx, tx, x.OwnerID, x.Generation, x.BackendPID); e != nil {
		return e
	}
	var state string
	var ownerGen *int64
	var attempt int64
	e := tx.QueryRow(ctx, `SELECT state,owner_generation,attempt_generation FROM background_jobs WHERE id=$1 FOR UPDATE`, key.job).Scan(&state, &ownerGen, &attempt)
	if e != nil || state != "running" || ownerGen == nil || *ownerGen != x.Generation || attempt != key.attempt {
		return ErrStaleAttempt
	}
	var deadline time.Time
	var receiptState string
	e = tx.QueryRow(ctx, `SELECT deadline_at,state FROM background_job_executions WHERE job_id=$1 AND attempt_generation=$2 AND executor_instance_id=$3 FOR UPDATE`, key.job, key.attempt, s.instance).Scan(&deadline, &receiptState)
	if e != nil {
		return ErrStaleAttempt
	}
	if receiptState != "accepted" && receiptState != "running" {
		return ErrStaleAttempt
	}
	var now time.Time
	if e = tx.QueryRow(ctx, `SELECT clock_timestamp() AT TIME ZONE 'UTC'`).Scan(&now); e != nil || enforceDeadline && !now.Before(deadline) {
		return ErrDeadline
	}
	return nil
}
