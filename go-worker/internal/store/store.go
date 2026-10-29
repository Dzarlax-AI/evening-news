package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrLeaseHeld = errors.New("worker lease is held by another process")
	ErrLeaseLost = errors.New("worker lease is no longer current")
	ErrNoJob     = errors.New("no job available")
)

// HeartbeatError reports only bounded diagnostic metadata. It deliberately
// omits PostgreSQL's error text, which may contain query or configuration data.
type HeartbeatError struct {
	Stage          string
	Reason         string
	SQLState       string
	BackendPID     int32
	Generation     int64
	MutexWait      time.Duration
	BeginDuration  time.Duration
	UpdateDuration time.Duration
	CommitDuration time.Duration
}

func (e *HeartbeatError) Error() string {
	if e == nil {
		return ErrLeaseLost.Error()
	}
	diagnostic := fmt.Sprintf("%s (heartbeat stage=%s reason=%s mutex_wait_ms=%d begin_ms=%d update_ms=%d commit_ms=%d backend_pid=%d generation=%d",
		ErrLeaseLost, e.Stage, e.Reason, e.MutexWait.Milliseconds(), e.BeginDuration.Milliseconds(),
		e.UpdateDuration.Milliseconds(), e.CommitDuration.Milliseconds(), e.BackendPID, e.Generation)
	if e.SQLState != "" {
		diagnostic += fmt.Sprintf(" sqlstate=%s", e.SQLState)
	}
	return diagnostic + ")"
}

func (e *HeartbeatError) Unwrap() error { return ErrLeaseLost }

type Job struct {
	ID                string
	Kind              string
	Payload           json.RawMessage
	Result            json.RawMessage
	QueuePosition     int64
	State             string
	OwnerGeneration   *int64
	AttemptGeneration int64
	CreatedAt         time.Time
	StartedAt         *time.Time
	FinishedAt        *time.Time
	ErrorCode         *string
	ScheduleKey       *string
	ScheduleRevision  *int64
}

type LeaseInfo struct {
	OwnerID    string
	Generation int64
	BackendPID int32
}

type Store struct {
	Pool       *pgxpool.Pool
	leaseConn  *pgx.Conn
	leaseMu    sync.Mutex
	ownerID    string
	generation int64
	backendPID int32
	closed     bool
}

func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	config.MaxConns = maxConns
	config.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, errors.New("database pool unavailable")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("database unavailable")
	}
	return &Store{Pool: pool}, nil
}

func (s *Store) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = s.Release(ctx)
	s.Pool.Close()
}

func rollbackTx(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func closeConn(conn *pgx.Conn) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Close(ctx)
}

func (s *Store) Ping(ctx context.Context) error {
	if err := s.Pool.Ping(ctx); err != nil {
		return errors.New("database unavailable")
	}
	return nil
}

func (s *Store) DatabaseNow(ctx context.Context) (time.Time, error) {
	var now time.Time
	if err := s.Pool.QueryRow(ctx, `SELECT clock_timestamp() AT TIME ZONE 'UTC'`).Scan(&now); err != nil {
		return time.Time{}, errors.New("database clock unavailable")
	}
	return now.UTC(), nil
}

func (s *Store) CheckSchema(ctx context.Context) error {
	var ok bool
	err := s.Pool.QueryRow(ctx, `SELECT
		to_regclass('background_jobs') IS NOT NULL
		AND to_regclass('background_worker_status') IS NOT NULL
		AND to_regclass('background_job_executions') IS NOT NULL`).Scan(&ok)
	if err != nil || !ok {
		return errors.New("worker schema is unavailable")
	}
	_, err = s.Pool.Exec(ctx, `SELECT id,kind,payload,result,queue_position,state,owner_generation,
		attempt_generation,created_at,started_at,finished_at,error_code,schedule_key,schedule_revision
		FROM background_jobs LIMIT 0`)
	if err != nil {
		return errors.New("worker job schema is incompatible")
	}
	_, err = s.Pool.Exec(ctx, `SELECT id,generation,owner_id,lock_backend_pid,heartbeat_at,status
		FROM background_worker_status LIMIT 0`)
	if err != nil {
		return errors.New("worker status schema is incompatible")
	}
	_, err = s.Pool.Exec(ctx, `SELECT job_id,attempt_generation,executor_instance_id,owner_id,
		worker_generation,deadline_at,state,result,error_code,legacy_completion_applied_at
		FROM background_job_executions LIMIT 0`)
	if err != nil {
		return errors.New("worker schema is incompatible")
	}
	return nil
}

func (s *Store) CheckWorkerHealth(ctx context.Context) error {
	var ownerID *string
	var generation int64
	var backendPID *int32
	var heartbeat *time.Time
	var status string
	err := s.Pool.QueryRow(ctx, `SELECT owner_id::text,generation,lock_backend_pid,heartbeat_at,status
		FROM background_worker_status WHERE id=1`).Scan(&ownerID, &generation, &backendPID, &heartbeat, &status)
	if err != nil || ownerID == nil || backendPID == nil || heartbeat == nil || generation <= 0 || status != "running" {
		return errors.New("worker lease is not healthy")
	}
	var held bool
	err = s.Pool.QueryRow(ctx, `SELECT (clock_timestamp() AT TIME ZONE 'UTC') - $1 <= interval '20 seconds'
		AND $1 <= (clock_timestamp() AT TIME ZONE 'UTC')
		AND EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$2 AND objsubid=2
		AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid)
		AND objid=(((hashtext(current_schema())::bigint+4294967296)%4294967296)::oid)
		AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, *heartbeat, *backendPID).Scan(&held)
	if err != nil || !held {
		return errors.New("worker advisory lease is not healthy")
	}
	return nil
}

func (s *Store) Acquire(ctx context.Context) (LeaseInfo, error) {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if s.leaseConn != nil || s.closed {
		return LeaseInfo{}, errors.New("worker lease already initialized")
	}
	connConfig := s.Pool.Config().ConnConfig.Copy()
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return LeaseInfo{}, errors.New("worker lease connection unavailable")
	}
	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(current_schema()))`).Scan(&acquired); err != nil {
		closeConn(conn)
		return LeaseInfo{}, errors.New("worker lease lock unavailable")
	}
	if !acquired {
		closeConn(conn)
		return LeaseInfo{}, ErrLeaseHeld
	}
	var pid int32
	if err := conn.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		closeConn(conn)
		return LeaseInfo{}, errors.New("worker lease identity unavailable")
	}
	ownerID, err := newUUID()
	if err != nil {
		closeConn(conn)
		return LeaseInfo{}, errors.New("worker owner identity unavailable")
	}
	tx, err := conn.Begin(ctx)
	if err == nil {
		var generation int64
		generation, err = updateOwner(ctx, tx, ownerID, pid)
		if err == nil {
			err = tx.Commit(ctx)
		} else {
			rollbackTx(tx)
		}
		if err == nil {
			s.leaseConn, s.ownerID, s.generation, s.backendPID = conn, ownerID, generation, pid
			return LeaseInfo{OwnerID: ownerID, Generation: generation, BackendPID: pid}, nil
		}
	}
	unlockCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(hashtext(current_database()), hashtext(current_schema()))`)
	cancel()
	closeConn(conn)
	return LeaseInfo{}, errors.New("worker lease status update failed")
}

func updateOwner(ctx context.Context, tx pgx.Tx, ownerID string, pid int32) (int64, error) {
	var generation int64
	err := tx.QueryRow(ctx, `UPDATE background_worker_status
		SET generation=generation+1, owner_id=$1, lock_backend_pid=$2,
		    heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC', status='running'
		WHERE id=1 RETURNING generation`, ownerID, pid).Scan(&generation)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, errors.New("worker status row is missing")
	}
	return generation, err
}

func (s *Store) Info() LeaseInfo {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	return LeaseInfo{OwnerID: s.ownerID, Generation: s.generation, BackendPID: s.backendPID}
}

func (s *Store) Heartbeat(ctx context.Context) error {
	return s.HeartbeatWithFailure(ctx, nil)
}

// HeartbeatWithFailure signals lease loss before cleanup. The callback must not
// block or call back into the store; it runs while the lease mutex is held.
func (s *Store) HeartbeatWithFailure(ctx context.Context, onFailure func()) error {
	lockStarted := time.Now()
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	mutexWait := time.Since(lockStarted)
	if s.leaseConn == nil || s.closed {
		if onFailure != nil {
			onFailure()
		}
		return ErrLeaseLost
	}
	fail := func() {
		s.closed = true
		if onFailure != nil {
			onFailure()
		}
	}
	beginStarted := time.Now()
	tx, err := s.leaseConn.Begin(ctx)
	beginDuration := time.Since(beginStarted)
	if err != nil {
		fail()
		return s.failHeartbeatLocked("begin", err, mutexWait, beginDuration, 0, 0)
	}
	updateStarted := time.Now()
	command, err := tx.Exec(ctx, `UPDATE background_worker_status
		SET heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC'
		WHERE id=1 AND owner_id=$1 AND generation=$2 AND lock_backend_pid=$3 AND status='running'`,
		s.ownerID, s.generation, s.backendPID)
	updateDuration := time.Since(updateStarted)
	if err != nil {
		fail()
		rollbackTx(tx)
		return s.failHeartbeatLocked("update", err, mutexWait, beginDuration, updateDuration, 0)
	}
	if command.RowsAffected() != 1 {
		fail()
		rollbackTx(tx)
		identity := s.heartbeatIdentityLocked()
		s.invalidateLeaseLocked()
		return &HeartbeatError{Stage: "update", Reason: "lease_mismatch", BackendPID: identity.BackendPID, Generation: identity.Generation, MutexWait: mutexWait,
			BeginDuration: beginDuration, UpdateDuration: updateDuration}
	}
	commitStarted := time.Now()
	err = tx.Commit(ctx)
	commitDuration := time.Since(commitStarted)
	if err != nil {
		fail()
		rollbackTx(tx)
		return s.failHeartbeatLocked("commit", err, mutexWait, beginDuration, updateDuration, commitDuration)
	}
	return nil
}

func (s *Store) failHeartbeatLocked(stage string, err error, mutexWait, beginDuration, updateDuration, commitDuration time.Duration) error {
	identity := s.heartbeatIdentityLocked()
	s.invalidateLeaseLocked()
	diagnostic := heartbeatError(stage, err)
	diagnostic.BackendPID = identity.BackendPID
	diagnostic.Generation = identity.Generation
	diagnostic.MutexWait = mutexWait
	diagnostic.BeginDuration = beginDuration
	diagnostic.UpdateDuration = updateDuration
	diagnostic.CommitDuration = commitDuration
	return diagnostic
}

func (s *Store) heartbeatIdentityLocked() LeaseInfo {
	return LeaseInfo{BackendPID: s.backendPID, Generation: s.generation}
}

func heartbeatError(stage string, err error) *HeartbeatError {
	diagnostic := &HeartbeatError{Stage: stage, Reason: "other"}
	if errors.Is(err, context.DeadlineExceeded) {
		diagnostic.Reason = "timeout"
	} else if errors.Is(err, context.Canceled) {
		diagnostic.Reason = "canceled"
	} else if errors.Is(err, pgx.ErrTxClosed) {
		diagnostic.Reason = "transaction_closed"
	} else if errors.Is(err, pgconn.ErrConnClosed) {
		diagnostic.Reason = "connection_closed"
	}
	diagnostic.SQLState = safePgError(err)
	if len(diagnostic.SQLState) != 5 || !isSQLState(diagnostic.SQLState) {
		diagnostic.SQLState = ""
	}
	if diagnostic.SQLState != "" {
		diagnostic.Reason = "sqlstate"
	}
	return diagnostic
}

func isSQLState(code string) bool {
	for _, char := range code {
		if (char < '0' || char > '9') && (char < 'A' || char > 'Z') {
			return false
		}
	}
	return true
}

func (s *Store) Release(_ context.Context) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if s.leaseConn == nil {
		return nil
	}
	conn := s.leaseConn
	s.leaseConn = nil
	var releaseErr error
	tx, err := conn.Begin(ctx)
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE background_worker_status
			SET owner_id=NULL, lock_backend_pid=NULL, heartbeat_at=NULL, status='stopped'
			WHERE id=1 AND owner_id=$1 AND generation=$2`, s.ownerID, s.generation)
		if err == nil {
			err = tx.Commit(ctx)
		} else {
			rollbackTx(tx)
		}
	}
	if err != nil {
		releaseErr = errors.New("worker lease release failed")
		closeConn(conn)
	} else {
		var unlocked bool
		if unlockErr := conn.QueryRow(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()), hashtext(current_schema()))`).Scan(&unlocked); unlockErr != nil || !unlocked {
			releaseErr = errors.New("worker advisory lock release failed")
			closeConn(conn)
		} else {
			closeConn(conn)
		}
	}
	s.ownerID, s.generation, s.backendPID = "", 0, 0
	return releaseErr
}

func (s *Store) invalidateLeaseLocked() {
	s.closed = true
	if s.leaseConn != nil {
		closeConn(s.leaseConn)
		s.leaseConn = nil
	}
	s.ownerID, s.generation, s.backendPID = "", 0, 0
}

func (s *Store) Claim(ctx context.Context, maxAttempts int64) (Job, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Job{}, errors.New("database unavailable")
	}
	defer rollbackTx(tx)
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext(current_database()),
		hashtext(current_schema()) # 1145132611)`); err != nil {
		return Job{}, errors.New("job claim coordination failed")
	}
	info, err := s.lockCurrentLease(ctx, tx)
	if err != nil {
		return Job{}, err
	}
	var active string
	err = tx.QueryRow(ctx, `SELECT id::text FROM background_jobs WHERE state='running' LIMIT 1 FOR UPDATE`).Scan(&active)
	if err == nil {
		return Job{}, ErrNoJob
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Job{}, errors.New("active job check failed")
	}
	job, err := scanJob(tx.QueryRow(ctx, `SELECT id::text,kind,payload,result,queue_position,state,owner_generation,
		attempt_generation,created_at,started_at,finished_at,error_code,schedule_key,schedule_revision
		FROM background_jobs WHERE state='queued' OR (state='interrupted' AND attempt_generation < $1)
		ORDER BY queue_position LIMIT 1 FOR UPDATE SKIP LOCKED`, maxAttempts))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNoJob
	}
	if err != nil {
		return Job{}, errors.New("job claim failed")
	}
	err = tx.QueryRow(ctx, `UPDATE background_jobs SET state='running', started_at=clock_timestamp() AT TIME ZONE 'UTC',
		finished_at=NULL,error_code=NULL,owner_generation=$2,attempt_generation=attempt_generation+1
		WHERE id=$1 RETURNING attempt_generation`, job.ID, info.Generation).Scan(&job.AttemptGeneration)
	if err != nil {
		return Job{}, errors.New("job claim update failed")
	}
	job.State = "running"
	job.OwnerGeneration = &info.Generation
	job.StartedAt = nil
	job.FinishedAt = nil
	job.ErrorCode = nil
	if err := tx.Commit(ctx); err != nil {
		return Job{}, errors.New("job claim commit failed")
	}
	return job, nil
}

func (s *Store) ActiveJobs(ctx context.Context) ([]Job, error) {
	rows, err := s.Pool.Query(ctx, `SELECT id::text,kind,payload,result,queue_position,state,owner_generation,
		attempt_generation,created_at,started_at,finished_at,error_code,schedule_key,schedule_revision
		FROM background_jobs WHERE state IN ('running','interrupted') ORDER BY queue_position`)
	if err != nil {
		return nil, errors.New("active job query failed")
	}
	defer rows.Close()
	var result []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, errors.New("active job read failed")
		}
		result = append(result, job)
	}
	return result, rows.Err()
}

func (s *Store) TerminalJobsNeedingEvents(ctx context.Context, limit int) ([]Job, error) {
	rows, err := s.Pool.Query(ctx, `SELECT j.id::text,j.kind,j.payload,j.result,j.queue_position,j.state,
		j.owner_generation,j.attempt_generation,j.created_at,j.started_at,j.finished_at,j.error_code,
		j.schedule_key,j.schedule_revision
		FROM background_jobs j JOIN background_job_executions e
		ON e.job_id=j.id AND e.attempt_generation=j.attempt_generation
		WHERE j.state IN ('succeeded','failed','delivery_unknown')
		AND e.legacy_completion_applied_at IS NULL
		ORDER BY j.finished_at LIMIT $1`, limit)
	if err != nil {
		return nil, errors.New("legacy completion scan failed")
	}
	defer rows.Close()
	var result []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, errors.New("legacy completion read failed")
		}
		result = append(result, job)
	}
	return result, rows.Err()
}

func (s *Store) Finish(ctx context.Context, job Job, state string, result map[string]any, errorCode *string) error {
	return s.finish(ctx, job, state, result, errorCode, false)
}

func (s *Store) FinishRecovered(ctx context.Context, job Job, state string, result map[string]any, errorCode *string) error {
	return s.finish(ctx, job, state, result, errorCode, true)
}

func (s *Store) finish(ctx context.Context, job Job, state string, result map[string]any, errorCode *string, recovered bool) error {
	if state != "succeeded" && state != "failed" && state != "interrupted" && state != "delivery_unknown" {
		return errors.New("invalid terminal job state")
	}
	if result == nil {
		result = map[string]any{"status": "warning"}
	}
	body, err := json.Marshal(result)
	if err != nil || len(body) > 4096 {
		return errors.New("job result exceeds its safe bound")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("job finish transaction unavailable")
	}
	defer rollbackTx(tx)
	info, err := s.lockCurrentLease(ctx, tx)
	if err != nil {
		return err
	}
	var ownerGeneration *int64
	var attempt int64
	var currentState string
	err = tx.QueryRow(ctx, `SELECT owner_generation,attempt_generation,state FROM background_jobs WHERE id=$1 FOR UPDATE`, job.ID).Scan(&ownerGeneration, &attempt, &currentState)
	validCurrentState := currentState == "running" || (recovered && currentState == "interrupted")
	if errors.Is(err, pgx.ErrNoRows) || attempt != job.AttemptGeneration || !validCurrentState || (!recovered && (ownerGeneration == nil || *ownerGeneration != info.Generation)) {
		return ErrLeaseLost
	}
	_, err = tx.Exec(ctx, `UPDATE background_jobs SET state=$2,result=$3::jsonb,error_code=$4,
		finished_at=clock_timestamp() AT TIME ZONE 'UTC', owner_generation=$5 WHERE id=$1`,
		job.ID, state, string(body), errorCode, info.Generation)
	if err != nil {
		return errors.New("job finish write failed")
	}
	if state != "interrupted" {
		if err := insertEventReceipt(ctx, tx, job, state, body, errorCode, info); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return errors.New("job finish commit failed")
	}
	return nil
}

func insertEventReceipt(ctx context.Context, tx pgx.Tx, job Job, state string, result []byte, errorCode *string, info LeaseInfo) error {
	if job.AttemptGeneration < 0 {
		return errors.New("invalid job attempt generation")
	}
	instanceID, err := newUUID()
	if err != nil {
		return errors.New("completion receipt identity unavailable")
	}
	marker := sha256.Sum256([]byte(fmt.Sprintf("go-finalized:%s:%d", job.ID, job.AttemptGeneration)))
	_, err = tx.Exec(ctx, `INSERT INTO background_job_executions
		(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,
		 state,result,error_code,finished_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,clock_timestamp() AT TIME ZONE 'UTC')
		ON CONFLICT (job_id,attempt_generation) DO NOTHING`,
		job.ID, job.AttemptGeneration, fmt.Sprintf("%x", marker), instanceID,
		info.OwnerID, info.Generation, state, string(result), errorCode)
	if err != nil {
		return errors.New("completion receipt write failed")
	}
	return nil
}

func (s *Store) MarkInterrupted(ctx context.Context, job Job, state, errorCode string) error {
	return s.Finish(ctx, job, state, map[string]any{"status": "warning"}, &errorCode)
}

func (s *Store) Prune(ctx context.Context, retention time.Duration, batch int) (int64, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, errors.New("retention transaction unavailable")
	}
	defer rollbackTx(tx)
	if _, err := s.lockCurrentLease(ctx, tx); err != nil {
		return 0, err
	}
	result, err := tx.Exec(ctx, `DELETE FROM background_jobs WHERE id IN (
		SELECT id FROM background_jobs WHERE state IN ('succeeded','failed')
		AND finished_at < (clock_timestamp() AT TIME ZONE 'UTC') - $1::interval
		AND NOT EXISTS (SELECT 1 FROM background_job_executions e
			WHERE e.job_id=background_jobs.id AND e.attempt_generation=background_jobs.attempt_generation
			AND e.legacy_completion_applied_at IS NULL)
		ORDER BY finished_at LIMIT $2 FOR UPDATE SKIP LOCKED)`,
		fmt.Sprintf("%f seconds", retention.Seconds()), batch)
	if err != nil {
		return 0, errors.New("job retention failed")
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, errors.New("job retention commit failed")
	}
	return result.RowsAffected(), nil
}

func (s *Store) FailExhaustedInterrupted(ctx context.Context, maxAttempts int64, batch int) (int64, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, errors.New("recovery transaction unavailable")
	}
	defer rollbackTx(tx)
	info, err := s.lockCurrentLease(ctx, tx)
	if err != nil {
		return 0, err
	}
	rows, err := tx.Query(ctx, `SELECT id::text,kind,payload,result,queue_position,state,owner_generation,
		attempt_generation,created_at,started_at,finished_at,error_code,schedule_key,schedule_revision
		FROM background_jobs WHERE state='interrupted' AND attempt_generation >= $1
		ORDER BY queue_position LIMIT $2 FOR UPDATE SKIP LOCKED`, maxAttempts, batch)
	if err != nil {
		return 0, errors.New("exhausted job scan failed")
	}
	var jobs []Job
	for rows.Next() {
		job, scanErr := scanJob(rows)
		if scanErr != nil {
			rows.Close()
			return 0, errors.New("exhausted job read failed")
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, errors.New("exhausted job read failed")
	}
	rows.Close()
	code := "worker_recovery_exhausted"
	result := []byte(`{"status":"incomplete"}`)
	for _, job := range jobs {
		if _, err := tx.Exec(ctx, `UPDATE background_jobs SET state='failed',result=$2::jsonb,error_code=$3,
			finished_at=clock_timestamp() AT TIME ZONE 'UTC',owner_generation=$4 WHERE id=$1`,
			job.ID, string(result), code, info.Generation); err != nil {
			return 0, errors.New("exhausted job update failed")
		}
		if err := insertEventReceipt(ctx, tx, job, "failed", result, &code, info); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, errors.New("recovery transaction commit failed")
	}
	return int64(len(jobs)), nil
}

func (s *Store) lockCurrentLease(ctx context.Context, tx pgx.Tx) (LeaseInfo, error) {
	info := s.Info()
	if info.OwnerID == "" || info.Generation <= 0 || info.BackendPID <= 0 {
		return LeaseInfo{}, ErrLeaseLost
	}
	var status string
	var heartbeat time.Time
	var ownerID *string
	var generation int64
	var backendPID *int32
	var heartbeatAt *time.Time
	err := tx.QueryRow(ctx, `SELECT owner_id::text,generation,lock_backend_pid,status,heartbeat_at
		FROM background_worker_status WHERE id=1 FOR UPDATE`).Scan(&ownerID, &generation, &backendPID, &status, &heartbeatAt)
	if err != nil || ownerID == nil || backendPID == nil || heartbeatAt == nil ||
		*ownerID != info.OwnerID || generation != info.Generation || *backendPID != info.BackendPID || status != "running" {
		return LeaseInfo{}, ErrLeaseLost
	}
	heartbeat = *heartbeatAt
	var databaseNow time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp() AT TIME ZONE 'UTC'`).Scan(&databaseNow); err != nil {
		return LeaseInfo{}, ErrLeaseLost
	}
	if heartbeat.After(databaseNow) || databaseNow.Sub(heartbeat) > 20*time.Second {
		return LeaseInfo{}, ErrLeaseLost
	}
	var held bool
	err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted
		AND pid=$1 AND objsubid=2
		AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid)
		AND objid=(((hashtext(current_schema())::bigint+4294967296)%4294967296)::oid)
		AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, info.BackendPID).Scan(&held)
	if err != nil || !held {
		return LeaseInfo{}, ErrLeaseLost
	}
	return info, nil
}

type rowScanner interface{ Scan(dest ...any) error }

func scanJob(row rowScanner) (Job, error) {
	var job Job
	var payload, result []byte
	err := row.Scan(&job.ID, &job.Kind, &payload, &result, &job.QueuePosition, &job.State,
		&job.OwnerGeneration, &job.AttemptGeneration, &job.CreatedAt, &job.StartedAt,
		&job.FinishedAt, &job.ErrorCode, &job.ScheduleKey, &job.ScheduleRevision)
	if err != nil {
		return Job{}, err
	}
	job.Payload = append(json.RawMessage(nil), payload...)
	job.Result = append(json.RawMessage(nil), result...)
	return job, nil
}

func newUUID() (string, error) {
	var bytes [16]byte
	if _, err := rand.Read(bytes[:]); err != nil {
		return "", err
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	encoded := hex.EncodeToString(bytes[:])
	return encoded[:8] + "-" + encoded[8:12] + "-" + encoded[12:16] + "-" + encoded[16:20] + "-" + encoded[20:], nil
}

func safePgError(err error) string {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return pgErr.Code
	}
	return ""
}
