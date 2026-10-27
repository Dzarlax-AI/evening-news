package execution

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"io"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

func (s *Server) acceptAttempt(ctx context.Context, e Envelope, digest string) (receipt Receipt, accepted bool, err error) {
	qctx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()
	tx, err := s.pool.BeginTx(qctx, pgx.TxOptions{})
	if err != nil {
		return receipt, false, err
	}
	defer tx.Rollback(context.Background())
	// Serialize concurrent duplicate submissions at the worker row and then re-read the receipt.
	var owner *string
	var generation int64
	var backend *int32
	var heartbeat *time.Time
	var status string
	if err = tx.QueryRow(qctx, `SELECT owner_id::text,generation,lock_backend_pid,heartbeat_at,status FROM background_worker_status WHERE id=1 FOR UPDATE`).Scan(&owner, &generation, &backend, &heartbeat, &status); err != nil {
		return receipt, false, ErrLeaseLost
	}
	if err = s.loadReceiptTx(qctx, tx, attemptKey{e.JobID, e.Attempt}, &receipt); err == nil {
		if receipt.Digest != digest {
			return receipt, false, ErrConflict
		}
		return receipt, false, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return receipt, false, err
	}
	if owner == nil || backend == nil || heartbeat == nil || status != "running" || *owner != e.OwnerID.String() || generation != e.Generation {
		return receipt, false, ErrLeaseLost
	}
	var now time.Time
	if err = tx.QueryRow(qctx, `SELECT clock_timestamp() AT TIME ZONE 'UTC'`).Scan(&now); err != nil {
		return receipt, false, err
	}
	if heartbeat.After(now) || now.Sub(*heartbeat) > HeartbeatStale {
		return receipt, false, ErrLeaseLost
	}
	if !e.Deadline.After(now) || e.Deadline.After(now.Add(MaxDeadline)) {
		return receipt, false, ErrDeadline
	}
	var kind string
	var payload []byte
	var state string
	var ownerGen *int64
	var attempt int64
	err = tx.QueryRow(qctx, `SELECT kind,payload::text,state,owner_generation,attempt_generation FROM background_jobs WHERE id=$1 FOR UPDATE`, e.JobID).Scan(&kind, &payload, &state, &ownerGen, &attempt)
	if err != nil || state != "running" || ownerGen == nil || *ownerGen != e.Generation || attempt != e.Attempt {
		return receipt, false, ErrStaleAttempt
	}
	validated, e2 := validatePayload(kind, json.RawMessage(payload))
	if e2 != nil {
		return receipt, false, ErrInvalidPayload
	}
	payload = validated
	if err = s.checkWorkerLeaseTx(qctx, tx, *owner, generation, *backend); err != nil {
		return receipt, false, err
	}
	if err = s.checkExecutionGuardTx(qctx, tx); err != nil {
		return receipt, false, err
	}
	if !e.Deadline.After(now) || e.Deadline.After(now.Add(MaxDeadline)) {
		return receipt, false, ErrDeadline
	}
	_, err = tx.Exec(qctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state,accepted_at) VALUES($1,$2,$3,$4,$5,$6,$7,'accepted',clock_timestamp() AT TIME ZONE 'UTC')`, e.JobID, e.Attempt, digest, s.instance, e.OwnerID, generation, e.Deadline.UTC())
	if err != nil {
		return receipt, false, err
	}
	if err = tx.Commit(qctx); err != nil {
		return receipt, false, err
	}
	ownerID, generationCopy, deadlineCopy := e.OwnerID.String(), generation, e.DeadlineRaw
	receipt = Receipt{JobID: e.JobID.String(), Attempt: e.Attempt, OwnerID: &ownerID, Generation: &generationCopy, DeadlineAt: &deadlineCopy, Instance: s.instance.String(), State: "accepted", Digest: digest, Kind: kind, Payload: payload}
	return receipt, true, nil
}

var (
	ErrConflict       = errors.New("receipt conflict")
	ErrDeadline       = errors.New("deadline outside allowed window")
	ErrInvalidPayload = errors.New("stored job is invalid")
	ErrBusy           = errors.New("executor busy")
)

func (s *Server) checkWorkerLeaseTx(ctx context.Context, tx pgx.Tx, owner string, generation int64, pid int32) error {
	// FOR UPDATE is the authority linearization point shared with worker
	// takeover: an old-generation write that already holds this row completes
	// before replacement, and writes starting after replacement observe the new
	// generation. Releasing the separate executor advisory session cannot
	// retroactively abort a transaction that already passed this fence.
	var held bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=(((hashtext(current_schema())::bigint+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, pid).Scan(&held)
	if err != nil {
		logExecutionFailure("worker_lease_fence", "advisory_lock_lookup_failed", err)
		return ErrLeaseLost
	}
	if !held {
		logExecutionFailure("worker_lease_fence", "worker_advisory_lock_not_held", nil)
		return ErrLeaseLost
	}
	var currentOwner *string
	var currentGeneration int64
	var currentPID *int32
	var beat *time.Time
	var st string
	err = tx.QueryRow(ctx, `SELECT owner_id::text,generation,lock_backend_pid,heartbeat_at,status FROM background_worker_status WHERE id=1 FOR UPDATE`).Scan(&currentOwner, &currentGeneration, &currentPID, &beat, &st)
	if err != nil {
		logExecutionFailure("worker_lease_fence", "worker_status_read_failed", err)
		return ErrLeaseLost
	}
	if currentOwner == nil || currentPID == nil || beat == nil || *currentOwner != owner || currentGeneration != generation || *currentPID != pid || st != "running" {
		logExecutionFailure("worker_lease_fence", "worker_status_mismatch", nil)
		return ErrLeaseLost
	}
	var now time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp() AT TIME ZONE 'UTC'`).Scan(&now); err != nil {
		logExecutionFailure("worker_lease_fence", "database_clock_read_failed", err)
		return ErrLeaseLost
	}
	if beat.After(now) || now.Sub(*beat) > HeartbeatStale {
		logExecutionFailure("worker_lease_fence", "worker_heartbeat_stale", nil)
		return ErrLeaseLost
	}
	return nil
}

func (s *Server) checkExecutionGuardTx(ctx context.Context, tx pgx.Tx) error {
	if s.guardPID <= 0 {
		return ErrNotReady
	}
	var held bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=((((hashtext(current_schema())::bigint # $2)+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, s.guardPID, ExecutionGuardXOR).Scan(&held)
	if err != nil {
		logExecutionFailure("execution_guard_fence", "advisory_lock_lookup_failed", err)
		return ErrNotReady
	}
	if !held {
		logExecutionFailure("execution_guard_fence", "execution_guard_not_held", nil)
		return ErrNotReady
	}
	return nil
}

func (s *Server) checkExecutionGuardPool(ctx context.Context) error {
	if s.guardPID <= 0 {
		return ErrNotReady
	}
	var held bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=((((hashtext(current_schema())::bigint # $2)+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, s.guardPID, ExecutionGuardXOR).Scan(&held)
	if err != nil || !held {
		return ErrNotReady
	}
	return nil
}

type Envelope struct {
	Version             int
	JobID, OwnerID      uuid.UUID
	Generation, Attempt int64
	Deadline            time.Time
	DeadlineRaw         string
	Digest              string
}

func parseEnvelope(raw []byte) (Envelope, string, error) {
	var e Envelope
	if len(raw) > MaxEnvelopeBytes || !utf8.Valid(raw) {
		return e, "", ErrInvalidPayload
	}
	if err := rejectDuplicateKeys(raw); err != nil {
		return e, "", err
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var m map[string]any
	if err := dec.Decode(&m); err != nil || m == nil {
		return e, "", ErrInvalidPayload
	}
	if dec.Decode(new(any)) != io.EOF {
		return e, "", ErrInvalidPayload
	}
	required := map[string]bool{"version": true, "job_id": true, "owner_id": true, "generation": true, "attempt_generation": true, "deadline_at": true}
	if len(m) != len(required) {
		return e, "", ErrInvalidPayload
	}
	for k := range m {
		if !required[k] {
			return e, "", ErrInvalidPayload
		}
	}
	if n, ok := m["version"].(json.Number); !ok || n.String() != "1" {
		return e, "", ErrInvalidPayload
	}
	e.Version = 1
	var err error
	e.JobID, err = canonicalUUIDValue(m["job_id"])
	if err != nil {
		return e, "", err
	}
	e.OwnerID, err = canonicalUUIDValue(m["owner_id"])
	if err != nil {
		return e, "", err
	}
	e.Generation, err = positiveInt(m["generation"])
	if err != nil {
		return e, "", err
	}
	e.Attempt, err = positiveInt(m["attempt_generation"])
	if err != nil {
		return e, "", err
	}
	var okString bool
	e.DeadlineRaw, okString = m["deadline_at"].(string)
	if !okString {
		return e, "", ErrInvalidPayload
	}
	e.Deadline, err = time.Parse(time.RFC3339Nano, e.DeadlineRaw)
	if err != nil {
		return e, "", ErrInvalidPayload
	}
	canonical := map[string]any{"version": 1, "job_id": e.JobID.String(), "owner_id": e.OwnerID.String(), "generation": e.Generation, "attempt_generation": e.Attempt, "deadline_at": e.DeadlineRaw}
	b, _ := json.Marshal(canonical)
	e.Digest = fmt.Sprintf("%x", sha256.Sum256(b))
	return e, e.Digest, nil
}

func canonicalUUIDValue(v any) (uuid.UUID, error) {
	s, ok := v.(string)
	if !ok {
		return uuid.Nil, ErrInvalidPayload
	}
	u, err := uuid.Parse(s)
	if err != nil || u.String() != s {
		return uuid.Nil, ErrInvalidPayload
	}
	return u, nil
}
func positiveInt(v any) (int64, error) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, ErrInvalidPayload
	}
	i, e := strconv.ParseInt(n.String(), 10, 64)
	if e != nil || i < 1 || i > 2147483647 || strings.ContainsAny(n.String(), ".eE+") {
		return 0, ErrInvalidPayload
	}
	return i, nil
}

func rejectDuplicateKeys(raw []byte) error {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var parse func() error
	parse = func() error {
		t, e := d.Token()
		if e != nil {
			return e
		}
		switch x := t.(type) {
		case json.Delim:
			if x == '{' {
				seen := map[string]bool{}
				for d.More() {
					k, e := d.Token()
					if e != nil {
						return e
					}
					key, ok := k.(string)
					if !ok || seen[key] {
						return ErrInvalidPayload
					}
					seen[key] = true
					if e = parse(); e != nil {
						return e
					}
				}
				_, e = d.Token()
				return e
			}
			if x == '[' {
				for d.More() {
					if e := parse(); e != nil {
						return e
					}
				}
				_, e := d.Token()
				return e
			}
		}
		return nil
	}
	if e := parse(); e != nil {
		return e
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrInvalidPayload
	}
	return nil
}

func (s *Server) loadReceipt(ctx context.Context, key attemptKey) (Receipt, error) {
	qctx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()
	var rec Receipt
	err := s.loadReceiptPool(qctx, key, &rec)
	return rec, err
}
func (s *Server) loadReceiptPool(ctx context.Context, key attemptKey, r *Receipt) error {
	return scanReceipt(s.pool.QueryRow(ctx, receiptSQL, key.job, key.attempt), r)
}
func (s *Server) loadReceiptTx(ctx context.Context, tx pgx.Tx, key attemptKey, r *Receipt) error {
	return scanReceipt(tx.QueryRow(ctx, receiptSQL, key.job, key.attempt), r)
}

const receiptSQL = `SELECT job_id::text,attempt_generation,owner_id,worker_generation,to_char(deadline_at AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS.US')||'Z',executor_instance_id::text,state,to_char(accepted_at,'YYYY-MM-DD"T"HH24:MI:SS.US'),COALESCE(to_char(started_at,'YYYY-MM-DD"T"HH24:MI:SS.US'),''),COALESCE(to_char(finished_at,'YYYY-MM-DD"T"HH24:MI:SS.US'),''),COALESCE(result::text,'null'),error_code,envelope_sha256 FROM background_job_executions WHERE job_id=$1 AND attempt_generation=$2`

func scanReceipt(row interface{ Scan(...any) error }, r *Receipt) error {
	var owner, deadline *string
	var accepted, started, finished, instance, state, result, digest string
	var attempt int64
	var generation *int64
	var errorCode *string
	err := row.Scan(&r.JobID, &attempt, &owner, &generation, &deadline, &instance, &state, &accepted, &started, &finished, &result, &errorCode, &digest)
	if err != nil {
		return err
	}
	r.Attempt = attempt
	r.OwnerID = owner
	r.Generation = generation
	r.DeadlineAt = deadline
	r.Instance = instance
	r.State = state
	r.AcceptedAt = accepted
	r.StartedAt = nullable(started)
	r.FinishedAt = nullable(finished)
	r.Result = json.RawMessage(result)
	r.ErrorCode = errorCode
	r.Digest = digest
	return nil
}
func nullable(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

type Receipt struct {
	JobID      string          `json:"job_id"`
	Attempt    int64           `json:"attempt_generation"`
	OwnerID    *string         `json:"owner_id"`
	Generation *int64          `json:"generation"`
	DeadlineAt *string         `json:"deadline_at"`
	Instance   string          `json:"executor_instance_id"`
	State      string          `json:"state"`
	AcceptedAt string          `json:"accepted_at"`
	StartedAt  *string         `json:"started_at"`
	FinishedAt *string         `json:"finished_at"`
	Result     json.RawMessage `json:"result"`
	ErrorCode  *string         `json:"error_code"`
	Digest     string          `json:"-"`
	Kind       string          `json:"-"`
	Payload    json.RawMessage `json:"-"`
}

func (r Receipt) Map() map[string]any {
	return map[string]any{"job_id": r.JobID, "attempt_generation": r.Attempt, "owner_id": r.OwnerID, "generation": r.Generation, "deadline_at": r.DeadlineAt, "executor_instance_id": r.Instance, "state": r.State, "accepted_at": r.AcceptedAt, "started_at": r.StartedAt, "finished_at": r.FinishedAt, "result": r.Result, "error_code": r.ErrorCode}
}
