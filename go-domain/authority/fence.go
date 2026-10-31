// Package authority checks the persisted Go execution lease in the same
// transaction as a domain mutation. It never opens an additional connection.
package authority

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrStale = errors.New("stale_fence")

type Fence struct {
	Pool                                                                *pgxpool.Pool
	JobID, OwnerID, ExecutorInstanceID, Kind                            string
	AttemptGeneration, Generation, WorkerBackendPID, ExecutorBackendPID int
	Deadline                                                            time.Time
	Payload                                                             json.RawMessage
}

// Validate locks the worker, job and receipt in the same order as the Python
// fence, until the caller commits. The expected fields must come from the
// independently authenticated execution authority, not from an untrusted caller.
func (f Fence) Validate(ctx context.Context, tx pgx.Tx) error {
	if f.JobID == "" || f.OwnerID == "" || f.ExecutorInstanceID == "" || f.AttemptGeneration <= 0 || f.Generation <= 0 || f.WorkerBackendPID <= 0 || f.ExecutorBackendPID <= 0 || !json.Valid(f.Payload) {
		return ErrStale
	}
	var valid bool
	err := tx.QueryRow(ctx, `SELECT owner_id=$1::uuid AND generation=$2 AND status='running' AND lock_backend_pid=$3
 AND heartbeat_at IS NOT NULL AND heartbeat_at <= (clock_timestamp() AT TIME ZONE 'UTC')
 AND heartbeat_at >= (clock_timestamp() AT TIME ZONE 'UTC') - interval '20 seconds'
 FROM background_worker_status WHERE id=1 FOR UPDATE`, f.OwnerID, f.Generation, f.WorkerBackendPID).Scan(&valid)
	if err != nil || !valid {
		return ErrStale
	}
	err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2
 AND classid=(((hashtext(current_database())::bigint + 4294967296) % 4294967296)::oid)
 AND objid=((((hashtext(current_schema()) # $2::integer)::bigint + 4294967296) % 4294967296)::oid)
 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, f.WorkerBackendPID, 0).Scan(&valid)
	if err != nil || !valid {
		return ErrStale
	}
	err = tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=$1 AND objsubid=2
 AND classid=(((hashtext(current_database())::bigint + 4294967296) % 4294967296)::oid)
 AND objid=((((hashtext(current_schema()) # $2::integer)::bigint + 4294967296) % 4294967296)::oid)
 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, f.ExecutorBackendPID, 0x454E4732).Scan(&valid)
	if err != nil || !valid {
		return ErrStale
	}
	err = tx.QueryRow(ctx, `SELECT state='running' AND owner_generation=$2 AND attempt_generation=$3 AND kind=$4 AND payload=$5::jsonb
 FROM background_jobs WHERE id=$1::uuid FOR UPDATE`, f.JobID, f.Generation, f.AttemptGeneration, f.Kind, []byte(f.Payload)).Scan(&valid)
	if err != nil || !valid {
		return ErrStale
	}
	err = tx.QueryRow(ctx, `SELECT executor_instance_id=$3::uuid AND owner_id=$4::uuid AND worker_generation=$5 AND deadline_at=$6
 AND state='running' AND deadline_at > (clock_timestamp() AT TIME ZONE 'UTC')
 FROM background_job_executions WHERE job_id=$1::uuid AND attempt_generation=$2 FOR UPDATE`, f.JobID, f.AttemptGeneration, f.ExecutorInstanceID, f.OwnerID, f.Generation, f.Deadline.UTC().Truncate(time.Microsecond)).Scan(&valid)
	if err != nil || !valid {
		return ErrStale
	}
	return nil
}
func (f Fence) Check(ctx context.Context) error {
	tx, err := f.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = f.Validate(ctx, tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ViberFenceIdentity ties Viber receipt mutations to this authenticated job.
func (f Fence) ViberFenceIdentity() (string, int, string, json.RawMessage) {
	return f.JobID, f.AttemptGeneration, f.Kind, f.Payload
}
