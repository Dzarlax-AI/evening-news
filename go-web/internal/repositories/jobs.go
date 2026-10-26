package repositories

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/jobs"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// JobRepository is the SQL-only durable acceptance/read adapter. It shares the
// application's bounded pgx pool; it owns no worker loop or network clients.
type JobRepository struct{ pool *pgxpool.Pool }

func NewJobRepository(pool *pgxpool.Pool) *JobRepository { return &JobRepository{pool: pool} }

const producerLock = `SELECT pg_advisory_xact_lock(hashtext(current_database()), hashtext(current_schema()) # 1145132610)`
const jobColumns = `id,kind,state,created_at,started_at,finished_at,result,error_code`

type scanner interface{ Scan(...any) error }

func scanJob(row scanner) (jobs.Job, error) {
	var out jobs.Job
	var raw []byte
	var errCode *string
	err := row.Scan(&out.ID, &out.Kind, &out.Status, &out.CreatedAt, &out.StartedAt, &out.FinishedAt, &raw, &errCode)
	if err != nil {
		return out, err
	}
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &out.Result); err != nil {
			return out, err
		}
	}
	out.ErrorCode = errCode
	return out, nil
}
func (r *JobRepository) Submit(ctx context.Context, cmd jobs.SubmitCommand, payload map[string]any, storedKey string) (jobs.SubmitResult, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return jobs.SubmitResult{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, producerLock); err != nil {
		return jobs.SubmitResult{}, err
	}
	result, err := enqueue(ctx, tx, cmd.Kind, payload, storedKey, true, nil)
	if err != nil {
		return jobs.SubmitResult{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return jobs.SubmitResult{}, err
	}
	return result, nil
}
func enqueue(ctx context.Context, tx pgx.Tx, kind string, payload map[string]any, storedKey string, coalesce bool, metadata *jobs.OccurrenceMetadata) (jobs.SubmitResult, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return jobs.SubmitResult{}, err
	}
	var existingID string
	var existingKind string
	var existingPayload []byte
	var existingKey *string
	var existingState string
	var existingScheduleKey *string
	var existingRevision *int
	var existingOccurrence *string
	var existingDue *time.Time
	find := `SELECT j.id,j.kind,j.payload,j.idempotency_key,j.state,j.schedule_key,j.schedule_revision,j.occurrence_id,j.scheduled_for FROM background_jobs j JOIN background_job_idempotency_keys k ON k.job_id=j.id WHERE k.key=$1 FOR UPDATE`
	err = tx.QueryRow(ctx, find, storedKey).Scan(&existingID, &existingKind, &existingPayload, &existingKey, &existingState, &existingScheduleKey, &existingRevision, &existingOccurrence, &existingDue)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `SELECT id,kind,payload,idempotency_key,state,schedule_key,schedule_revision,occurrence_id,scheduled_for FROM background_jobs WHERE idempotency_key=$1 FOR UPDATE`, storedKey).Scan(&existingID, &existingKind, &existingPayload, &existingKey, &existingState, &existingScheduleKey, &existingRevision, &existingOccurrence, &existingDue)
	}
	if err == nil {
		return existingJob(ctx, tx, existingID, kind, encoded, storedKey, metadata)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return jobs.SubmitResult{}, err
	}
	if metadata != nil {
		err = tx.QueryRow(ctx, `SELECT id,kind,payload,idempotency_key,state,schedule_key,schedule_revision,occurrence_id,scheduled_for FROM background_jobs WHERE occurrence_id=$1 FOR UPDATE`, metadata.OccurrenceID).Scan(&existingID, &existingKind, &existingPayload, &existingKey, &existingState, &existingScheduleKey, &existingRevision, &existingOccurrence, &existingDue)
		if err == nil {
			return existingJob(ctx, tx, existingID, kind, encoded, storedKey, metadata)
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return jobs.SubmitResult{}, err
		}
	}
	var coalesceKey any
	if coalesce {
		canonical, _ := json.Marshal(payload)
		sum := sha256Sum([]byte(kind + "\x00" + string(canonical)))
		coalesceKey = sum
	}
	if coalesce {
		err = tx.QueryRow(ctx, `SELECT id,kind,payload,idempotency_key,state,schedule_key,schedule_revision,occurrence_id,scheduled_for FROM background_jobs WHERE coalesce_key=$1 AND state IN ('queued','running','interrupted') FOR UPDATE`, coalesceKey).Scan(&existingID, &existingKind, &existingPayload, &existingKey, &existingState, &existingScheduleKey, &existingRevision, &existingOccurrence, &existingDue)
		if err == nil {
			if existingKind != kind || !jsonSemanticEqual(existingPayload, encoded) || !sameMetadata(existingScheduleKey, existingRevision, existingOccurrence, existingDue, metadata) {
				return jobs.SubmitResult{}, jobs.ErrIdempotencyConflict
			}
			var aliases int
			if err = tx.QueryRow(ctx, `SELECT count(*) FROM background_job_idempotency_keys WHERE job_id=$1`, existingID).Scan(&aliases); err != nil {
				return jobs.SubmitResult{}, err
			}
			if aliases >= jobs.MaxIdempotencyAliasesPerJob {
				return jobs.SubmitResult{}, jobs.ErrQueueFull
			}
			if _, err = tx.Exec(ctx, `INSERT INTO background_job_idempotency_keys(key,job_id) VALUES($1,$2)`, storedKey, existingID); err != nil {
				return jobs.SubmitResult{}, err
			}
			j, e := loadJob(ctx, tx, existingID)
			return jobs.SubmitResult{Job: j, Created: false}, e
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return jobs.SubmitResult{}, err
		}
	}
	var pending int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM background_jobs WHERE state IN ('queued','interrupted')`).Scan(&pending); err != nil {
		return jobs.SubmitResult{}, err
	}
	if pending >= jobs.MaxPendingJobs {
		return jobs.SubmitResult{}, jobs.ErrQueueFull
	}
	var scheduleKey, occurrenceID any
	var revision any
	var due any
	if metadata != nil {
		scheduleKey = metadata.ScheduleKey
		revision = metadata.ScheduleRevision
		occurrenceID = metadata.OccurrenceID
		due = metadata.ScheduledFor
	}
	// BackgroundJob.id has a Python-side uuid.uuid4 default in the ORM, not a
	// PostgreSQL DEFAULT. Generate the UUID here so Go writes work against the
	// actual schema created by Base.metadata.create_all as well as migrations.
	idBytes := make([]byte, 16)
	if _, err = rand.Read(idBytes); err != nil {
		return jobs.SubmitResult{}, err
	}
	idBytes[6] = (idBytes[6] & 0x0f) | 0x40
	idBytes[8] = (idBytes[8] & 0x3f) | 0x80
	id := fmt.Sprintf("%x-%x-%x-%x-%x", idBytes[:4], idBytes[4:6], idBytes[6:8], idBytes[8:10], idBytes[10:])
	_, err = tx.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,coalesce_key,state,schedule_key,schedule_revision,occurrence_id,scheduled_for) VALUES($1,$2,$3,$4,$5,'queued',$6,$7,$8,$9)`, id, kind, encoded, storedKey, coalesceKey, scheduleKey, revision, occurrenceID, due)
	if err != nil {
		return jobs.SubmitResult{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO background_job_idempotency_keys(key,job_id) VALUES($1,$2)`, storedKey, id); err != nil {
		return jobs.SubmitResult{}, err
	}
	j, err := loadJob(ctx, tx, id)
	return jobs.SubmitResult{Job: j, Created: true}, err
}
func existingJob(ctx context.Context, tx pgx.Tx, id, kind string, payload []byte, key string, meta *jobs.OccurrenceMetadata) (jobs.SubmitResult, error) {
	// The existing idempotency alias is authoritative only when request semantics match.
	var storedPayload []byte
	var storedKind string
	var sk, occ *string
	var rev *int
	var due *time.Time
	err := tx.QueryRow(ctx, `SELECT kind,payload,schedule_key,schedule_revision,occurrence_id,scheduled_for FROM background_jobs WHERE id=$1`, id).Scan(&storedKind, &storedPayload, &sk, &rev, &occ, &due)
	if err != nil {
		return jobs.SubmitResult{}, err
	}
	if storedKind != kind || !jsonSemanticEqual(storedPayload, payload) || !sameMetadata(sk, rev, occ, due, meta) {
		return jobs.SubmitResult{}, jobs.ErrIdempotencyConflict
	}
	var count int
	err = tx.QueryRow(ctx, `SELECT count(*) FROM background_job_idempotency_keys WHERE key=$1`, key).Scan(&count)
	if err != nil {
		return jobs.SubmitResult{}, err
	}
	if count == 0 {
		if _, err = tx.Exec(ctx, `INSERT INTO background_job_idempotency_keys(key,job_id) VALUES($1,$2) ON CONFLICT(key) DO NOTHING`, key, id); err != nil {
			return jobs.SubmitResult{}, err
		}
	}
	j, err := loadJob(ctx, tx, id)
	return jobs.SubmitResult{Job: j, Created: false}, err
}
func jsonSemanticEqual(a, b []byte) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xb, _ := json.Marshal(x)
	yb, _ := json.Marshal(y)
	return string(xb) == string(yb)
}
func sameMetadata(sk *string, rev *int, occ *string, due *time.Time, m *jobs.OccurrenceMetadata) bool {
	if m == nil {
		return sk == nil && rev == nil && occ == nil && due == nil
	}
	return sk != nil && *sk == m.ScheduleKey && rev != nil && *rev == m.ScheduleRevision && occ != nil && *occ == m.OccurrenceID && due != nil && due.Equal(m.ScheduledFor)
}
func (r *JobRepository) Get(ctx context.Context, id string) (jobs.Job, error) {
	j, err := loadJob(ctx, r.pool, id)
	if err != nil {
		return j, err
	}
	var healthy bool
	err = r.pool.QueryRow(ctx, `SELECT status='running' AND heartbeat_at >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - interval '20 seconds' FROM background_worker_status WHERE id=1`).Scan(&healthy)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return jobs.Job{}, err
	}
	j.WorkerAvailable = healthy
	return j, nil
}
func loadJob(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id string) (jobs.Job, error) {
	j, e := scanJob(q.QueryRow(ctx, `SELECT `+jobColumns+` FROM background_jobs WHERE id=$1`, id))
	if errors.Is(e, pgx.ErrNoRows) {
		return j, jobs.ErrNotFound
	}
	return j, e
}
func (r *JobRepository) Status(ctx context.Context) (jobs.WorkerStatus, error) {
	var s jobs.WorkerStatus
	err := r.pool.QueryRow(ctx, `SELECT generation,status,heartbeat_at,(status='running' AND heartbeat_at >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - interval '20 seconds') FROM background_worker_status WHERE id=1`).Scan(&s.Generation, &s.Status, &s.HeartbeatAt, &s.Healthy)
	if errors.Is(err, pgx.ErrNoRows) {
		return s, nil
	}
	return s, err
}
func (r *JobRepository) AcceptOccurrence(ctx context.Context, cmd jobs.OccurrenceCommand, payload map[string]any, meta jobs.OccurrenceMetadata, envelopeHash string, receiptLimit int64) (jobs.AcceptedOccurrence, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return jobs.AcceptedOccurrence{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, producerLock); err != nil {
		return jobs.AcceptedOccurrence{}, err
	}
	var priorHash, priorJob string
	err = tx.QueryRow(ctx, `SELECT envelope_sha256,job_id FROM scheduler_occurrence_receipts WHERE occurrence_id=$1 FOR UPDATE`, meta.OccurrenceID).Scan(&priorHash, &priorJob)
	if err == nil {
		if priorHash != envelopeHash {
			return jobs.AcceptedOccurrence{}, jobs.ErrIdempotencyConflict
		}
		if err = tx.Commit(ctx); err != nil {
			return jobs.AcceptedOccurrence{}, err
		}
		return jobs.AcceptedOccurrence{OccurrenceID: meta.OccurrenceID, JobID: priorJob, Created: false}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return jobs.AcceptedOccurrence{}, err
	}
	if !cmd.WorkerMode || cmd.DispatchMode != "service" {
		return jobs.AcceptedOccurrence{}, jobs.ErrWorkerUnavailable
	}
	var healthy bool
	err = tx.QueryRow(ctx, `SELECT status='running' AND heartbeat_at >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') - interval '20 seconds' FROM background_worker_status WHERE id=1`).Scan(&healthy)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && !healthy {
		return jobs.AcceptedOccurrence{}, jobs.ErrWorkerUnavailable
	}
	if err != nil {
		return jobs.AcceptedOccurrence{}, err
	}
	var count int
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM scheduler_occurrence_receipts`).Scan(&count); err != nil {
		return jobs.AcceptedOccurrence{}, err
	}
	if int64(count) >= receiptLimit {
		return jobs.AcceptedOccurrence{}, jobs.ErrQueueFull
	}
	key := "scheduler:v1:" + meta.OccurrenceID
	sum := sha256Sum([]byte(key))
	result, err := enqueue(ctx, tx, cmd.Operation, payload, sum, false, &meta)
	if err != nil {
		return jobs.AcceptedOccurrence{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO scheduler_occurrence_receipts(occurrence_id,envelope_sha256,job_id) VALUES($1,$2,$3)`, meta.OccurrenceID, envelopeHash, result.Job.ID); err != nil {
		return jobs.AcceptedOccurrence{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return jobs.AcceptedOccurrence{}, err
	}
	return jobs.AcceptedOccurrence{OccurrenceID: meta.OccurrenceID, JobID: result.Job.ID, Created: result.Created}, nil
}
func sha256Sum(b []byte) string { sum := sha256.Sum256(b); return fmt.Sprintf("%x", sum) }
