package store

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestScheduledDigestRetryRequiresDeliveryProof(t *testing.T) {
	prePublish, publishStarted := false, true
	for _, tc := range []struct {
		name      string
		phase     *bool
		state     string
		recovered bool
		reason    string
		wantRetry bool
	}{
		{name: "transient processing failure", phase: &prePublish, state: "failed", reason: "digest_processing_failed", wantRetry: true},
		{name: "recovered before publish", phase: &prePublish, state: "delivery_unknown", recovered: true, wantRetry: true},
		{name: "publish may have happened", phase: &publishStarted, state: "delivery_unknown", recovered: true},
		{name: "old executor", phase: nil, state: "delivery_unknown", recovered: true},
		{name: "configuration failure", phase: &prePublish, state: "failed", reason: "telegram_configuration_incomplete"},
		{name: "explicit interruption", phase: &prePublish, state: "interrupted"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			owner, ctx, cleanup := testStore(t)
			defer cleanup()
			info, err := owner.Acquire(ctx)
			if err != nil {
				t.Fatal(err)
			}
			const jobID = "0a000000-0000-4000-8000-000000000001"
			for _, statement := range []string{
				`ALTER TABLE background_jobs ADD COLUMN occurrence_id uuid, ADD COLUMN scheduled_for timestamp, ADD COLUMN digest_retry_at timestamp`,
				`ALTER TABLE background_job_executions ADD COLUMN legacy_alert_state text, ADD COLUMN digest_delivery_started boolean`,
			} {
				if _, err := owner.Pool.Exec(ctx, statement); err != nil {
					t.Fatal(err)
				}
			}
			_, err = owner.Pool.Exec(ctx, `INSERT INTO background_jobs
				(id,kind,queue_position,state,owner_generation,attempt_generation,occurrence_id,scheduled_for)
				VALUES ($1,'news_digest',1,'running',$2,1,$1,timezone('UTC',clock_timestamp())-interval '1 minute')`, jobID, info.Generation)
			if err != nil {
				t.Fatal(err)
			}
			_, err = owner.Pool.Exec(ctx, `INSERT INTO background_job_executions
				(job_id,attempt_generation,envelope_sha256,executor_instance_id,state,finished_at,digest_delivery_started)
				VALUES ($1,1,'fixture',$1,$2,timezone('UTC',clock_timestamp()),$3)`, jobID, tc.state, tc.phase)
			if err != nil {
				t.Fatal(err)
			}
			job := Job{ID: jobID, Kind: "news_digest", AttemptGeneration: 1}
			code := "worker_error"
			err = owner.finish(ctx, job, tc.state, map[string]any{"failure_reason": tc.reason}, &code, tc.recovered)
			if !tc.wantRetry {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := owner.Claim(ctx, 3); !errors.Is(err, ErrNoJob) {
					t.Fatalf("unsafe retry candidate: %v", err)
				}
				return
			}
			if !errors.Is(err, ErrDigestRetryScheduled) {
				t.Fatalf("schedule retry: %v", err)
			}
			if _, err := owner.Claim(ctx, 3); !errors.Is(err, ErrNoJob) {
				t.Fatalf("retry must wait: %v", err)
			}
			if err := owner.Release(ctx); err != nil {
				t.Fatal(err)
			}
			replacement := &Store{Pool: owner.Pool}
			if _, err := replacement.Acquire(ctx); err != nil {
				t.Fatal(err)
			}
			defer replacement.Release(context.Background())
			if _, err := replacement.Claim(ctx, 3); !errors.Is(err, ErrNoJob) {
				t.Fatalf("replacement worker ignored persisted delay: %v", err)
			}
			if _, err := owner.Pool.Exec(ctx, `UPDATE background_jobs SET digest_retry_at=timezone('UTC',clock_timestamp())-interval '1 second' WHERE id=$1`, jobID); err != nil {
				t.Fatal(err)
			}
			retry, err := replacement.Claim(ctx, 3)
			if err != nil || retry.ID != job.ID || retry.AttemptGeneration != 2 || retry.DigestRetryDeadline == nil {
				t.Fatalf("retry did not reuse durable job: %+v %v", retry, err)
			}
			now, err := owner.DatabaseNow(ctx)
			if err != nil || retry.DigestRetryDeadline.Sub(now) < 28*time.Minute || retry.DigestRetryDeadline.Sub(now) > 30*time.Minute {
				t.Fatalf("invalid publication window: %v %v", retry.DigestRetryDeadline, err)
			}
			for generation := int64(2); generation <= 3; generation++ {
				_, err := owner.Pool.Exec(ctx, `INSERT INTO background_job_executions
					(job_id,attempt_generation,envelope_sha256,executor_instance_id,state,finished_at,digest_delivery_started)
					VALUES ($1,$2,'fixture',$1,'failed',timezone('UTC',clock_timestamp()),false)`, jobID, generation)
				if err != nil {
					t.Fatal(err)
				}
				err = replacement.Finish(ctx, retry, "failed", map[string]any{"failure_reason": "digest_processing_failed"}, &code)
				if generation == 3 {
					if err != nil {
						t.Fatal(err)
					}
					if _, err := replacement.Claim(ctx, 3); !errors.Is(err, ErrNoJob) {
						t.Fatalf("a third retry is prohibited: %v", err)
					}
					break
				}
				if !errors.Is(err, ErrDigestRetryScheduled) {
					t.Fatalf("schedule second retry: %v", err)
				}
				var next time.Time
				if err := owner.Pool.QueryRow(ctx, `SELECT digest_retry_at FROM background_jobs WHERE id=$1`, jobID).Scan(&next); err != nil {
					t.Fatal(err)
				}
				if next.Sub(now) < 14*time.Minute || next.Sub(now) > 16*time.Minute {
					t.Fatalf("second retry anchor: %v", next)
				}
				if _, err := owner.Pool.Exec(ctx, `UPDATE background_jobs SET digest_retry_at=timezone('UTC',clock_timestamp())-interval '1 second'
					WHERE id=$1`, jobID); err != nil {
					t.Fatal(err)
				}
				retry, err = replacement.Claim(ctx, 3)
				if err != nil || retry.AttemptGeneration != 3 {
					t.Fatalf("second retry: %+v %v", retry, err)
				}
			}
		})
	}
}

func TestConsecutiveHeartbeatCommitsSixSecondsKeepCurrentLease(t *testing.T) {
	owner, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := owner.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := owner.Pool.Exec(ctx, `CREATE FUNCTION delay_digest_heartbeat() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(6); RETURN NEW; END $$;
		CREATE CONSTRAINT TRIGGER delay_digest_heartbeat AFTER UPDATE ON background_worker_status
		DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION delay_digest_heartbeat()`); err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		err := owner.Heartbeat(callCtx)
		cancel()
		if err != nil {
			t.Fatalf("6-second COMMIT %d must keep the lease: %v", attempt, err)
		}
		if err := owner.CheckWorkerHealth(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := owner.Pool.Exec(ctx, `DROP TRIGGER delay_digest_heartbeat ON background_worker_status`); err != nil {
		t.Fatal(err)
	}
}
