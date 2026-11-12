package orchestration

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-execution"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Exercise the authenticated domain receipt, pipeline, lease fence and terminal
// execution write together. The worker consumes this durable terminal receipt.
func TestSerbianFailurePersistsTruthfulExecutionReceipt(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL required")
	}
	if !isLoopbackPostgresDSN(dsn) {
		t.Fatal("loopback PostgreSQL required")
	}
	for _, tc := range []struct {
		name, kind, operation, domainCode, state, code, reason string
		parentIncomplete                                       bool
	}{
		{"missing_category", "serbian_digest_retry", "serbian_digest_retry", "serbian_category_translation_missing", "failed", "worker_error", "serbian_category_translation_missing", false},
		{"send_rejected", "serbian_digest_retry", "serbian_digest_retry", "serbian_delivery_rejected", "failed", "worker_error", "serbian_delivery_rejected", false},
		{"parent_ru_rejection_unchanged", "telegram_digest", "digest_publish", "telegram_delivery_rejected", "delivery_unknown", "external_outcome_unknown", "digest_publish_failed", false},
		{"send_unknown", "serbian_digest_retry", "serbian_digest_retry", "external_outcome_unknown", "delivery_unknown", "external_outcome_unknown", "digest_publish_failed", false},
		{"receipt_missing", "serbian_digest_retry", "serbian_digest_retry", "prepared_digest_missing", "delivery_unknown", "external_outcome_unknown", "digest_publish_failed", false},
		{"parent_ru_confirmed_sr_missing", "telegram_digest", "", "", "failed", "worker_error", "serbian_category_translation_missing", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			admin, err := pgxpool.New(ctx, dsn)
			if err != nil {
				t.Fatal(err)
			}
			defer admin.Close()
			schema := "sr_receipt_" + strings.ReplaceAll(uuid.NewString(), "-", "")
			q := pgx.Identifier{schema}.Sanitize()
			if _, err = admin.Exec(ctx, `CREATE SCHEMA `+q); err != nil {
				t.Fatal(err)
			}
			defer admin.Exec(context.Background(), `DROP SCHEMA `+q+` CASCADE`)
			createSchemaTables(t, ctx, admin, schema)
			for _, sql := range []string{
				`ALTER TABLE ` + q + `.background_jobs ADD COLUMN kind text, ADD COLUMN initialized_at timestamp`,
				`ALTER TABLE ` + q + `.background_job_executions ADD COLUMN accepted_at timestamp DEFAULT clock_timestamp(), ADD COLUMN started_at timestamp`,
			} {
				if _, err = admin.Exec(ctx, sql); err != nil {
					t.Fatal(err)
				}
			}
			worker := connectLockConn(t, ctx, dsn, schema, 0)
			defer worker.Close(context.Background())
			var pid int32
			if err = worker.QueryRow(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
				t.Fatal(err)
			}
			owner, job := uuid.NewString(), uuid.NewString()
			if _, err = admin.Exec(ctx, `INSERT INTO `+q+`.background_worker_status VALUES(1,$1,7,$2,timezone('UTC',clock_timestamp()),'running')`, owner, pid); err != nil {
				t.Fatal(err)
			}
			payload := `{}`
			if tc.kind == "serbian_digest_retry" {
				payload = `{"russian_edition_id":"60000000-0000-4000-8000-000000000006"}`
			}
			if _, err = admin.Exec(ctx, `INSERT INTO `+q+`.background_jobs(id,kind,payload,state,owner_generation,attempt_generation) VALUES($1,$2,$3::jsonb,'running',7,1)`, job, tc.kind, payload); err != nil {
				t.Fatal(err)
			}
			f := &fakeServices{operationFailures: map[string]string{tc.operation: tc.domainCode}}
			if tc.parentIncomplete {
				f.operationResults = map[string]any{
					"digest_publish": map[string]any{"success": true, "parts_sent": 1, "parts_total": 1, "rich_message": true, "serbian_delivery": map[string]any{"status": "incomplete", "error_code": "serbian_category_translation_missing"}},
				}
			}
			pipeline := testPipeline(t, f)
			srv, err := execution.New(execution.Config{DatabaseURL: dsn, Schema: schema, Token: "fixture-token", QueryTimeout: time.Second, GuardInterval: 25 * time.Millisecond}, execution.Handlers{Pipeline: pipeline})
			if err != nil {
				t.Fatal(err)
			}
			if err = srv.Open(ctx); err != nil {
				t.Fatal(err)
			}
			defer srv.Close(context.Background())
			body := fmt.Sprintf(`{"version":1,"job_id":%q,"owner_id":%q,"generation":7,"attempt_generation":1,"deadline_at":%q}`, job, owner, time.Now().UTC().Add(time.Minute).Format(time.RFC3339Nano))
			r := httptest.NewRequest("POST", "/internal/v1/job-executions", strings.NewReader(body))
			r.Header.Set("Authorization", "Bearer fixture-token")
			w := httptest.NewRecorder()
			srv.Handler().ServeHTTP(w, r)
			if w.Code != 202 {
				t.Fatalf("admission status=%d", w.Code)
			}
			var state, code string
			var result []byte
			until := time.Now().Add(5 * time.Second)
			for time.Now().Before(until) {
				err = admin.QueryRow(ctx, `SELECT state,COALESCE(error_code,''),result FROM `+q+`.background_job_executions WHERE job_id=$1 AND attempt_generation=1`, job).Scan(&state, &code, &result)
				if err == nil && state != "accepted" && state != "running" {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}
			var projected map[string]any
			if err != nil || state != tc.state || code != tc.code || json.Unmarshal(result, &projected) != nil || projected["failure_reason"] != tc.reason {
				t.Fatalf("terminal receipt state=%q code=%q result=%s err=%v", state, code, result, err)
			}
		})
	}
}
