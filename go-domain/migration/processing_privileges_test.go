package migration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type processingFixture struct {
	ctx                              context.Context
	pool                             *pgxpool.Pool
	runtime, owner, callback, schema string
}

func newProcessingFixture(t *testing.T) processingFixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not configured")
	}
	verifyLoopback(t, dsn)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(nonce[:])
	schema := "processing_" + suffix
	runtime := "proc_rt_" + suffix
	owner := "proc_own_" + suffix
	callback := "proc_cb_" + suffix
	ident := func(s string) string { return pgx.Identifier{s}.Sanitize() }
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+ident(schema)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, e := admin.Exec(ctx, "DROP SCHEMA IF EXISTS "+ident(schema)+" CASCADE")
		if e != nil {
			t.Errorf("drop fixture schema: %v", e)
		}
		for _, r := range []string{runtime, owner, callback} {
			if _, e = admin.Exec(ctx, "DROP ROLE IF EXISTS "+ident(r)); e != nil {
				t.Errorf("drop fixture role: %v", e)
			}
		}
		admin.Close(ctx)
	})
	if _, err = admin.Exec(ctx, "CREATE ROLE "+ident(callback)+" NOLOGIN"); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "GRANT USAGE ON SCHEMA "+ident(schema)+" TO "+ident(callback)); err != nil {
		t.Fatal(err)
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	result, err := Run(ctx, pool, true, false)
	if err != nil || len(result.Errors) > 0 {
		t.Fatalf("apply migrations: %+v %v", result, err)
	}
	_, testPath, _, ok := goruntime.Caller(0)
	if !ok {
		t.Fatal("cannot locate provisioning SQL")
	}
	data, err := os.ReadFile(filepath.Join(filepath.Dir(testPath), "..", "..", "deploy", "news-processing", "provision-runtime-role.sql"))
	if err != nil {
		t.Fatal(err)
	}
	lines := []string{}
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), `\`) || strings.TrimSpace(line) == "SELECT 1 / 0;" {
			continue
		}
		lines = append(lines, line)
	}
	sql := strings.Join(lines, "\n")
	sql = strings.ReplaceAll(sql, ` :"news_schema"`, ` `+ident(schema))
	sql = strings.ReplaceAll(sql, `:'news_callback_role'`, `'`+callback+`'`)
	sql = strings.ReplaceAll(sql, `:"news_callback_role"`, ident(callback))
	sql = strings.ReplaceAll(sql, "news_processing_fence_owner", owner)
	sql = strings.ReplaceAll(sql, "news_processing_runtime", runtime)
	for _, stmt := range strings.Split(sql, ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err = pool.Exec(ctx, stmt); err != nil {
			t.Fatalf("provision fixture: %v", err)
		}
	}
	if _, err = pool.Exec(ctx, `INSERT INTO articles(id,title,url,content) VALUES(1,'Synthetic story','https://fixture.invalid/story','Synthetic text'); INSERT INTO categories(id,name,display_name) VALUES(1,'Tech','Tech')`); err != nil {
		t.Fatal(err)
	}
	return processingFixture{ctx, pool, runtime, owner, callback, schema}
}
func (f processingFixture) roleTx(t *testing.T, role string) pgx.Tx {
	t.Helper()
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f.ctx, "SET LOCAL ROLE "+pgx.Identifier{role}.Sanitize()); err != nil {
		tx.Rollback(f.ctx)
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Rollback(f.ctx) })
	return tx
}
func requireDenied(t *testing.T, err error) {
	t.Helper()
	var pgerr *pgconn.PgError
	if !errors.As(err, &pgerr) || pgerr.Code != "42501" {
		t.Fatalf("expected permission denied (42501), got %v", err)
	}
}
func TestProcessingPrivileges(t *testing.T) {
	f := newProcessingFixture(t)
	ctx := f.ctx
	t.Run("permitted writes", func(t *testing.T) {
		tx := f.roleTx(t, f.runtime)
		if _, err := tx.Exec(ctx, `SELECT id,kind,payload,state,attempt_generation FROM background_jobs LIMIT 1`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `UPDATE articles SET summary='Synthetic summary',summary_processed=true WHERE id=1`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `INSERT INTO article_categories(article_id,category_id,confidence,ai_category) VALUES(1,1,0.9,'synthetic category')`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(ctx, `DELETE FROM article_categories WHERE article_id=1`); err != nil {
			t.Fatal(err)
		}
		var processed bool
		if err := tx.QueryRow(ctx, `SELECT summary_processed FROM articles WHERE id=1`).Scan(&processed); err != nil || !processed {
			t.Fatal(processed, err)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
	})
	for _, q := range []string{`DELETE FROM articles WHERE id=1`, `UPDATE articles SET fetched_at=now() WHERE id=1`, `UPDATE background_jobs SET state='failed'`, `UPDATE background_worker_status SET generation=99`, `UPDATE background_job_executions SET state='failed'`, `INSERT INTO daily_summaries DEFAULT VALUES`} {
		t.Run(q, func(t *testing.T) { tx := f.roleTx(t, f.runtime); _, err := tx.Exec(ctx, q); requireDenied(t, err) })
	}
	var flags bool
	if err := f.pool.QueryRow(ctx, `SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls OR rolinherit FROM pg_roles WHERE rolname=$1`, f.runtime).Scan(&flags); err != nil || flags {
		t.Fatalf("runtime role flags=%v err=%v", flags, err)
	}
	var member bool
	if err := f.pool.QueryRow(ctx, `SELECT pg_has_role($1,$2,'MEMBER')`, f.runtime, f.owner).Scan(&member); err != nil || member {
		t.Fatalf("owner membership=%v err=%v", member, err)
	}
	function := f.schema + ".fence_article_processing_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp without time zone)"
	for _, role := range []string{f.runtime, f.callback} {
		var yes bool
		if err := f.pool.QueryRow(ctx, `SELECT has_function_privilege($1,$2,'EXECUTE')`, role, function).Scan(&yes); err != nil || !yes {
			t.Fatal(role, yes, err)
		}
	}
	var public bool
	if err := f.pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_proc p, LATERAL aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a WHERE p.oid=$1::regprocedure AND a.grantee=0 AND a.privilege_type='EXECUTE')`, function).Scan(&public); err != nil || public {
		t.Fatal(public, err)
	}
}

func TestProcessingFenceJobKindsAndLease(t *testing.T) {
	f := newProcessingFixture(t)
	ctx := f.ctx
	lease, err := f.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	var pid int
	if err = lease.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(new(any), &pid); err != nil {
		t.Fatal(err)
	}
	defer lease.Exec(ctx, `SELECT pg_advisory_unlock_all()`)
	const owner = "20000000-0000-4000-8000-000000000002"
	const executor = "30000000-0000-4000-8000-000000000003"
	deadline := time.Now().UTC().Add(2 * time.Minute).Truncate(time.Microsecond)
	if _, err = f.pool.Exec(ctx, `UPDATE background_worker_status SET generation=7,owner_id=$1,lock_backend_pid=$2,heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC',status='running' WHERE id=1`, owner, pid); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		kind, payload string
		allowed       bool
	}{{"news_processing", `{}`, true}, {"news_digest", `{"run_processing":true}`, true}, {"article_reprocess", `{"article_ids":[1],"force":true}`, true}, {"reprocess_failed", `{"limit":20,"dry_run":true}`, true}, {"news_digest", `{"run_processing":false}`, false}, {"telegram_digest", `{}`, false}}
	seq := 0
	for _, role := range []string{f.runtime, f.callback} {
		for _, tc := range cases {
			seq++
			job := fmt.Sprintf("10000000-0000-4000-8000-%012d", seq)
			run := fmt.Sprintf("40000000-0000-4000-8000-%012d", seq)
			t.Run(fmt.Sprintf("%s/%s/%s", role, tc.kind, tc.payload), func(t *testing.T) {
				if _, err := f.pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES($1,$2,$3::jsonb,$4,'running',7,3)`, job, tc.kind, tc.payload, job); err != nil {
					t.Fatal(err)
				}
				if _, err := f.pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,3,$2,$3,$4,7,$5,'running')`, job, strings.Repeat("a", 64), executor, owner, deadline); err != nil {
					t.Fatal(err)
				}
				call := `SELECT run_id,run_state FROM fence_article_processing_attempt($1,$2,3,$3,$4,7,$5,$6,$7)`
				args := []any{run, job, strings.Repeat("b", 64), owner, pid, executor, deadline}
				tx := f.roleTx(t, role)
				var gotID, gotState string
				err := tx.QueryRow(ctx, call, args...).Scan(&gotID, &gotState)
				if !tc.allowed {
					var pgerr *pgconn.PgError
					if !errors.As(err, &pgerr) || !strings.Contains(pgerr.Message, "stale_processing_job") {
						t.Fatalf("expected stale_processing_job, got %v", err)
					}
					return
				}
				if err != nil || gotID != run || gotState != "running" {
					t.Fatalf("allowed run=%s state=%s err=%v", gotID, gotState, err)
				}
				if err = tx.Commit(ctx); err != nil {
					t.Fatal(err)
				}
				for _, change := range []struct {
					index int
					value any
				}{{3, "20000000-0000-4000-8000-000000000099"}, {4, pid + 100000}, {5, "30000000-0000-4000-8000-000000000099"}, {6, deadline.Add(time.Second)}} {
					stale := append([]any(nil), args...)
					stale[change.index] = change.value
					tx := f.roleTx(t, role)
					err := tx.QueryRow(ctx, call, stale...).Scan(&gotID, &gotState)
					if err == nil {
						t.Fatalf("stale argument %d accepted", change.index)
					}
					_ = tx.Rollback(ctx)
				}
			})
		}
	}
}
