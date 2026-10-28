package backup

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestCopyTreeDereferencesSymlinksAndRejectsCycles(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source")
	if err := os.Mkdir(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "file"), []byte("fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("file", filepath.Join(source, "link")); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(root, "copy")
	if err := copyTree(context.Background(), source, dst); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(filepath.Join(dst, "link"))
	if err != nil || !info.Mode().IsRegular() {
		t.Fatalf("link not dereferenced: %v %v", info, err)
	}
	data, err := os.ReadFile(filepath.Join(dst, "link"))
	if err != nil || string(data) != "fixture" {
		t.Fatalf("data=%q err=%v", data, err)
	}
	if err = os.WriteFile(filepath.Join(root, "outside"), []byte("private"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(filepath.Join(root, "outside"), filepath.Join(source, "escape")); err != nil {
		t.Fatal(err)
	}
	if err = copyTree(context.Background(), source, filepath.Join(root, "escaped")); err == nil {
		t.Fatal("external symlink accepted")
	}
	if err = os.Remove(filepath.Join(source, "escape")); err != nil {
		t.Fatal(err)
	}
	if err = os.Symlink(".", filepath.Join(source, "cycle")); err != nil {
		t.Fatal(err)
	}
	if err = copyTree(context.Background(), source, filepath.Join(root, "cyclic")); err == nil {
		t.Fatal("directory cycle accepted")
	}
}

func TestBackupStaleAttemptCannotPublishPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL required")
		}
		t.Skip("TEST_DATABASE_URL not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Host != "127.0.0.1" && cfg.ConnConfig.Host != "localhost" && cfg.ConnConfig.Host != "::1" {
		t.Fatal("loopback fixture required")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("backup_fence_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	result, err := migration.Run(ctx, pool, true, false)
	if err != nil || len(result.Errors) != 0 {
		t.Fatalf("migration failed: %v", err)
	}
	worker, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer worker.Release()
	executor, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer executor.Release()
	var workerPID, executorPID int
	if err = worker.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(new(any), &workerPID); err != nil {
		t.Fatal(err)
	}
	if err = executor.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # 1162757938),pg_backend_pid()`).Scan(new(any), &executorPID); err != nil {
		t.Fatal(err)
	}
	const job = "10000000-0000-4000-8000-000000000001"
	const owner = "20000000-0000-4000-8000-000000000002"
	const instance = "30000000-0000-4000-8000-000000000003"
	deadline := time.Now().UTC().Add(5 * time.Minute).Truncate(time.Microsecond)
	if _, err = pool.Exec(ctx, `UPDATE background_worker_status SET owner_id=$1,generation=1,lock_backend_pid=$2,status='running',heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=1`, owner, workerPID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES($1,'backup','{}','backup-fence','running',1,1)`, job); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, job, strings.Repeat("a", 64), instance, owner, deadline); err != nil {
		t.Fatal(err)
	}
	fence := authority.Fence{Pool: pool, JobID: job, OwnerID: owner, ExecutorInstanceID: instance, Kind: "backup", AttemptGeneration: 1, Generation: 1, WorkerBackendPID: workerPID, ExecutorBackendPID: executorPID, Deadline: deadline, Payload: []byte(`{}`)}
	if err = fence.Check(ctx); err != nil {
		t.Fatalf("fixture fence invalid: %v", err)
	}
	root := t.TempDir()
	marker := filepath.Join(root, "started")
	unblock := filepath.Join(root, "unblock")
	t.Setenv("BACKUP_TEST_MARKER", marker)
	t.Setenv("BACKUP_TEST_UNBLOCK", unblock)
	script := filepath.Join(root, "fake-pg-dump")
	if err = os.WriteFile(script, []byte("#!/bin/sh\ntouch \"$BACKUP_TEST_MARKER\"\nwhile [ ! -e \"$BACKUP_TEST_UNBLOCK\" ]; do sleep .02; done\nprintf '%s' 'fixture dump'\n"), 0700); err != nil {
		t.Fatal(err)
	}
	svc := New(root, dsn)
	svc.DumpBinary = script
	done := make(chan error, 1)
	go func() { _, err := svc.Create(ctx, fence); done <- err }()
	until := time.Now().Add(3 * time.Second)
	for time.Now().Before(until) {
		if _, err = os.Stat(marker); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil {
		_ = os.WriteFile(unblock, nil, 0600)
		<-done
		t.Fatal("dump did not start")
	}
	if _, err = pool.Exec(ctx, `UPDATE background_jobs SET attempt_generation=2 WHERE id=$1`, job); err != nil {
		_ = os.WriteFile(unblock, nil, 0600)
		<-done
		t.Fatal(err)
	}
	if err = os.WriteFile(unblock, nil, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case err = <-done:
		if !errors.Is(err, authority.ErrStale) {
			t.Fatalf("stale backup returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("stale backup did not finish")
	}
	entries, err := os.ReadDir(filepath.Join(root, "backups"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("stale backup left archive or staging: %v %v", entries, err)
	}
}

func TestBackupCancellationKillsAndReapsDump(t *testing.T) {
	root := t.TempDir()
	pidFile := filepath.Join(root, "dump.pid")
	t.Setenv("BACKUP_TEST_PID_FILE", pidFile)
	script := filepath.Join(root, "fake-pg-dump")
	if err := os.WriteFile(script, []byte("#!/bin/sh\nprintf '%s' \"$$\" > \"$BACKUP_TEST_PID_FILE\"\nexec sleep 30\n"), 0700); err != nil {
		t.Fatal(err)
	}
	svc := New(root, "postgres://fixture:fixture@127.0.0.1:1/gate_db?sslmode=disable")
	svc.DumpBinary = script
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := svc.Create(ctx, authority.Fence{JobID: "00000000-0000-0000-0000-000000000001", AttemptGeneration: 1})
		done <- err
	}()
	var raw []byte
	var err error
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		raw, err = os.ReadFile(pidFile)
		if err == nil && len(raw) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(raw) == 0 {
		cancel()
		<-done
		t.Fatal("fake dump did not start")
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		cancel()
		<-done
		t.Fatal(err)
	}
	cancel()
	select {
	case err = <-done:
		if err == nil {
			t.Fatal("canceled backup succeeded")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("dump cancellation did not finish")
	}
	if err = syscall.Kill(pid, 0); !errors.Is(err, syscall.ESRCH) {
		t.Fatalf("dump process remains: %v", err)
	}
	entries, err := os.ReadDir(filepath.Join(root, "backups"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging or archive remains: %v %v", entries, err)
	}
}

func TestBackupVersionProbeFailureDoesNotCreateStaging(t *testing.T) {
	t.Setenv("NEWS_BACKUP_MATCH_PG_MAJOR", "1")
	root := t.TempDir()
	pool, err := pgxpool.New(context.Background(), "postgres://fixture:fixture@127.0.0.1:1/gate_db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	pool.Close()
	svc := New(root, "postgres://fixture:fixture@127.0.0.1:1/gate_db?sslmode=disable")
	_, err = svc.Create(context.Background(), authority.Fence{Pool: pool})
	if err == nil || err.Error() != "database backup version unavailable" {
		t.Fatalf("unexpected error: %v", err)
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("staging created on version error: %v %v", entries, err)
	}
}

func TestCancelledCopyLeavesNoCompletedArchive(t *testing.T) {
	source := filepath.Join(t.TempDir(), "input")
	if err := os.WriteFile(source, []byte("synthetic fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "output")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := copyTree(ctx, source, target); !errors.Is(err, context.Canceled) {
		t.Fatalf("copy error=%v", err)
	}
	if _, err := os.Stat(target); !os.IsNotExist(err) {
		t.Fatalf("canceled copy created output: %v", err)
	}
}

func TestServerVersionNumberPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL required in CI")
		}
		t.Skip("TEST_DATABASE_URL not set")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Host != "127.0.0.1" && cfg.ConnConfig.Host != "localhost" && cfg.ConnConfig.Host != "::1" {
		t.Fatal("loopback fixture required")
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	version, err := serverVersionNumber(context.Background(), pool)
	if err != nil || version/10000 < 16 || version/10000 > 17 {
		t.Fatalf("server version probe failed: version=%d error=%v", version, err)
	}
}
