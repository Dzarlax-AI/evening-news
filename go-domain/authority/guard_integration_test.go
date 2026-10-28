package authority

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type guardProbePause struct {
	armed   atomic.Bool
	started chan struct{}
	resume  chan struct{}
}

func (p *guardProbePause) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "pg_locks") && p.armed.CompareAndSwap(true, false) {
		close(p.started)
		<-p.resume
	}
	return ctx
}
func (*guardProbePause) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestGuardPostgres(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("TEST_DATABASE_URL required in CI")
		}
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("TEST_DATABASE_URL must use loopback")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("guard_test_%d", time.Now().UnixNano())
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+ident+" CASCADE")
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pause := &guardProbePause{started: make(chan struct{}), resume: make(chan struct{})}
	cfg.ConnConfig.Tracer = pause
	cfg.MaxConns = 3
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	first, err := AcquireGuard(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close(context.Background())
	if err = first.Check(ctx); err != nil {
		t.Fatal(err)
	}
	pid := first.BackendPID()
	cancelled, cancelRequest := context.WithCancel(ctx)
	cancelRequest()
	if err = first.Check(cancelled); !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled check: %v", err)
	}
	// Hold the serialized guard session to prove a waiting caller can cancel
	// without waiting for, or revoking, another owner's probe.
	<-first.lock
	waiting, cancelWaiting := context.WithCancel(ctx)
	waitDone := make(chan error, 1)
	go func() { waitDone <- first.Check(waiting) }()
	cancelWaiting()
	select {
	case err = <-waitDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting check: %v", err)
		}
	case <-time.After(time.Second):
		first.lock <- struct{}{}
		t.Fatal("waiting cancellation did not return")
	}
	first.lock <- struct{}{}
	// Cancel after the probe has started but before it reaches PostgreSQL.
	pause.armed.Store(true)
	active, cancelActive := context.WithCancel(ctx)
	activeDone := make(chan error, 1)
	go func() { activeDone <- first.Check(active) }()
	select {
	case <-pause.started:
	case <-time.After(time.Second):
		close(pause.resume)
		t.Fatal("probe did not start")
	}
	cancelActive()
	close(pause.resume)
	if err = <-activeDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("active canceled check: %v", err)
	}
	if first.BackendPID() != pid || first.Check(ctx) != nil {
		t.Fatal("canceled check revoked the healthy singleton")
	}
	second, err := AcquireGuard(ctx, pool)
	if second != nil {
		defer second.Close(context.Background())
	}
	if !errors.Is(err, ErrGuardUnavailable) {
		t.Fatalf("second acquire: %v", err)
	}
	var killed bool
	if err = admin.QueryRow(ctx, "SELECT pg_terminate_backend($1)", pid).Scan(&killed); err != nil || !killed {
		t.Fatalf("terminate: %v", err)
	}
	if err = first.Check(ctx); !errors.Is(err, ErrGuardUnavailable) {
		t.Fatalf("lost guard: %v", err)
	}
	if first.BackendPID() != 0 {
		t.Fatal("revoked PID retained")
	}
	if err = first.Close(ctx); err != nil {
		t.Fatal(err)
	}
	next, err := AcquireGuard(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err = next.Close(ctx); err != nil {
		t.Fatal(err)
	}
	third, err := AcquireGuard(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	closing, cancelClosing := context.WithCancel(ctx)
	cancelClosing()
	if err = third.Close(closing); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled close: %v", err)
	}
	if third.BackendPID() != 0 {
		t.Fatal("canceled close retained the guarded connection")
	}
	fourth, err := AcquireGuard(ctx, pool)
	if err != nil {
		t.Fatalf("acquire after canceled close: %v", err)
	}
	if err = fourth.Close(ctx); err != nil {
		t.Fatal(err)
	}
	poolDone := make(chan struct{})
	go func() { pool.Close(); close(poolDone) }()
	select {
	case <-poolDone:
	case <-time.After(3 * time.Second):
		t.Fatal("pool close blocked after canceled guard close")
	}
}
