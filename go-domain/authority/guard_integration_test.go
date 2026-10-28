package authority

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

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
	second, err := AcquireGuard(ctx, pool)
	if second != nil {
		defer second.Close(context.Background())
	}
	if !errors.Is(err, ErrGuardUnavailable) {
		t.Fatalf("second acquire: %v", err)
	}
	pid := first.BackendPID()
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
	if err = third.Close(ctx); err != nil {
		t.Fatal(err)
	}
}
