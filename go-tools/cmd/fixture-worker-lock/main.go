// fixture-worker-lock induces lease loss in an isolated test database only.
package main

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"time"
)

func run() error {
	if os.Getenv("NEWS_FIXTURE_CONFIRM") != "disposable-fixture-only" || os.Getenv("FIXTURE_DATABASE_URL") == "" {
		return fmt.Errorf("fixture configuration required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, e := pgx.Connect(ctx, os.Getenv("FIXTURE_DATABASE_URL"))
	if e != nil {
		return e
	}
	defer conn.Close(context.Background())
	var database string
	if e = conn.QueryRow(ctx, "SELECT current_database()").Scan(&database); e != nil || database != "gate_db" {
		return fmt.Errorf("fixture database required")
	}
	tx, e := conn.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	var id int
	if e = tx.QueryRow(ctx, "SELECT id FROM background_worker_status WHERE id=1 FOR UPDATE").Scan(&id); e != nil {
		return e
	}
	fmt.Println("fixture_lock_acquired")
	time.Sleep(22 * time.Second)
	return tx.Commit(ctx)
}
func main() {
	if run() != nil {
		fmt.Fprintln(os.Stderr, "fixture lock failed")
		os.Exit(1)
	}
}
