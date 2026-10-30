package appsettings

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestOperationalPostgresCASAndImmutableAttemptSnapshot(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for operational settings PostgreSQL tests")
	}
	u, err := url.Parse(dsn)
	if err != nil || !isOperationalLoopback(u.Hostname()) {
		t.Fatal("operational settings PostgreSQL tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("connect PostgreSQL fixture: %v", err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("operational_settings_test_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatalf("create isolated schema: %v", err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE") }()
	for _, statement := range []string{
		"CREATE TABLE " + quoted + ".settings(key text PRIMARY KEY,value jsonb NOT NULL,updated_at timestamptz NOT NULL DEFAULT now())",
		"CREATE TABLE " + quoted + ".background_jobs(id uuid PRIMARY KEY,state text NOT NULL,attempt_generation integer NOT NULL)",
		"INSERT INTO " + quoted + ".background_jobs(id,state,attempt_generation) VALUES('20000000-0000-4000-8000-000000000002','running',1)",
	} {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("prepare isolated schema: %v", err)
		}
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("parse fixture connection")
	}
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	poolConfig.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatal("open isolated schema pool")
	}
	defer pool.Close()
	store := Store{Pool: pool}
	initial, err := store.Operational(ctx)
	if err != nil || initial.Revision != 0 || initial.LimitsEnabled || initial.MaxArticles != 50 || initial.PerSource != 50 || initial.Days != 1 || !initial.ProcessingEnabled || !initial.DeliveryEnabled {
		t.Fatalf("initial defaults=%+v err=%v", initial, err)
	}

	updates := []OperationalUpdate{operationalUpdate(0, 11), operationalUpdate(0, 22)}
	results := make(chan struct {
		value Operational
		err   error
	}, len(updates))
	var wg sync.WaitGroup
	for _, update := range updates {
		update := update
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, err := store.UpdateOperational(ctx, update)
			results <- struct {
				value Operational
				err   error
			}{value, err}
		}()
	}
	wg.Wait()
	close(results)
	var winner Operational
	successes, conflicts := 0, 0
	for result := range results {
		switch {
		case result.err == nil:
			successes++
			winner = result.value
		case errors.Is(result.err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected CAS result: %v", result.err)
		}
	}
	if successes != 1 || conflicts != 1 || winner.Revision != 1 {
		t.Fatalf("CAS race successes=%d conflicts=%d winner=%+v", successes, conflicts, winner)
	}

	if _, err := store.UpdateTelegram(ctx, TelegramUpdate{ChatID: "@ru", SerbianChatID: "@sr", SerbianDeliveryEnabled: true}); err != nil {
		t.Fatal(err)
	}
	jobID := "20000000-0000-4000-8000-000000000002"
	pinned, err := store.PinOperational(ctx, jobID, 1)
	if err != nil || pinned.Settings.Revision != 1 || pinned.Settings.MaxArticles != winner.MaxArticles {
		t.Fatalf("pin initial snapshot=%+v err=%v", pinned, err)
	}
	if pinned.Telegram == nil || pinned.Telegram.ChatID != "@ru" || pinned.Telegram.SerbianChatID != "@sr" {
		t.Fatal("Telegram destinations not pinned")
	}
	if _, err := store.UpdateTelegram(ctx, TelegramUpdate{ExpectedRevision: 1, ChatID: "@changed-ru", SerbianChatID: "@changed-sr", SerbianDeliveryEnabled: true}); err != nil {
		t.Fatal(err)
	}
	changed := operationalUpdate(1, winner.MaxArticles+10)
	updated, err := store.UpdateOperational(ctx, changed)
	if err != nil || updated.Revision != 2 {
		t.Fatalf("change settings after pin: value=%+v err=%v", updated, err)
	}
	repinned, err := store.PinOperational(ctx, jobID, 1)
	if err != nil || !reflect.DeepEqual(repinned, pinned) {
		t.Fatalf("repin changed an immutable attempt snapshot: first=%+v second=%+v err=%v", pinned, repinned, err)
	}
	readBack, err := store.JobOperational(ctx, jobID, 1)
	if err != nil || !reflect.DeepEqual(readBack, pinned) {
		t.Fatalf("read pinned snapshot=%+v err=%v", readBack, err)
	}
	if _, err := store.JobOperational(ctx, "50000000-0000-4000-8000-000000000005", 1); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("missing pinned snapshot error=%v, want pgx.ErrNoRows", err)
	}
	if _, err := pool.Exec(ctx, `UPDATE background_jobs SET attempt_generation=2 WHERE id=$1::uuid`, jobID); err != nil {
		t.Fatal("advance job attempt")
	}
	if _, err := store.PinOperational(ctx, jobID, 1); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale attempt pin error=%v, want conflict", err)
	}
	newAttempt, err := store.PinOperational(ctx, jobID, 2)
	if err != nil || newAttempt.Settings.Revision != 2 || newAttempt.Settings.MaxArticles != updated.MaxArticles {
		t.Fatalf("new attempt snapshot=%+v err=%v", newAttempt, err)
	}
}

func operationalUpdate(revision int64, maxArticles int) OperationalUpdate {
	return OperationalUpdate{
		ExpectedRevision:  revision,
		LimitsEnabled:     true,
		MaxArticles:       maxArticles,
		PerSource:         7,
		Days:              3,
		NewestDate:        "2026-10-29",
		ProcessingEnabled: true,
		DeliveryEnabled:   true,
	}
}

func isOperationalLoopback(host string) bool {
	host = strings.ToLower(strings.Trim(host, "[]"))
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
