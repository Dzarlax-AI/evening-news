package dispatch

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestDispatcherMeasuresRetryAndLeaseFromResponseCompletion(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("SCHEDULER_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("SCHEDULER_TEST_DATABASE_URL is not set")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Hostname() != "localhost" && parsed.Hostname() != "127.0.0.1") {
		t.Fatal("dispatcher integration tests require localhost PostgreSQL")
	}
	admin, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal("could not connect to isolated scheduler test database")
	}
	defer admin.Close()
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("could not create isolated schema name")
	}
	schema := "scheduler_test_dispatch_" + hex.EncodeToString(random[:])
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := store.Open(ctx, dsn, schema, 8, true)
	if err != nil {
		t.Fatal("could not open scheduler store")
	}
	defer db.Close()
	if _, err := db.SetPaused(ctx, 0, false, time.Now().UTC()); err != nil {
		t.Fatal("could not enable dispatcher integration fixture")
	}

	const retryID = "00000000-0000-4000-8000-000000000011"
	const staleID = "00000000-0000-4000-8000-000000000012"
	retryResponseCompleted := make(chan time.Time, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.Header.Get("Idempotency-Key") {
		case retryID:
			time.Sleep(150 * time.Millisecond)
			w.Header().Set("Retry-After", "3")
			w.WriteHeader(http.StatusServiceUnavailable)
			retryResponseCompleted <- time.Now().UTC()
		case staleID:
			time.Sleep(150 * time.Millisecond)
			w.WriteHeader(http.StatusUnauthorized)
		default:
			http.Error(w, "unexpected occurrence", http.StatusBadRequest)
		}
	}))
	defer server.Close()
	if _, err := db.Pool.Exec(ctx, `INSERT INTO `+pgx.Identifier{schema, "occurrences"}.Sanitize()+` (occurrence_id,schedule_key,schedule_revision,scheduled_for,target_alias,operation,payload,status,attempts,available_at) VALUES ($1::uuid,'test:retry',1,clock_timestamp(),'worker','op','{}'::jsonb,'pending',0,clock_timestamp()-interval '1 second'),($2::uuid,'test:stale',1,clock_timestamp(),'worker','op','{}'::jsonb,'pending',0,clock_timestamp()-interval '1 second')`, retryID, staleID); err != nil {
		t.Fatal("could not seed dispatcher retry occurrences")
	}
	targets := map[string]model.Target{"worker": {URL: server.URL, Token: "test-target-token"}}
	dispatcher := New(db, targets, 2*time.Second, 1)
	claimed, err := db.Claim(ctx, time.Now().UTC().Truncate(time.Microsecond), 10*time.Second, 1)
	if err != nil || len(claimed) != 1 || claimed[0].Envelope.OccurrenceID != retryID {
		t.Fatal("could not claim the retry timing occurrence")
	}
	start := time.Now().UTC()
	dispatcher.send(ctx, claimed[0])
	var responseCompleted time.Time
	select {
	case responseCompleted = <-retryResponseCompleted:
	case <-ctx.Done():
		t.Fatal("delayed HTTP response was not observed within the fixture deadline")
	}
	var available, updated time.Time
	if err := db.Pool.QueryRow(ctx, `SELECT available_at,updated_at FROM `+pgx.Identifier{schema, "occurrences"}.Sanitize()+` WHERE occurrence_id=$1::uuid`, retryID).Scan(&available, &updated); err != nil {
		t.Fatal("could not inspect persisted retry timing")
	}
	if responseCompleted.Sub(start) < 140*time.Millisecond {
		t.Fatalf("retry response completed before the expected delay: %s", responseCompleted.Sub(start))
	}
	if updated.Before(responseCompleted.Add(-time.Millisecond)) {
		t.Fatalf("retry persistence timestamp %s predates delayed HTTP response completion %s", updated, responseCompleted)
	}
	if got := available.Sub(updated); got != 3*time.Second {
		t.Fatalf("persisted Retry-After interval = %s, want exactly 3s from response completion", got)
	}

	claimed, err = db.Claim(ctx, time.Now().UTC().Truncate(time.Microsecond), 50*time.Millisecond, 1)
	if err != nil || len(claimed) != 1 || claimed[0].Envelope.OccurrenceID != staleID {
		t.Fatal("could not claim the lease-expiry occurrence")
	}
	dispatcher.send(ctx, claimed[0])
	var status string
	if err := db.Pool.QueryRow(ctx, `SELECT status FROM `+pgx.Identifier{schema, "occurrences"}.Sanitize()+` WHERE occurrence_id=$1::uuid`, staleID).Scan(&status); err != nil {
		t.Fatal("could not inspect expired occurrence")
	}
	if status != "leased" {
		t.Fatalf("expired HTTP attempt mutated lease state to %q", status)
	}
	var outstanding int
	if err := db.Pool.QueryRow(ctx, fmt.Sprintf(`SELECT count(*) FROM %s WHERE status='leased' AND lease_until<=clock_timestamp()`, pgx.Identifier{schema, "occurrences"}.Sanitize())).Scan(&outstanding); err != nil || outstanding != 1 {
		t.Fatal("expired occurrence did not remain visible as leased for recovery")
	}
}
