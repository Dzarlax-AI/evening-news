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
	const malformedID = "00000000-0000-4000-8000-000000000013"
	const rejectedID = "00000000-0000-4000-8000-000000000014"
	const slowID = "00000000-0000-4000-8000-000000000015"
	const fastID = "00000000-0000-4000-8000-000000000016"
	retryResponseCompleted := make(chan time.Time, 1)
	slowStarted, releaseSlow := make(chan struct{}), make(chan struct{})
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
		case malformedID:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_, _ = w.Write([]byte(`{"status":"accepted"}`))
		case rejectedID:
			w.WriteHeader(http.StatusUnauthorized)
		case slowID, fastID:
			if r.Header.Get("Idempotency-Key") == slowID {
				close(slowStarted)
				<-releaseSlow
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"version":1,"occurrence_id":%q,"job_id":"00000000-0000-4000-8000-000000000099","status":"accepted"}`, r.Header.Get("Idempotency-Key"))
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
	if _, err := db.Pool.Exec(ctx, `DELETE FROM `+pgx.Identifier{schema, "occurrences"}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ id, want string }{{malformedID, "pending"}, {rejectedID, "blocked"}} {
		if _, err := db.Pool.Exec(ctx, `INSERT INTO `+pgx.Identifier{schema, "occurrences"}.Sanitize()+` (occurrence_id,schedule_key,schedule_revision,scheduled_for,target_alias,operation,payload,status,attempts,available_at) VALUES ($1::uuid,'test:receipt',1,clock_timestamp(),'worker','op','{}'::jsonb,'pending',0,clock_timestamp()-interval '1 second')`, tc.id); err != nil {
			t.Fatal(err)
		}
		claimed, err = db.Claim(ctx, time.Now().UTC(), 10*time.Second, 1)
		if err != nil || len(claimed) != 1 || claimed[0].Envelope.OccurrenceID != tc.id {
			t.Fatal("could not claim receipt fixture")
		}
		dispatcher.send(ctx, claimed[0])
		if err = db.Pool.QueryRow(ctx, `SELECT status FROM `+pgx.Identifier{schema, "occurrences"}.Sanitize()+` WHERE occurrence_id=$1::uuid`, tc.id).Scan(&status); err != nil || status != tc.want {
			t.Fatalf("receipt state=%q want=%q err=%v", status, tc.want, err)
		}
		if _, err = db.Pool.Exec(ctx, `DELETE FROM `+pgx.Identifier{schema, "occurrences"}.Sanitize()+` WHERE occurrence_id=$1::uuid`, tc.id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Pool.Exec(ctx, `INSERT INTO `+pgx.Identifier{schema, "occurrences"}.Sanitize()+` (occurrence_id,schedule_key,schedule_revision,scheduled_for,target_alias,operation,payload,status,attempts,available_at) VALUES ($1::uuid,'test:slow',1,clock_timestamp(),'slow','op','{}'::jsonb,'pending',0,clock_timestamp()-interval '1 second'),($2::uuid,'test:fast',1,clock_timestamp(),'fast','op','{}'::jsonb,'pending',0,clock_timestamp()-interval '1 second')`, slowID, fastID); err != nil {
		t.Fatal(err)
	}
	parallel := New(db, map[string]model.Target{"slow": {URL: server.URL, Token: "fixture"}, "fast": {URL: server.URL, Token: "fixture"}}, 3*time.Second, 2)
	cycleDone := make(chan error, 1)
	go func() { cycleDone <- parallel.Cycle(ctx, time.Now().UTC()) }()
	select {
	case <-slowStarted:
	case <-ctx.Done():
		close(releaseSlow)
		t.Fatal("slow target not reached")
	}
	fastAccepted := false
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		if err = db.Pool.QueryRow(ctx, `SELECT status FROM `+pgx.Identifier{schema, "occurrences"}.Sanitize()+` WHERE occurrence_id=$1::uuid`, fastID).Scan(&status); err == nil && status == "accepted" {
			fastAccepted = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	close(releaseSlow)
	if err = <-cycleDone; err != nil || !fastAccepted {
		t.Fatalf("fast alias did not progress while slow was blocked: %v", err)
	}
	db.Close()
	reopened, err := store.Open(ctx, dsn, schema, 8, true)
	if err != nil {
		t.Fatal("scheduler store did not reopen")
	}
	defer reopened.Close()
	if err = reopened.Pool.QueryRow(ctx, `SELECT status FROM `+pgx.Identifier{schema, "occurrences"}.Sanitize()+` WHERE occurrence_id=$1::uuid`, fastID).Scan(&status); err != nil || status != "accepted" {
		t.Fatalf("accepted outbox state was not durable across reopen: %q %v", status, err)
	}
}
