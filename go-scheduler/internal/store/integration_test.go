package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresPersistenceAndFencing(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("SCHEDULER_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("SCHEDULER_TEST_DATABASE_URL is not set")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Hostname() != "localhost" && parsed.Hostname() != "127.0.0.1") {
		t.Fatal("scheduler integration tests require a localhost PostgreSQL test URL")
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
	name := "scheduler_test_" + hex.EncodeToString(random[:]) + "_" + strings.ReplaceAll(time.Now().UTC().Format("150405.000000000"), ".", "")
	if len(name) > 63 {
		name = name[:63]
	}
	if _, err := admin.Exec(context.Background(), `CREATE SCHEMA `+pgx.Identifier{name}.Sanitize()); err != nil {
		t.Fatal("could not create isolated scheduler test schema")
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{name}.Sanitize()+` CASCADE`)
	})

	// Concurrent first startup must serialize DDL and the control-row seed.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			db, err := Open(ctx, dsn, name, 4, true)
			if err == nil {
				db.Close()
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal("concurrent scheduler startup migration failed")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := Open(ctx, dsn, name, 8, true)
	if err != nil {
		t.Fatal("could not open scheduler store")
	}
	defer db.Close()

	now := time.Date(2026, 10, 23, 15, 0, 0, 123456000, time.UTC)
	numeric := json.Number("9007199254740993")
	mutation := model.Mutation{
		ExpectedRevision: 0,
		Enabled:          true,
		Calendar:         model.Calendar{Type: "interval", Timezone: "UTC", IntervalMinutes: intPtr(60)},
		Target:           "worker",
		Operation:        "opaque.operation",
		Payload:          map[string]any{"nested": map[string]any{"large": numeric, "decimal": json.Number("0.1234567890123456789")}},
	}
	targets := map[string]model.Target{"worker": {URL: "https://fixture.invalid", Token: "fixture-secret"}}
	created, _, err := db.PutSchedule(ctx, "test:numeric", mutation, targets, now)
	if err != nil {
		t.Fatal("could not create schedule with opaque numeric payload")
	}
	got, err := db.GetSchedule(ctx, created.Key)
	if err != nil {
		t.Fatal("could not reload schedule payload")
	}
	if got.Payload["nested"].(map[string]any)["large"] != numeric || got.Payload["nested"].(map[string]any)["decimal"] != json.Number("0.1234567890123456789") {
		t.Fatal("JSON numbers changed during PostgreSQL round trip")
	}
	if _, changed, err := db.PutSchedule(ctx, "test:numeric", model.Mutation{ExpectedRevision: 1, Enabled: mutation.Enabled, Calendar: mutation.Calendar, Target: mutation.Target, Operation: mutation.Operation, Payload: got.Payload}, targets, now.Add(time.Second)); err != nil || changed {
		t.Fatal("identical schedule update should be idempotent")
	}

	control, err := db.GetControl(ctx)
	if err != nil || !control.Paused {
		t.Fatal("first initialization did not persist paused state")
	}
	due := now.Add(-time.Second)
	importMutation := mutation
	importMutation.Payload = map[string]any{"n": json.Number("9007199254740993")}
	importItem := ImportItem{Key: "news:legacy", Enabled: true, Calendar: importMutation.Calendar, Target: "worker", Operation: "news_digest", Payload: importMutation.Payload, NextDueAt: &due}
	imported, err := db.Import(ctx, control.Revision, []ImportItem{importItem}, targets, now)
	if err != nil || len(imported.Created) != 1 {
		t.Fatal("legacy schedule import failed")
	}
	importedSchedule, err := db.GetSchedule(ctx, importItem.Key)
	if err != nil || importedSchedule.NextDueAt == nil || !importedSchedule.NextDueAt.Equal(due) {
		t.Fatal("import did not preserve the exact due instant")
	}
	if _, err = db.Import(ctx, control.Revision, []ImportItem{importItem}, targets, now.Add(time.Second)); err != nil {
		t.Fatal("identical import was not idempotent")
	}
	if err = db.Pool.QueryRow(ctx, `SELECT next_due_at FROM `+db.schedules+` WHERE schedule_key=$1`, importItem.Key).Scan(&due); err != nil {
		t.Fatal("could not check microsecond due timestamp")
	}

	if _, err = db.SetPaused(ctx, control.Revision, false, now); err != nil {
		t.Fatal("could not resume scheduler")
	}
	if _, err = db.Pool.Exec(ctx, `UPDATE `+db.schedules+` SET next_due_at=$2 WHERE schedule_key=$1`, "test:numeric", now.Add(-time.Second)); err != nil {
		t.Fatal("could not prepare overdue schedule")
	}
	generated, err := db.GenerateDue(ctx, now, 100, 1)
	if err != nil || generated != 1 {
		t.Fatal("outbox capacity did not permit exactly one occurrence")
	}
	remaining, err := db.GetSchedule(ctx, "test:numeric")
	if err != nil || remaining.NextDueAt == nil || !remaining.NextDueAt.Before(now) {
		t.Fatal("outbox cap advanced a due schedule without inserting its occurrence")
	}
	claimed, err := db.Claim(ctx, now, 10*time.Second, 1)
	if err != nil || len(claimed) != 1 {
		t.Fatal("could not claim generated occurrence")
	}
	if _, err := db.Pool.Exec(ctx, `UPDATE `+db.outbox+` SET lease_until=clock_timestamp()-interval '1 microsecond' WHERE occurrence_id=$1::uuid`, claimed[0].Envelope.OccurrenceID); err != nil {
		t.Fatal("could not expire the claimed lease")
	}
	if err := db.Ack(ctx, claimed[0].Envelope.OccurrenceID, claimed[0].LeaseToken, "00000000-0000-4000-8000-000000000001", now.Add(10*time.Second)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("expired lease incorrectly acknowledged an occurrence")
	}
	if generated, err = db.GenerateDue(ctx, now.Add(9*time.Second), 100, 10); err != nil || generated != 1 {
		t.Fatal("a separate due schedule was not allowed while the first remains leased")
	}
	var sameScheduleOpen int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM `+db.outbox+` WHERE schedule_key=$1 AND status<>'accepted'`, importItem.Key).Scan(&sameScheduleOpen); err != nil || sameScheduleOpen != 1 {
		t.Fatal("a schedule created multiple non-accepted occurrences")
	}
	if _, err = db.SetPaused(ctx, 1, true, now.Add(11*time.Second)); err != nil {
		t.Fatal("could not pause scheduler")
	}
	newClaims, err := db.Claim(ctx, now.Add(12*time.Second), 10*time.Second, 10)
	if err != nil || len(newClaims) != 0 {
		t.Fatal("paused scheduler claimed new dispatch work")
	}
}

func TestPostgresMigrationWorksForPrecreatedSchemaOwner(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("SCHEDULER_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("SCHEDULER_TEST_DATABASE_URL is not set")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Hostname() != "localhost" && parsed.Hostname() != "127.0.0.1") {
		t.Fatal("scheduler integration tests require a localhost PostgreSQL test URL")
	}
	admin, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal("could not connect to isolated scheduler test database")
	}
	defer admin.Close()

	var random [12]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("could not create isolated role name")
	}
	suffix := hex.EncodeToString(random[:])
	role := "scheduler_role_test_" + suffix
	schema := "scheduler_owner_test_" + suffix
	privateSchema := "news_private_test_" + suffix
	password := hex.EncodeToString(random[:])
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if _, err := admin.Exec(ctx, `CREATE ROLE `+pgx.Identifier{role}.Sanitize()+` LOGIN PASSWORD '`+password+`'`); err != nil {
		var pgErr *pgconn.PgError
		if errors.As(err, &pgErr) && pgErr.Code == "42501" {
			t.Skip("test database user cannot create an isolated role")
		}
		t.Fatal("could not create isolated scheduler test role")
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS `+pgx.Identifier{privateSchema}.Sanitize()+` CASCADE`)
		_, _ = admin.Exec(context.Background(), `DROP ROLE IF EXISTS `+pgx.Identifier{role}.Sanitize())
	})
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()+` AUTHORIZATION `+pgx.Identifier{role}.Sanitize()); err != nil {
		t.Fatal("could not pre-create scheduler schema")
	}
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{privateSchema}.Sanitize()); err != nil {
		t.Fatal("could not create isolated private schema")
	}
	if _, err := admin.Exec(ctx, `CREATE TABLE `+pgx.Identifier{privateSchema, "news_rows"}.Sanitize()+` (id integer)`); err != nil {
		t.Fatal("could not create isolated private table")
	}

	parsed.User = url.UserPassword(role, password)
	db, err := Open(ctx, parsed.String(), schema, 4, true)
	if err != nil {
		t.Fatalf("pre-created schema owner could not migrate: %v", err)
	}
	defer db.Close()
	var hasAccess bool
	if err := db.Pool.QueryRow(ctx, `SELECT has_schema_privilege(current_user, $1, 'USAGE')`, privateSchema).Scan(&hasAccess); err != nil || hasAccess {
		t.Fatal("scheduler role unexpectedly has access to the private news schema")
	}
	if _, err := db.Pool.Exec(ctx, `SELECT id FROM `+pgx.Identifier{privateSchema, "news_rows"}.Sanitize()); err == nil {
		t.Fatal("scheduler role unexpectedly read a private news table")
	}
}

func intPtr(value int) *int { return &value }
