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
		Payload:          map[string]any{"nested": map[string]any{"large": numeric, "decimal": json.Number("0.1234567890123456789"), "exponent": json.Number("1e2"), "small": json.Number("1e-7")}},
	}
	targets := map[string]model.Target{"worker": {URL: "https://fixture.invalid", Token: "fixture-secret"}}
	_, err = db.Import(ctx, 0, []ImportItem{{Key: "test:enabled-without-due", Enabled: true, Calendar: mutation.Calendar, Target: "worker", Operation: "opaque.operation", Payload: map[string]any{}}}, targets, now)
	if err == nil {
		t.Fatal("enabled import without next_due_at must be rejected")
	}
	if _, err := db.GetSchedule(ctx, "test:enabled-without-due"); !errors.Is(err, ErrNotFound) {
		t.Fatal("invalid enabled import unexpectedly created a schedule")
	}
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
	semanticPayload := map[string]any{"nested": map[string]any{"large": numeric, "decimal": json.Number("0.1234567890123456789"), "exponent": json.Number("100.0"), "small": json.Number("0.0000001")}}
	if _, changed, err := db.PutSchedule(ctx, "test:numeric", model.Mutation{ExpectedRevision: 1, Enabled: mutation.Enabled, Calendar: mutation.Calendar, Target: mutation.Target, Operation: mutation.Operation, Payload: semanticPayload}, targets, now.Add(2*time.Second)); err != nil || changed {
		t.Fatal("JSONB-equivalent number spellings must not create a new schedule revision")
	}

	control, err := db.GetControl(ctx)
	if err != nil || !control.Paused {
		t.Fatal("first initialization did not persist paused state")
	}
	due := now.Add(-time.Second)
	importMutation := mutation
	importMutation.Payload = map[string]any{"n": json.Number("9007199254740993"), "exponent": json.Number("1e2"), "small": json.Number("1e-7")}
	importItem := ImportItem{Key: "news:legacy", Enabled: true, Calendar: importMutation.Calendar, Target: "worker", Operation: "news_digest", Payload: importMutation.Payload, NextDueAt: &due}
	imported, err := db.Import(ctx, control.Revision, []ImportItem{importItem}, targets, now)
	if err != nil || len(imported.Created) != 1 {
		t.Fatal("legacy schedule import failed")
	}
	importedSchedule, err := db.GetSchedule(ctx, importItem.Key)
	if err != nil || importedSchedule.NextDueAt == nil || !importedSchedule.NextDueAt.Equal(due) {
		t.Fatal("import did not preserve the exact due instant")
	}
	semanticImport := importItem
	semanticImport.Payload = map[string]any{"n": json.Number("9.007199254740993e15"), "exponent": json.Number("100.0"), "small": json.Number("0.0000001")}
	if _, err = db.Import(ctx, control.Revision, []ImportItem{semanticImport}, targets, now.Add(time.Second)); err != nil {
		t.Fatal("JSONB-equivalent import payload should remain idempotent")
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
	if _, err := db.Pool.Exec(ctx, `UPDATE `+db.outbox+` SET lease_until=clock_timestamp()+interval '1 hour' WHERE occurrence_id=$1::uuid`, claimed[0].Envelope.OccurrenceID); err != nil {
		t.Fatal("could not prepare active lease inventory")
	}
	clockSkewClaims, err := db.Claim(ctx, time.Now().UTC().Add(24*time.Hour), 10*time.Second, 10)
	if err != nil || len(clockSkewClaims) != 0 {
		t.Fatal("future application clock stole a lease that was live according to PostgreSQL")
	}
	if _, err := db.SetPaused(ctx, 1, true, now.Add(10*time.Second)); err != nil {
		t.Fatal("could not pause scheduler with a live lease")
	}
	liveInventory, _, err := db.ListOccurrences(ctx, importItem.Key, "", 100)
	if err != nil || len(liveInventory) != 1 || liveInventory[0].TargetAlias != "worker" || liveInventory[0].Status != "leased" || liveInventory[0].LeaseExpiresAt == nil || !liveInventory[0].LeaseActive {
		t.Fatal("inventory did not identify the active lease while paused")
	}
	if _, err := db.Pool.Exec(ctx, `UPDATE `+db.outbox+` SET lease_until=clock_timestamp()-interval '1 microsecond' WHERE occurrence_id=$1::uuid`, claimed[0].Envelope.OccurrenceID); err != nil {
		t.Fatal("could not expire the claimed lease")
	}
	expiredInventory, _, err := db.ListOccurrences(ctx, importItem.Key, "", 100)
	if err != nil || len(expiredInventory) != 1 || expiredInventory[0].TargetAlias != "worker" || expiredInventory[0].Status != "leased" || expiredInventory[0].LeaseExpiresAt == nil || expiredInventory[0].LeaseActive {
		t.Fatal("inventory did not report the expired lease as inactive while paused")
	}
	if err := db.Ack(ctx, claimed[0].Envelope.OccurrenceID, claimed[0].LeaseToken, "00000000-0000-4000-8000-000000000001", now.Add(10*time.Second)); !errors.Is(err, ErrLeaseLost) {
		t.Fatal("expired lease incorrectly acknowledged an occurrence")
	}
	newClaims, err := db.Claim(ctx, now.Add(12*time.Second), 10*time.Second, 10)
	if err != nil || len(newClaims) != 0 {
		t.Fatal("paused scheduler claimed new dispatch work")
	}
	if _, err := db.SetPaused(ctx, 2, false, now.Add(13*time.Second)); err != nil {
		t.Fatal("could not resume scheduler after lease inspection")
	}
	reclaimed, err := db.Claim(ctx, time.Now().UTC().Add(-24*time.Hour), 10*time.Second, 1)
	if err != nil || len(reclaimed) != 1 || reclaimed[0].Envelope.OccurrenceID != claimed[0].Envelope.OccurrenceID {
		t.Fatal("past application clock prevented reclaiming a lease that had expired according to PostgreSQL")
	}
	if generated, err = db.GenerateDue(ctx, now.Add(9*time.Second), 100, 10); err != nil || generated != 1 {
		t.Fatal("a separate due schedule was not allowed while the first remains leased")
	}
	var sameScheduleOpen int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM `+db.outbox+` WHERE schedule_key=$1 AND status<>'accepted'`, importItem.Key).Scan(&sameScheduleOpen); err != nil || sameScheduleOpen != 1 {
		t.Fatal("a schedule created multiple non-accepted occurrences")
	}
	if _, err = db.SetPaused(ctx, 3, true, now.Add(14*time.Second)); err != nil {
		t.Fatal("could not pause scheduler")
	}
	newClaims, err = db.Claim(ctx, now.Add(15*time.Second), 10*time.Second, 10)
	if err != nil || len(newClaims) != 0 {
		t.Fatal("paused scheduler claimed new dispatch work")
	}
	if _, err := db.Pool.Exec(ctx, `UPDATE `+db.outbox+` SET lease_until=clock_timestamp()-interval '1 microsecond' WHERE occurrence_id=$1::uuid`, claimed[0].Envelope.OccurrenceID); err != nil {
		t.Fatal("could not expire retained lease before schedule deletion")
	}
	if err := db.DeleteSchedule(ctx, importItem.Key, 1); err != nil {
		t.Fatal("could not delete schedule with retained occurrence")
	}
	orphanedInventory, _, err := db.ListOccurrences(ctx, "", "", 100)
	if err != nil {
		t.Fatal("could not list inventory after schedule deletion")
	}
	foundOrphan := false
	for _, item := range orphanedInventory {
		if item.ScheduleKey == importItem.Key {
			foundOrphan = item.TargetAlias == "worker" && item.Status == "leased" && item.LeaseExpiresAt != nil && !item.LeaseActive
		}
	}
	if !foundOrphan {
		t.Fatal("schedule deletion removed or altered its occurrence inventory")
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

func TestPostgresAvailabilityUsesDatabaseClock(t *testing.T) {
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
	defer admin.Close()

	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("could not create isolated schema name")
	}
	schema := "scheduler_clock_test_" + hex.EncodeToString(random[:])
	if _, err := admin.Exec(context.Background(), `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal("could not create isolated scheduler test schema")
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := Open(ctx, dsn, schema, 4, true)
	if err != nil {
		t.Fatal("could not open scheduler store")
	}
	defer db.Close()
	control, err := db.GetControl(ctx)
	if err != nil {
		t.Fatal("could not read scheduler control")
	}
	if _, err := db.SetPaused(ctx, control.Revision, false, time.Now().UTC()); err != nil {
		t.Fatal("could not resume scheduler")
	}
	targets := map[string]model.Target{"worker": {URL: "https://fixture.invalid", Token: "fixture-secret"}}
	for _, offset := range []time.Duration{-2 * time.Minute, 2 * time.Minute} {
		t.Run(offset.String(), func(t *testing.T) {
			appNow := time.Now().UTC().Add(offset).Truncate(time.Microsecond)
			key := "clock:" + strings.ReplaceAll(offset.String(), "-", "minus-")
			mutation := model.Mutation{Enabled: true, Calendar: model.Calendar{Type: "interval", Timezone: "UTC", IntervalMinutes: intPtr(10)}, Target: "worker", Operation: "clock_test", Payload: map[string]any{}}
			if _, _, err := db.PutSchedule(ctx, key, mutation, targets, appNow); err != nil {
				t.Fatal("could not create clock test schedule")
			}
			if _, err := db.Pool.Exec(ctx, `UPDATE `+db.schedules+` SET next_due_at=clock_timestamp()-interval '10 minutes' WHERE schedule_key=$1`, key); err != nil {
				t.Fatal("could not make test schedule due")
			}
			if generated, err := db.GenerateDue(ctx, appNow, 10, 10); err != nil || generated != 1 {
				t.Fatal("could not generate a due occurrence")
			}
			var occurrenceID string
			var available time.Time
			if err := db.Pool.QueryRow(ctx, `SELECT occurrence_id::text,available_at FROM `+db.outbox+` WHERE schedule_key=$1`, key).Scan(&occurrenceID, &available); err != nil {
				t.Fatal("could not read generated occurrence")
			}
			dbNow := postgresNow(t, ctx, db)
			if delta := available.Sub(dbNow); delta < -2*time.Second || delta > 2*time.Second {
				t.Fatalf("generated occurrence availability did not use the database clock: delta=%s", delta)
			}
			claimed, err := db.Claim(ctx, appNow, 30*time.Second, 1)
			if err != nil || len(claimed) != 1 || claimed[0].Envelope.OccurrenceID != occurrenceID {
				t.Fatal("generated occurrence was not immediately claimable")
			}

			if err := db.Retry(ctx, occurrenceID, claimed[0].LeaseToken, "temporary", appNow.Add(time.Minute), appNow); err != nil {
				t.Fatal("could not schedule retry")
			}
			var retryAt time.Time
			if err := db.Pool.QueryRow(ctx, `SELECT available_at FROM `+db.outbox+` WHERE occurrence_id=$1::uuid`, occurrenceID).Scan(&retryAt); err != nil {
				t.Fatal("could not read retry availability")
			}
			dbNow = postgresNow(t, ctx, db)
			if delta := retryAt.Sub(dbNow); delta < 58*time.Second || delta > 62*time.Second {
				t.Fatalf("retry delay was not preserved relative to PostgreSQL clock: delta=%s", delta)
			}
			if claims, err := db.Claim(ctx, appNow, 30*time.Second, 1); err != nil || len(claims) != 0 {
				t.Fatal("delayed retry was claimable before its delay elapsed")
			}

			if _, err := db.Pool.Exec(ctx, `UPDATE `+db.outbox+` SET available_at=clock_timestamp()-interval '1 microsecond' WHERE occurrence_id=$1::uuid`, occurrenceID); err != nil {
				t.Fatal("could not release retry for blocked-retry check")
			}
			claimed, err = db.Claim(ctx, appNow, 30*time.Second, 1)
			if err != nil || len(claimed) != 1 {
				t.Fatal("could not reclaim retry occurrence")
			}
			if err := db.Block(ctx, occurrenceID, claimed[0].LeaseToken, "invalid_target", appNow); err != nil {
				t.Fatal("could not block occurrence")
			}
			if _, err := db.RetryBlocked(ctx, occurrenceID, appNow); err != nil {
				t.Fatal("could not retry blocked occurrence")
			}
			if err := db.Pool.QueryRow(ctx, `SELECT available_at FROM `+db.outbox+` WHERE occurrence_id=$1::uuid`, occurrenceID).Scan(&available); err != nil {
				t.Fatal("could not read blocked retry availability")
			}
			dbNow = postgresNow(t, ctx, db)
			if delta := available.Sub(dbNow); delta < -2*time.Second || delta > 2*time.Second {
				t.Fatalf("blocked retry availability did not use the database clock: delta=%s", delta)
			}
			claimed, err = db.Claim(ctx, appNow, 30*time.Second, 1)
			if err != nil || len(claimed) != 1 || claimed[0].Envelope.OccurrenceID != occurrenceID {
				t.Fatal("blocked retry was not immediately claimable")
			}
		})
	}
}

func postgresNow(t *testing.T, ctx context.Context, db *Store) time.Time {
	t.Helper()
	var now time.Time
	if err := db.Pool.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal("could not read PostgreSQL clock")
	}
	return now.UTC()
}

func intPtr(value int) *int { return &value }

func TestPostgresImportedDailyDueConsumesScheduledLocalDate(t *testing.T) {
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

	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("could not create isolated schema name")
	}
	schema := "scheduler_daily_dst_" + hex.EncodeToString(random[:])
	if _, err := admin.Exec(context.Background(), `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal("could not create isolated scheduler test schema")
	}
	t.Cleanup(func() {
		if _, err := admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`); err != nil {
			t.Error("could not clean up isolated scheduler test schema")
		}
		admin.Close()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	db, err := Open(ctx, dsn, schema, 4, true)
	if err != nil {
		t.Fatal("could not open scheduler store")
	}
	defer db.Close()
	control, err := db.GetControl(ctx)
	if err != nil {
		t.Fatal("could not read scheduler control")
	}
	targets := map[string]model.Target{"worker": {URL: "https://fixture.invalid", Token: "fixture-secret"}}
	hour, minute := 19, 0
	importedDue := time.Date(2026, 10, 25, 17, 0, 0, 0, time.UTC)
	item := ImportItem{
		Key:      "news:legacy-daily-19",
		Enabled:  true,
		Calendar: model.Calendar{Type: "daily", Timezone: "Europe/Belgrade", Hour: &hour, Minute: &minute},
		Target:   "worker", Operation: "news_processing", Payload: map[string]any{}, NextDueAt: &importedDue,
	}
	if _, err := db.Import(ctx, control.Revision, []ImportItem{item}, targets, importedDue); err != nil {
		t.Fatal("could not import legacy daily schedule")
	}
	control, err = db.GetControl(ctx)
	if err != nil {
		t.Fatal("could not reload scheduler control after import")
	}
	if _, err := db.SetPaused(ctx, control.Revision, false, time.Now().UTC()); err != nil {
		t.Fatal("could not resume scheduler")
	}
	stored, err := db.GetSchedule(ctx, item.Key)
	if err != nil || stored.NextDueAt == nil || !stored.NextDueAt.Equal(importedDue) {
		t.Fatal("import did not preserve the exact legacy due instant")
	}
	if generated, err := db.GenerateDue(ctx, importedDue, 10, 10); err != nil || generated != 1 {
		t.Fatal("first imported occurrence was not generated")
	}
	var occurrenceID string
	var scheduledFor time.Time
	if err := db.Pool.QueryRow(ctx, `SELECT occurrence_id::text,scheduled_for FROM `+db.outbox+` WHERE schedule_key=$1`, item.Key).Scan(&occurrenceID, &scheduledFor); err != nil || !scheduledFor.Equal(importedDue) {
		t.Fatal("first occurrence did not retain the exact imported due instant")
	}
	stored, err = db.GetSchedule(ctx, item.Key)
	wantNext := time.Date(2026, 10, 26, 18, 0, 0, 0, time.UTC)
	if err != nil || stored.NextDueAt == nil || !stored.NextDueAt.Equal(wantNext) {
		t.Fatal("next due time did not advance past the imported scheduled local date")
	}
	claimed, err := db.Claim(ctx, importedDue, 30*time.Second, 1)
	if err != nil || len(claimed) != 1 || claimed[0].Envelope.OccurrenceID != occurrenceID {
		t.Fatal("could not claim the imported occurrence")
	}
	if err := db.Ack(ctx, occurrenceID, claimed[0].LeaseToken, "00000000-0000-4000-8000-000000000002", time.Now().UTC()); err != nil {
		t.Fatal("could not accept the imported occurrence")
	}
	if generated, err := db.GenerateDue(ctx, time.Date(2026, 10, 25, 18, 0, 0, 0, time.UTC), 10, 10); err != nil || generated != 0 {
		t.Fatal("scheduler generated a second occurrence on the imported local date")
	}
	var occurrenceCount int
	if err := db.Pool.QueryRow(ctx, `SELECT count(*) FROM `+db.outbox+` WHERE schedule_key=$1`, item.Key).Scan(&occurrenceCount); err != nil || occurrenceCount != 1 {
		t.Fatal("imported local date produced more than one occurrence")
	}
}
