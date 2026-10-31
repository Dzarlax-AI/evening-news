package store

import (
	"errors"
	"testing"
)

func TestViberAdmissionFailureDoesNotBlockTelegramClaim(t *testing.T) {
	s, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `CREATE TABLE viber_admission_probe(marker integer); CREATE FUNCTION enqueue_viber_digest_delivery() RETURNS boolean LANGUAGE plpgsql AS $f$ BEGIN INSERT INTO viber_admission_probe VALUES(1); RAISE EXCEPTION 'synthetic Viber insert failure'; END $f$`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		id := insertTestJob(t, ctx, s, "queued", 0)
		if _, err := s.Pool.Exec(ctx, `UPDATE background_jobs SET kind='telegram_digest' WHERE id=$1::uuid`, id); err != nil {
			t.Fatal(err)
		}
		job, err := s.Claim(ctx, 3)
		if err != nil || job.ID != id || job.Kind != "telegram_digest" {
			t.Fatalf("Viber failure blocked unrelated Telegram claim: %v", err)
		}
		var n int
		if err = s.Pool.QueryRow(ctx, `SELECT count(*) FROM viber_admission_probe`).Scan(&n); err != nil || n != 0 {
			t.Fatal("failed Viber subtransaction leaked changes", err)
		}
		if !s.viberAdmissionFailed.Load() {
			t.Fatal("admission failure diagnostic missing")
		}
		if _, err = s.Pool.Exec(ctx, `UPDATE background_jobs SET state='succeeded' WHERE id=$1::uuid`, id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Pool.Exec(ctx, `CREATE OR REPLACE FUNCTION enqueue_viber_digest_delivery() RETURNS boolean LANGUAGE sql AS 'SELECT false'`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, 3); !errors.Is(err, ErrNoJob) {
		t.Fatal("claim did not recover after admission repair", err)
	}
	if s.viberAdmissionFailed.Load() {
		t.Fatal("recovered admission diagnostic remained set")
	}
}

func TestViberAdmissionReconciliationCommitsWithoutAJob(t *testing.T) {
	s, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Pool.Exec(ctx, `CREATE TABLE viber_reconciliation_probe(id integer,state text);INSERT INTO viber_reconciliation_probe VALUES(1,'pending_request'),(2,'expired'); CREATE FUNCTION enqueue_viber_digest_delivery() RETURNS boolean LANGUAGE plpgsql AS $f$ BEGIN UPDATE viber_reconciliation_probe SET state=CASE WHEN id=1 THEN 'waiting_edition' ELSE 'exhausted' END; RETURN false; END $f$`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Claim(ctx, 3); !errors.Is(err, ErrNoJob) {
		t.Fatal("expected an idle queue", err)
	}
	var waiting, exhausted int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state='waiting_edition'),count(*) FILTER(WHERE state='exhausted') FROM viber_reconciliation_probe`).Scan(&waiting, &exhausted); err != nil || waiting != 1 || exhausted != 1 {
		t.Fatal("idle queue rolled back reconciliation", err)
	}
}
