package viberdelivery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type testFence struct {
	fail bool
	job  *string
	id   *string
}

func (f testFence) ViberFenceIdentity() (string, int, string, json.RawMessage) {
	if *f.id == "" {
		return *f.job, 1, "news_digest", json.RawMessage(`{}`)
	}
	b, _ := json.Marshal(map[string]string{"delivery_id": *f.id})
	return *f.job, 1, "viber_digest_delivery", b
}

func (f testFence) Validate(context.Context, pgx.Tx) error {
	if f.fail {
		return ErrConflict
	}
	return nil
}
func TestPostgresState(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL unavailable")
	}
	u, e := url.Parse(dsn)
	if e != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("loopback database required")
	}
	for k := range u.Query() {
		switch strings.ToLower(k) {
		case "host", "hostaddr", "service", "servicefile":
			t.Fatal("invalid database routing")
		}
	}
	ctx := context.Background()
	admin, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("viber_test_%d", time.Now().UnixNano())
	_, e = admin.Exec(ctx, "CREATE SCHEMA "+schema)
	if e != nil {
		t.Fatal(e)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	ddl := `CREATE TABLE integration_credentials(provider text primary key CHECK(provider IN ('gemini','openai','telegram')),revision bigint,ciphertext bytea);
 CREATE TABLE settings(key text primary key,value jsonb,updated_at timestamptz);
 CREATE TABLE editorial_digest_editions(id uuid primary key,job_id uuid,attempt_generation int,locale text,created_at timestamp,snapshot_hash text,edition jsonb,UNIQUE(job_id,attempt_generation,locale));
 CREATE TABLE background_jobs(id uuid primary key,kind text,payload jsonb,idempotency_key text,state text);`
	if _, e = pool.Exec(ctx, ddl); e != nil {
		t.Fatal(e)
	}
	sql, e := os.ReadFile("../migration/sql/020.sql")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, strings.ReplaceAll(string(sql), "__SCHEMA_Q__", pgx.Identifier{schema}.Sanitize())); e != nil {
		t.Fatal(e)
	}
	const ru = "10000000-0000-4000-8000-000000000001"
	const sr = "10000000-0000-4000-8000-000000000002"
	const job = "10000000-0000-4000-8000-000000000003"
	const instance = "10000000-0000-4000-8000-000000000004"
	_, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions VALUES($1,$3,1,'ru',now(),'hash','{}'),($2,$3,1,'sr-Latn',now(),'hash','{}'); INSERT INTO integration_credentials VALUES('viber',1,'\\x01'); INSERT INTO settings VALUES('viber_settings','{"delivery_enabled":true,"credential_revision":1,"verified_credential_revision":1,"webhook_ready":true,"channel_id":"channel","sender_id":"sender"}',now())`, pgx.QueryExecModeSimpleProtocol, ru, sr, job)
	if e != nil {
		t.Fatal(e)
	}
	activation := time.Now().UTC().Add(-72 * time.Hour).Truncate(time.Microsecond)
	if _, e = pool.Exec(ctx, `UPDATE settings SET value=jsonb_set(value,'{enabled_from}',to_jsonb($1::timestamptz)) WHERE key='viber_settings'`, activation); e != nil {
		t.Fatal(e)
	}
	s := PostgresStore{Pool: pool, Fence: testFence{}, JobID: job, AttemptGeneration: 1, DomainInstanceID: instance}
	var binding string
	s.Fence = testFence{job: &s.JobID, id: &binding}
	id, e := s.Bind(ctx, BindInput{RussianEditionID: ru, ChannelID: "channel", SenderID: "sender", SettingsRevision: 1, CredentialRevision: 1, EnabledFrom: activation})
	if e != nil {
		t.Fatal(e)
	}
	binding = id
	var enqueued bool
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatalf("enqueue: %v", e)
	}
	if e = pool.QueryRow(ctx, `SELECT last_job_id::text FROM viber_digest_deliveries WHERE id=$1`, id).Scan(&s.JobID); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='running' WHERE id=$1`, s.JobID); e != nil {
		t.Fatal(e)
	}
	if e = s.Materialize(ctx, id, "v1", []PartInput{{"first"}, {"second"}}); e != nil {
		t.Fatal(e)
	}
	d, e := s.Load(ctx, id)
	if e != nil || len(d.Parts) != 2 || d.SerbianEditionID != sr {
		t.Fatalf("load: %v %+v", e, d)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_digest_parts SET text='changed' WHERE delivery_id=$1`, id); e == nil {
		t.Fatal("immutable part changed")
	}
	binding = "10000000-0000-4000-8000-000000000099"
	if _, e = s.Reserve(ctx, id, 0, d.Parts[0].Hash); !errors.Is(e, ErrConflict) {
		t.Fatal("mismatched job payload accepted")
	}
	binding = id
	var wg sync.WaitGroup
	results := make(chan Reservation, 2)
	failures := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); r, e := s.Reserve(ctx, id, 0, d.Parts[0].Hash); results <- r; failures <- e }()
	}
	wg.Wait()
	close(results)
	close(failures)
	for e := range failures {
		if e != nil {
			t.Fatal(e)
		}
	}
	reserved, flight := 0, 0
	for r := range results {
		if r == Reserved {
			reserved++
		}
		if r == InFlight {
			flight++
		}
	}
	if reserved != 1 || flight != 1 {
		t.Fatal("reservation race", reserved, flight)
	}
	var r Reservation
	if r, e = s.Reserve(ctx, id, 0, d.Parts[0].Hash); e != nil || r != InFlight {
		t.Fatalf("repeat %s %v", r, e)
	}
	// Another binding for the same channel must wait even after the per-post timer elapses.
	const ru2 = "10000000-0000-4000-8000-000000000011"
	const sr2 = "10000000-0000-4000-8000-000000000012"
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions VALUES($1,$3,2,'ru',now(),'hash','{}'),($2,$3,2,'sr-Latn',now(),'hash','{}')`, ru2, sr2, job); e != nil {
		t.Fatal(e)
	}
	s2 := PostgresStore{Pool: pool, JobID: job, AttemptGeneration: 1, DomainInstanceID: instance}
	var binding2 string
	s2.Fence = testFence{job: &s2.JobID, id: &binding2}
	id2, e := s2.Bind(ctx, BindInput{RussianEditionID: ru2, ChannelID: "channel", SenderID: "sender", SettingsRevision: 1, CredentialRevision: 1, EnabledFrom: activation})
	if e != nil {
		t.Fatal(e)
	}
	binding2 = id2
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatal("second enqueue", e)
	}
	if e = pool.QueryRow(ctx, `SELECT last_job_id::text FROM viber_digest_deliveries WHERE id=$1`, id2).Scan(&s2.JobID); e != nil {
		t.Fatal(e)
	}
	if e = s2.Materialize(ctx, id2, "v1", []PartInput{{"other"}}); e != nil {
		t.Fatal(e)
	}
	d2, e := s2.Load(ctx, id2)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_channel_leases SET next_post_at=now()-interval '1 second'`); e != nil {
		t.Fatal(e)
	}
	if r, e = s2.Reserve(ctx, id2, 0, d2.Parts[0].Hash); e != nil || r != Wait {
		t.Fatalf("channel lease %s %v", r, e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO settings VALUES('operational_delivery_enabled','false',now())`); e != nil {
		t.Fatal(e)
	}
	if r, e = s2.Reserve(ctx, id2, 0, d2.Parts[0].Hash); e != nil || r != InFlight {
		t.Fatalf("global disable %s %v", r, e)
	}
	d2, e = s2.Load(ctx, id2)
	if e != nil || d2.State != "cancelled" {
		t.Fatalf("global disable state %s %v", d2.State, e)
	}
	if _, e = pool.Exec(ctx, `DELETE FROM settings WHERE key='operational_delivery_enabled'`); e != nil {
		t.Fatal(e)
	}

	if e = s.Finish(ctx, id, 0, Result{State: Sent, MessageToken: "123"}); e != nil {
		t.Fatal(e)
	}
	if r, e = s.Reserve(ctx, id, 0, d.Parts[0].Hash); e != nil || r != AlreadySent {
		t.Fatalf("sent %s %v", r, e)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_channel_leases SET next_post_at=now()-interval '1 second'`); e != nil {
		t.Fatal(e)
	}
	if r, e = s.Reserve(ctx, id, 1, d.Parts[1].Hash); e != nil || r != Reserved {
		t.Fatalf("second %s %v", r, e)
	}
	if e = s.Finish(ctx, id, 1, Result{State: Rejected, StatusCode: 429}); e != nil {
		t.Fatal(e)
	}
	var seconds float64
	if e = pool.QueryRow(ctx, `SELECT EXTRACT(EPOCH FROM (next_dispatch_at-clock_timestamp())) FROM viber_digest_deliveries WHERE id=$1`, id).Scan(&seconds); e != nil || seconds < 25 {
		t.Fatalf("cooldown %f %v", seconds, e)
	}
	if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='failed' WHERE id=$1`, s.JobID); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || enqueued {
		t.Fatal("early retry", e)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_channel_leases SET cooldown_until=now()-interval '1 second',next_post_at=now()-interval '1 second'; UPDATE viber_digest_deliveries SET next_dispatch_at=now()-interval '1 second' WHERE id=$1`, pgx.QueryExecModeSimpleProtocol, id); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatal("retry not enqueued", e)
	}
	if e = pool.QueryRow(ctx, `SELECT last_job_id::text FROM viber_digest_deliveries WHERE id=$1`, id).Scan(&s.JobID); e != nil {
		t.Fatal(e)
	}
	if r, e = s.Reserve(ctx, id, 0, d.Parts[0].Hash); e != nil || r != AlreadySent {
		t.Fatalf("retry resent first %s %v", r, e)
	}
	if r, e = s.Reserve(ctx, id, 1, d.Parts[1].Hash); e != nil || r != Reserved {
		t.Fatalf("retry reserve %s %v", r, e)
	}
	if e = s.Finish(ctx, id, 1, Result{State: Unknown}); e != nil {
		t.Fatal(e)
	}
	d, e = s.Load(ctx, id)
	if e != nil || d.State != "delivery_unknown" {
		t.Fatalf("unknown: %+v %v", d, e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || enqueued {
		t.Fatalf("unknown requeued %v", e)
	}
	// A delayed Serbian edition is paired by the originating generation, not date.
	const ru3 = "10000000-0000-4000-8000-000000000021"
	const sr3 = "10000000-0000-4000-8000-000000000022"
	const other = "10000000-0000-4000-8000-000000000023"
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions VALUES($1,$2,3,'ru',now()-interval '1 day','hash','{}'),($3,$2,4,'sr-Latn',now(),'other','{}')`, ru3, job, other); e != nil {
		t.Fatal(e)
	}
	s3 := PostgresStore{Pool: pool, JobID: job, AttemptGeneration: 1, DomainInstanceID: instance}
	var binding3 string
	s3.Fence = testFence{job: &s3.JobID, id: &binding3}
	id3, e := s3.Bind(ctx, BindInput{RussianEditionID: ru3, ChannelID: "channel", SenderID: "sender", SettingsRevision: 1, CredentialRevision: 1, EnabledFrom: activation})
	if e != nil {
		t.Fatal(e)
	}
	binding3 = id3
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || enqueued {
		t.Fatal("wrong Serbian edition paired", e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions VALUES($1,$2,3,'sr-Latn',now(),'hash','{}')`, sr3, job); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatal("matching delayed edition not promoted", e)
	}
	var pair string
	if e = pool.QueryRow(ctx, `SELECT serbian_edition_id::text FROM viber_digest_deliveries WHERE id=$1`, id3).Scan(&pair); e != nil || pair != sr3 {
		t.Fatal("wrong paired edition", e)
	}
	if e = pool.QueryRow(ctx, `SELECT last_job_id::text FROM viber_digest_deliveries WHERE id=$1`, id3).Scan(&s3.JobID); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='running' WHERE id=$1; UPDATE viber_channel_leases SET next_post_at=now()-interval '1 second',cooldown_until=now()-interval '1 second'`, pgx.QueryExecModeSimpleProtocol, s3.JobID); e != nil {
		t.Fatal(e)
	}
	if e = s3.Materialize(ctx, id3, "v1", []PartInput{{"third"}}); e != nil {
		t.Fatal(e)
	}
	d3, e := s3.Load(ctx, id3)
	if e != nil {
		t.Fatal(e)
	}
	// A manual test holding the same channel blocks digest POST reservations.
	const testID = "10000000-0000-4000-8000-000000000098"
	if _, e = pool.Exec(ctx, `INSERT INTO viber_test_receipts(request_id,channel_id,sender_id,settings_revision,credential_revision,state) VALUES($1,'channel','sender',1,1,'sending'); UPDATE viber_channel_leases SET delivery_id=NULL,test_request_id=$1,lease_until=now()+interval '1 minute',next_post_at=now()-interval '1 second',cooldown_until=now()-interval '1 second' WHERE channel_id='channel'`, pgx.QueryExecModeSimpleProtocol, testID); e != nil {
		t.Fatal(e)
	}
	if r, e = s3.Reserve(ctx, id3, 0, d3.Parts[0].Hash); e != nil || r != Wait {
		t.Fatalf("manual test lease did not block digest: %s %v", r, e)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_channel_leases SET test_request_id=NULL,lease_until=NULL WHERE channel_id='channel'`); e != nil {
		t.Fatal(e)
	}
	if r, e = s3.Reserve(ctx, id3, 0, d3.Parts[0].Hash); e != nil || r != Reserved {
		t.Fatalf("takeover setup %s %v", r, e)
	}
	if e = s3.CheckSendAuthority(ctx, id3, 0); e != nil {
		t.Fatalf("valid pre-send authority: %v", e)
	}
	if _, e = pool.Exec(ctx, `UPDATE settings SET value=jsonb_set(value,'{enabled_from}',to_jsonb($1::timestamptz)) WHERE key='viber_settings'`, activation.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if e = s3.CheckSendAuthority(ctx, id3, 0); e == nil {
		t.Fatal("changed epoch authorized POST")
	}
	if _, e = pool.Exec(ctx, `UPDATE settings SET value=jsonb_set(value,'{enabled_from}',to_jsonb($1::timestamptz)) WHERE key='viber_settings'`, activation); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_channel_leases SET lease_until=clock_timestamp()-interval '1 second' WHERE delivery_id=$1`, id3); e != nil {
		t.Fatal(e)
	}
	if e = s3.CheckSendAuthority(ctx, id3, 0); e == nil {
		t.Fatal("expired lease authorized POST")
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_channel_leases SET lease_until=clock_timestamp()+interval '60 seconds' WHERE delivery_id=$1`, id3); e != nil {
		t.Fatal(e)
	}
	s3.DomainInstanceID = "10000000-0000-4000-8000-000000000099"
	d3, e = s3.Load(ctx, id3)
	if e != nil || d3.State != "delivery_unknown" {
		t.Fatalf("takeover %s %v", d3.State, e)
	}
	// A transient binding failure leaves a durable request for worker admission.
	const ru4 = "10000000-0000-4000-8000-000000000031"
	const sr4 = "10000000-0000-4000-8000-000000000032"
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions VALUES($1,$3,5,'ru',now(),'hash','{}'),($2,$3,5,'sr-Latn',now(),'hash','{}')`, ru4, sr4, job); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `CREATE FUNCTION deny_binding_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER deny_binding_fixture BEFORE INSERT ON viber_digest_deliveries FOR EACH ROW EXECUTE FUNCTION deny_binding_fixture()`); e != nil {
		t.Fatal(e)
	}
	s4 := PostgresStore{Pool: pool, JobID: job, AttemptGeneration: 1, DomainInstanceID: instance}
	var binding4 string
	s4.Fence = testFence{job: &s4.JobID, id: &binding4}
	input4 := BindInput{RussianEditionID: ru4, ChannelID: "channel", SenderID: "sender", SettingsRevision: 1, CredentialRevision: 1, EnabledFrom: activation}
	if e = s4.Request(ctx, input4); e != nil {
		t.Fatal("request not durable", e)
	}
	var requestState string
	if e = pool.QueryRow(ctx, `SELECT state FROM viber_publication_requests WHERE russian_edition_id=$1`, ru4).Scan(&requestState); e != nil || requestState != "pending" {
		t.Fatal("pending request missing", e)
	}
	if _, e = pool.Exec(ctx, `DROP TRIGGER deny_binding_fixture ON viber_digest_deliveries;DROP FUNCTION deny_binding_fixture()`); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatal("outbox recovery failed", e)
	}
	if e = pool.QueryRow(ctx, `SELECT d.id::text,d.last_job_id::text,r.state FROM viber_digest_deliveries d JOIN viber_publication_requests r ON r.russian_edition_id=d.russian_edition_id WHERE d.russian_edition_id=$1`, ru4).Scan(&binding4, &s4.JobID, &requestState); e != nil || requestState != "materialized" {
		t.Fatal("request not materialized", e)
	}
	if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='running' WHERE id=$1`, s4.JobID); e != nil {
		t.Fatal(e)
	}
	if e = s4.Materialize(ctx, binding4, "v1", []PartInput{{"expired"}}); e != nil {
		t.Fatal(e)
	}
	d4, e := s4.Load(ctx, binding4)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_digest_deliveries SET created_at=now()-interval '25 hours' WHERE id=$1`, binding4); e != nil {
		t.Fatal(e)
	}
	if r, e = s4.Reserve(ctx, binding4, 0, d4.Parts[0].Hash); e != nil || r != InFlight {
		t.Fatal("expired queued job reserved", e)
	}
	d4, e = s4.Load(ctx, binding4)
	if e != nil || d4.State != "exhausted" {
		t.Fatal("expired delivery not exhausted", e)
	}
	// A durable pending request is invalidated when its activation epoch changes.
	const ru6 = "10000000-0000-4000-8000-000000000051"
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions VALUES($1,$2,7,'ru',now(),'hash','{}')`, ru6, job); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `CREATE FUNCTION deny_binding_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER deny_binding_fixture BEFORE INSERT ON viber_digest_deliveries FOR EACH ROW EXECUTE FUNCTION deny_binding_fixture()`); e != nil {
		t.Fatal(e)
	}
	s6 := PostgresStore{Pool: pool, JobID: job, AttemptGeneration: 1, DomainInstanceID: instance}
	var binding6 string
	s6.Fence = testFence{job: &s6.JobID, id: &binding6}
	input6 := input4
	input6.RussianEditionID = ru6
	if e = s6.Request(ctx, input6); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `DROP TRIGGER deny_binding_fixture ON viber_digest_deliveries;DROP FUNCTION deny_binding_fixture()`); e != nil {
		t.Fatal(e)
	}
	// A queued binding and an older pinned request cannot revive across a switch cycle.
	const ru5 = "10000000-0000-4000-8000-000000000041"
	const sr5 = "10000000-0000-4000-8000-000000000042"
	const ru7 = "10000000-0000-4000-8000-000000000061"
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions VALUES($1,$3,6,'ru',now(),'hash','{}'),($2,$3,6,'sr-Latn',now(),'hash','{}'),($4,$3,8,'ru',now(),'hash','{}')`, ru5, sr5, job, ru7); e != nil {
		t.Fatal(e)
	}
	s5 := PostgresStore{Pool: pool, JobID: job, AttemptGeneration: 1, DomainInstanceID: instance}
	var binding5 string
	s5.Fence = testFence{job: &s5.JobID, id: &binding5}
	input5 := input4
	input5.RussianEditionID = ru5
	binding5, e = s5.Bind(ctx, input5)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_digest_deliveries SET last_job_id=$2 WHERE id=$1`, binding5, job); e != nil {
		t.Fatal(e)
	}
	if e = s5.Materialize(ctx, binding5, "v1", []PartInput{{"old queued"}}); e != nil {
		t.Fatal(e)
	}
	d5, e := s5.Load(ctx, binding5)
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO settings VALUES('operational_delivery_enabled','false',now()); UPDATE settings SET value=jsonb_set(value,'{enabled_from}',to_jsonb($1::timestamptz)) WHERE key='viber_settings'; UPDATE settings SET value='true' WHERE key='operational_delivery_enabled'`, pgx.QueryExecModeSimpleProtocol, activation.Add(time.Second)); e != nil {
		t.Fatal(e)
	}
	if r, e = s5.Reserve(ctx, binding5, 0, d5.Parts[0].Hash); e != nil || r != InFlight {
		t.Fatal("stale queued binding reserved", e)
	}
	d5, e = s5.Load(ctx, binding5)
	if e != nil || d5.State != "configuration_changed" {
		t.Fatal("stale queued binding not stopped", e)
	}
	input7 := input4
	input7.RussianEditionID = ru7
	if e = s6.Request(ctx, input7); e != nil {
		t.Fatal(e)
	}
	if _, e = s6.Bind(ctx, input7); !errors.Is(e, ErrConflict) {
		t.Fatal("stale activation bound", e)
	}

	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT state FROM viber_publication_requests WHERE russian_edition_id=$1`, ru6).Scan(&requestState); e != nil || requestState != "cancelled" {
		t.Fatal("stale outbox request not cancelled", e)
	}
	var count int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM viber_publication_requests WHERE russian_edition_id=$1`, ru7).Scan(&count); e != nil || count != 0 {
		t.Fatal("stale activation queued", e)
	}
	// Viber-only request write was unavailable: the worker reconstructs from the
	// shared publication and frozen configuration even after the parent failed.
	const recoveryJob = "40000000-0000-4000-8000-000000000001"
	const ru8 = "40000000-0000-4000-8000-000000000002"
	const sr8 = "40000000-0000-4000-8000-000000000003"
	if _, e = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state) VALUES($1,'news_digest','{}','recovery','failed')`, recoveryJob); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions VALUES($1,$3,1,'ru',now(),'hash','{}'),($2,$3,1,'sr-Latn',now(),'hash','{}')`, ru8, sr8, recoveryJob); e != nil {
		t.Fatal(e)
	}
	selected, _ := json.Marshal(map[string]any{"job_id": recoveryJob, "attempt_generation": 1, "russian_edition_id": ru8, "publication_at": time.Now().UTC().Add(-time.Second)})
	config, _ := json.Marshal(map[string]any{"job_id": recoveryJob, "attempt_generation": 1, "settings": map[string]any{"delivery_enabled": true, "serbian_enabled": true}, "viber": map[string]any{"revision": 1, "delivery_enabled": true, "channel_id": "channel", "sender_id": "sender", "credential_revision": 1, "verified_credential_revision": 1, "webhook_ready": true, "enabled_from": activation.Add(time.Second)}})
	if _, e = pool.Exec(ctx, `INSERT INTO settings VALUES($1,$2::jsonb,now()),($3,$4::jsonb,now())`, "managed_digest_publication:"+recoveryJob+":1", selected, "managed_job_config:"+recoveryJob+":1", config); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatalf("shared publication not recovered %v", e)
	}
	var recoveredRU, recoveredSR, recoveredState string
	if e = pool.QueryRow(ctx, `SELECT d.russian_edition_id::text,d.serbian_edition_id::text,r.state FROM viber_digest_deliveries d JOIN viber_publication_requests r ON r.russian_edition_id=d.russian_edition_id WHERE d.russian_edition_id=$1`, ru8).Scan(&recoveredRU, &recoveredSR, &recoveredState); e != nil || recoveredRU != ru8 || recoveredSR != sr8 || recoveredState != "materialized" {
		t.Fatal("wrong recovered binding", e)
	}
	// An explicitly disabled publication is never backfilled.
	const disabledJob = "50000000-0000-4000-8000-000000000001"
	const ru9 = "50000000-0000-4000-8000-000000000002"
	if _, e = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state) VALUES($1,'news_digest','{"send_telegram":false}','disabled','failed')`, disabledJob); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions VALUES($1,$2,1,'ru',now(),'hash','{}')`, ru9, disabledJob); e != nil {
		t.Fatal(e)
	}
	selected, _ = json.Marshal(map[string]any{"job_id": disabledJob, "attempt_generation": 1, "russian_edition_id": ru9, "publication_at": time.Now().UTC().Add(-time.Second)})
	config, _ = json.Marshal(map[string]any{"job_id": disabledJob, "attempt_generation": 1, "settings": map[string]any{"delivery_enabled": true, "serbian_enabled": true}, "viber": map[string]any{"revision": 1, "delivery_enabled": true, "channel_id": "channel", "sender_id": "sender", "credential_revision": 1, "verified_credential_revision": 1, "webhook_ready": true, "enabled_from": activation.Add(time.Second)}})
	if _, e = pool.Exec(ctx, `INSERT INTO settings VALUES($1,$2::jsonb,now()),($3,$4::jsonb,now())`, "managed_digest_publication:"+disabledJob+":1", selected, "managed_job_config:"+disabledJob+":1", config); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM viber_publication_requests WHERE russian_edition_id=$1`, ru9).Scan(&count); e != nil || count != 0 {
		t.Fatal("disabled publication was reconstructed", e)
	}
}
