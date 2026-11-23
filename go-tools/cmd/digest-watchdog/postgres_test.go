package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresReader(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("requires isolated loopback TEST_DATABASE_URL")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid test database connection")
	}
	isLoopback := func(host string) bool {
		return host == "127.0.0.1" || host == "localhost" || host == "::1"
	}
	if !isLoopback(cfg.ConnConfig.Host) {
		t.Fatal("test database must be loopback")
	}
	for _, fallback := range cfg.ConnConfig.Fallbacks {
		if !isLoopback(fallback.Host) {
			t.Fatal("test database fallback must be loopback")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		t.Fatal("cannot connect isolated database")
	}
	defer admin.Close()
	schema := fmt.Sprintf("watchdog_test_%d", time.Now().UnixNano())
	quoted := `"` + schema + `"`
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal("cannot create test schema")
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+quoted+" CASCADE")
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	db, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	_, err = db.Exec(ctx, `CREATE TABLE schedules(schedule_key text, next_due_at timestamptz,enabled boolean,operation text,payload jsonb);
 CREATE TABLE control(id int,paused boolean);
 CREATE TABLE occurrences(schedule_key text,scheduled_for timestamptz,operation text,payload jsonb);
 CREATE TABLE background_jobs(id uuid,state text,digest_retry_at timestamp,attempt_generation int,payload jsonb,schedule_key text,scheduled_for timestamp,created_at timestamp);
 CREATE TABLE background_job_executions(job_id uuid,attempt_generation int,digest_delivery_started boolean);
 CREATE TABLE settings(key text,value json);
 CREATE TABLE editorial_digest_editions(id uuid,job_id uuid,locale text,attempt_generation int);
 CREATE TABLE digest_delivery_intents(id uuid,edition_id uuid,destination_hash text,state text);
 CREATE TABLE digest_delivery_parts(intent_id uuid,state text);
 CREATE TABLE viber_digest_deliveries(russian_edition_id uuid,channel_id text,state text);`)
	if err != nil {
		t.Fatal("cannot create test tables")
	}
	due := time.Now().UTC().Truncate(time.Second).Add(-31 * time.Minute)
	o := occurrence{Key: "news:test", Due: due, Enabled: true, SendTelegram: true}
	_, err = db.Exec(ctx, `INSERT INTO control VALUES(1,false);INSERT INTO schedules VALUES('news:test',$1,true,'news_digest','{}')`, pgx.QueryExecModeSimpleProtocol, due)
	if err != nil {
		t.Fatal("cannot seed schedule")
	}
	list, err := readSchedules(ctx, db, quoted)
	if err != nil || len(list) != 1 || !list[0].SendTelegram {
		t.Fatal("schedule query failed")
	}
	a, err := readJob(ctx, db, o, time.Now())
	if err != nil || decision(time.Now(), o, a, false) == "" {
		t.Fatal("missing job did not alert")
	}
	job := "10000000-0000-4000-8000-000000000001"
	edition := "20000000-0000-4000-8000-000000000001"
	intent := "30000000-0000-4000-8000-000000000001"
	_, err = db.Exec(ctx, `INSERT INTO background_jobs VALUES($1,'succeeded',NULL,1,'{}','news:test',$2,now());
 INSERT INTO settings VALUES($3,$4::json);INSERT INTO editorial_digest_editions VALUES($5,$1,'ru',1);INSERT INTO digest_delivery_intents VALUES($6,$5,$7,'completed')`, pgx.QueryExecModeSimpleProtocol, job, due, fmt.Sprintf("managed_job_config:%s:1", job), `{"settings":{"delivery_enabled":true,"serbian_enabled":false},"telegram":{"chat_id":"expected"}}`, edition, intent, fmt.Sprintf("%x", sha256.Sum256([]byte("wrong"))))
	if err != nil {
		t.Fatal("cannot seed delivery")
	}
	a, err = readJob(ctx, db, o, time.Now())
	if err != nil || decision(time.Now(), o, a, false) == "" {
		t.Fatal("wrong destination counted as delivered")
	}
	expected := fmt.Sprintf("%x", sha256.Sum256([]byte("expected")))
	if _, err = db.Exec(ctx, `UPDATE digest_delivery_intents SET destination_hash=$1`, expected); err != nil {
		t.Fatal("cannot update receipt")
	}
	a, err = readJob(ctx, db, o, time.Now())
	if err != nil || decision(time.Now(), o, a, false) != "" {
		t.Fatal("confirmed receipt not recognized")
	}
	if _, err = db.Exec(ctx, `UPDATE settings SET value='{"settings":{"delivery_enabled":true,"serbian_enabled":true},
		"telegram":{"chat_id":"expected","serbian_chat_id":"serbian","serbian_delivery_enabled":true},
		"viber":{"delivery_enabled":true,"webhook_ready":true,"channel_id":"viber"}}'::json`); err != nil {
		t.Fatal("cannot enable second locale")
	}
	a, err = readJob(ctx, db, o, time.Now())
	if err != nil || len(a.missing) != 2 || a.missing[0] != "Telegram sr-Latn" || a.missing[1] != "Viber sr-Latn" {
		t.Fatalf("partial delivery assessment: %+v %v", a, err)
	}
	if _, err = db.Exec(ctx, `UPDATE settings SET value='{"settings":{"delivery_enabled":true,"serbian_enabled":false},
		"telegram":{"chat_id":"expected"}}'::json`); err != nil {
		t.Fatal("cannot restore single locale")
	}
	if _, err = db.Exec(ctx, `UPDATE editorial_digest_editions SET job_id='10000000-0000-4000-8000-000000000099';
		INSERT INTO settings VALUES($1, json_build_object('job_id',$2::text,'attempt_generation',1,'russian_edition_id',$3::text))`,
		pgx.QueryExecModeSimpleProtocol, "managed_digest_publication:"+job+":1", job, edition); err != nil {
		t.Fatal("cannot pin edition saved by another job")
	}
	a, err = readJob(ctx, db, o, time.Now())
	if err != nil || decision(time.Now(), o, a, false) != "" {
		t.Fatal("receipt for reused edition not recognized")
	}
	if _, err = db.Exec(ctx, `UPDATE background_jobs SET state='delivery_unknown'; INSERT INTO background_job_executions VALUES($1,1,NULL)`, pgx.QueryExecModeSimpleProtocol, job); err != nil {
		t.Fatal("cannot seed uncertain job")
	}
	a, err = readJob(ctx, db, o, time.Now())
	if err != nil || a.unknown || decision(time.Now(), o, a, false) != "" {
		t.Fatal("completed receipt did not resolve job uncertainty")
	}
	if _, err = db.Exec(ctx, `UPDATE digest_delivery_intents SET state='active';UPDATE background_job_executions SET digest_delivery_started=false`); err != nil {
		t.Fatal("cannot seed pre-delivery state")
	}
	a, err = readJob(ctx, db, o, time.Now())
	if err != nil || a.unknown || decision(due.Add(time.Minute), o, a, false) != "" {
		t.Fatal("pre-delivery failure treated as unknown")
	}
	if _, err = db.Exec(ctx, `UPDATE background_jobs SET state='failed',digest_retry_at=$1`, due.Add(10*time.Minute)); err != nil {
		t.Fatal("cannot seed retry")
	}
	a, err = readJob(ctx, db, o, due.Add(15*time.Minute))
	if err != nil || !a.pending || decision(due.Add(15*time.Minute), o, a, false) != "" {
		t.Fatal("safe retry did not wait")
	}
	if _, err = db.Exec(ctx, `UPDATE background_jobs SET state='delivery_unknown',digest_retry_at=NULL;UPDATE background_job_executions SET digest_delivery_started=true`); err != nil {
		t.Fatal("cannot seed uncertain outcome")
	}
	if _, err = db.Exec(ctx, `UPDATE digest_delivery_intents SET state='active';INSERT INTO digest_delivery_parts VALUES($1,'unknown')`, pgx.QueryExecModeSimpleProtocol, intent); err != nil {
		t.Fatal("cannot seed unknown receipt")
	}
	a, err = readJob(ctx, db, o, time.Now())
	if err != nil || !a.unknown || decision(due.Add(time.Minute), o, a, false) == "" {
		t.Fatal("unknown delivery did not alert immediately")
	}
	if _, err = db.Exec(ctx, `UPDATE control SET paused=true;UPDATE schedules SET enabled=false`); err != nil {
		t.Fatal("cannot pause schedule")
	}
	st := state{map[string]occurrence{key(o): o}, map[string]bool{}}
	if err = refreshControls(ctx, db, quoted, &st); err != nil {
		t.Fatal("cannot refresh controls")
	}
	if decision(time.Now(), st.Occurrences[key(o)], a, false) != "" {
		t.Fatal("disabled/paused schedule alerted")
	}

	t.Setenv("WATCHDOG_STATE_FILE", filepath.Join(t.TempDir(), "state.json"))
	t.Setenv("WATCHDOG_SCHEDULER_DATABASE_URL", dsn)
	t.Setenv("WATCHDOG_NEWS_DATABASE_URL", dsn)
	t.Setenv("WATCHDOG_TELEGRAM_TOKEN", "fixture")
	t.Setenv("WATCHDOG_TELEGRAM_CHAT_ID", "fixture")
	t.Setenv("WATCHDOG_MONITOR_FROM", due.Format(time.RFC3339))
	t.Setenv("WATCHDOG_SCHEDULER_SCHEMA", schema+"_missing")
	t.Setenv("WATCHDOG_ARCHIVE_URL", "")
	if err := run(); err == nil || err.Error() != "cannot read digest schedules" {
		t.Fatalf("scheduler startup error reported incorrectly: %v", err)
	}

}
