package store

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresOutcomeRejectsArticleChangedDuringAI(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for Go article processing PostgreSQL tests")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("article processing PostgreSQL tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect synthetic PostgreSQL: %v", err)
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("article_processing_test_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err := admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = admin.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE") }()
	statements := []string{
		`CREATE TABLE ` + q + `.article_processing_runs(id uuid primary key,state text not null)`,
		`CREATE TABLE ` + q + `.articles(id bigint primary key,title text not null,url text not null,content text,summary text,summary_processed boolean not null,category_processed boolean not null,ad_processed boolean not null,is_advertisement boolean,processing_attempts integer not null,processing_state text not null,next_attempt_at timestamp,last_processing_error text,processed boolean not null)`,
		`CREATE TABLE ` + q + `.article_processing_items(run_id uuid not null,article_id bigint not null,article_attempt integer not null,input_fingerprint text not null,claim_token uuid not null,state text not null,result jsonb not null,updated_at timestamp not null default now(),primary key(run_id,article_id))`,
		`CREATE FUNCTION ` + q + `.fence_article_processing_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp) RETURNS TABLE(run_id uuid,run_state text) LANGUAGE sql AS $$ SELECT r.id,r.state FROM ` + q + `.article_processing_runs r WHERE r.id=$1 $$`,
		`INSERT INTO ` + q + `.article_processing_runs(id,state) VALUES('10000000-0000-4000-8000-000000000001','running')`,
		`INSERT INTO ` + q + `.articles(id,title,url,content,summary,summary_processed,category_processed,ad_processed,is_advertisement,processing_attempts,processing_state,processed) VALUES(7,'Fixture title','https://fixture.invalid/a','source content','old summary',true,false,false,false,0,'pending',false)`,
	}
	for _, statement := range statements {
		if _, err := admin.Exec(ctx, statement); err != nil {
			t.Fatalf("setup isolated processing schema: %v", err)
		}
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns, cfg.MinConns = 2, 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	s := &Store{Pool: pool}
	article := Article{ID: 7, Title: "Fixture title", URL: "https://fixture.invalid/a", Content: "source content", Summary: "old summary", SummaryProcessed: true, IsAdvertisement: boolPointer(false)}
	f := Fence{RunID: "10000000-0000-4000-8000-000000000001", Envelope: Envelope{JobID: "20000000-0000-4000-8000-000000000001", AttemptGeneration: 1, OwnerID: "30000000-0000-4000-8000-000000000001", Generation: 1, BackendPID: 1, ExecutorInstanceID: "40000000-0000-4000-8000-000000000001", DeadlineAt: time.Now().Add(time.Hour)}}
	attempt, fp, token, err := s.ClaimArticle(ctx, f, article, false, time.Minute, 30*time.Minute)
	if err != nil || attempt != 1 {
		t.Fatalf("claim article: attempt=%d err=%v", attempt, err)
	}
	if _, err := admin.Exec(ctx, `UPDATE `+q+`.articles SET summary='admin correction' WHERE id=7`); err != nil {
		t.Fatal(err)
	}
	newSummary := "stale AI summary"
	err = s.SaveOutcome(ctx, f, article, attempt, token, fp, Persisted{State: "completed", Attempts: 1, Processed: true, Fields: map[string]any{"summary": newSummary, "summary_processed": true}})
	if err == nil {
		t.Fatal("stale AI outcome overwrote an article changed during processing")
	}
	var got string
	if err := admin.QueryRow(ctx, `SELECT summary FROM `+q+`.articles WHERE id=7`).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != "admin correction" {
		t.Fatalf("stale AI summary replaced concurrent edit: %q", got)
	}
}

func boolPointer(v bool) *bool { return &v }
