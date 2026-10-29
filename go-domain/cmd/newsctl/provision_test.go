package main

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"github.com/jackc/pgx/v5"
)

// This test needs a dedicated disposable PostgreSQL server. Never point it at
// the ordinary application database: it provisions fixed installation names.
func TestFreshProvision(t *testing.T) {
	dsn := os.Getenv("NEWS_INSTALLER_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("dedicated installer fixture not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/news_fixture" {
		t.Fatal("installer fixture must be explicit loopback news_fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(context.Background())
	var clean bool
	if err = admin.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM pg_database WHERE datname IN ('newsdb','schedulerdb')) AND NOT EXISTS(SELECT 1 FROM pg_roles WHERE rolname='news_owner')`).Scan(&clean); err != nil || !clean {
		t.Fatal("installer requires a clean disposable server")
	}
	dir := t.TempDir()
	l := managedsetup.Layout{Setup: filepath.Join(dir, "setup"), Web: filepath.Join(dir, "web"), Domain: filepath.Join(dir, "domain"), Background: filepath.Join(dir, "background"), Extractor: filepath.Join(dir, "extractor"), Postgres: filepath.Join(dir, "pg"), PostgresSecrets: filepath.Join(dir, "pg-secret"), WebUID: -1, DomainUID: -1, BackgroundUID: -1, ExtractorUID: -1}
	if err = os.Mkdir(l.Postgres, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err = managedsetup.Prepare(l, "https://fixture.invalid"); err != nil {
		t.Fatal(err)
	}
	s, err := managedsetup.ReadState(l.Setup)
	if err != nil {
		t.Fatal(err)
	}
	var statement string
	if err = admin.QueryRow(ctx, `SELECT format('CREATE ROLE news_owner LOGIN SUPERUSER PASSWORD %L',$1::text)`, s.Passwords["owner"]).Scan(&statement); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, statement); err != nil {
		t.Fatal("fixture owner creation failed")
	}
	defer func() {
		cleanup, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		for _, db := range []string{"newsdb", "schedulerdb"} {
			if _, e := admin.Exec(cleanup, "DROP DATABASE IF EXISTS "+pgx.Identifier{db}.Sanitize()+" WITH (FORCE)"); e != nil {
				t.Error("fixture database cleanup failed")
			}
		}
		for _, role := range []string{"news_web_runtime", "news_domain_runtime", "news_worker_runtime", "news_execution_runtime", "news_ingestion_runtime", "news_processing_runtime", "scheduler_runtime", "news_ingestion_fence_owner", "news_processing_fence_owner", "news_owner"} {
			if _, e := admin.Exec(cleanup, "DROP ROLE IF EXISTS "+pgx.Identifier{role}.Sanitize()); e != nil {
				t.Error("fixture role cleanup failed", role)
			}
		}
	}()
	for i := 0; i < 2; i++ {
		if err = provisionAt(ctx, l, u.Host); err != nil {
			t.Fatalf("provision pass%d: %v", i, err)
		}
	}
	for key, role := range roleNames {
		db := "newsdb"
		if key == "scheduler" {
			db = "schedulerdb"
		}
		v := *u
		v.User = url.UserPassword(role, s.Passwords[key])
		v.Path = "/" + db
		q := v.Query()
		if key != "scheduler" {
			q.Set("search_path", "news")
		}
		v.RawQuery = q.Encode()
		conn, e := pgx.Connect(ctx, v.String())
		if e != nil {
			t.Fatalf("runtime role %s cannot connect", key)
		}
		if key != "scheduler" && key != "domain" {
			_, e = conn.Exec(ctx, `SELECT * FROM integration_credentials`)
			if e == nil {
				t.Errorf("%s read integration credentials", key)
			}
		}
		conn.Close(ctx)
	}
	if _, err = managedsetup.Prepare(l, s.Site); err != nil {
		t.Fatal(fmt.Errorf("repeat prepare: %w", err))
	}
	if os.Getenv("NEWS_INSTALLER_TEST_RECOVERY") == "1" {
		testRecoveryRoundTrip(t, ctx, l, s, u.Host, admin)
	}
}
