package migration

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Exercise the actual embedded installer policy against a migrated, isolated
// schema. SET ROLE verifies denial even though the fixture connection is owner.
func TestManagedRolePrivileges(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not configured")
	}
	verifyLoopback(t, dsn)
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(nonce[:])
	schema := "managed_" + suffix
	ident := func(s string) string { return pgx.Identifier{s}.Sanitize() }
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+ident(schema)); err != nil {
		t.Fatal(err)
	}
	roles := map[string]string{}
	defer func() {
		_, _ = admin.Exec(ctx, "DROP SCHEMA "+ident(schema)+" CASCADE")
		for _, role := range roles {
			if _, err := admin.Exec(ctx, "DROP ROLE "+ident(role)); err != nil {
				t.Error(err)
			}
		}
	}()
	for _, name := range []string{"news_web_runtime", "news_domain_runtime", "news_worker_runtime", "news_execution_runtime", "news_ingestion_runtime", "news_processing_runtime", "news_ingestion_fence_owner", "news_processing_fence_owner"} {
		role := "m_" + strings.TrimPrefix(name, "news_") + "_" + suffix
		if _, err = admin.Exec(ctx, "CREATE ROLE "+ident(role)+" NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOBYPASSRLS"); err != nil {
			t.Fatal(err)
		}
		roles[name] = role
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if result, err := Run(ctx, pool, true, false); err != nil || len(result.Errors) > 0 {
		t.Fatalf("migrate: %v %+v", err, result.Errors)
	}
	_, here, _, _ := runtime.Caller(0)
	raw, err := os.ReadFile(filepath.Join(filepath.Dir(here), "../../go-tools/managedsetup/grants.sql"))
	if err != nil {
		t.Fatal(err)
	}
	sql := strings.ReplaceAll(string(raw), "news.", ident(schema)+".")
	sql = strings.ReplaceAll(sql, "SCHEMA news ", "SCHEMA "+ident(schema)+" ")
	for name, role := range roles {
		sql = strings.ReplaceAll(sql, name, ident(role))
	}
	// Applying the same policy twice is required by idempotent provisioning.
	for i := 0; i < 2; i++ {
		if _, err = pool.Exec(ctx, sql); err != nil {
			t.Fatalf("policy application %d: %v", i, err)
		}
	}
	for name, role := range roles {
		var elevated bool
		if err = pool.QueryRow(ctx, `SELECT rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls OR rolinherit FROM pg_roles WHERE rolname=$1`, role).Scan(&elevated); err != nil || elevated {
			t.Fatalf("elevated role %s: %v", name, err)
		}
	}
	for name, queries := range map[string][]string{
		"news_web_runtime":        {"SELECT * FROM integration_credentials", "UPDATE background_worker_status SET generation=99", "UPDATE articles SET content='changed'"},
		"news_domain_runtime":     {"SELECT * FROM managed_admin", "SELECT * FROM setup_tokens", "UPDATE background_worker_status SET generation=99", "UPDATE background_jobs SET state='failed'", "DELETE FROM articles"},
		"news_worker_runtime":     {"SELECT * FROM integration_credentials", "SELECT * FROM managed_admin", "UPDATE articles SET content='changed'"},
		"news_execution_runtime":  {"SELECT * FROM integration_credentials", "UPDATE background_worker_status SET generation=99", "INSERT INTO articles(title,url) VALUES('x','https://fixture.invalid')"},
		"news_ingestion_runtime":  {"SELECT * FROM integration_credentials", "UPDATE background_jobs SET state='failed'", "UPDATE articles SET summary='changed'"},
		"news_processing_runtime": {"SELECT * FROM integration_credentials", "UPDATE background_jobs SET state='failed'", "DELETE FROM articles"},
	} {
		for _, query := range queries {
			t.Run(name+"/"+query, func(t *testing.T) {
				tx, err := pool.Begin(ctx)
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback(ctx)
				if _, err = tx.Exec(ctx, "SET LOCAL ROLE "+ident(roles[name])); err != nil {
					t.Fatal(err)
				}
				_, err = tx.Exec(ctx, query)
				requireDenied(t, err)
			})
		}
	}
	for name, query := range map[string]string{
		"news_web_runtime":       "SELECT * FROM managed_admin; SELECT * FROM installation_state; SELECT * FROM sources; SELECT * FROM editorial_digest_editions; SELECT j.id FROM background_jobs j JOIN background_job_idempotency_keys k ON k.job_id=j.id FOR UPDATE; SELECT occurrence_id FROM scheduler_occurrence_receipts FOR UPDATE",
		"news_domain_runtime":    "SELECT * FROM integration_credentials; SELECT * FROM background_worker_status FOR UPDATE; SELECT * FROM article_processing_items FOR UPDATE",
		"news_worker_runtime":    "SELECT * FROM background_worker_status FOR UPDATE; SELECT * FROM background_jobs FOR UPDATE",
		"news_execution_runtime": "SELECT * FROM background_worker_status FOR UPDATE; SELECT * FROM background_jobs FOR UPDATE",
	} {
		t.Run(name+"/allowed", func(t *testing.T) {
			tx, err := pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(ctx)
			if _, err = tx.Exec(ctx, "SET LOCAL ROLE "+ident(roles[name])); err != nil {
				t.Fatal(err)
			}
			if _, err = tx.Exec(ctx, query); err != nil {
				t.Fatal(err)
			}
		})
	}
}
