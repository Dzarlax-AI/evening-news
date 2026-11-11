package repositories

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The fixture refuses non-loopback DSNs and creates all objects in a random schema.
func TestPostgresEditorialDigestCatalogSnapshot(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" {
		t.Fatalf("refusing non-loopback TEST_DATABASE_URL host %q", parsed.Hostname())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	adminCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := pgxpool.NewWithConfig(ctx, adminCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var random [6]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(random[:])
	schema, role := "digest_catalog_"+suffix, "digest_read_"+suffix
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	roleCreated := false
	defer func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		if roleCreated {
			_, _ = admin.Exec(cleanCtx, `DROP OWNED BY `+role)
			_, _ = admin.Exec(cleanCtx, `DROP ROLE `+role)
		}
		_, _ = admin.Exec(cleanCtx, `DROP SCHEMA `+schema+` CASCADE`)
	}()
	if _, err = admin.Exec(ctx, `CREATE ROLE `+role+` NOLOGIN`); err != nil {
		t.Fatalf("isolated fixture must allow temporary role creation: %v", err)
	}
	roleCreated = true
	adminReaderCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	adminReaderCfg.ConnConfig.RuntimeParams["search_path"] = schema
	adminPool, err := pgxpool.NewWithConfig(ctx, adminReaderCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer adminPool.Close()
	_, err = adminPool.Exec(ctx, `CREATE TABLE editorial_digest_editions(
		id uuid PRIMARY KEY,edition_date date NOT NULL,locale text NOT NULL,snapshot_hash text,
		snapshot jsonb NOT NULL,edition jsonb NOT NULL,created_at timestamp NOT NULL)`)
	if err != nil {
		t.Fatal(err)
	}
	const baseSnapshot = `{"date":"2026-11-10","articles":[{"id":"42","title":"Saved title","content":"Private source body","source_name":"Fixture source","url":"https://example.test/story","category_id":"7","category_name":"Topic"}]}`
	const baseEdition = `{"version":"1","date":"2026-11-10","locale":"ru","stories":[{"id":"main-1","kind":"main","title":"Saved headline","summary":"Saved summary","primary_article_id":"42","article_ids":["42"],"evidence":[]}]}`
	rows := []struct{ id, date, locale, created string }{
		{"11111111-1111-4111-8111-111111111111", "2026-11-10", "ru", "2026-11-10 10:00:00"},
		{"22222222-2222-4222-8222-222222222222", "2026-11-10", "ru", "2026-11-10 11:00:00"},
		{"ffffffff-ffff-4fff-8fff-ffffffffffff", "2026-11-10", "ru", "2026-11-10 11:00:00"},
		{"33333333-3333-4333-8333-333333333333", "2026-11-09", "ru", "2026-11-09 11:00:00"},
		{"44444444-4444-4444-8444-444444444444", "2026-10-31", "ru", "2026-10-31 11:00:00"},
		{"55555555-5555-4555-8555-555555555555", "2026-11-11", "sr-Latn", "2026-11-11 11:00:00"},
	}
	for _, row := range rows {
		if _, err = adminPool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,snapshot_hash,snapshot,edition,created_at) VALUES($1,$2,$3,'fixture-hash',$4,$5,$6)`, row.id, row.date, row.locale, baseSnapshot, baseEdition, row.created); err != nil {
			t.Fatal(err)
		}
	}
	readerSchemaCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	readerSchemaCfg.ConnConfig.RuntimeParams["search_path"] = schema
	ownerPool, err := pgxpool.NewWithConfig(ctx, readerSchemaCfg)
	if err != nil {
		t.Fatal(err)
	}
	defer ownerPool.Close()
	owner := &Postgres{pool: ownerPool}
	got, err := owner.EditorialDigestCatalog(ctx, "ru", "", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got.Month != "2026-11" || got.EarliestDate != "2026-10-31" || got.LatestDate != "2026-11-10" || got.LatestID != "ffffffff-ffff-4fff-8fff-ffffffffffff" || len(got.Issues) != 2 || got.Issues[0].ID != "ffffffff-ffff-4fff-8fff-ffffffffffff" || got.Issues[1].Date != "2026-11-09" {
		t.Fatalf("unexpected RU catalog: %+v", got)
	}
	empty, err := owner.EditorialDigestCatalog(ctx, "ru", "2026-09", time.Now())
	if err != nil || empty.Month != "2026-09" || len(empty.Issues) != 0 || empty.LatestID != got.LatestID {
		t.Fatalf("unexpected explicit empty month: %+v, %v", empty, err)
	}
	sr, err := owner.EditorialDigestCatalog(ctx, "sr-Latn", "", time.Now())
	if err != nil || sr.LatestID != "55555555-5555-4555-8555-555555555555" || sr.EarliestDate != "2026-11-11" || len(sr.Issues) != 1 {
		t.Fatalf("locale data leaked or missing: %+v, %v", sr, err)
	}

	// A later insertion must not change a previously pinned ID's content.
	const newerID = "00000000-0000-4000-8000-000000000001"
	newerEdition := `{"version":"1","date":"2026-11-10","locale":"ru","stories":[{"id":"new","kind":"main","title":"Newer title","summary":"Newer","primary_article_id":"42","article_ids":["42"],"evidence":[]}]}`
	if _, err = ownerPool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,snapshot_hash,snapshot,edition,created_at) VALUES($1,'2026-11-10','ru','new-hash',$2,$3,'2026-11-10 12:00:00')`, newerID, baseSnapshot, newerEdition); err != nil {
		t.Fatal(err)
	}
	pinned, err := owner.EditorialDigest(ctx, time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC), "11111111-1111-4111-8111-111111111111", "ru")
	if err != nil || pinned == nil || pinned.ID != "11111111-1111-4111-8111-111111111111" || pinned.Edition.Stories[0].Title != "Saved headline" {
		t.Fatalf("pinned edition changed after insert: %+v, %v", pinned, err)
	}
	if _, err = ownerPool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,snapshot_hash,snapshot,edition,created_at) VALUES('99999999-9999-4999-8999-999999999999','2026-11-10','ru','broken-hash',$1,'"not an object"','2026-11-10 13:00:00')`, baseSnapshot); err != nil {
		t.Fatal(err)
	}
	if _, err = owner.EditorialDigest(ctx, time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC), "99999999-9999-4999-8999-999999999999", "ru"); err == nil {
		t.Fatal("scalar edition JSON unexpectedly decoded")
	}

	// A restricted runtime role can read the catalog with SELECT only; denied
	// privileges are errors, while an absent table is an empty installation.
	if _, err = admin.Exec(ctx, `GRANT USAGE ON SCHEMA `+schema+` TO `+role); err != nil {
		t.Fatal(err)
	}
	if _, err = adminPool.Exec(ctx, `GRANT SELECT ON editorial_digest_editions TO `+role); err != nil {
		t.Fatal(err)
	}
	roleCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	roleCfg.ConnConfig.RuntimeParams["search_path"] = schema
	roleCfg.AfterConnect = func(connectCtx context.Context, conn *pgx.Conn) error {
		_, roleErr := conn.Exec(connectCtx, `SET ROLE `+role)
		return roleErr
	}
	rolePool, err := pgxpool.NewWithConfig(ctx, roleCfg)
	if err != nil {
		t.Fatal(err)
	}
	roleReader := &Postgres{pool: rolePool}
	if _, err = roleReader.EditorialDigestCatalog(ctx, "ru", "2026-11", time.Now()); err != nil {
		rolePool.Close()
		t.Fatalf("select-only role could not read catalog: %v", err)
	}
	if _, err = adminPool.Exec(ctx, `REVOKE SELECT ON editorial_digest_editions FROM `+role); err != nil {
		rolePool.Close()
		t.Fatal(err)
	}
	if _, err = roleReader.EditorialDigestCatalog(ctx, "ru", "2026-11", time.Now()); err == nil {
		rolePool.Close()
		t.Fatal("catalog read succeeded after SELECT was revoked")
	}
	rolePool.Close()
	if _, err = adminPool.Exec(ctx, `DROP TABLE editorial_digest_editions`); err != nil {
		t.Fatal(err)
	}
	missing, err := owner.EditorialDigestCatalog(ctx, "ru", "", time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC))
	if err != nil || missing.Month != "2026-11" || len(missing.Issues) != 0 || missing.LatestID != "" {
		t.Fatalf("missing table should be empty current-month catalog: %+v, %v", missing, err)
	}
	missingDetail, err := owner.EditorialDigest(ctx, time.Date(2026, 11, 10, 0, 0, 0, 0, time.UTC), "", "ru")
	if err != nil || missingDetail != nil {
		t.Fatalf("missing table detail should be not found: %+v, %v", missingDetail, err)
	}
	unavailableCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	unavailableCfg.ConnConfig.RuntimeParams["search_path"] = schema
	unavailablePool, err := pgxpool.NewWithConfig(ctx, unavailableCfg)
	if err != nil {
		t.Fatal(err)
	}
	unavailable := &Postgres{pool: unavailablePool}
	unavailable.Close()
	if _, err = unavailable.EditorialDigestCatalog(ctx, "ru", "", time.Now()); err == nil {
		t.Fatal("closed database pool unexpectedly succeeded")
	}
}
