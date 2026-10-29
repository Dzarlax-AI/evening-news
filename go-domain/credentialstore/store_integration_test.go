package credentialstore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestStorePostgresCASAndTombstone(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	ctx := context.Background()
	admin, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	schema := fmt.Sprintf("credential_fixture_%d", time.Now().UnixNano())
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+quoted+" CASCADE")
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
	_, err = pool.Exec(ctx, `CREATE TABLE installation_state(id smallint primary key,installation_id text not null); INSERT INTO installation_state VALUES(1,'fixture'); CREATE TABLE integration_credentials(provider text primary key,revision bigint not null,envelope_version int,key_id text,nonce bytea,ciphertext bytea,updated_at timestamptz default now())`)
	if err != nil {
		t.Fatal(err)
	}
	s := Store{Pool: pool, Key: bytes.Repeat([]byte{3}, 32), KeyID: "test-key", InstallationID: "fixture"}
	m, err := s.Put(ctx, "gemini", 0, []byte("fake-secret"))
	if err != nil || m.Revision != 1 || !m.Configured {
		t.Fatalf("put %v %+v", err, m)
	}
	if _, err = s.Put(ctx, "gemini", 0, []byte("stale")); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict %v", err)
	}
	var cipher []byte
	if err = pool.QueryRow(ctx, `SELECT ciphertext FROM integration_credentials WHERE provider='gemini'`).Scan(&cipher); err != nil || bytes.Contains(cipher, []byte("fake-secret")) {
		t.Fatal("plaintext in storage")
	}
	p, _, err := s.Resolve(ctx, "gemini")
	if err != nil || string(p) != "fake-secret" {
		t.Fatal("decrypt failed")
	}
	m, err = s.Delete(ctx, "gemini", 1)
	if err != nil || m.Revision != 2 || m.Configured {
		t.Fatal("delete failed")
	}
	p, m, err = s.Resolve(ctx, "gemini")
	if err != nil || p != nil || m.Revision != 2 || m.Configured {
		t.Fatal("tombstone failed")
	}
	if _, err = s.Put(ctx, "gemini", 1, []byte("stale")); !errors.Is(err, ErrConflict) {
		t.Fatal("stale write accepted")
	}
	s.InstallationID = "different"
	if _, err = s.Metadata(ctx, "gemini"); !errors.Is(err, ErrUnavailable) {
		t.Fatal("wrong installation accepted")
	}
}
