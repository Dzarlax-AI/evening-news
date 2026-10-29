package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestManagedClaimRaceAndRevocation(t *testing.T) {
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
	b := make([]byte, 8)
	if _, err = rand.Read(b); err != nil {
		t.Fatal(err)
	}
	schema := "auth_test_" + hex.EncodeToString(b)
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
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
	_, err = pool.Exec(ctx, `CREATE TABLE installation_state(id smallint primary key,installation_id text); CREATE TABLE managed_admin(id smallint primary key,username text,password_hash text,revision bigint default 1,created_at timestamptz default now()); CREATE TABLE setup_tokens(id smallint primary key,token_hash bytea,expires_at timestamptz,consumed_at timestamptz); INSERT INTO installation_state VALUES(1,'fixture')`)
	if err != nil {
		t.Fatal(err)
	}
	token := "01234567890123456789012345678901"
	hash := sha256.Sum256([]byte(token))
	if _, err = pool.Exec(ctx, `INSERT INTO setup_tokens VALUES(1,$1,now()+interval '15 minutes',NULL)`, hash[:]); err != nil {
		t.Fatal(err)
	}
	store := NewManagedStore(pool, "fixture")
	if _, err = pool.Exec(ctx, `UPDATE setup_tokens SET expires_at=now()-interval '1 minute'`); err != nil {
		t.Fatal(err)
	}
	if err = store.claim(ctx, token, "admin", "a very long password"); err == nil {
		t.Fatal("expired token accepted")
	}
	if _, err = pool.Exec(ctx, `UPDATE setup_tokens SET expires_at=now()+interval '15 minutes'`); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	success := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); success <- store.claim(ctx, token, "admin", "a very long password") == nil }()
	}
	wg.Wait()
	close(success)
	n := 0
	for ok := range success {
		if ok {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("successful claims: %d", n)
	}
	svc := New(Config{JWTSecret: "fixture-secret"})
	svc.UseManaged(store)
	jwt, err := svc.Mint("admin", LevelAdmin)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := svc.verify(jwt); !ok {
		t.Fatal("new session rejected")
	}
	if _, err = pool.Exec(ctx, `UPDATE managed_admin SET revision=revision+1 WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, ok := svc.verify(jwt); ok {
		t.Fatal("reset did not revoke session")
	}
	if err = store.claim(ctx, token, "second", "a very long password"); err == nil {
		t.Fatal("replayed claim succeeded")
	}
}
func TestManagedCookieCSRF(t *testing.T) {
	h := ProtectManagedCookies(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, tt := range []struct {
		origin, token string
		want          int
	}{{"", "", 403}, {"https://evil.test", "1", 403}, {"https://news.test", "1", 204}} {
		r := httptest.NewRequest("POST", "https://news.test/api/v1/test", nil)
		r.AddCookie(&http.Cookie{Name: AdminCookie, Value: "cookie"})
		r.Header.Set("Origin", tt.origin)
		r.Header.Set("X-CSRF-Token", tt.token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tt.want {
			t.Fatalf("origin %s: got %d", tt.origin, w.Code)
		}
	}
}
