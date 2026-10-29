package auth

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-web/internal/transport"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestManagedClaimTransportAndCookieEndToEnd(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated TEST_DATABASE_URL not configured")
	}
	ctx := context.Background()
	root, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	random := make([]byte, 8)
	if _, err = rand.Read(random); err != nil {
		t.Fatal(err)
	}
	schema := "auth_http_" + hex.EncodeToString(random)
	if _, err = root.Exec(ctx, "CREATE SCHEMA "+schema); err != nil {
		t.Fatal(err)
	}
	defer root.Exec(ctx, "DROP SCHEMA "+schema+" CASCADE")
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
	_, err = pool.Exec(ctx, `CREATE TABLE installation_state(id smallint primary key,installation_id text);CREATE TABLE managed_admin(id smallint primary key,username text,password_hash text,revision bigint default 1,created_at timestamptz default now());CREATE TABLE setup_tokens(id smallint primary key,token_hash bytea,expires_at timestamptz,consumed_at timestamptz);INSERT INTO installation_state VALUES(1,'fixture')`)
	if err != nil {
		t.Fatal(err)
	}
	token := "synthetic-token-at-least-thirty-two-bytes"
	hash := sha256.Sum256([]byte(token))
	if _, err = pool.Exec(ctx, `INSERT INTO setup_tokens VALUES(1,$1,now()+interval '15 minutes',NULL)`, hash[:]); err != nil {
		t.Fatal(err)
	}
	service := New(Config{Username: "fixture-admin", JWTSecret: "synthetic-signing-key-not-an-integration-secret"})
	service.UseManaged(NewManagedStore(pool, "fixture"))
	mux := http.NewServeMux()
	service.Register(mux)
	handler, err := (transport.Policy{TrustedHosts: []string{"fixture.invalid"}, AllowedOrigins: []string{"https://fixture.invalid"}, PublicOrigin: "https://fixture.invalid"}).Wrap(mux)
	if err != nil {
		t.Fatal(err)
	}
	claim, _ := json.Marshal(map[string]string{"username": "fixture-admin", "password": "synthetic-password-long", "token": token})
	for i, tc := range []struct {
		scheme, origin, csrf string
		extra                string
		want                 int
	}{
		{"http", "https://fixture.invalid", "1", "", 403},
		{"https", "https://other.invalid", "1", "", 403},
		{"https", "https://fixture.invalid", "", "", 403},
		{"https", "https://fixture.invalid", "1", "{}", 422},
	} {
		req := httptest.NewRequest("POST", tc.scheme+"://fixture.invalid/api/v1/setup/claim", bytes.NewReader(append(append([]byte{}, claim...), []byte(tc.extra)...)))
		req.RemoteAddr = fmt.Sprintf("203.0.113.%d:1234", i+1)
		req.Header.Set("Origin", tc.origin)
		req.Header.Set("X-CSRF-Token", tc.csrf)
		req.Header.Set("X-Forwarded-Proto", "https")
		out := httptest.NewRecorder()
		handler.ServeHTTP(out, req)
		if out.Code != tc.want {
			t.Errorf("case %d status %d want%d", i, out.Code, tc.want)
		}
	}
	req := httptest.NewRequest("POST", "https://fixture.invalid/api/v1/setup/claim", bytes.NewReader(claim))
	req.Header.Set("Origin", "https://fixture.invalid")
	req.Header.Set("X-CSRF-Token", "1")
	out := httptest.NewRecorder()
	handler.ServeHTTP(out, req)
	if out.Code != 201 {
		t.Fatalf("claim status%d", out.Code)
	}
	cookies := out.Result().Cookies()
	if len(cookies) != 1 || !cookies[0].Secure || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteLaxMode {
		t.Fatal("public session cookie protections missing")
	}
	me := httptest.NewRequest("GET", "https://fixture.invalid/auth/me", nil)
	me.AddCookie(cookies[0])
	out = httptest.NewRecorder()
	handler.ServeHTTP(out, me)
	if out.Code != 200 {
		t.Fatal("claimed cookie cannot authenticate")
	}
	login := httptest.NewRequest("POST", "https://fixture.invalid/auth/login", nil)
	login.SetBasicAuth("fixture-admin", "synthetic-password-long")
	login.Header.Set("Origin", "https://fixture.invalid")
	login.Header.Set("X-CSRF-Token", "1")
	out = httptest.NewRecorder()
	handler.ServeHTTP(out, login)
	if out.Code != 200 {
		t.Fatal("managed login failed")
	}
}
