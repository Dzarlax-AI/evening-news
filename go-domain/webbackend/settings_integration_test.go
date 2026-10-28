package webbackend

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTelegramSettingsUsesDecodedDatabaseOverrides(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("loopback fixture required")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("web_settings_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+q+" CASCADE")
	_, err = admin.Exec(ctx, "CREATE TABLE "+q+`.settings(key text primary key,value jsonb not null); INSERT INTO `+q+`.settings VALUES ('telegram_chat_id','"db-chat"'),('telegram_service_chat_id','"db-service"')`)
	if err != nil {
		t.Fatal(err)
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
	s := Server{Guard: testGuard{}, Pool: pool, ChatID: "env-chat", ServiceChatID: "env-service"}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest("GET", "/internal/web/v1/telegram/settings", nil))
	var v map[string]any
	if err = json.Unmarshal(w.Body.Bytes(), &v); err != nil || w.Code != 200 || v["chat_id"] != "db-chat" || v["service_chat_id"] != "db-service" || v["chat_id_source"] != "db" {
		t.Fatalf("status=%d body=%s err=%v", w.Code, w.Body.String(), err)
	}
}

func TestCategoryAnalysisFakeProvider(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("loopback fixture required")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	schema := fmt.Sprintf("web_category_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(ctx, "DROP SCHEMA "+q+" CASCADE")
	sql := `CREATE TABLE articles(id integer,title text);CREATE TABLE article_categories(article_id integer,ai_category text);CREATE TABLE category_mapping(ai_category text);CREATE TABLE categories(name text,display_name text);INSERT INTO articles VALUES (1,'A fixture story');INSERT INTO article_categories VALUES (1,'Machine learning');INSERT INTO categories VALUES ('Tech','Technology')`
	if _, err = admin.Exec(ctx, "SET search_path TO "+q); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, sql); err != nil {
		t.Fatal(err)
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
	called := false
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !bytes.Contains(body, []byte("A fixture story")) || !bytes.Contains(body, []byte("Machine learning")) {
			t.Errorf("missing prompt context")
		}
		called = true
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"candidates":[{"content":{"parts":[{"text":"{\"suggestions\":[{\"category\":\"Tech (Technology)\",\"confidence\":0.85,\"reasoning\":\"matching\"}]}"}]}}],"usageMetadata":{}}`))
	}))
	defer fake.Close()
	runtime := airuntime.NewServer(airuntime.Config{RPS: 10, APIKey: "fixture", Model: "fixture-model", Endpoint: fake.URL + "/v1/models"}, pool)
	s := Server{Guard: testGuard{}, Pool: pool, Runtime: runtime}
	handler := Boundary{Token: "fixture", Next: s.Handler()}
	req := httptest.NewRequest("POST", "/internal/web/v1/categories/ai-category-analysis", strings.NewReader(`{}`))
	req.Header.Set("Authorization", "Bearer fixture")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 200 || !called || !strings.Contains(w.Body.String(), `"suggested_fixed_category":"Tech"`) {
		t.Fatalf("status=%d called=%v body=%s", w.Code, called, w.Body.String())
	}
}
