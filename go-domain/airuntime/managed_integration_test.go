package airuntime

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func managedFixture(t *testing.T) (*Server, *credentialstore.Store) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated TEST_DATABASE_URL not configured")
	}
	if err := validateLoopbackTestDatabaseDSN(dsn); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	root, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("managed_ai_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, err = root.Exec(ctx, "CREATE SCHEMA "+q); err != nil {
		root.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { root.Exec(ctx, "DROP SCHEMA "+q+" CASCADE"); root.Close() })
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	_, err = pool.Exec(ctx, `CREATE TABLE settings(key text primary key,value json not null,updated_at timestamp default now());CREATE TABLE installation_state(id smallint primary key,installation_id text);INSERT INTO installation_state VALUES(1,'fixture');CREATE TABLE integration_credentials(provider text primary key,revision bigint not null,envelope_version int,key_id text,nonce bytea,ciphertext bytea,updated_at timestamptz default now())`)
	if err != nil {
		t.Fatal(err)
	}
	store := &credentialstore.Store{Pool: pool, Key: bytes.Repeat([]byte{3}, 32), KeyID: "fixture-key", InstallationID: "fixture"}
	server := NewServer(Config{Model: "gemini-model", DigestModel: "gemini-model", TextModel: "gemini-model", Endpoint: "https://generativelanguage.googleapis.com/v1/models", APIKey: "legacy-must-not-revive", OpenAIKey: "legacy-must-not-revive", OpenAIEndpoint: "https://api.openai.com/v1", OpenAIArticleModel: "openai-model", OpenAIDigestModel: "openai-model", OpenAITextModel: "openai-model", TelegramToken: "legacy-must-not-revive", TelegramMain: "legacy-main", TelegramEndpoint: "https://api.telegram.org", RPS: 100}, pool)
	server.SetCredentialStore(store)
	return server, store
}

func TestManagedProviderCredentialSnapshotRotationAndDeletion(t *testing.T) {
	for _, name := range []string{"gemini", "openai"} {
		t.Run(name, func(t *testing.T) {
			server, store := managedFixture(t)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			server.config.ActiveProvider = name
			calls := 0
			var keys []string
			server.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
				calls++
				key := r.Header.Get("x-goog-api-key")
				if name == "openai" {
					key = strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				}
				keys = append(keys, key)
				if name == "openai" {
					return response(200, nil, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"synthetic"}]}]}`), nil
				}
				return response(200, nil, `{"candidates":[{"content":{"parts":[{"text":"synthetic"}]},"finishReason":"STOP"}]}`), nil
			})}
			if _, err := server.snapshot(ctx, "text"); err == nil {
				t.Fatal("absent managed key revived legacy credential")
			}
			if _, err := store.Put(ctx, name, 0, []byte("synthetic-key-one")); err != nil {
				t.Fatal(err)
			}
			first, err := server.snapshot(ctx, "text")
			if err != nil {
				t.Fatal(err)
			}
			if calls != 0 {
				t.Fatal("saving or snapshotting made a provider request")
			}
			if _, err = store.Put(ctx, name, 1, []byte("synthetic-key-two")); err != nil {
				t.Fatal(err)
			}
			second, err := server.snapshot(ctx, "text")
			if err != nil {
				t.Fatal(err)
			}
			req := generateRequest{Mode: "text", Prompt: "synthetic input", Options: generateOptions{MaxTokens: 10}}
			if _, err = server.generateWithSnapshot(ctx, first, req); err != nil {
				t.Fatal(err)
			}
			if _, err = server.generateWithSnapshot(ctx, second, req); err != nil {
				t.Fatal(err)
			}
			if len(keys) != 2 || keys[0] != "synthetic-key-one" || keys[1] != "synthetic-key-two" {
				t.Fatal("running operation did not retain its credential snapshot")
			}
			if _, err = store.Delete(ctx, name, 2); err != nil {
				t.Fatal(err)
			}
			if _, err = server.snapshot(ctx, "text"); err == nil {
				t.Fatal("deleted key revived legacy credential")
			}
			if calls != 2 {
				t.Fatal("deletion made an external request")
			}
		})
	}
}

func TestManagedTelegramClearedDestinationAndStoreFailureDoNotFallBack(t *testing.T) {
	server, store := managedFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := store.Put(ctx, "telegram", 0, []byte("synthetic-bot")); err != nil {
		t.Fatal(err)
	}
	calls := 0
	server.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		var p map[string]any
		if json.NewDecoder(r.Body).Decode(&p) != nil || p["chat_id"] != "synthetic-main" {
			t.Error("wrong destination")
		}
		return response(200, nil, `{"ok":true,"result":{"message_id":1}}`), nil
	})}
	settings := appsettings.Store{Pool: server.db}
	if _, err := settings.UpdateTelegram(ctx, appsettings.TelegramUpdate{ChatID: "synthetic-main"}); err != nil {
		t.Fatal(err)
	}
	req := telegramRequest{Channel: "main", Method: "sendMessage", Text: "synthetic message"}
	if _, err := server.sendTelegram(ctx, req); err != nil {
		t.Fatal(err)
	}
	if _, err := settings.UpdateTelegram(ctx, appsettings.TelegramUpdate{ExpectedRevision: 1}); err != nil {
		t.Fatal(err)
	}
	if _, err := server.sendTelegram(ctx, req); err == nil {
		t.Fatal("cleared destination revived legacy default")
	}
	if _, err := server.managedDestination(ctx); err == nil {
		t.Fatal("digest destination revived legacy default")
	}
	if _, err := server.db.Exec(ctx, `DROP TABLE settings`); err != nil {
		t.Fatal(err)
	}
	if _, err := server.sendTelegram(ctx, req); err == nil {
		t.Fatal("settings failure fell back to legacy")
	}
	if calls != 1 {
		t.Fatal("failed configuration sent a provider request")
	}
}
