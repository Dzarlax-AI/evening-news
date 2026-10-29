package airuntime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func TestAISettingsPostgresCASResetAndRestart(t *testing.T) {
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
	defer root.Close()
	schema := fmt.Sprintf("ai_settings_test_%d", time.Now().UnixNano())
	_, err = root.Exec(ctx, `CREATE SCHEMA `+schema)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Exec(ctx, `DROP SCHEMA `+schema+` CASCADE`)
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
	if _, err = pool.Exec(ctx, `CREATE TABLE settings(key text primary key,value json not null,updated_at timestamp default now())`); err != nil {
		t.Fatal(err)
	}
	config := Config{APIKey: "fake-gemini", Model: "gemini-model", OpenAIKey: "fake-openai", OpenAIArticleModel: "openai-article", OpenAIDigestModel: "openai-digest", OpenAITextModel: "openai-text", RPS: 100}
	s := NewServer(config, pool)
	initial, err := s.GetAISettings(ctx)
	if err != nil || initial.Revision != 0 || initial.Source != "env" {
		t.Fatalf("initial=%+v err=%v", initial, err)
	}
	update := AISettingsUpdate{ExpectedRevision: 0, ActiveProvider: "openai", Profiles: initial.Profiles}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := s.UpdateAISettings(ctx, update); errs <- err }()
	}
	wg.Wait()
	close(errs)
	success, conflict := 0, 0
	for err := range errs {
		if err == nil {
			success++
		} else if errors.Is(err, ErrAISettingsConflict) {
			conflict++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatalf("success=%d conflict=%d", success, conflict)
	}
	config.APIKey = ""
	restarted := NewServer(config, pool)
	if err = restarted.ValidateAISettings(ctx); err != nil {
		t.Fatalf("persisted openai with inactive key: %v", err)
	}
	current, err := restarted.GetAISettings(ctx)
	if err != nil || current.Revision != 1 || current.ActiveProvider != "openai" || current.Source != "override" {
		t.Fatalf("current=%+v err=%v", current, err)
	}

	// A profile change while a retry is pending must not change the in-flight model.
	first := make(chan struct{})
	var calls atomic.Int32
	s.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body struct {
			Model string `json:"model"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			return nil, err
		}
		if body.Model != "openai-text" {
			t.Errorf("retry used model %q", body.Model)
		}
		if calls.Add(1) == 1 {
			close(first)
			return response(500, nil, `{}`), nil
		}
		return response(200, nil, `{"status":"completed","output":[{"type":"message","content":[{"type":"output_text","text":"ok"}]}]}`), nil
	})}
	snap, err := s.snapshot(ctx, "text")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.generateWithSnapshot(ctx, snap, generateRequest{Mode: "text", Prompt: "fixture", Options: generateOptions{MaxTokens: 10}})
		done <- err
	}()
	<-first
	switched := update
	switched.ExpectedRevision = 1
	switched.Profiles.OpenAI.Models.Text = "new-text"
	if _, err = s.UpdateAISettings(ctx, switched); err != nil {
		t.Fatal(err)
	}
	if err = <-done; err != nil || calls.Load() != 2 {
		t.Fatalf("retry err=%v calls=%d", err, calls.Load())
	}
	current, err = restarted.GetAISettings(ctx)
	if err != nil || current.Revision != 2 || current.Profiles.OpenAI.Models.Text != "new-text" {
		t.Fatalf("switch=%+v err=%v", current, err)
	}
	invalid := update
	invalid.ExpectedRevision = 2
	invalid.Profiles.OpenAI.Models.Text = "invalid/model"
	if _, err = restarted.UpdateAISettings(ctx, invalid); !errors.Is(err, ErrAISettingsInvalid) {
		t.Fatalf("invalid: %v", err)
	}
	current2, _ := restarted.GetAISettings(ctx)
	if current2 != current {
		t.Fatal("invalid update changed settings")
	}
	config.APIKey = "fake-gemini"
	resetter := NewServer(config, pool)
	reset, err := resetter.UpdateAISettings(ctx, AISettingsUpdate{ExpectedRevision: 2, ResetToEnv: true})
	if err != nil || reset.Revision != 3 || reset.Source != "env" || reset.ActiveProvider != "gemini" {
		t.Fatalf("reset=%+v err=%v", reset, err)
	}
	var exists bool
	if err = pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM settings WHERE key='ai_settings')`).Scan(&exists); err != nil || exists {
		t.Fatalf("override still exists: %v", err)
	}
	if _, err = resetter.UpdateAISettings(ctx, update); !errors.Is(err, ErrAISettingsConflict) {
		t.Fatalf("stale writer after reset: %v", err)
	}
}
