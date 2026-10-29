package management

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

type fixtureReadiness struct {
	raw   string
	err   error
	calls int
}

func (f *fixtureReadiness) Configuration(_ context.Context, method, group string, _ json.RawMessage) (json.RawMessage, error) {
	f.calls++
	if method != "GET" || group != "readiness" {
		return nil, errors.New("unexpected operation")
	}
	return json.RawMessage(f.raw), f.err
}
func TestConfigurationPreflightDoesNotQueueWithoutIntegrations(t *testing.T) {
	f := &fixtureReadiness{raw: `{"ai_configured":false,"telegram_configured":false}`}
	check := NewConfigurationPreflight(f)
	for _, kind := range []string{"news_processing", "article_reprocess", "reprocess_failed", "summaries_generate", "telegram_digest"} {
		err := check(context.Background(), kind, nil)
		var e *APIError
		if !errors.As(err, &e) || e.Status != 409 {
			t.Fatalf("%s should be blocked before queue: %v", kind, err)
		}
	}
	before := f.calls
	if err := check(context.Background(), "backup", nil); err != nil || before != f.calls {
		t.Fatal("backup should not depend on AI")
	}
	f.raw = `{"ai_configured":true,"telegram_configured":false}`
	if err := check(context.Background(), "news_processing", nil); err != nil {
		t.Fatal(err)
	}
	if err := check(context.Background(), "news_digest", map[string]any{"send_telegram": true}); err == nil {
		t.Fatal("delivery admitted without Telegram")
	}
	f.err = errors.New("unavailable")
	var e *APIError
	if !errors.As(check(context.Background(), "news_processing", nil), &e) || e.Status != 503 {
		t.Fatal("backend error did not fail closed")
	}
}

func TestCollectionWithoutProcessingDoesNotRequireAI(t *testing.T) {
	f := &fixtureReadiness{raw: `{"ai_configured":false,"telegram_configured":false,"processing_enabled":false,"delivery_enabled":false}`}
	check := NewConfigurationPreflight(f)
	if err := check(context.Background(), "news_processing", nil); err != nil {
		t.Fatal("collection-only job was blocked", err)
	}
	for _, kind := range []string{"article_reprocess", "telegram_digest"} {
		var api *APIError
		if !errors.As(check(context.Background(), kind, nil), &api) || api.Status != 409 {
			t.Fatal("disabled operation admitted", kind)
		}
	}
}
