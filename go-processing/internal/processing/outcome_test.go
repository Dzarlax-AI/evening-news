package processing

import (
	"testing"
	"time"
)

func TestBusyRefundsAttemptAndUsesUnicodeErrorBound(t *testing.T) {
	now := time.Date(2026, 10, 26, 0, 0, 0, 0, time.UTC)
	got := Outcome{State: StateRetryWait, Error: "busy", RetryAfterSeconds: 120, Fields: map[string]any{"summary": "сохранено"}}.Persist(1, now, Limits{MaxAttempts: 3})
	if got.State != StateRetryWait || got.Attempts != 0 || got.NextAttemptAt == nil || got.NextAttemptAt.Sub(now) != 120*time.Second || got.Fields["summary"] != "сохранено" {
		t.Fatalf("unexpected busy outcome: %#v", got)
	}
}
func TestRetryPolicyExhaustsOnlyGenuineFailures(t *testing.T) {
	now := time.Now()
	got := Outcome{State: StateRetryWait, Error: "provider_error"}.Persist(3, now, Limits{MaxAttempts: 3})
	if got.State != StateExhausted || !got.Processed || got.NextAttemptAt != nil {
		t.Fatalf("unexpected exhaustion: %#v", got)
	}
}
func TestChallengeDetection(t *testing.T) {
	if !IsChallengeText("Проверка вашего браузера") || IsChallengeText("Cloudflare published results") {
		t.Fatal("challenge classification mismatch")
	}
}
