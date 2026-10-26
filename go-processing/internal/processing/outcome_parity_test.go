package processing

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestAdmissionBusyRefundsEveryBudgetPosition(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	for _, code := range []string{"busy", "native_browser_busy"} {
		for _, attempt := range []int{0, 1, 2, 3} {
			for _, retry := range []any{nil, true, false, -1, -1.0, math.NaN(), math.Inf(1), math.Inf(-1), "invalid", map[string]any{}, []any{}} {
				got := (Outcome{State: StateRetryWait, Error: code, RetryAfterSeconds: retry}).Persist(attempt, now, Limits{MaxAttempts: 3})
				if got.State != StateRetryWait || got.Processed || got.Attempts != max(0, attempt-1) || got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(now.Add(time.Minute)) {
					t.Fatalf("busy %s attempt %d malformed retry %#v: %#v", code, attempt, retry, got)
				}
			}
		}
	}
}

func TestOnlyAdmissionFailureRefundsAndSuccessfulRetrySpendsOneAttempt(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	limits := Limits{MaxAttempts: 3, Initial: 30 * time.Minute, Later: 6 * time.Hour}
	for _, code := range []string{"unavailable", "deadline_exceeded", "event_overflow", "APIError", "extraction_unavailable"} {
		for attempt := 1; attempt <= 3; attempt++ {
			got := (Outcome{State: StateRetryWait, Error: code}).Persist(attempt, now, limits)
			if got.Attempts != attempt {
				t.Fatalf("genuine %s refunded attempt %d", code, attempt)
			}
			if attempt == 3 {
				if got.State != StateExhausted || !got.Processed || got.NextAttemptAt != nil {
					t.Fatalf("failure did not exhaust: %#v", got)
				}
			} else {
				delay := limits.Initial
				if attempt > 1 {
					delay = limits.Later
				}
				if got.State != StateRetryWait || got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(now.Add(delay)) {
					t.Fatalf("backoff changed: %#v", got)
				}
			}
		}
	}
	attempts := 0
	for i := 0; i < 8; i++ {
		attempts = (Outcome{State: StateRetryWait, Error: "busy"}).Persist(attempts+1, now, limits).Attempts
	}
	done := (Outcome{State: StateCompleted, Fields: map[string]any{"summary": "synthetic saved summary"}}).Persist(attempts+1, now, limits)
	if done.Attempts != 1 || done.State != StateCompleted || !done.Processed || done.Fields["summary"] != "synthetic saved summary" {
		t.Fatalf("busy then completion lost result/budget: %#v", done)
	}
	for _, state := range []string{StateCompleted, StateRejected, StateExhausted} {
		got := (Outcome{State: state, Error: "busy"}).Persist(2, now, limits)
		if got.Attempts != 2 || got.NextAttemptAt != nil {
			t.Fatalf("terminal outcome refunded attempts: %#v", got)
		}
	}
}

func TestRetryAfterAndPartialFieldsSurviveRetry(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	for _, retry := range []any{7200, 7200.0, "7200"} {
		got := (Outcome{State: StateRetryWait, Error: "missing_categories", RetryAfterSeconds: retry, Fields: map[string]any{"summary": "synthetic partial summary", "summary_processed": true}}).Persist(1, now, Limits{MaxAttempts: 3})
		if got.NextAttemptAt == nil || !got.NextAttemptAt.Equal(now.Add(2*time.Hour)) || got.Fields["summary_processed"] != true || got.Attempts != 1 {
			t.Fatalf("partial stage/Retry-After changed: %#v", got)
		}
	}
	for _, retry := range []any{math.MaxFloat64, "1e300"} {
		got := (Outcome{State: StateRetryWait, Error: "busy", RetryAfterSeconds: retry}).Persist(1, now, Limits{})
		if got.NextAttemptAt == nil || !got.NextAttemptAt.After(now) {
			t.Fatal("extreme retry duration overflowed")
		}
	}
	got := (Outcome{State: StateRetryWait, Error: strings.Repeat("界", 2500)}).Persist(1, now, Limits{})
	if got.Error == nil || len([]rune(*got.Error)) != 2000 {
		t.Fatal("error bound must use Unicode characters")
	}
}
