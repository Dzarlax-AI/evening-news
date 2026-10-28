package terminalevents

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestAlertsDeduplicateRemindRecoverAndRetry(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	var sent []string
	ok := true
	a := NewAlerts(func(_ context.Context, title, message string) bool { sent = append(sent, message); return ok })
	a.now = func() time.Time { return now }
	ctx := context.Background()
	if !a.Failure(ctx, "key", "x", "failure") || !a.Active("key") {
		t.Fatal("initial alert")
	}
	if a.Failure(ctx, "key", "x", "failure") {
		t.Fatal("duplicate sent")
	}
	now = now.Add(time.Hour)
	if !a.Failure(ctx, "key", "x", "failure") || !strings.Contains(sent[1], "1") {
		t.Fatal("reminder")
	}
	if !a.Recovery(ctx, "key", "x", "ok") || a.Active("key") {
		t.Fatal("recovery")
	}
	ok = false
	if a.Failure(ctx, "key", "x", "failure") || a.Active("key") {
		t.Fatal("failed delivery made incident active")
	}
	ok = true
	if !a.Failure(ctx, "key", "x", "failure") {
		t.Fatal("failed delivery was not retried")
	}
}

func TestOperationalTracesMatchLegacy(t *testing.T) {
	raw, err := os.ReadFile("testdata/legacy-operational-alert-traces.json")
	if err != nil {
		t.Fatal(err)
	}
	type call struct {
		Title   string `json:"title"`
		Message string `json:"message"`
	}
	var corpus struct {
		Cases []struct {
			Name  string `json:"name"`
			Steps []struct {
				Time    float64 `json:"time"`
				Method  string  `json:"method"`
				Outcome any     `json:"sender_outcome"`
				Title   string  `json:"title"`
				Message string  `json:"message"`
				Result  bool    `json:"result"`
				Calls   []call  `json:"sent_calls"`
				State   struct {
					Active     bool    `json:"active"`
					Last       float64 `json:"last_sent_at"`
					Suppressed int     `json:"suppressed"`
				} `json:"state"`
			} `json:"steps"`
		} `json:"cases"`
	}
	if err = json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, tc := range corpus.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			var now time.Time
			var calls []call
			var outcome any
			a := NewAlerts(func(_ context.Context, title, message string) bool {
				calls = append(calls, call{title, message})
				if outcome == "raise" {
					panic("fixture error")
				}
				return outcome == true
			})
			a.now = func() time.Time { return now }
			for i, step := range tc.Steps {
				now = time.Unix(0, 0).Add(time.Duration(step.Time * float64(time.Second)))
				outcome = step.Outcome
				calls = []call{}
				var result bool
				if step.Method == "failure" {
					result = a.Failure(context.Background(), "fixture", step.Title, step.Message)
				} else {
					result = a.Recovery(context.Background(), "fixture", step.Title, step.Message)
				}
				state := a.states["fixture"]
				last := float64(0)
				if !state.last.IsZero() {
					last = float64(state.last.UnixNano()) / float64(time.Second)
				}
				if result != step.Result || !reflect.DeepEqual(calls, step.Calls) || state.active != step.State.Active || last != step.State.Last || state.suppressed != step.State.Suppressed {
					t.Fatalf("step %d result=%v calls=%+v state=%+v", i, result, calls, state)
				}
			}
		})
	}
}
