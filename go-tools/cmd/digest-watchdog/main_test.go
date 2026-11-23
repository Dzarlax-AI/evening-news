package main

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDecision(t *testing.T) {
	due := time.Date(2026, 11, 23, 18, 0, 0, 0, time.UTC)
	o := occurrence{Key: "news:1", Due: due, Enabled: true, SendTelegram: true}
	tests := []struct {
		name string
		now  time.Time
		o    occurrence
		a    assessment
		err  bool
		want bool
	}{
		{"before deadline", due.Add(29 * time.Minute), o, assessment{}, false, false},
		{"missing", due.Add(30 * time.Minute), o, assessment{}, false, true},
		{"retry waiting", due.Add(20 * time.Minute), o, assessment{pending: true}, false, false},
		{"ambiguous immediately", due.Add(time.Minute), o, assessment{unknown: true}, false, true},
		{"success", due.Add(31 * time.Minute), o, assessment{status: "succeeded"}, false, false},
		{"success without receipt", due.Add(31 * time.Minute), o, assessment{status: "succeeded", missing: []string{"Telegram ru"}}, false, true},
		{"database unavailable", due.Add(31 * time.Minute), o, assessment{}, true, true},
		{"paused", due.Add(31 * time.Minute), occurrence{Due: due, Enabled: true, Paused: true, SendTelegram: true}, assessment{}, false, false},
		{"disabled", due.Add(31 * time.Minute), occurrence{Due: due}, assessment{}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := decision(tt.now, tt.o, tt.a, tt.err) != ""; got != tt.want {
				t.Fatalf("got alert %v", got)
			}
		})
	}
}
func TestStateSurvivesRestart(t *testing.T) {
	p := filepath.Join(t.TempDir(), "state.json")
	s := state{map[string]occurrence{}, map[string]bool{"one": true}}
	if e := save(p, s); e != nil {
		t.Fatal(e)
	}
	got, e := load(p)
	if e != nil || !got.Alerted["one"] {
		t.Fatalf("state not preserved: %v", e)
	}
}

func TestExpiredRunDoesNotReserveAlert(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	s := state{map[string]occurrence{}, map[string]bool{}}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if err := reserveAlert(ctx, path, s, "missed"); err == nil {
		t.Fatal("expired watchdog run reserved an alert")
	}
	persisted, err := load(path)
	if err != nil || s.Alerted["missed"] || persisted.Alerted["missed"] {
		t.Fatalf("expired run changed alert state: %v", err)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestFakeTelegram(t *testing.T) {
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/botfake/sendMessage" {
			t.Error("wrong path")
		}
		if err := r.ParseForm(); err != nil {
			t.Error(err)
		}
		if r.Form.Get("chat_id") != "local" || r.Form.Get("text") != "test" {
			t.Error("wrong message")
		}
		return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"ok":true}`)), Header: make(http.Header)}, nil
	})}
	if err := send(context.Background(), client, "https://fake.invalid", "fake", "local", "test"); err != nil {
		t.Fatal(err)
	}
}

func TestActivationBoundary(t *testing.T) {
	from := time.Date(2026, 11, 23, 18, 0, 0, 0, time.UTC)
	if trackOccurrence(occurrence{Due: from.Add(-time.Second)}, from) {
		t.Fatal("old backlog admitted")
	}
	if !trackOccurrence(occurrence{Due: from}, from) {
		t.Fatal("boundary occurrence excluded")
	}
}
