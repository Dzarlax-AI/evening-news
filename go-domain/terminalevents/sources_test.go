package terminalevents

import (
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestScheduledSourceHysteresisAndUncertainDelivery(t *testing.T) {
	state := sourceLedger{Version: 1, Sources: map[string]*sourceState{}}
	item := sourceObservation{ID: 1, Type: "rss", Config: strings.Repeat("a", 64), State: "warning", Code: "transport_failed"}
	notice, ok := prepareSourceNotice(&state, 1, "j1", []sourceObservation{item}, nil, map[string]bool{})
	if !ok || notice != nil {
		t.Fatal("first failure sent an alert")
	}
	prev := int64(1)
	notice, ok = prepareSourceNotice(&state, 2, "j2", []sourceObservation{item}, &prev, map[string]bool{})
	if !ok || notice == nil || notice.Title != "Scheduled source warning" {
		t.Fatalf("notice=%+v", notice)
	}
	finishSourceNotice(&state, notice.Events, "j2", true)
	item.State = "healthy"
	prev = 2
	notice, ok = prepareSourceNotice(&state, 3, "j3", []sourceObservation{item}, &prev, map[string]bool{})
	if !ok || notice != nil {
		t.Fatal("first healthy observation sent recovery")
	}
	prev = 3
	notice, ok = prepareSourceNotice(&state, 4, "j4", []sourceObservation{item}, &prev, map[string]bool{})
	if !ok || notice == nil || notice.Title != "Scheduled source recovered" {
		t.Fatalf("recovery=%+v", notice)
	}
	finishSourceNotice(&state, notice.Events, "j4", false)
	if state.Sources["1"].Incident != "unknown" {
		t.Fatal("uncertain recovery replay risk")
	}
}

func TestSourceTracesMatchLegacy(t *testing.T) {
	raw, err := os.ReadFile("testdata/legacy-source-alert-traces.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Cases []struct {
			Name  string `json:"name"`
			Steps []struct {
				Position    int64  `json:"position"`
				Previous    *int64 `json:"previous_position"`
				Job         string `json:"job_id"`
				Observation struct {
					Sources []sourceObservation `json:"sources"`
				} `json:"observation"`
				Delivered *bool `json:"delivered"`
				Decision  struct {
					Event *struct {
						Title   string `json:"title"`
						Message string `json:"message"`
					} `json:"event"`
				} `json:"decision"`
				State json.RawMessage `json:"state_after"`
			} `json:"steps"`
		} `json:"cases"`
	}
	if err = json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, tc := range corpus.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			state := sourceLedger{Version: 1, Sources: map[string]*sourceState{}}
			for _, step := range tc.Steps {
				notice, ok := prepareSourceNotice(&state, step.Position, step.Job, step.Observation.Sources, step.Previous, map[string]bool{})
				if !ok {
					t.Fatalf("position %d invalid state", step.Position)
				}
				if (notice == nil) != (step.Decision.Event == nil) {
					t.Fatalf("position %d notice=%+v expected=%+v", step.Position, notice, step.Decision.Event)
				}
				if notice != nil {
					if notice.Title != step.Decision.Event.Title || notice.Message != step.Decision.Event.Message {
						t.Fatalf("position %d notice=%+v expected=%+v", step.Position, notice, step.Decision.Event)
					}
					if step.Delivered != nil {
						finishSourceNotice(&state, notice.Events, step.Job, *step.Delivered)
					}
				}
				got, _ := json.Marshal(state)
				var a, b any
				_ = json.Unmarshal(got, &a)
				_ = json.Unmarshal(step.State, &b)
				if !reflect.DeepEqual(a, b) {
					t.Fatalf("position %d\ngot=%s\nexpected=%s", step.Position, got, step.State)
				}
			}
		})
	}
}

func TestDeferredSourceNoticeRemainsEligible(t *testing.T) {
	state := sourceLedger{Version: 1, Sources: map[string]*sourceState{}}
	item := sourceObservation{ID: 1, Type: "rss", Config: strings.Repeat("a", 64), State: "warning", Code: "transport_failed"}
	prepareSourceNotice(&state, 1, "j1", []sourceObservation{item}, nil, nil)
	prev := int64(1)
	notice, _ := prepareSourceNotice(&state, 2, "j2", []sourceObservation{item}, &prev, nil)
	deferSourceNotice(&state, notice.Events, "j2")
	prev = 2
	notice, _ = prepareSourceNotice(&state, 3, "j3", []sourceObservation{item}, &prev, nil)
	if notice == nil {
		t.Fatal("deferred warning was lost")
	}
	finishSourceNotice(&state, notice.Events, "j3", true)
	item.State = "healthy"
	prev = 3
	prepareSourceNotice(&state, 4, "j4", []sourceObservation{item}, &prev, nil)
	prev = 4
	notice, _ = prepareSourceNotice(&state, 5, "j5", []sourceObservation{item}, &prev, nil)
	deferSourceNotice(&state, notice.Events, "j5")
	prev = 5
	notice, _ = prepareSourceNotice(&state, 6, "j6", []sourceObservation{item}, &prev, nil)
	if notice == nil || !notice.Events[0].Recovery {
		t.Fatal("deferred recovery was lost")
	}
}

func TestGenericAlertSuppression(t *testing.T) {
	for _, tc := range []struct {
		kind           string
		watchdog, want bool
	}{
		{"news_processing", false, true}, {"news_processing", true, true},
		{"news_digest", false, false}, {"news_digest", true, true},
		{"telegram_digest", false, false}, {"telegram_digest", true, true},
		{"backup", true, false},
	} {
		if got := suppressGenericAlert(tc.kind, tc.watchdog); got != tc.want {
			t.Errorf("%s watchdog=%v got %v", tc.kind, tc.watchdog, got)
		}
	}
}
