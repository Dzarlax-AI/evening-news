package incidentbundle

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-tools/incidentfilter"
)

func TestNavigationReasonsSurviveBothIncidentFilters(t *testing.T) {
	for _, reason := range []string{"navigation_http", "navigation_proxy", "navigation_redirect", "navigation_empty_response", "navigation_other"} {
		t.Run(reason, func(t *testing.T) {
			line := `{"event":"page_monitor_failure","event_utc":"2026-10-29T00:58:00Z","source_id":10,"stage":"navigate","reason":"` + reason + `","error_code":"transport_failed","url":"URL_CANARY","errorText":"ERROR_CANARY"}`
			for name, event := range map[string]map[string]any{"bundle": ParseLogLine(line), "remote": incidentfilter.ParseLogLine(line)} {
				if event["reason"] != reason || event["stage"] != "navigate" {
					t.Fatalf("%s dropped navigation classification: %v", name, event)
				}
				encoded, err := json.Marshal(event)
				if err != nil || strings.Contains(string(encoded), "CANARY") {
					t.Fatalf("%s leaked provider details", name)
				}
			}
		})
	}
}

func TestArbitraryNavigationReasonIsNotExported(t *testing.T) {
	line := `{"event":"page_monitor_failure","event_utc":"2026-10-29T00:58:00Z","stage":"navigate","reason":"navigation_http https://URL_CANARY?token=SECRET_CANARY"}`
	for name, event := range map[string]map[string]any{"bundle": ParseLogLine(line), "remote": incidentfilter.ParseLogLine(line)} {
		if event["reason"] != "other" {
			t.Fatalf("%s accepted arbitrary navigation reason", name)
		}
		encoded, err := json.Marshal(event)
		if err != nil || strings.Contains(string(encoded), "CANARY") {
			t.Fatalf("%s leaked arbitrary navigation reason", name)
		}
	}
}
