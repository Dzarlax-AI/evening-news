package incidentfilter

import (
	"bytes"
	"strings"
	"testing"
)

func TestFilterSanitizesAndBounds(t *testing.T) {
	var out bytes.Buffer
	line := `{"event":"worker_heartbeat_failure","event_utc":"2026-10-27T10:00:00Z","reason":"timeout","msg":"CANARY"}`
	if err := Filter(strings.NewReader(line), &out, "background"); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "CANARY") || !strings.Contains(out.String(), `"source":"background"`) {
		t.Fatal(out.String())
	}
	out.Reset()
	if err := Filter(strings.NewReader(strings.Repeat(line+"\n", 65)), &out, "background"); err == nil || err.Error() != "log_overflow" {
		t.Fatal(err)
	}
	if err := Filter(strings.NewReader(""), &out, "bad"); err == nil {
		t.Fatal("invalid role")
	}
}

func TestLongLineDoesNotAdvanceAndPreservesPriorEvent(t *testing.T) {
	var out bytes.Buffer
	line := `{"event":"worker_heartbeat_failure","event_utc":"2026-10-27T10:00:00Z"}`
	err := Filter(strings.NewReader(line+"\n"+strings.Repeat("x", 8193)+"\n"), &out, "background")
	if err == nil || err.Error() != "log_overflow" || !strings.Contains(out.String(), "worker_heartbeat_failure") || strings.Contains(out.String(), strings.Repeat("x", 20)) {
		t.Fatal(err, out.String())
	}
}
