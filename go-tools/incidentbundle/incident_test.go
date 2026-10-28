package incidentbundle

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReplaySanitizesAndDeduplicates(t *testing.T) {
	dir := t.TempDir()
	fixture := filepath.Join(dir, "fixture.jsonl")
	out := filepath.Join(dir, "out")
	rows := []string{`{"kind":"snapshot","snapshot":{"captured_at":"2026-10-27T10:00:00Z","worker":{"generation":7,"pid":123,"heartbeat_at":"2026-10-27T10:00:00Z","owner":"SECRET"},"pg_stat_activity":[{"state":"idle in transaction","query":"SQL_CANARY","blocker_pids":[88]}]}}`, `{"kind":"event","event":{"event":"page_monitor_failure","event_utc":"2026-10-27T10:00:00Z","stage":"navigate","reason":"timeout","source_id":41,"url":"URL_CANARY"}}`}
	if err := os.WriteFile(fixture, []byte(strings.Join(append(rows, rows[1]), "\n")), 0600); err != nil {
		t.Fatal(err)
	}
	n, _, err := Replay(fixture, out, 2, 3, MaxOutputBytes)
	if err != nil || n != 1 {
		t.Fatalf("count %d err %v", n, err)
	}
	files, err := filepath.Glob(filepath.Join(out, "incident-*", "bundle-*.json"))
	if err != nil || len(files) != 1 {
		t.Fatal(files, err)
	}
	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, x := range []string{"SQL_CANARY", "SECRET", "URL_CANARY"} {
		if strings.Contains(string(data), x) {
			t.Fatal("leaked", x)
		}
	}
	var b map[string]any
	if err = json.Unmarshal(data, &b); err != nil {
		t.Fatal(err)
	}
	if b["evidence_status"].(map[string]any)["current_snapshot"] != "within_one_second" {
		t.Fatal(b)
	}
	st, _ := os.Stat(files[0])
	if st.Mode().Perm() != 0600 {
		t.Fatal(st.Mode())
	}
	st, _ = os.Stat(filepath.Dir(files[0]))
	if st.Mode().Perm() != 0700 {
		t.Fatal(st.Mode())
	}
}
func TestLogAndBounds(t *testing.T) {
	e := ParseLogLine(`event=worker_heartbeat_failure event_utc=2026-10-27T10:00:00Z stage=commit reason=timeout generation=7 msg="CANARY"`)
	if e["generation"] != int64(7) || e["msg"] != nil {
		t.Fatal(e)
	}
	if ParseLogLine(`event=worker_heartbeat_failure`) != nil {
		t.Fatal("missing timestamp accepted")
	}
	if _, _, err := Replay("missing", t.TempDir(), 0, 1, 1); err == nil {
		t.Fatal("accepted invalid duration")
	}
}
func TestMissingDistinctFromEmpty(t *testing.T) {
	missing := SanitizedSnapshot(readJSON([]byte(`{}`)))
	empty := SanitizedSnapshot(readJSON([]byte(`{"pg_stat_activity":[]}`)))
	if !strings.Contains(strings.Join(missing["missing"].([]string), ","), "pg_stat_activity") || strings.Contains(strings.Join(empty["missing"].([]string), ","), "pg_stat_activity") {
		t.Fatal(missing, empty)
	}
}
