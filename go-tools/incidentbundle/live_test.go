package incidentbundle

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func testConfig() LiveConfig {
	return LiveConfig{Host: "example", Background: "background", Domain: "domain", Extractor: "extractor", DB: "db", DBUser: "user", DBName: "aistack", Schema: "news"}
}
func TestPollPartialAndCursorRecovery(t *testing.T) {
	c := testConfig()
	cursors := map[string]string{}
	calls := 0
	runner := func(ctx context.Context, host, script string) ([]byte, error) {
		calls++
		if calls == 1 {
			if !strings.Contains(script, "BEGIN READ ONLY") || !strings.Contains(script, "PIPESTATUS") || strings.Contains(script, "python3") {
				t.Fatal("bad remote script")
			}
			return []byte("log_cursor_seed=2026-10-27T10:00:00Z\nlog={\"event\":\"worker_heartbeat_failure\",\"event_utc\":\"2026-10-27T10:00:01Z\",\"reason\":\"timeout\",\"msg\":\"CANARY\"}\n"), errors.New("failed")
		}
		if !strings.Contains(script, "LOG_CURSOR_BACKGROUND='2026-10-27T10:00:00Z'") || !strings.Contains(script, "LOG_CURSOR_EXTRACTOR='2026-10-27T10:00:00Z'") {
			t.Fatal("cursor lost")
		}
		return []byte("log_cursor_background=2026-10-27T10:00:13Z\nlog_cursor_extractor=2026-10-27T10:00:14Z\n"), nil
	}
	s, e := Poll(context.Background(), c, cursors, runner)
	b, _ := json.Marshal(s)
	if strings.Contains(string(b), "CANARY") || len(e) != 1 || cursors["background"] != "" || cursors["extractor"] != "" || cursors["_seed"] != "2026-10-27T10:00:00Z" {
		t.Fatal(s, e, cursors)
	}
	Poll(context.Background(), c, cursors, runner)
	if cursors["background"] != "2026-10-27T10:00:13Z" || cursors["extractor"] != "2026-10-27T10:00:14Z" {
		t.Fatal(cursors)
	}
}
func TestCollectLiveDeduplicatesAndComputesDeltas(t *testing.T) {
	calls := 0
	runner := func(ctx context.Context, host, script string) ([]byte, error) {
		calls++
		id := strings.Repeat("a", 32)
		usec, periods := 10, 1
		if calls > 1 {
			usec, periods = 15, 3
		}
		if calls > 2 {
			id = strings.Repeat("b", 32)
			usec, periods = 1, 1
		}
		event := `{"event":"page_monitor_failure","event_utc":"2026-10-27T10:00:00Z","source_id":` + string(rune('0'+calls)) + `,"stage":"navigate","reason":"timeout"}`
		return []byte("background_container=" + id + "\nbackground_started=2026-10-27T10:00:00Z\nbackground_throttled_usec=" + json.Number(string(rune('0'+usec/10))+string(rune('0'+usec%10))).String() + "\nbackground_throttled_periods=" + string(rune('0'+periods)) + "\nlog=" + event + "\nlog=" + event + "\n"), nil
	}
	dir := t.TempDir()
	n, _, err := CollectLive(dir, testConfig(), 5, 3, MaxOutputBytes, runner)
	if err != nil || n != 3 {
		t.Fatal(n, err)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "incident-*", "bundle-*.json"))
	for i, f := range files {
		data, _ := os.ReadFile(f)
		var b map[string]any
		if json.Unmarshal(data, &b) != nil {
			t.Fatal("invalid bundle")
		}
		delta := b["current_snapshot"].(map[string]any)["containers"].(map[string]any)["background"].(map[string]any)["cpu_throttled_usec_delta"]
		if i == 1 && delta != float64(5) {
			t.Fatal(delta)
		}
		if i != 1 && delta != nil {
			t.Fatal(delta)
		}
	}
}
func TestLiveRejectsUnsafeConfig(t *testing.T) {
	c := testConfig()
	c.Host = "host;unsafe"
	called := false
	_, _, err := CollectLive(t.TempDir(), c, 1, 1, 1000, func(context.Context, string, string) ([]byte, error) { called = true; return nil, nil })
	if err == nil || called {
		t.Fatal("unsafe config reached runner")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s, _ := Poll(ctx, testConfig(), nil, func(context.Context, string, string) ([]byte, error) { return nil, errors.New("timeout") })
	if !strings.Contains(strings.Join(s["collection_errors"].([]string), ","), "timeout") {
		t.Fatal(s)
	}
}

func TestRemotePipelineRequiresBothSuccess(t *testing.T) {
	dir := t.TempDir()
	docker := filepath.Join(dir, "docker")
	script := `#!/bin/sh
case "$1" in
 inspect) exit 1 ;;
 logs) if [ "$FAKE_MODE" = fail ]; then exit 1; fi; printf '%s\n' raw ;;
 exec) if [ "$2" = -i ]; then cat >/dev/null; printf '%s\n' 'log={"event":"worker_heartbeat_failure","event_utc":"2026-10-27T10:00:00Z"}'; if [ "$FAKE_MODE" = overflow ]; then exit 3; fi; exit 0; fi; exit 1 ;;
esac
`
	if err := os.WriteFile(docker, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "timeout"), []byte("#!/bin/sh\nshift\nexec \"$@\"\n"), 0700); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		mode      string
		cursor    bool
		errorText string
	}{{"ok", true, ""}, {"fail", false, "background_log_error=log_unavailable"}, {"overflow", false, "background_log_error=log_overflow"}} {
		t.Run(tc.mode, func(t *testing.T) {
			cmd := exec.Command("bash", "-c", scriptFor(testConfig(), map[string]string{}))
			cmd.Env = append(os.Environ(), "PATH="+dir+":"+os.Getenv("PATH"), "FAKE_MODE="+tc.mode)
			out, err := cmd.Output()
			if err != nil {
				t.Fatal(err)
			}
			s := string(out)
			if strings.Contains(s, "log_cursor_background=") != tc.cursor || tc.errorText != "" && !strings.Contains(s, tc.errorText) {
				t.Fatal(s)
			}
			if strings.Contains(s, "raw") {
				t.Fatal("raw log escaped")
			}
		})
	}
}
