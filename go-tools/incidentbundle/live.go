package incidentbundle

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"os/exec"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)
var pgIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
var cursorPattern = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\dZ$`)

// LiveConfig contains explicitly provided SSH and container identifiers.
type LiveConfig struct{ Host, Background, Extractor, Domain, DB, DBUser, DBName, Schema string }

func (c LiveConfig) Validate() error {
	for _, v := range []string{c.Host, c.Background, c.Domain, c.DB, c.DBUser} {
		if !identifier.MatchString(v) {
			return errors.New("invalid live configuration")
		}
	}
	if c.Extractor != "" && !identifier.MatchString(c.Extractor) {
		return errors.New("invalid live configuration")
	}
	if !pgIdentifier.MatchString(c.DBName) || !pgIdentifier.MatchString(c.Schema) {
		return errors.New("invalid live configuration")
	}
	return nil
}
func validCursor(v string) bool {
	if !cursorPattern.MatchString(v) {
		return false
	}
	_, err := time.Parse(time.RFC3339, v)
	return err == nil
}

// Runner invokes the remote script and must return only bounded stdout, never stderr.
type Runner func(context.Context, string, string) ([]byte, error)

// SSHRunner executes a validated script against the configured SSH alias.
func SSHRunner(ctx context.Context, host, script string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=5", host, "bash", "-s")
	cmd.Stdin = strings.NewReader(script)
	var output cappedBuffer
	cmd.Stdout = &output
	cmd.Stderr = io.Discard
	err := cmd.Run()
	return output.Bytes(), err
}

type cappedBuffer struct{ bytes.Buffer }

func (b *cappedBuffer) Write(p []byte) (int, error) {
	n := len(p)
	remaining := 1024*1024 - b.Len()
	if remaining > 0 {
		if len(p) > remaining {
			p = p[:remaining]
		}
		_, _ = b.Buffer.Write(p)
	}
	return n, nil
}
func scriptFor(c LiveConfig, cursors map[string]string) string {
	s := remoteScriptPrefix + remoteLogScript
	values := map[string]string{"BACKGROUND": c.Background, "DOMAIN": c.Domain, "EXTRACTOR": c.Extractor, "DB": c.DB, "DBUSER": c.DBUser, "DBNAME": c.DBName, "SCHEMA": c.Schema}
	for _, role := range []string{"background", "extractor"} {
		if validCursor(cursors[role]) {
			values["LOG_CURSOR_"+strings.ToUpper(role)] = cursors[role]
		} else if validCursor(cursors["_seed"]) {
			values["LOG_CURSOR_"+strings.ToUpper(role)] = cursors["_seed"]
		} else {
			values["LOG_CURSOR_"+strings.ToUpper(role)] = ""
		}
	}
	for k, v := range values {
		s = strings.ReplaceAll(s, "@"+k+"@", v)
	}
	return s
}
func intText(s string) any {
	if len(s) == 0 || len(s) > 18 {
		return nil
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return nil
		}
	}
	return json.Number(s)
}

// ParseLiveOutput applies an allowlist and updates only valid reported cursors.
func ParseLiveOutput(output []byte, cursors map[string]string) (object, []object) {
	raw := object{"pg_stat_activity": nil, "pg_metrics": object{}, "host": object{"memory_psi": object{}}, "containers": object{}, "collection_errors": []any{}}
	events := []object{}
	if len(output) > 1024*1024 {
		output = output[:1024*1024]
	}
	for i, line := range strings.Split(string(output), "\n") {
		if i >= 1024 {
			break
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		switch {
		case k == "log":
			if e := ParseLogLine(v); e != nil {
				events = append(events, e)
			}
		case k == "db_json":
			db := obj(readJSON([]byte(v)))
			for _, key := range []string{"captured_at", "worker", "pg_stat_activity", "pg_metrics"} {
				if value, ok := db[key]; ok {
					raw[key] = value
				}
			}
		case k == "log_cursor_seed" && validCursor(v):
			if cursors != nil && cursors["_seed"] == "" {
				cursors["_seed"] = v
			}
		case (k == "log_cursor_background" || k == "log_cursor_extractor") && validCursor(v):
			if cursors != nil {
				cursors[strings.TrimPrefix(k, "log_cursor_")] = v
			}
		case choices("background_error extractor_error postgres_error background_log_error extractor_log_error database_error")[k]:
			raw["collection_errors"] = append(raw["collection_errors"].([]any), v)
		case choices("memtotal memavailable swaptotal swapfree")[k]:
			names := map[string]string{"memtotal": "memory_total_bytes", "memavailable": "memory_available_bytes", "swaptotal": "swap_total_bytes", "swapfree": "swap_free_bytes"}
			if n := intText(v); n != nil {
				x, _ := strconv.ParseInt(string(n.(json.Number)), 10, 64)
				if x <= math.MaxInt64/1024 {
					obj(raw["host"])[names[k]] = json.Number(strconv.FormatInt(x*1024, 10))
				}
			}
		case k == "psi_some" || k == "psi_full":
			if n, e := strconv.ParseFloat(v, 64); e == nil && n >= 0 && n <= 100 {
				obj(obj(raw["host"])["memory_psi"])[strings.TrimPrefix(k, "psi_")+"_avg10"] = json.Number(v)
			}
		default:
			role, field, ok := strings.Cut(k, "_")
			if !ok || !choices("background extractor postgres")[role] {
				continue
			}
			containers := obj(raw["containers"])
			if containers[role] == nil {
				containers[role] = object{}
			}
			c := obj(containers[role])
			fields := map[string]string{"container": "container_id", "started": "started_at", "restarts": "restart_count", "oomkills": "oom_count", "memory_current": "memory_current_bytes", "file": "file_bytes", "inactive_file": "inactive_file_bytes", "periods": "cpu_periods", "throttled_periods": "cpu_throttled_periods", "throttled_usec": "cpu_throttled_usec"}
			if field == "oom" {
				if v == "true" {
					c["oom_killed"] = true
				} else if v == "false" {
					c["oom_killed"] = false
				}
			} else if name, ok := fields[field]; ok {
				if field == "container" || field == "started" {
					c[name] = v
				} else {
					c[name] = intText(v)
					if field == "file" {
						c["cache_bytes"] = intText(v)
					}
				}
			}
		}
	}
	for _, v := range obj(raw["containers"]) {
		c := obj(v)
		a, aok := c["memory_current_bytes"].(json.Number)
		b, bok := c["inactive_file_bytes"].(json.Number)
		if aok && bok {
			x, _ := a.Int64()
			y, _ := b.Int64()
			if x < y {
				x = y
			}
			c["working_set_bytes"] = json.Number(strconv.FormatInt(x-y, 10))
		}
		delete(c, "memory_current_bytes")
		delete(c, "inactive_file_bytes")
	}
	return raw, events
}

// Poll collects one bounded SSH response. Partial stdout is salvaged on timeout.
func Poll(ctx context.Context, c LiveConfig, cursors map[string]string, runner Runner) (object, []object) {
	output, err := runner(ctx, c.Host, scriptFor(c, cursors))
	raw, events := ParseLiveOutput(output, cursors)
	if err != nil {
		kind := "ssh_failed"
		if ctx.Err() != nil {
			kind = "timeout"
		}
		raw["collection_errors"] = append(raw["collection_errors"].([]any), kind)
	}
	return SanitizedSnapshot(raw), events
}
func updateDeltas(snapshot, previous object) {
	for name, v := range obj(snapshot["containers"]) {
		current := obj(v)
		old := obj(obj(previous["containers"])[name])
		same := current["container_id"] != nil && current["started_at"] != nil && current["container_id"] == old["container_id"] && current["started_at"] == old["started_at"]
		for _, pair := range [][2]string{{"cpu_throttled_usec", "cpu_throttled_usec_delta"}, {"cpu_throttled_periods", "cpu_throttled_periods_delta"}} {
			current[pair[1]] = nil
			n, nok := current[pair[0]].(int64)
			o, ook := old[pair[0]].(int64)
			if same && nok && ook && n >= o {
				current[pair[1]] = n - o
			}
		}
		key := "container_" + name + "_cpu_delta"
		missing := snapshot["missing"].([]string)
		updated := make([]string, 0, len(missing)+1)
		for _, m := range missing {
			if m != key {
				updated = append(updated, m)
			}
		}
		if current["cpu_throttled_usec_delta"] == nil || current["cpu_throttled_periods_delta"] == nil {
			updated = append(updated, key)
		}
		snapshot["missing"] = updated
	}
}

// CollectLive runs an explicitly configured, bounded remote observation.
func CollectLive(output string, c LiveConfig, seconds, maxBundles, byteLimit int, runner Runner) (count, size int, err error) {
	if e := c.Validate(); e != nil {
		return 0, 0, e
	}
	if seconds < 1 || seconds > MaxSeconds || maxBundles < 1 || maxBundles > MaxBundles || byteLimit < 1 || byteLimit > MaxOutputBytes {
		return 0, 0, errors.New("limits exceed hard safety bound")
	}
	if runner == nil {
		return 0, 0, errors.New("runner required")
	}
	dir, e := outputDir(output)
	if e != nil {
		return 0, 0, e
	}
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	var previous object
	ring := []object{}
	seen := map[string]bool{}
	cursors := map[string]string{}
	var last time.Time
	for time.Now().Before(deadline) && count < maxBundles && size < byteLimit {
		if !last.IsZero() {
			delay := time.Until(last.Add(time.Second))
			if delay > 0 {
				if remaining := time.Until(deadline); delay > remaining {
					delay = remaining
				}
				time.Sleep(delay)
			}
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			break
		}
		timeout := remaining
		if timeout > 12*time.Second {
			timeout = 12 * time.Second
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		snapshot, events := Poll(ctx, c, cursors, runner)
		cancel()
		last = time.Now()
		updateDeltas(snapshot, previous)
		previous = snapshot
		ring = append(ring, snapshot)
		if len(ring) > 30 {
			ring = ring[1:]
		}
		for _, event := range events {
			if count >= maxBundles {
				break
			}
			fp := fingerprint(event)
			if seen[fp] {
				continue
			}
			if len(seen) >= 256 {
				clear(seen)
			}
			seen[fp] = true
			size, err = writeBundle(dir, makeBundle(event, ring), count, size, byteLimit)
			if err != nil {
				return count, size, err
			}
			count++
			if size >= byteLimit {
				break
			}
		}
	}
	return count, size, nil
}
