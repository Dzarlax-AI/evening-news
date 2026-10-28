// Package incidentbundle produces bounded, allowlisted offline incident evidence.
package incidentbundle

import (
	"bufio"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

const MaxSeconds = 1800
const MaxOutputBytes = 5 * 1024 * 1024
const MaxBundles = 3
const MaxInputLine = 256 * 1024

type object = map[string]any

func obj(v any) object {
	if m, ok := v.(map[string]any); ok {
		return m
	}
	return object{}
}
func choices(s string) map[string]bool {
	m := map[string]bool{}
	for _, v := range strings.Fields(s) {
		m[v] = true
	}
	return m
}

var stages = choices("admission readiness discovery websocket events create_context create_target attach_target configure navigate dom cleanup parse validation")
var reasons = choices("context_timeout context_cancelled event_overflow ownership_loss unsupported client_disconnected malformed_detach owned_detach target_crashed protocol_error transport_error operation_failed navigation_failed evaluation_failed navigation_dns navigation_connection navigation_tls navigation_aborted navigation_other browser_busy resource_limit sqlstate timeout canceled transaction_closed connection_closed other lease_mismatch policy_denied unavailable")
var codes = choices("cancelled deadline_exceeded event_overflow invalid_input native_browser_busy native_browser_policy_denied parse_failed public_url_denied resource_limit transport_failed unavailable unsupported_selector response_too_large page_monitor_unavailable invalid_runner_response other")
var hexID = regexp.MustCompile(`^[a-fA-F0-9]{12,64}$`)
var tokenID = regexp.MustCompile(`^[a-f0-9]{64}$`)
var sqlstate = regexp.MustCompile(`^[A-Z0-9]{5}$`)
var waitEvent = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)

func safeTime(v any) any {
	s, ok := v.(string)
	if !ok || len(s) > 40 {
		return nil
	}
	t, e := time.Parse(time.RFC3339Nano, s)
	if e != nil {
		return nil
	}
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}
func integer(v any, min, max int64) any {
	n, ok := v.(json.Number)
	if !ok {
		return nil
	}
	i, e := n.Int64()
	if e != nil || i < min || i > max {
		return nil
	}
	return i
}
func number(v any) any {
	n, ok := v.(json.Number)
	if !ok {
		return nil
	}
	f, e := n.Float64()
	if e != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f > 1e18 {
		return nil
	}
	return n
}
func finite(v any, allowed map[string]bool) string {
	s, ok := v.(string)
	if ok && allowed[s] {
		return s
	}
	return "other"
}
func matched(v any, re *regexp.Regexp) any {
	s, ok := v.(string)
	if ok && re.MatchString(s) {
		return s
	}
	return nil
}
func readJSON(b []byte) any {
	var v any
	d := json.NewDecoder(strings.NewReader(string(b)))
	d.UseNumber()
	if d.Decode(&v) != nil {
		return nil
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil
	}
	return v
}
func SanitizedEvent(v any) object {
	r := obj(v)
	typ, _ := r["event"].(string)
	if typ != "worker_heartbeat_failure" && typ != "page_monitor_failure" {
		return nil
	}
	stamp := safeTime(r["event_utc"])
	if stamp == nil {
		return nil
	}
	var source any
	if s, ok := r["source"].(string); ok && (s == "background" || s == "extractor") {
		source = s
	}
	out := object{"event": typ, "event_utc": stamp, "source": source}
	out["reason"] = finite(r["reason"], reasons)
	if typ == "worker_heartbeat_failure" {
		out["stage"] = finite(r["stage"], choices("begin update commit unknown"))
		out["sqlstate"] = matched(r["sqlstate"], sqlstate)
		for _, k := range []string{"backend_pid", "generation", "mutex_wait_ms", "begin_ms", "update_ms", "commit_ms"} {
			min, max := int64(0), int64(math.MaxInt64)
			if k == "backend_pid" || k == "generation" {
				min = 1
			}
			if k == "backend_pid" {
				max = math.MaxInt32
			}
			out[k] = integer(r[k], min, max)
		}
	} else {
		out["stage"] = finite(r["stage"], stages)
		out["request_token"] = matched(r["request_token"], tokenID)
		out["source_id"] = integer(r["source_id"], 1, math.MaxInt64)
		out["elapsed_ms"] = integer(r["elapsed_ms"], 0, math.MaxInt64)
		out["outcome"] = finite(r["outcome"], choices("success partial failed rejected"))
		out["error_code"] = finite(r["error_code"], codes)
	}
	return out
}
func SanitizedSnapshot(v any) object {
	r := obj(v)
	out := object{"captured_at": safeTime(r["captured_at"])}
	missing := []string{}
	w := obj(r["worker"])
	worker := object{"generation": integer(w["generation"], 1, math.MaxInt64), "pid": integer(w["pid"], 1, math.MaxInt32), "heartbeat_at": safeTime(w["heartbeat_at"])}
	out["worker"] = worker
	if worker["generation"] == nil || worker["pid"] == nil || worker["heartbeat_at"] == nil {
		missing = append(missing, "worker")
	}
	activities := []any{}
	if list, ok := r["pg_stat_activity"].([]any); ok {
		for i, v := range list {
			if i >= 64 {
				break
			}
			a, ok := v.(map[string]any)
			if !ok {
				continue
			}
			blockers := []any{}
			if bs, ok := a["blocker_pids"].([]any); ok {
				for i, b := range bs {
					if i >= 16 {
						break
					}
					if n := integer(b, 1, math.MaxInt32); n != nil {
						blockers = append(blockers, n)
					}
				}
			}
			var wt any
			if a["wait_type"] != nil {
				wt = finite(a["wait_type"], choices("LWLock Lock BufferPin Activity Client Extension IO IPC Timeout"))
			}
			activities = append(activities, object{"pid": integer(a["pid"], 1, math.MaxInt32), "state": finite(a["state"], choices("active idle")), "wait_type": wt, "wait_event": matched(a["wait_event"], waitEvent), "transaction_age_seconds": number(a["transaction_age_seconds"]), "blocker_pids": blockers})
			state, _ := a["state"].(string)
			if state == "idle in transaction" || state == "idle in transaction (aborted)" || state == "fastpath function call" || state == "disabled" {
				activities[len(activities)-1].(object)["state"] = state
			}
		}
	} else {
		missing = append(missing, "pg_stat_activity")
	}
	out["pg_stat_activity"] = activities
	metrics := obj(r["pg_metrics"])
	mo := object{}
	metricMissing := false
	for _, k := range []string{"wal_bytes", "checkpoints_timed", "checkpoints_requested", "checkpoint_write_time_ms", "checkpoint_sync_time_ms", "buffers_checkpoint", "buffers_clean"} {
		mo[k] = number(metrics[k])
		if mo[k] == nil {
			metricMissing = true
		}
	}
	out["pg_metrics"] = mo
	if metricMissing {
		missing = append(missing, "pg_metrics")
	}
	h := obj(r["host"])
	psi := obj(h["memory_psi"])
	host := object{}
	hostMissing := false
	for _, k := range []string{"memory_total_bytes", "memory_available_bytes", "swap_total_bytes", "swap_free_bytes"} {
		host[k] = integer(h[k], 0, math.MaxInt64)
		if host[k] == nil {
			hostMissing = true
		}
	}
	for _, k := range []string{"some", "full"} {
		key := "memory_psi_" + k + "_avg10"
		host[key] = number(psi[k+"_avg10"])
		if host[key] == nil {
			hostMissing = true
		}
	}
	out["host"] = host
	if hostMissing {
		missing = append(missing, "host")
	}
	containers, ok := r["containers"].(map[string]any)
	if !ok {
		containers = object{"background": r["cgroup"]}
	}
	co := object{}
	for _, name := range []string{"background", "extractor", "postgres"} {
		c := obj(containers[name])
		x := object{}
		for _, k := range []string{"working_set_bytes", "cache_bytes", "file_bytes", "cpu_throttled_usec", "cpu_throttled_usec_delta", "cpu_periods", "cpu_throttled_periods", "cpu_throttled_periods_delta", "restart_count", "oom_count"} {
			x[k] = integer(c[k], 0, math.MaxInt64)
		}
		x["container_id"] = matched(c["container_id"], hexID)
		x["started_at"] = safeTime(c["started_at"])
		if b, ok := c["oom_killed"].(bool); ok {
			x["oom_killed"] = b
		} else {
			x["oom_killed"] = nil
		}
		co[name] = x
		for _, k := range []string{"working_set_bytes", "file_bytes", "cpu_throttled_usec", "cpu_periods", "cpu_throttled_periods", "container_id", "started_at", "restart_count", "oom_count", "oom_killed"} {
			if x[k] == nil {
				missing = append(missing, "container_"+name)
				break
			}
		}
		if c["cpu_throttled_usec_delta"] == nil || c["cpu_throttled_periods_delta"] == nil {
			missing = append(missing, "container_"+name+"_cpu_delta")
		}
	}
	out["containers"] = co
	errs := []string{}
	if list, ok := r["collection_errors"].([]any); ok {
		allowed := choices("ssh_failed timeout log_overflow remote_snapshot_failed database_unavailable container_unavailable cgroup_unavailable log_unavailable")
		for i, e := range list {
			if i >= 16 {
				break
			}
			errs = append(errs, finite(e, allowed))
		}
	}
	out["collection_errors"] = errs
	out["missing"] = missing
	return out
}

func ParseLogLine(line string) object {
	if v := readJSON([]byte(line)); v != nil {
		r := obj(v)
		if _, ok := r["event_utc"]; !ok {
			r["event_utc"] = r["time"]
		}
		return SanitizedEvent(r)
	}
	fields := object{}
	var parts []string
	var b strings.Builder
	var quote rune
	escaped := false
	for _, ch := range line {
		if escaped {
			b.WriteRune(ch)
			escaped = false
			continue
		}
		if ch == '\\' && quote != '\'' {
			escaped = true
			continue
		}
		if quote != 0 {
			if ch == quote {
				quote = 0
			} else {
				b.WriteRune(ch)
			}
			continue
		}
		if ch == '\'' || ch == '"' {
			quote = ch
			continue
		}
		if ch == ' ' || ch == '\t' || ch == '\n' {
			if b.Len() > 0 {
				parts = append(parts, b.String())
				b.Reset()
			}
		} else {
			b.WriteRune(ch)
		}
	}
	if quote != 0 || escaped {
		return nil
	}
	if b.Len() > 0 {
		parts = append(parts, b.String())
	}
	numeric := choices("generation backend_pid mutex_wait_ms begin_ms update_ms commit_ms source_id elapsed_ms")
	for _, p := range parts {
		k, v, ok := strings.Cut(p, "=")
		if !ok {
			continue
		}
		if numeric[k] {
			if len(v) > 18 || v == "" {
				fields[k] = nil
				continue
			}
			valid := true
			for _, c := range v {
				if c < '0' || c > '9' {
					valid = false
					break
				}
			}
			if valid {
				fields[k] = json.Number(v)
			} else {
				fields[k] = nil
			}
		} else {
			fields[k] = v
		}
	}
	return SanitizedEvent(fields)
}
func makeBundle(event object, ring []object) object {
	var current any
	status := "late_or_missing"
	if len(ring) > 0 {
		current = ring[len(ring)-1]
		a, _ := time.Parse(time.RFC3339Nano, event["event_utc"].(string))
		if s, ok := ring[len(ring)-1]["captured_at"].(string); ok {
			b, e := time.Parse(time.RFC3339Nano, s)
			if e == nil && a.Sub(b) <= time.Second && b.Sub(a) <= time.Second {
				status = "within_one_second"
			}
		}
	}
	pre := append([]object{}, ring...)
	if len(pre) > 0 {
		pre = pre[:len(pre)-1]
	}
	return object{"collected_at": time.Now().UTC().Format("2006-01-02T15:04:05.000Z"), "trigger": event, "pre_event_snapshots": pre, "current_snapshot": current, "evidence_status": object{"current_snapshot": status, "snapshot_count": len(ring)}}
}
func fingerprint(e object) string {
	v := object{}
	for _, k := range []string{"event", "event_utc", "source", "request_token", "source_id", "backend_pid", "generation", "stage", "reason"} {
		v[k] = e[k]
	}
	b, _ := json.Marshal(v)
	return string(b)
}
func outputDir(parent string) (string, error) {
	if err := os.MkdirAll(parent, 0700); err != nil {
		return "", err
	}
	for range 8 {
		var r [3]byte
		if _, err := rand.Read(r[:]); err != nil {
			return "", err
		}
		path := filepath.Join(parent, "incident-"+time.Now().UTC().Format("20060102T150405Z")+"-"+hex.EncodeToString(r[:]))
		if err := os.Mkdir(path, 0700); err == nil {
			return path, nil
		} else if !os.IsExist(err) {
			return "", err
		}
	}
	return "", errors.New("cannot create unique output directory")
}
func writeBundle(dir string, v object, count, size, limit int) (int, error) {
	data, e := json.Marshal(v)
	if e != nil {
		return size, e
	}
	data = append(data, '\n')
	if len(data) > limit-size {
		return size, errors.New("output_limit")
	}
	f, e := os.OpenFile(filepath.Join(dir, fmt.Sprintf("bundle-%d.json", count+1)), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return size, e
	}
	_, e = f.Write(data)
	closeErr := f.Close()
	if e != nil {
		return size, e
	}
	if closeErr != nil {
		return size, closeErr
	}
	return size + len(data), nil
}

// Replay reads a regular JSONL fixture with hard limits and private output permissions.
func Replay(path, output string, duration, maxBundles, byteLimit int) (count, size int, err error) {
	if duration < 1 || duration > MaxSeconds || maxBundles < 1 || maxBundles > MaxBundles || byteLimit < 1 || byteLimit > MaxOutputBytes {
		return 0, 0, errors.New("limits exceed hard safety bound")
	}
	pre, e := os.Stat(path)
	if e != nil || !pre.Mode().IsRegular() {
		return 0, 0, errors.New("fixture must be a regular file")
	}
	f, e := os.Open(path)
	if e != nil {
		return 0, 0, e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || !info.Mode().IsRegular() {
		return 0, 0, errors.New("fixture must be a regular file")
	}
	dir, e := outputDir(output)
	if e != nil {
		return 0, 0, e
	}
	deadline := time.Now().Add(time.Duration(duration) * time.Second)
	reader := bufio.NewReaderSize(f, MaxInputLine+1)
	ring := []object{}
	seen := map[string]bool{}
	for time.Now().Before(deadline) && count < maxBundles && size < byteLimit {
		line, e := reader.ReadSlice('\n')
		if e == bufio.ErrBufferFull {
			for e == bufio.ErrBufferFull && time.Now().Before(deadline) {
				_, e = reader.ReadSlice('\n')
			}
			continue
		}
		if len(line) > MaxInputLine {
			continue
		}
		if len(line) == 0 && e != nil {
			break
		}
		item := obj(readJSON(line))
		kind, _ := item["kind"].(string)
		switch kind {
		case "snapshot":
			ring = append(ring, SanitizedSnapshot(item["snapshot"]))
			if len(ring) > 30 {
				ring = ring[1:]
			}
		case "event", "log":
			var event object
			if kind == "event" {
				event = SanitizedEvent(item["event"])
			} else if s, ok := item["line"].(string); ok {
				event = ParseLogLine(s)
			}
			if event != nil {
				fp := fingerprint(event)
				if !seen[fp] {
					if len(seen) >= 256 {
						clear(seen)
					}
					seen[fp] = true
					size, err = writeBundle(dir, makeBundle(event, ring), count, size, byteLimit)
					if err != nil {
						return count, size, err
					}
					count++
				}
			}
		}
		if e == io.EOF {
			break
		}
		if e != nil {
			return count, size, e
		}
	}
	return count, size, nil
}
