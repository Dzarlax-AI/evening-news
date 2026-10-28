package incidentfilter

import (
	"encoding/json"
	"io"
	"math"
	"regexp"
	"strings"
	"time"
)

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
var tokenID = regexp.MustCompile(`^[a-f0-9]{64}$`)
var sqlstate = regexp.MustCompile(`^[A-Z0-9]{5}$`)

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
