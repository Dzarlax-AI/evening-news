// Package schedulerbridge provides offline scheduler cutover and rollback inventory.
package schedulerbridge

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"
	_ "time/tzdata"

	"github.com/jackc/pgx/v5"
)

var operations = map[string]bool{"news_processing": true, "news_digest": true, "telegram_digest": true, "backup": true, "reprocess_failed": true}
var newsKey = regexp.MustCompile(`^news:[1-9][0-9]*$`)
var schedulerKey = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
var uuidRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
var timestampRE = regexp.MustCompile(`^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z$`)

type Object = map[string]any

func fail(code string) error { return errors.New(code) }
func utc(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format("2006-01-02T15:04:05.000000Z")
}
func writeArtifact(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fail("artifact_invalid")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return fail("artifact_create_failed")
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(path)
		}
	}()
	if _, err = f.Write(append(data, '\n')); err != nil {
		return fail("artifact_write_failed")
	}
	if err = f.Close(); err != nil {
		return fail("artifact_write_failed")
	}
	ok = true
	return nil
}
func openDB(ctx context.Context, dsn string) (*pgx.Conn, error) {
	dsn = strings.Replace(dsn, "postgresql+asyncpg://", "postgresql://", 1)
	c, e := pgx.ParseConfig(dsn)
	if e != nil || !map[string]bool{"127.0.0.1": true, "localhost": true, "::1": true}[c.Host] {
		return nil, fail("snapshot_database_must_be_loopback")
	}
	p, e := pgx.ConnectConfig(ctx, c)
	if e != nil {
		return nil, fail("snapshot_database_unavailable")
	}
	return p, nil
}
func payload(kind string, raw any, strict bool) (Object, error) {
	if !operations[kind] {
		return nil, fail("unsupported_schedule_operation")
	}
	v, ok := raw.(map[string]any)
	if !ok {
		v = Object{}
	}
	allowed := map[string]bool{"timeout_seconds": true}
	if kind == "news_digest" {
		allowed["run_processing"] = true
		allowed["send_telegram"] = true
	}
	if kind == "reprocess_failed" {
		allowed["limit"] = true
		allowed["dry_run"] = true
	}
	out := Object{}
	for k, x := range v {
		if k == "interval_minutes" && !strict {
			continue
		}
		if !allowed[k] {
			return nil, fail("unsupported_task_config_field")
		}
		out[k] = x
	}
	if strict {
		for k, x := range out {
			switch k {
			case "run_processing", "send_telegram", "dry_run":
				if _, ok := x.(bool); !ok {
					return nil, fail("invalid_schedule_payload")
				}
			case "timeout_seconds", "limit":
				n, ok := integer(x)
				max := 86400
				if k == "limit" {
					max = 100
				}
				if !ok || n < 1 || n > int64(max) {
					return nil, fail("invalid_schedule_payload")
				}
			}
		}
		b, _ := json.Marshal(out)
		if len(b) > 4096 {
			return nil, fail("invalid_schedule_payload")
		}
	}
	return out, nil
}
func integer(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		x, e := n.Int64()
		return x, e == nil
	case float64:
		if n != float64(int64(n)) {
			return 0, false
		}
		return int64(n), true
	case int:
		return int64(n), true
	case int64:
		return n, true
	}
	return 0, false
}
func timestamp(s any) bool {
	v, ok := s.(string)
	if !ok || !timestampRE.MatchString(v) {
		return false
	}
	_, e := time.Parse(time.RFC3339Nano, v)
	return e == nil
}
func object(v any) (Object, bool) { m, ok := v.(map[string]any); return m, ok }
func exact(m Object, keys ...string) bool {
	if len(m) != len(keys) {
		return false
	}
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}
func validateItem(m Object) error {
	if !exact(m, "schedule_key", "enabled", "calendar", "target_alias", "operation", "payload", "next_due_at") {
		return fail("invalid_schedule_fields")
	}
	op, ok := m["operation"].(string)
	key, keyOK := m["schedule_key"].(string)
	_, en := m["enabled"].(bool)
	cal, calOK := object(m["calendar"])
	pay, payOK := object(m["payload"])
	if !ok || !operations[op] || !keyOK || (!newsKey.MatchString(key) && key != "news:"+op) || !en || m["target_alias"] != "news-worker" || !calOK || !payOK {
		return fail("invalid_schedule_values")
	}
	typ, ok := cal["type"].(string)
	allowed := map[string]map[string]bool{"daily": {"type": true, "timezone": true, "hour": true, "minute": true, "weekdays": true}, "weekly": {"type": true, "timezone": true, "hour": true, "minute": true, "weekdays": true}, "hourly": {"type": true, "timezone": true, "minute": true, "weekdays": true}, "interval": {"type": true, "timezone": true, "interval_minutes": true}}
	fields := allowed[typ]
	if !ok || fields == nil {
		return fail("invalid_schedule_calendar")
	}
	for k := range cal {
		if !fields[k] {
			return fail("invalid_schedule_calendar")
		}
	}
	tz, ok := cal["timezone"].(string)
	if !ok || tz == "" {
		return fail("invalid_schedule_timezone")
	}
	if _, e := time.LoadLocation(tz); e != nil {
		return fail("invalid_schedule_timezone")
	}
	if _, e := payload(op, pay, true); e != nil {
		return e
	}
	if m["next_due_at"] != nil && !timestamp(m["next_due_at"]) {
		return fail("invalid_schedule_due")
	}
	return nil
}
func service(base, token string, allowHTTP bool) (string, error) {
	u, e := url.Parse(strings.TrimRight(base, "/"))
	if e != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Scheme != "https" && !(allowHTTP && u.Scheme == "http")) || strings.ContainsAny(base, " \n\r\t") || token == "" {
		return "", fail("scheduler_service_configuration_invalid")
	}
	return strings.TrimRight(base, "/"), nil
}
func HTTPClient(timeout time.Duration) *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.Proxy = nil
	t.DisableCompression = true
	t.DialContext = (&net.Dialer{Timeout: timeout}).DialContext
	return &http.Client{Transport: t, Timeout: timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}
