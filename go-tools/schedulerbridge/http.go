package schedulerbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

type ServiceConfig struct {
	URL, Token string
	AllowHTTP  bool
	Timeout    time.Duration
	Client     *http.Client
}

func (c ServiceConfig) client() (*http.Client, string, error) {
	base, e := service(c.URL, c.Token, c.AllowHTTP)
	if e != nil {
		return nil, "", e
	}
	timeout := c.Timeout
	if timeout <= 0 || timeout > 10*time.Minute {
		return nil, "", fail("invalid_timeout")
	}
	h := c.Client
	if h == nil {
		h = HTTPClient(timeout)
	} else {
		copy := *h
		copy.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
		h = &copy
	}
	return h, base, nil
}
func request(ctx context.Context, h *http.Client, method, url, token string, body any) (any, error) {
	var data []byte
	var e error
	if body != nil {
		data, e = json.Marshal(body)
		if e != nil {
			return nil, fail("invalid_request")
		}
	}
	req, e := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(data))
	if e != nil {
		return nil, fail("invalid_request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept-Encoding", "identity")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, e := h.Do(req)
	if e != nil {
		return nil, fail("scheduler_unavailable")
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 512*1024+1))
	if e != nil || len(raw) > 512*1024 {
		return nil, fail("scheduler_response_too_large")
	}
	if resp.StatusCode >= 300 {
		return nil, fail("scheduler_http_error")
	}
	if resp.StatusCode == 204 {
		return nil, nil
	}
	if v := resp.Header.Get("Content-Encoding"); v != "" && !strings.EqualFold(v, "identity") {
		return nil, fail("scheduler_encoding_invalid")
	}
	typ, _, e := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if e != nil || typ != "application/json" {
		return nil, fail("scheduler_response_not_json")
	}
	var value any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if e = dec.Decode(&value); e != nil {
		return nil, fail("scheduler_json_invalid")
	}
	if e = dec.Decode(new(any)); e != io.EOF {
		return nil, fail("scheduler_json_invalid")
	}
	return value, nil
}

// Import sends one validated atomic import with an expected control revision.
func Import(ctx context.Context, c ServiceConfig, path, revision string) (any, error) {
	raw, e := os.ReadFile(path)
	if e != nil {
		return nil, fail("snapshot_read_failed")
	}
	var snapshot Object
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&snapshot) != nil || snapshot["format"] != "news-scheduler-snapshot-v1" {
		return nil, fail("invalid_snapshot")
	}
	items, ok := snapshot["schedules"].([]any)
	if !ok || len(items) < 1 || len(items) > 100 {
		return nil, fail("invalid_schedule_count")
	}
	seen := map[string]bool{}
	for _, v := range items {
		m, ok := object(v)
		if !ok {
			return nil, fail("invalid_schedule_fields")
		}
		if e = validateItem(m); e != nil {
			return nil, e
		}
		key := m["schedule_key"].(string)
		if seen[key] {
			return nil, fail("duplicate_schedule_key")
		}
		seen[key] = true
	}
	if revision == "" {
		return nil, fail("invalid_control_revision")
	}
	for _, c := range revision {
		if c < '0' || c > '9' {
			return nil, fail("invalid_control_revision")
		}
	}
	n, e := strconv.ParseInt(revision, 10, 64)
	if e != nil {
		return nil, fail("invalid_control_revision")
	}
	h, base, e := c.client()
	if e != nil {
		return nil, e
	}
	defer h.CloseIdleConnections()
	value, err := request(ctx, h, "POST", base+"/v1/import", c.Token, Object{"expected_control_revision": n, "items": items})
	if err != nil {
		return nil, err
	}
	result, ok := object(value)
	if !ok || !exact(result, "control_revision", "created", "unchanged") {
		return nil, fail("invalid_import_response")
	}
	rev, ok := integer(result["control_revision"])
	if !ok || rev < 0 {
		return nil, fail("invalid_import_response")
	}
	for _, field := range []string{"created", "unchanged"} {
		list, ok := result[field].([]any)
		if !ok || len(list) > 100 {
			return nil, fail("invalid_import_response")
		}
		for _, v := range list {
			key, ok := v.(string)
			if !ok || !seen[key] {
				return nil, fail("invalid_import_response")
			}
		}
	}
	return result, nil
}
