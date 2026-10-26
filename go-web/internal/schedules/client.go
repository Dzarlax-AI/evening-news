package schedules

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	MaxResponseBytes = 64 * 1024
	MaxRequestBytes  = 16 * 1024
	SchedulePageSize = 2
	maxScheduleCount = 10000
)

type ClientConfig struct {
	Endpoint  string
	Token     string
	AllowHTTP bool
	Timeout   time.Duration
}

type SchedulerError struct {
	Code   string
	Status int
}

func (e *SchedulerError) Error() string { return e.Code }

type Client struct {
	endpoint string
	token    string
	http     *http.Client
}

func NewClient(cfg ClientConfig) (*Client, error) {
	parsed, err := url.Parse(cfg.Endpoint)
	if err != nil || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Scheme != "https" && !(cfg.AllowHTTP && parsed.Scheme == "http")) {
		return nil, errors.New("invalid scheduler endpoint")
	}
	if cfg.Token == "" {
		return nil, errors.New("scheduler API token is required")
	}
	if cfg.Timeout <= 0 || cfg.Timeout > 30*time.Second {
		return nil, errors.New("scheduler timeout must be between 1 and 30 seconds")
	}
	transport := &http.Transport{Proxy: nil, DisableCompression: true, MaxIdleConnsPerHost: 2, IdleConnTimeout: 30 * time.Second}
	httpClient := &http.Client{Timeout: cfg.Timeout, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Client{endpoint: strings.TrimRight(cfg.Endpoint, "/"), token: cfg.Token, http: httpClient}, nil
}

func NewClientFromEnv() (*Client, error) {
	allowHTTP := false
	if raw := os.Getenv("SCHEDULER_API_ALLOW_HTTP"); raw != "" {
		parsed, err := strconv.ParseBool(raw)
		if err != nil {
			return nil, errors.New("invalid scheduler HTTP policy")
		}
		allowHTTP = parsed
	}
	timeout := 5 * time.Second
	if raw := os.Getenv("SCHEDULER_HTTP_TIMEOUT_SECONDS"); raw != "" {
		seconds, err := strconv.ParseFloat(raw, 64)
		if err != nil || seconds <= 0 || seconds > 30 {
			return nil, errors.New("invalid scheduler timeout")
		}
		timeout = time.Duration(seconds * float64(time.Second))
	}
	return NewClient(ClientConfig{Endpoint: os.Getenv("SCHEDULER_API_URL"), Token: os.Getenv("SCHEDULER_API_TOKEN"), AllowHTTP: allowHTTP, Timeout: timeout})
}

func NewHandlerFromEnv(jobs JobsSnapshotReader) *Handler {
	client, err := NewClientFromEnv()
	if err != nil {
		client = nil
	}
	return NewHandler(NewRoot(client, jobs, nil))
}

func (c *Client) Request(ctx context.Context, method, path string, body any, expected ...int) (any, error) {
	if !strings.HasPrefix(path, "/v1/") || hasDotDotSegment(path) {
		return nil, errors.New("invalid scheduler path")
	}
	var payload []byte
	var err error
	if body != nil {
		payload, err = json.Marshal(body)
		if err != nil {
			return nil, &SchedulerError{Code: "invalid_request"}
		}
		if len(payload) > MaxRequestBytes {
			return nil, &SchedulerError{Code: "request_too_large"}
		}
	}
	request, err := http.NewRequestWithContext(ctx, method, c.endpoint+path, bytes.NewReader(payload))
	if err != nil {
		return nil, &SchedulerError{Code: "invalid_request"}
	}
	request.Header.Set("Authorization", "Bearer "+c.token)
	request.Header.Set("Accept", "application/json")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := c.http.Do(request)
	if err != nil {
		var netErr net.Error
		if errors.As(err, &netErr) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return nil, &SchedulerError{Code: "transport_error"}
		}
		return nil, &SchedulerError{Code: "transport_error"}
	}
	defer response.Body.Close()
	if !containsStatus(expected, response.StatusCode) {
		return nil, &SchedulerError{Code: fmt.Sprintf("http_%d", response.StatusCode), Status: response.StatusCode}
	}
	if response.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if strings.ToLower(response.Header.Get("Content-Encoding")) != "" && strings.ToLower(response.Header.Get("Content-Encoding")) != "identity" {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	if mediaType(response.Header.Get("Content-Type")) != "application/json" {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	reader := io.LimitReader(response.Body, MaxResponseBytes+1)
	raw, err := io.ReadAll(reader)
	if err != nil {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	if len(raw) > MaxResponseBytes {
		return nil, &SchedulerError{Code: "response_too_large"}
	}
	var result any
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err = decoder.Decode(&result); err != nil {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	var extra any
	if err = decoder.Decode(&extra); err != io.EOF {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	return result, nil
}

func (c *Client) ListSchedules(ctx context.Context) ([]map[string]any, error) {
	all := make([]map[string]any, 0)
	cursor := ""
	seen := map[string]bool{}
	for {
		path := "/v1/schedules?limit=" + fmt.Sprint(SchedulePageSize)
		if cursor != "" {
			path += "&cursor=" + url.QueryEscape(cursor)
		}
		value, err := c.Request(ctx, http.MethodGet, path, nil, http.StatusOK)
		if err != nil {
			return nil, err
		}
		result, ok := value.(map[string]any)
		if !ok || !exactKeys(result, "schedules", "next_cursor") {
			return nil, &SchedulerError{Code: "invalid_response"}
		}
		rows, ok := result["schedules"].([]any)
		if !ok || len(rows) > SchedulePageSize {
			return nil, &SchedulerError{Code: "invalid_response"}
		}
		for _, row := range rows {
			resource, ok := row.(map[string]any)
			if !ok || ValidateScheduleResource(resource) != nil {
				return nil, &SchedulerError{Code: "invalid_response"}
			}
			all = append(all, resource)
		}
		if len(all) > maxScheduleCount {
			return nil, &SchedulerError{Code: "schedule_inventory_too_large"}
		}
		next := result["next_cursor"]
		if next == nil {
			return all, nil
		}
		cursor, ok = next.(string)
		if !ok || seen[cursor] {
			return nil, &SchedulerError{Code: "invalid_response"}
		}
		seen[cursor] = true
	}
}

func (c *Client) GetSchedule(ctx context.Context, key string) (map[string]any, error) {
	if !scheduleKeyRE.MatchString(key) {
		return nil, &SchedulerError{Code: "invalid_request"}
	}
	value, err := c.Request(ctx, http.MethodGet, "/v1/schedules/"+url.PathEscape(key), nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	resource, ok := value.(map[string]any)
	if !ok || ValidateScheduleResource(resource) != nil {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	return resource, nil
}

func (c *Client) PutSchedule(ctx context.Context, key string, body map[string]any) (map[string]any, error) {
	if !scheduleKeyRE.MatchString(key) {
		return nil, &SchedulerError{Code: "invalid_request"}
	}
	value, err := c.Request(ctx, http.MethodPut, "/v1/schedules/"+url.PathEscape(key), body, http.StatusOK, http.StatusCreated)
	if err != nil {
		var serviceErr *SchedulerError
		if !errors.As(err, &serviceErr) || serviceErr.Code != "transport_error" {
			return nil, err
		}
		current, getErr := c.GetSchedule(ctx, key)
		if getErr != nil {
			return nil, err
		}
		desired := map[string]any{}
		for k, v := range body {
			if k != "expected_revision" {
				desired[k] = v
			}
		}
		for k, want := range desired {
			if got, ok := current[k]; !ok || !jsonEqual(got, want) {
				return nil, err
			}
		}
		value = current
	}
	resource, ok := value.(map[string]any)
	if !ok || ValidateScheduleResource(resource) != nil || resource["schedule_key"] != key {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	return resource, nil
}

func (c *Client) DeleteSchedule(ctx context.Context, key string, revision int64) error {
	if !scheduleKeyRE.MatchString(key) || revision < 1 {
		return &SchedulerError{Code: "invalid_request"}
	}
	_, err := c.Request(ctx, http.MethodDelete, "/v1/schedules/"+url.PathEscape(key), map[string]any{"expected_revision": revision}, http.StatusNoContent)
	return err
}

func (c *Client) GetStatus(ctx context.Context) (map[string]any, error) {
	value, err := c.Request(ctx, http.MethodGet, "/v1/status", nil, http.StatusOK)
	if err != nil {
		return nil, err
	}
	result, ok := value.(map[string]any)
	if !ok {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	if _, ok := result["paused"].(bool); !ok {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	if _, exists := result["control_revision"]; !exists {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	if _, ok := integer(result["control_revision"]); !ok || mustInt(result["control_revision"]) < 0 {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	if _, exists := result["pending_outbox"]; !exists {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	if _, ok := integer(result["pending_outbox"]); !ok || mustInt(result["pending_outbox"]) < 0 {
		return nil, &SchedulerError{Code: "invalid_response"}
	}
	for _, field := range []string{"last_tick_at", "last_dispatch_at"} {
		if v := result[field]; v != nil {
			if str, ok := v.(string); !ok || !isUTCTimestamp(str) {
				return nil, &SchedulerError{Code: "invalid_response"}
			}
		}
	}
	return result, nil
}

var scheduleKeyRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._:-]{0,127}$`)
var utcTimestampRE = regexp.MustCompile(`^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{1,6})?Z$`)

func ValidateScheduleResource(v map[string]any) error {
	if !exactKeys(v, "schedule_key", "revision", "enabled", "calendar", "target_alias", "operation", "payload", "next_due_at", "created_at", "updated_at") {
		return errors.New("invalid response")
	}
	key, ok := v["schedule_key"].(string)
	if !ok || !scheduleKeyRE.MatchString(key) {
		return errors.New("invalid response")
	}
	revision, ok := integer(v["revision"])
	if !ok || revision < 1 {
		return errors.New("invalid response")
	}
	if _, ok = v["enabled"].(bool); !ok {
		return errors.New("invalid response")
	}
	calendar, ok := v["calendar"].(map[string]any)
	if !ok {
		return errors.New("invalid response")
	}
	typ, ok := calendar["type"].(string)
	if !ok {
		return errors.New("invalid response")
	}
	if _, ok = calendar["timezone"].(string); !ok {
		return errors.New("invalid response")
	}
	switch typ {
	case "daily", "weekly":
		if _, ok = integer(calendar["hour"]); !ok {
			return errors.New("invalid response")
		}
		if _, ok = integer(calendar["minute"]); !ok {
			return errors.New("invalid response")
		}
	case "hourly":
		if _, ok = integer(calendar["minute"]); !ok {
			return errors.New("invalid response")
		}
	case "interval":
		if _, ok = integer(calendar["interval_minutes"]); !ok {
			return errors.New("invalid response")
		}
	default:
		return errors.New("invalid response")
	}
	if _, ok = v["target_alias"].(string); !ok {
		return errors.New("invalid response")
	}
	if _, ok = v["operation"].(string); !ok {
		return errors.New("invalid response")
	}
	if _, ok = v["payload"].(map[string]any); !ok {
		return errors.New("invalid response")
	}
	if v["next_due_at"] != nil {
		due, ok := v["next_due_at"].(string)
		if !ok || !isUTCTimestamp(due) {
			return errors.New("invalid response")
		}
	}
	for _, field := range []string{"created_at", "updated_at"} {
		value, ok := v[field].(string)
		if !ok || !isUTCTimestamp(value) {
			return errors.New("invalid response")
		}
	}
	return nil
}

func isUTCTimestamp(v string) bool {
	if !utcTimestampRE.MatchString(v) {
		return false
	}
	parsed, err := time.Parse(time.RFC3339Nano, v)
	return err == nil && parsed.Location() == time.UTC
}
func exactKeys(m map[string]any, keys ...string) bool {
	if len(m) != len(keys) {
		return false
	}
	for _, key := range keys {
		if _, ok := m[key]; !ok {
			return false
		}
	}
	return true
}
func integer(v any) (int64, bool) {
	switch n := v.(type) {
	case json.Number:
		i, err := n.Int64()
		return i, err == nil
	case int:
		return int64(n), true
	case int64:
		return n, true
	default:
		return 0, false
	}
}
func mustInt(v any) int64 { n, _ := integer(v); return n }
func containsStatus(expected []int, status int) bool {
	for _, v := range expected {
		if v == status {
			return true
		}
	}
	return false
}
func mediaType(v string) string {
	v, _, _ = strings.Cut(v, ";")
	return strings.ToLower(strings.TrimSpace(v))
}
func hasDotDotSegment(path string) bool {
	for _, part := range strings.Split(path, "/") {
		if part == ".." || part == "." {
			return true
		}
	}
	return false
}
func jsonEqual(a, b any) bool {
	left, err := json.Marshal(a)
	if err != nil {
		return false
	}
	right, err := json.Marshal(b)
	return err == nil && bytes.Equal(left, right)
}
