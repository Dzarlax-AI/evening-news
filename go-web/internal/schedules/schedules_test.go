package schedules

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

const scheduleTimestamp = "2026-10-25T01:30:00Z"

func scheduleResource(key, operation, target string, revision int64) map[string]any {
	return map[string]any{
		"schedule_key": key, "revision": json.Number(fmt.Sprint(revision)), "enabled": true,
		"calendar":     map[string]any{"type": "daily", "timezone": "Europe/Belgrade", "hour": json.Number("3"), "minute": json.Number("15"), "weekdays": []any{json.Number("1"), json.Number("7")}},
		"target_alias": target, "operation": operation, "payload": map[string]any{"timeout_seconds": json.Number("120")},
		"next_due_at": scheduleTimestamp, "created_at": "2026-10-01T10:00:00Z", "updated_at": "2026-10-20T10:00:00Z",
	}
}

func scheduleJSON(value any) []byte { data, _ := json.Marshal(value); return data }
func writeScheduleJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(scheduleJSON(value))
}

func TestSchedulerClientValidatesFixedEndpointAndAuthentication(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/prefix/v1/status" {
			t.Errorf("unexpected path %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer fixture-secret" {
			t.Errorf("authorization header missing")
		}
		if r.Header.Get("Accept") != "application/json" {
			t.Errorf("accept=%q", r.Header.Get("Accept"))
		}
		writeScheduleJSON(w, 200, map[string]any{"paused": false, "control_revision": 2, "pending_outbox": 0})
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Endpoint: server.URL + "/prefix", Token: "fixture-secret", AllowHTTP: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.GetStatus(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, cfg := range []ClientConfig{
		{Endpoint: "http://example.test", Token: "t", Timeout: time.Second},
		{Endpoint: "https://user:pass@example.test", Token: "t", Timeout: time.Second},
		{Endpoint: "https://example.test?x=1", Token: "t", Timeout: time.Second},
		{Endpoint: "https://example.test", Timeout: time.Second},
		{Endpoint: "https://example.test", Token: "t", Timeout: 31 * time.Second},
	} {
		if _, err := NewClient(cfg); err == nil {
			t.Fatalf("accepted invalid config: %+v", cfg)
		}
	}
}

func TestSchedulerClientPaginationAndResponseValidation(t *testing.T) {
	var mu sync.Mutex
	paths := []string{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.RequestURI())
		mu.Unlock()
		if r.URL.Query().Get("cursor") == "second page&x" {
			writeScheduleJSON(w, 200, map[string]any{"schedules": []any{scheduleResource("news:backup", "backup", "news-worker", 3)}, "next_cursor": nil})
			return
		}
		writeScheduleJSON(w, 200, map[string]any{"schedules": []any{scheduleResource("news:news_processing", "news_processing", "news-worker", 1)}, "next_cursor": "second page&x"})
	}))
	defer server.Close()
	client, err := NewClient(ClientConfig{Endpoint: server.URL, Token: "t", AllowHTTP: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := client.ListSchedules(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("schedule count=%d", len(rows))
	}
	if len(paths) != 2 || !strings.Contains(paths[1], "cursor=second+page%26x") {
		t.Fatalf("pagination paths=%v", paths)
	}

	bad := scheduleResource("news:backup", "backup", "news-worker", 1)
	bad["extra"] = true
	if err := ValidateScheduleResource(bad); err == nil {
		t.Fatal("accepted a resource with extra fields")
	}
	bad = scheduleResource("news:backup", "backup", "news-worker", 1)
	bad["created_at"] = "2026-10-01T10:00:00+00:00"
	if err := ValidateScheduleResource(bad); err == nil {
		t.Fatal("accepted a non-canonical UTC timestamp")
	}
}

func TestSchedulerClientDoesNotFollowRedirectOrAcceptCompressedResponse(t *testing.T) {
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "https://attacker.invalid/collect", http.StatusFound)
	}))
	defer redirect.Close()
	client, _ := NewClient(ClientConfig{Endpoint: redirect.URL, Token: "never-forward", AllowHTTP: true, Timeout: time.Second})
	_, err := client.GetStatus(context.Background())
	var schedulerErr *SchedulerError
	if !errors.As(err, &schedulerErr) || schedulerErr.Status != http.StatusFound {
		t.Fatalf("redirect error=%v", err)
	}

	compressed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write([]byte("{}"))
	}))
	defer compressed.Close()
	client, _ = NewClient(ClientConfig{Endpoint: compressed.URL, Token: "t", AllowHTTP: true, Timeout: time.Second})
	_, err = client.GetStatus(context.Background())
	if !errors.As(err, &schedulerErr) || schedulerErr.Code != "invalid_response" {
		t.Fatalf("compressed response error=%v", err)
	}
}

func TestSchedulerClientTimeoutAndResponseSizeAreBounded(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(75 * time.Millisecond)
		writeScheduleJSON(w, 200, map[string]any{"paused": false, "control_revision": 1, "pending_outbox": 0})
	}))
	defer slow.Close()
	client, _ := NewClient(ClientConfig{Endpoint: slow.URL, Token: "t", AllowHTTP: true, Timeout: 15 * time.Millisecond})
	_, err := client.GetStatus(context.Background())
	var schedulerErr *SchedulerError
	if !errors.As(err, &schedulerErr) || schedulerErr.Code != "transport_error" {
		t.Fatalf("timeout error=%v", err)
	}

	large := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"padding":"` + strings.Repeat("x", MaxResponseBytes) + `"}`))
	}))
	defer large.Close()
	client, _ = NewClient(ClientConfig{Endpoint: large.URL, Token: "t", AllowHTTP: true, Timeout: time.Second})
	_, err = client.GetStatus(context.Background())
	if !errors.As(err, &schedulerErr) || schedulerErr.Code != "response_too_large" {
		t.Fatalf("large response error=%v", err)
	}
}

type jobsReader struct {
	snapshot JobsSnapshot
	err      error
	gotKeys  []string
}

func (j *jobsReader) Snapshot(_ context.Context, keys []string) (JobsSnapshot, error) {
	j.gotKeys = append([]string(nil), keys...)
	return j.snapshot, j.err
}

func TestScheduleRootFiltersOwnershipMergesJobsAndPreservesDSTTimestamp(t *testing.T) {
	resources := []map[string]any{
		scheduleResource("news:news_processing", "news_processing", "news-worker", 3),
		scheduleResource("foreign:backup", "backup", "news-worker", 2),
		scheduleResource("news:backup", "backup", "other-worker", 2),
		scheduleResource("news:01", "backup", "news-worker", 4),
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/schedules" {
			if r.URL.Query().Get("cursor") != "" {
				writeScheduleJSON(w, 200, map[string]any{"schedules": resources[2:], "next_cursor": nil})
				return
			}
			writeScheduleJSON(w, 200, map[string]any{"schedules": resources[:2], "next_cursor": "page-two"})
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Endpoint: server.URL, Token: "t", AllowHTTP: true, Timeout: time.Second})
	created := time.Date(2026, 10, 25, 3, 0, 0, 0, time.UTC)
	finished := time.Date(2026, 10, 25, 4, 0, 0, 0, time.UTC)
	successAt := time.Date(2026, 10, 24, 4, 0, 0, 0, time.UTC)
	jobs := &jobsReader{snapshot: JobsSnapshot{Latest: map[string]JobStatus{"news:news_processing": {CreatedAt: &created, FinishedAt: &finished, State: "failed", ErrorCode: "worker_error", Result: map[string]any{"duration_seconds": 12}}}, Running: map[string]bool{"news:news_processing": true}, LastSuccess: map[string]JobStatus{"news:news_processing": {FinishedAt: &successAt}}}}
	clock := func() time.Time { return time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC) }
	root := NewRoot(client, jobs, clock)
	result, err := root.Settings(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	settings := result["settings"].([]map[string]any)
	if len(settings) != 2 {
		t.Fatalf("settings=%#v", settings)
	}
	first := settings[0]
	if first["schedule_key"] != "news:news_processing" {
		t.Fatalf("first=%#v", first)
	}
	if first["next_run"] != "2026-10-25T01:30:00" || first["timezone"] != "Europe/Belgrade" {
		t.Fatalf("DST mapping changed: %#v", first)
	}
	if first["last_run"] != "2026-10-25T03:00:00" || first["last_status"] != "failed" || first["last_error"] != "worker_error" || first["last_success_at"] != "2026-10-24T04:00:00" || first["is_running"] != true {
		t.Fatalf("job merge=%#v", first)
	}
	if got := jobs.gotKeys; len(got) != 2 || got[0] != "news:01" || got[1] != "news:news_processing" {
		t.Fatalf("jobs keys=%v", got)
	}
	if result["timestamp"] != "2026-10-26T12:00:00" {
		t.Fatalf("timestamp=%v", result["timestamp"])
	}
}

func TestScheduleRootMapsRevisionConflictAndUnknownOwner(t *testing.T) {
	conflict := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/schedules" {
			writeScheduleJSON(w, 200, map[string]any{"schedules": []any{scheduleResource("news:backup", "backup", "news-worker", 2)}, "next_cursor": nil})
			return
		}
		if r.Method == http.MethodPut {
			writeScheduleJSON(w, 409, map[string]string{"detail": "raw private detail"})
			return
		}
		http.NotFound(w, r)
	}))
	defer conflict.Close()
	client, _ := NewClient(ClientConfig{Endpoint: conflict.URL, Token: "t", AllowHTTP: true, Timeout: time.Second})
	root := NewRoot(client, nil, nil)
	_, err := root.Update(context.Background(), "backup", ScheduleUpdateRequest{})
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 409 || apiErr.Detail != "Schedule changed concurrently" {
		t.Fatalf("conflict error=%v", err)
	}

	unknown := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeScheduleJSON(w, 200, map[string]any{"schedules": []any{scheduleResource("news:backup", "backup", "other-worker", 2)}, "next_cursor": nil})
	}))
	defer unknown.Close()
	client, _ = NewClient(ClientConfig{Endpoint: unknown.URL, Token: "t", AllowHTTP: true, Timeout: time.Second})
	root = NewRoot(client, nil, nil)
	_, err = root.Update(context.Background(), "backup", ScheduleUpdateRequest{})
	if !errors.As(err, &apiErr) || apiErr.Status != 404 {
		t.Fatalf("unknown owner error=%v", err)
	}
}

func TestScheduleRootReportsUnavailableServiceAndJobsReader(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	endpoint := closed.URL
	closed.Close()
	client, err := NewClient(ClientConfig{Endpoint: endpoint, Token: "t", AllowHTTP: true, Timeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	root := NewRoot(client, nil, nil)
	_, err = root.Settings(context.Background())
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 503 || apiErr.Detail != "Scheduler service is unavailable" {
		t.Fatalf("unavailable service error=%v", err)
	}

	resource := scheduleResource("news:backup", "backup", "news-worker", 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeScheduleJSON(w, 200, map[string]any{"schedules": []any{resource}, "next_cursor": nil})
	}))
	defer server.Close()
	client, _ = NewClient(ClientConfig{Endpoint: server.URL, Token: "t", AllowHTTP: true, Timeout: time.Second})
	root = NewRoot(client, nil, nil)
	_, err = root.Settings(context.Background())
	if !errors.As(err, &apiErr) || apiErr.Status != 503 || apiErr.Detail != "News execution status is unavailable" {
		t.Fatalf("missing job snapshot error=%v", err)
	}
}

func TestScheduleRootUpdateRetainsTimezoneAndRevision(t *testing.T) {
	resource := scheduleResource("news:backup", "backup", "news-worker", 7)
	var putBody map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/v1/schedules" {
			writeScheduleJSON(w, 200, map[string]any{"schedules": []any{resource}, "next_cursor": nil})
			return
		}
		if r.Method == http.MethodPut {
			decoder := json.NewDecoder(r.Body)
			decoder.UseNumber()
			_ = decoder.Decode(&putBody)
			updated := scheduleResource("news:backup", "backup", "news-worker", 8)
			updated["calendar"] = putBody["calendar"]
			updated["enabled"] = putBody["enabled"]
			updated["payload"] = putBody["payload"]
			writeScheduleJSON(w, 200, updated)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, _ := NewClient(ClientConfig{Endpoint: server.URL, Token: "t", AllowHTTP: true, Timeout: time.Second})
	root := NewRoot(client, nil, nil)
	minute := 45
	result, err := root.Update(context.Background(), "backup", ScheduleUpdateRequest{Minute: &minute})
	if err != nil {
		t.Fatal(err)
	}
	if result["setting"].(map[string]any)["schedule_revision"] != json.Number("8") {
		t.Fatalf("updated setting=%#v", result)
	}
	if putBody["expected_revision"] != json.Number("7") {
		t.Fatalf("revision not carried: %#v", putBody)
	}
	calendar := putBody["calendar"].(map[string]any)
	updatedMinute, ok := integer(calendar["minute"])
	if calendar["timezone"] != "Europe/Belgrade" || calendar["type"] != "daily" || !ok || updatedMinute != int64(minute) {
		t.Fatalf("calendar changed: %#v", calendar)
	}
}
