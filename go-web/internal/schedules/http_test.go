package schedules

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/auth"
)

type fakeScheduleRoot struct {
	settings    map[string]any
	create      map[string]any
	update      map[string]any
	delete      map[string]any
	status      map[string]any
	err         error
	created     ScheduleCreateRequest
	updated     ScheduleUpdateRequest
	updatedName string
}

func (f *fakeScheduleRoot) Settings(context.Context) (map[string]any, error) {
	return f.settings, f.err
}
func (f *fakeScheduleRoot) Create(_ context.Context, r ScheduleCreateRequest) (map[string]any, error) {
	f.created = r
	return f.create, f.err
}
func (f *fakeScheduleRoot) Update(_ context.Context, name string, r ScheduleUpdateRequest) (map[string]any, error) {
	f.updatedName = name
	f.updated = r
	return f.update, f.err
}
func (f *fakeScheduleRoot) Delete(context.Context, string) (map[string]any, error) {
	return f.delete, f.err
}
func (f *fakeScheduleRoot) Status(context.Context) (map[string]any, error) { return f.status, f.err }

func TestScheduleHTTPAuthRoleAndCSRFContract(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "schedule-test-secret", Development: true})
	adminToken, _ := service.Mint("admin", auth.LevelAdmin)
	readerToken, _ := service.Mint("reader", auth.LevelAPIRead)
	root := &fakeScheduleRoot{settings: map[string]any{"settings": []any{}, "timestamp": "2026-10-25T01:30:00"}, create: map[string]any{"success": true}, update: map[string]any{"success": true}, delete: map[string]any{"success": true}, status: map[string]any{"paused": false}}
	mux := http.NewServeMux()
	NewHandler(root).Register(mux, service)
	tests := []struct {
		name, authorization, cookie, origin, fetchSite string
		status                                         int
		calls                                          int
	}{
		{"anonymous", "", "", "", "", 401, 0},
		{"reader", "Bearer " + readerToken, "", "", "", 403, 0},
		{"hostile cookie origin", "", adminToken, "https://evil.example", "cross-site", 403, 0},
		{"same origin cookie", "", adminToken, "https://news.example", "same-origin", 200, 1},
		{"native bearer", "Bearer " + adminToken, adminToken, "https://evil.example", "cross-site", 200, 1},
		{"native basic", "Basic " + base64.StdEncoding.EncodeToString([]byte("admin:secret")), adminToken, "https://evil.example", "cross-site", 200, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodPost, "https://news.example/api/v1/schedule/settings", strings.NewReader(`{"task_name":"backup"}`))
			r.Host = "news.example"
			if tc.authorization != "" {
				r.Header.Set("Authorization", tc.authorization)
			}
			if tc.cookie != "" {
				r.AddCookie(&http.Cookie{Name: auth.AdminCookie, Value: tc.cookie})
			}
			if tc.origin != "" {
				r.Header.Set("Origin", tc.origin)
			}
			if tc.fetchSite != "" {
				r.Header.Set("Sec-Fetch-Site", tc.fetchSite)
			}
			w := httptest.NewRecorder()
			mux.ServeHTTP(w, r)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			if tc.name == "anonymous" && w.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatalf("challenge=%q", w.Header().Get("WWW-Authenticate"))
			}
		})
	}
}

func TestScheduleHTTPCreateDefaultsUpdateBodyAndRoutes(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "schedule-test-secret"})
	token, _ := service.Mint("admin", auth.LevelAdmin)
	root := &fakeScheduleRoot{create: map[string]any{"created": true}, update: map[string]any{"updated": true}, delete: map[string]any{"deleted": true}, status: map[string]any{"paused": false}}
	mux := http.NewServeMux()
	NewHandler(root).Register(mux, service)
	call := func(method, path, body string) (int, string) {
		r := httptest.NewRequest(method, "https://news.example"+path, strings.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, r)
		return w.Code, w.Body.String()
	}
	status, body := call(http.MethodPost, "/api/v1/schedule/settings", `{"task_name":"backup","unexpected":true}`)
	if status != 200 || !strings.Contains(body, `"created":true`) {
		t.Fatalf("create=%d %s", status, body)
	}
	if root.created.TaskName != "backup" || !root.created.Enabled || root.created.ScheduleType != "daily" || root.created.Hour != 0 || root.created.Minute != 0 || root.created.Timezone != "Europe/Belgrade" || len(root.created.Weekdays) != 7 {
		t.Fatalf("create defaults=%+v", root.created)
	}
	status, _ = call(http.MethodPut, "/api/v1/schedule/settings/backup", `{"minute":42,"task_config":{"timeout_seconds":3600}}`)
	if status != 200 || root.updatedName != "backup" || root.updated.Minute == nil || *root.updated.Minute != 42 || !root.updated.TaskConfigSet {
		t.Fatalf("update status=%d req=%+v", status, root.updated)
	}
	if status, body = call(http.MethodDelete, "/api/v1/schedule/settings/backup", ""); status != 200 || !strings.Contains(body, `"deleted":true`) {
		t.Fatalf("delete=%d %s", status, body)
	}
	if status, body = call(http.MethodGet, "/api/v1/schedule/status", ""); status != 200 || !strings.Contains(body, `"paused":false`) {
		t.Fatalf("status=%d %s", status, body)
	}
}

func TestScheduleHTTPValidationAndErrorMapping(t *testing.T) {
	service := auth.New(auth.Config{Username: "admin", Password: "secret", JWTSecret: "schedule-test-secret"})
	token, _ := service.Mint("admin", auth.LevelAdmin)
	root := &fakeScheduleRoot{settings: map[string]any{}}
	mux := http.NewServeMux()
	NewHandler(root).Register(mux, service)
	request := httptest.NewRequest(http.MethodPost, "https://news.example/api/v1/schedule/settings", strings.NewReader(`{"enabled":true}`))
	request.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, request)
	var validation map[string]any
	if w.Code != 422 || json.Unmarshal(w.Body.Bytes(), &validation) != nil || validation["detail"] == nil {
		t.Fatalf("validation=%d %s", w.Code, w.Body.String())
	}
	root.err = &APIError{Status: 409, Detail: "Schedule changed concurrently"}
	request = httptest.NewRequest(http.MethodGet, "https://news.example/api/v1/schedule/settings", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, request)
	if w.Code != 409 || !strings.Contains(w.Body.String(), "Schedule changed concurrently") {
		t.Fatalf("mapped error=%d %s", w.Code, w.Body.String())
	}
}
