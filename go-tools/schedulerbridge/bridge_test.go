package schedulerbridge

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestValidationAndPrivateArtifact(t *testing.T) {
	item := Object{"schedule_key": "news:42", "enabled": true, "calendar": Object{"type": "interval", "timezone": "Europe/Belgrade", "interval_minutes": 45}, "target_alias": "news-worker", "operation": "news_digest", "payload": Object{"run_processing": false, "send_telegram": true}, "next_due_at": "2026-10-23T17:30:19.728869Z"}
	if e := validateItem(item); e != nil {
		t.Fatal(e)
	}
	if _, e := payload("backup", Object{"keep_days": 14, "api_token": "secret"}, false); e == nil {
		t.Fatal("secret accepted")
	}
	record, e := rollbackRecord(Object{"schedule_key": "news:42", "enabled": true, "calendar": item["calendar"], "target_alias": "news-worker", "operation": "news_digest", "payload": item["payload"], "next_due_at": item["next_due_at"], "revision": 7}, Object{"state": "succeeded", "result_status": "success", "created_at": "2026-10-23T16:00:00Z", "finished_at": "2026-10-23T16:02:00Z"})
	if e != nil || record["last_success_at"] != "2026-10-23T16:02:00Z" || record["next_run"] != item["next_due_at"] {
		t.Fatalf("record=%v err=%v", record, e)
	}
	path := filepath.Join(t.TempDir(), "artifact")
	if e = writeArtifact(path, Object{"ok": true}); e != nil {
		t.Fatal(e)
	}
	info, e := os.Stat(path)
	if e != nil || info.Mode().Perm() != 0600 {
		t.Fatal("permissions")
	}
	if e = writeArtifact(path, Object{"ok": false}); e == nil {
		t.Fatal("overwrite accepted")
	}
}
func fixtureDB(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Fatal("TEST_DATABASE_URL required")
	}
	u, e := url.Parse(dsn)
	if e != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1" && u.Hostname() != "::1") {
		t.Fatal("loopback fixture required")
	}
	ctx := context.Background()
	admin, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := fmt.Sprintf("bridge_test_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+q); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE")
		_ = admin.Close(context.Background())
	})
	query := u.Query()
	query.Set("search_path", schema)
	u.RawQuery = query.Encode()
	scoped := u.String()
	db, e := pgx.Connect(ctx, scoped)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close(ctx)
	sql := `CREATE TABLE schedule_settings(id integer,task_name text,enabled boolean,schedule_type text,hour integer,minute integer,weekdays json,timezone text,task_config json,next_run timestamp,last_run timestamp,last_finished_at timestamp,last_success_at timestamp,last_status text,last_duration_seconds numeric); CREATE TABLE background_jobs(id uuid,kind text,state text,schedule_key text,occurrence_id uuid,scheduled_for timestamp,created_at timestamp,finished_at timestamp,result jsonb); INSERT INTO schedule_settings VALUES(42,'news_digest',true,'interval',9,0,'[]','Europe/Belgrade','{"interval_minutes":45,"run_processing":false,"send_telegram":true,"timeout_seconds":120}','2026-10-23 17:30:19.728869','2026-10-23 16:00:00','2026-10-23 16:02:00','2026-10-23 16:02:00','success',120.5); INSERT INTO background_jobs VALUES('10000000-0000-4000-8000-000000000001','news_digest','succeeded','news:42','20000000-0000-4000-8000-000000000002','2026-10-23 17:30:19.728869','2026-10-23 16:00:00','2026-10-23 16:02:00','{"status":"success"}')`
	if _, e = db.Exec(ctx, sql); e != nil {
		t.Fatal(e)
	}
	return scoped
}
func TestExportAndRollbackRealPostgres(t *testing.T) {
	dsn := fixtureDB(t)
	ctx := context.Background()
	dir := t.TempDir()
	snapshot := filepath.Join(dir, "snapshot.json")
	if e := Export(ctx, dsn, snapshot); e != nil {
		t.Fatal(e)
	}
	raw, e := os.ReadFile(snapshot)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(raw, []byte(`2026-10-23T17:30:19.728869Z`)) || !bytes.Contains(raw, []byte(`news:42`)) {
		t.Fatalf("bad snapshot %s", raw)
	}
	var got struct {
		Schedules []Object `json:"schedules"`
		History   []Object `json:"legacy_run_history"`
	}
	if e = json.Unmarshal(raw, &got); e != nil {
		t.Fatal(e)
	}
	goldenRaw, e := os.ReadFile("testdata/legacy-schedule-golden.json")
	if e != nil {
		t.Fatal(e)
	}
	var golden struct {
		Item    Object `json:"item"`
		History Object `json:"history"`
	}
	if e = json.Unmarshal(goldenRaw, &golden); e != nil {
		t.Fatal(e)
	}
	if len(got.Schedules) != 1 || len(got.History) != 1 || !reflect.DeepEqual(got.Schedules[0], golden.Item) || !reflect.DeepEqual(got.History[0], golden.History) {
		t.Fatalf("legacy mismatch schedule=%v history=%v", got.Schedules, got.History)
	}

	status := Object{"paused": true, "control_revision": 4, "pending_outbox": 0}
	var requestCount int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		if r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("auth missing")
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/v1/status":
			_ = json.NewEncoder(w).Encode(status)
		case "/v1/schedules":
			if r.URL.Query().Get("limit") != "2" {
				t.Error("invalid schedule page")
			}
			_ = json.NewEncoder(w).Encode(Object{"schedules": []any{Object{"schedule_key": "news:42", "revision": 3, "enabled": true, "calendar": Object{"type": "interval", "timezone": "Europe/Belgrade", "interval_minutes": 45}, "target_alias": "news-worker", "operation": "news_digest", "payload": Object{}, "next_due_at": "2026-10-23T17:30:19.728869Z", "created_at": "2026-10-22T00:00:00Z", "updated_at": "2026-10-23T00:00:00Z"}}, "next_cursor": nil})
		case "/v1/occurrences":
			_ = json.NewEncoder(w).Encode(Object{"occurrences": []any{}, "next_cursor": nil})
		case "/v1/import":
			var b Object
			_ = json.NewDecoder(r.Body).Decode(&b)
			if b["expected_control_revision"] != float64(4) {
				t.Error("revision not sent")
			}
			_ = json.NewEncoder(w).Encode(Object{"control_revision": 4, "created": []string{"news:42"}, "unchanged": []string{}})
		default:
			t.Errorf("unexpected route")
		}
	}))
	defer server.Close()
	cfg := ServiceConfig{URL: server.URL, Token: "fixture-token", AllowHTTP: true, Timeout: time.Second}
	if _, e = Import(ctx, cfg, snapshot, "4"); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(dir, "rollback.json")
	if e = RollbackInventory(ctx, cfg, dsn, path); e != nil {
		t.Fatal(e)
	}
	raw, e = os.ReadFile(path)
	if e != nil {
		t.Fatal(e)
	}
	if !bytes.Contains(raw, []byte(`"last_status": "success"`)) || !bytes.Contains(raw, []byte(`news:42`)) || requestCount != 5 {
		t.Fatalf("unexpected inventory requests=%d body=%s", requestCount, raw)
	}
	if strings.Contains(string(raw), "fixture-token") {
		t.Fatal("token leaked")
	}
}

func TestRollbackRejectsUnpausedLeaseAndRevisionDrift(t *testing.T) {
	valid := func() Object {
		return Object{"occurrence_id": "10000000-0000-4000-8000-000000000001", "schedule_key": "news:77", "schedule_revision": 1, "scheduled_for": "2026-10-23T00:00:00Z", "status": "leased", "attempts": 0, "created_at": "2026-10-23T00:00:00Z", "updated_at": "2026-10-23T00:00:00Z", "job_id": nil, "target_alias": "news-worker", "lease_expires_at": "2026-10-23T00:00:00Z", "lease_active": true}
	}
	for _, mode := range []string{"unpaused", "lease", "drift", "missing_lease"} {
		t.Run(mode, func(t *testing.T) {
			calls := []string{}
			statuses := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls = append(calls, r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				switch r.URL.Path {
				case "/v1/status":
					statuses++
					paused := mode != "unpaused"
					revision := 4
					if mode == "drift" && statuses > 1 {
						revision = 5
					}
					_ = json.NewEncoder(w).Encode(Object{"paused": paused, "control_revision": revision, "pending_outbox": 0})
				case "/v1/schedules":
					_ = json.NewEncoder(w).Encode(Object{"schedules": []any{}, "next_cursor": nil})
				case "/v1/occurrences":
					rows := []any{}
					if mode == "lease" || mode == "missing_lease" {
						item := valid()
						if mode == "missing_lease" {
							delete(item, "lease_active")
						}
						rows = append(rows, item)
					}
					_ = json.NewEncoder(w).Encode(Object{"occurrences": rows, "next_cursor": nil})
				}
			}))
			defer server.Close()
			path := filepath.Join(t.TempDir(), "snapshot")
			err := RollbackInventory(context.Background(), ServiceConfig{URL: server.URL, Token: "fixture", AllowHTTP: true, Timeout: time.Second}, "", path)
			if err == nil {
				t.Fatal("unsafe inventory accepted")
			}
			if _, e := os.Stat(path); !os.IsNotExist(e) {
				t.Fatal("artifact created on failure")
			}
			if mode == "unpaused" && len(calls) != 1 {
				t.Fatalf("listing followed unpaused: %v", calls)
			}
			if mode == "lease" && len(calls) != 3 {
				t.Fatalf("inflight lease checked too late: %v", calls)
			}
		})
	}
}
func TestPaginationAndResponseLimits(t *testing.T) {
	for _, mode := range []string{"cursor", "oversized", "encoding", "redirect"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch mode {
				case "cursor":
					w.Header().Set("Content-Type", "application/json")
					_ = json.NewEncoder(w).Encode(Object{"schedules": []any{}, "next_cursor": "repeat"})
				case "oversized":
					w.Header().Set("Content-Type", "application/json")
					_, _ = w.Write(bytes.Repeat([]byte("x"), 512*1024+1))
				case "encoding":
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Content-Encoding", "gzip")
					_, _ = w.Write([]byte(`{}`))
				case "redirect":
					http.Redirect(w, r, "https://example.invalid/", http.StatusFound)
				}
			}))
			defer server.Close()
			cfg := ServiceConfig{URL: server.URL, Token: "fixture", AllowHTTP: true, Timeout: time.Second}
			client, base, e := cfg.client()
			if e != nil {
				t.Fatal(e)
			}
			if mode == "cursor" {
				_, e = pageAll(context.Background(), cfg, client, base, "/v1/schedules", "schedules", 2, 10000)
			} else {
				_, e = request(context.Background(), client, "GET", base, cfg.Token, nil)
			}
			if e == nil {
				t.Fatal("invalid response accepted")
			}
		})
	}
}
func TestExportRejectsActiveWork(t *testing.T) {
	dsn := fixtureDB(t)
	db, e := pgx.Connect(context.Background(), dsn)
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close(context.Background())
	if _, e = db.Exec(context.Background(), `UPDATE background_jobs SET state='running'`); e != nil {
		t.Fatal(e)
	}
	path := filepath.Join(t.TempDir(), "out")
	if e = Export(context.Background(), dsn, path); e == nil {
		t.Fatal("active work accepted")
	}
	if _, e = os.Stat(path); !os.IsNotExist(e) {
		t.Fatal("unsafe artifact created")
	}
}

func TestSnapshotValidationRejectsMalformedOrSensitivePayload(t *testing.T) {
	base := func() Object {
		return Object{"schedule_key": "news:42", "enabled": true, "calendar": Object{"type": "daily", "timezone": "UTC", "hour": 9, "minute": 0}, "target_alias": "news-worker", "operation": "news_digest", "payload": Object{}, "next_due_at": "2026-10-23T17:30:19Z"}
	}
	for _, change := range []func(Object){func(m Object) { m["payload"] = Object{"api_token": "hidden"} }, func(m Object) { m["payload"] = Object{"timeout_seconds": false} }, func(m Object) { m["calendar"].(Object)["timezone"] = "Not/AZone" }, func(m Object) { m["next_due_at"] = "2026-02-30T00:00:00Z" }, func(m Object) { m["schedule_key"] = "news:0" }, func(m Object) { m["target_alias"] = "other-worker" }} {
		m := base()
		change(m)
		if err := validateItem(m); err == nil {
			t.Fatalf("invalid snapshot accepted")
		}
	}
}
