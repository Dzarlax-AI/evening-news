package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/store"
	"github.com/jackc/pgx/v5"
)

func TestSettingsRecoverySchedulesNoReplay(t *testing.T) {
	raw := os.Getenv("NEWS_SETTINGS_RECOVERY_TEST_URL")
	if raw == "" {
		t.Skip("isolated settings recovery fixture not configured")
	}
	cfg, e := pgx.ParseConfig(raw)
	if e != nil || cfg.Database != "settings_recovery_fixture" || (cfg.Host != "localhost" && cfg.Host != "127.0.0.1") {
		t.Fatal("explicit loopback settings fixture required")
	}
	ctx := context.Background()
	admin, e := pgx.Connect(ctx, raw)
	if e != nil {
		t.Fatal("fixture connection failed")
	}
	defer admin.Close(ctx)
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	suffix := hex.EncodeToString(b)
	names := []string{"scheduler_restore_src_" + suffix, "scheduler_restore_dst_" + suffix}
	stores := []*store.Store{}
	defer func() {
		for _, s := range stores {
			s.Close()
		}
		for _, name := range names {
			_, _ = admin.Exec(ctx, "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		}
	}()
	apis := []*API{}
	for _, name := range names {
		if _, e = admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); e != nil {
			t.Fatal("fixture database creation failed")
		}
		u, _ := url.Parse(raw)
		u.Path = "/" + name
		s, e := store.Open(ctx, u.String(), "scheduler_service", 4, true)
		if e != nil {
			t.Fatal("scheduler schema initialization failed")
		}
		stores = append(stores, s)
		apis = append(apis, &API{Store: s, Token: "fixture-control-token", Targets: map[string]model.Target{"worker": {URL: "https://fixture.invalid", Token: "fixture-receiver-token"}}})
	}
	request := func(a *API, method, path string, payload []byte) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(string(payload)))
		r.Header.Set("Authorization", "Bearer fixture-control-token")
		w := httptest.NewRecorder()
		a.Handler().ServeHTTP(w, r)
		return w
	}
	for i := 0; i < 2; i++ {
		payload := fmt.Sprintf(`{"expected_revision":0,"enabled":%v,"calendar":{"type":"daily","timezone":"Europe/Belgrade","hour":18,"minute":30,"weekdays":[]},"target_alias":"worker","operation":"news_digest","payload":{}}`, i == 0)
		w := request(apis[0], "PUT", fmt.Sprintf("/v1/schedules/news:%d", i+1), []byte(payload))
		if w.Code != 201 {
			t.Fatal("source schedule create failed")
		}
	}
	// Read only the exact documented definition export; do not copy live store rows.
	path, e := filepath.Abs("../../../deploy/recovery/export-schedules.sql")
	if e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command("psql", "-XqAt", "-v", "ON_ERROR_STOP=1", "-h", cfg.Host, "-p", fmt.Sprint(cfg.Port), "-U", cfg.User, "-d", names[0], "-f", path)
	cmd.Env = append(os.Environ(), "PGPASSWORD="+cfg.Password)
	out, e := cmd.Output()
	if e != nil {
		t.Fatal("documented schedule export failed")
	}
	for _, forbidden := range []string{"next_due_at", "occurrence_id", "lease_token", "attempts"} {
		if strings.Contains(string(out), forbidden) {
			t.Fatal("live scheduler state in settings archive")
		}
	}
	var doc struct {
		Format    string           `json:"format"`
		Schedules []map[string]any `json:"schedules"`
	}
	if json.Unmarshal(out, &doc) != nil || doc.Format != "news-schedules-v1" || len(doc.Schedules) != 2 {
		t.Fatal("definition export invalid")
	}
	for _, definition := range doc.Schedules {
		key := definition["schedule_key"].(string)
		delete(definition, "schedule_key")
		definition["expected_revision"] = 0
		body, _ := json.Marshal(definition)
		w := request(apis[1], http.MethodPut, "/v1/schedules/"+key, body)
		if w.Code != 201 {
			t.Fatal("recovery schedule create failed")
		}
		var s model.Schedule
		if json.Unmarshal(w.Body.Bytes(), &s) != nil {
			t.Fatal("schedule response failed")
		}
		if s.Enabled && (s.NextDueAt == nil || !s.NextDueAt.After(time.Now())) {
			t.Fatal("enabled recovery schedule did not get a future due instant")
		}
		if !s.Enabled && s.NextDueAt != nil {
			t.Fatal("disabled recovery schedule has a due instant")
		}
		if w = request(apis[1], http.MethodPut, "/v1/schedules/"+key, body); w.Code != 409 {
			t.Fatal("clean-target schedule replay did not conflict")
		}
	}
	w := request(apis[1], "GET", "/v1/status", nil)
	var status struct {
		Paused  bool `json:"paused"`
		Pending int  `json:"pending_outbox"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &status) != nil || !status.Paused || status.Pending != 0 {
		t.Fatal("restored scheduler released control or replayed work")
	}
}
