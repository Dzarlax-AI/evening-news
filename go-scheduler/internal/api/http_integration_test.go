package api

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestHTTPEmptyCreateAndImportContract(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv("SCHEDULER_TEST_DATABASE_URL"))
	if dsn == "" {
		t.Skip("SCHEDULER_TEST_DATABASE_URL is not set")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Hostname() != "localhost" && parsed.Hostname() != "127.0.0.1") {
		t.Fatal("scheduler HTTP integration tests require localhost PostgreSQL")
	}
	admin, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal("could not connect to isolated scheduler test database")
	}
	defer admin.Close()
	var random [8]byte
	if _, err := rand.Read(random[:]); err != nil {
		t.Fatal("could not create isolated schema name")
	}
	schema := "scheduler_test_http_" + hex.EncodeToString(random[:])
	t.Cleanup(func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	db, err := store.Open(ctx, dsn, schema, 8, true)
	if err != nil {
		t.Fatal("could not open scheduler store")
	}
	defer db.Close()

	const token = "integration-only-scheduler-token"
	service := &API{Store: db, Token: token, Targets: map[string]model.Target{"worker": {URL: "https://fixture.invalid", Token: "fixture-token"}}}
	request := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+token)
		resp := httptest.NewRecorder()
		service.Handler().ServeHTTP(resp, req)
		return resp
	}

	listed := request(http.MethodGet, "/v1/schedules", "")
	if listed.Code != http.StatusOK {
		t.Fatalf("empty schedule list returned status %d", listed.Code)
	}
	var listBody struct {
		Schedules []json.RawMessage `json:"schedules"`
	}
	if err := json.Unmarshal(listed.Body.Bytes(), &listBody); err != nil || listBody.Schedules == nil || len(listBody.Schedules) != 0 {
		t.Fatal("empty schedule list must serialize as an empty array")
	}

	first := request(http.MethodPut, "/v1/schedules/fixture:first", `{"expected_revision":0,"enabled":true,"calendar":{"type":"daily","timezone":"UTC","hour":18,"minute":30,"weekdays":[]},"target_alias":"worker","operation":"news_digest","payload":{}}`)
	if first.Code != http.StatusCreated {
		t.Fatalf("first schedule create returned status %d: %s", first.Code, first.Body.String())
	}
	repeat := request(http.MethodPut, "/v1/schedules/fixture:first", `{"expected_revision":1,"enabled":true,"calendar":{"type":"daily","timezone":"UTC","hour":18,"minute":30,"weekdays":[]},"target_alias":"worker","operation":"news_digest","payload":{}}`)
	if repeat.Code != http.StatusOK {
		t.Fatalf("identical schedule update returned status %d: %s", repeat.Code, repeat.Body.String())
	}
	var schedule model.Schedule
	if err := json.Unmarshal(repeat.Body.Bytes(), &schedule); err != nil || schedule.Revision != 1 {
		t.Fatal("empty weekday representation changed the schedule revision")
	}

	existingInventory := request(http.MethodGet, "/v1/occurrences?schedule_key=fixture:first", "")
	if existingInventory.Code != http.StatusOK {
		t.Fatalf("existing schedule with no occurrences returned status %d: %s", existingInventory.Code, existingInventory.Body.String())
	}
	var emptyInventory struct {
		Occurrences []json.RawMessage `json:"occurrences"`
	}
	if err := json.Unmarshal(existingInventory.Body.Bytes(), &emptyInventory); err != nil || emptyInventory.Occurrences == nil || len(emptyInventory.Occurrences) != 0 {
		t.Fatal("existing schedule with no occurrences must return an empty array")
	}
	missingInventory := request(http.MethodGet, "/v1/occurrences?schedule_key=fixture:missing", "")
	if missingInventory.Code != http.StatusNotFound {
		t.Fatalf("unknown schedule occurrence filter returned status %d: %s", missingInventory.Code, missingInventory.Body.String())
	}

	invalidEnabledImport := request(http.MethodPost, "/v1/import", `{"expected_control_revision":0,"items":[{"schedule_key":"news:missing-due","enabled":true,"calendar":{"type":"daily","timezone":"UTC","hour":18,"minute":30},"target_alias":"worker","operation":"news_digest","payload":{},"next_due_at":null}]}`)
	if invalidEnabledImport.Code != http.StatusUnprocessableEntity {
		t.Fatalf("enabled import with null next_due_at returned status %d: %s", invalidEnabledImport.Code, invalidEnabledImport.Body.String())
	}

	importBody := `{"expected_control_revision":0,"items":[{"schedule_key":"news:microseconds","enabled":true,"calendar":{"type":"daily","timezone":"UTC","hour":18,"minute":30,"weekdays":[]},"target_alias":"worker","operation":"news_digest","payload":{},"next_due_at":"2030-01-02T17:30:19.728869Z"},{"schedule_key":"news:disabled","enabled":false,"calendar":{"type":"daily","timezone":"UTC","hour":18,"minute":30,"weekdays":[]},"target_alias":"worker","operation":"news_processing","payload":{},"next_due_at":null}]}`
	imported := request(http.MethodPost, "/v1/import", importBody)
	if imported.Code != http.StatusOK {
		t.Fatalf("valid import returned status %d: %s", imported.Code, imported.Body.String())
	}
	var importedBody store.ImportResult
	if err := json.Unmarshal(imported.Body.Bytes(), &importedBody); err != nil || len(importedBody.Created) != 2 {
		t.Fatal("valid import did not create both schedules")
	}
	again := request(http.MethodPost, "/v1/import", importBody)
	if again.Code != http.StatusOK {
		t.Fatalf("idempotent import returned status %d: %s", again.Code, again.Body.String())
	}
	var repeatedBody store.ImportResult
	if err := json.Unmarshal(again.Body.Bytes(), &repeatedBody); err != nil || len(repeatedBody.Unchanged) != 2 {
		t.Fatal("identical import was not returned as unchanged")
	}
	got, err := db.GetSchedule(ctx, "news:microseconds")
	if err != nil || got.NextDueAt == nil || got.NextDueAt.UTC().Format(time.RFC3339Nano) != "2030-01-02T17:30:19.728869Z" {
		t.Fatal("HTTP import did not preserve its exact microsecond timestamp")
	}
	now := time.Now().UTC().Truncate(time.Microsecond)
	schedules := pgx.Identifier{schema, "schedules"}.Sanitize()
	if _, err := db.Pool.Exec(ctx, `UPDATE `+schedules+` SET next_due_at=$2 WHERE schedule_key=$1`, "news:microseconds", now.Add(-time.Second)); err != nil {
		t.Fatal("could not prepare occurrence inventory")
	}
	if _, err := db.SetPaused(ctx, 0, false, now); err != nil {
		t.Fatal("could not resume scheduler for occurrence inventory")
	}
	if generated, err := db.GenerateDue(ctx, now, 100, 100); err != nil || generated != 1 {
		t.Fatal("could not generate an inventory occurrence")
	}
	if claimed, err := db.Claim(ctx, now, 30*time.Second, 1); err != nil || len(claimed) != 1 {
		t.Fatal("could not lease an inventory occurrence")
	}
	leasedInventory := request(http.MethodGet, "/v1/occurrences?schedule_key=news:microseconds", "")
	if leasedInventory.Code != http.StatusOK {
		t.Fatalf("leased occurrence inventory returned status %d: %s", leasedInventory.Code, leasedInventory.Body.String())
	}
	var leasedBody struct {
		Occurrences []struct {
			TargetAlias    string     `json:"target_alias"`
			Status         string     `json:"status"`
			LeaseExpiresAt *time.Time `json:"lease_expires_at"`
			LeaseActive    bool       `json:"lease_active"`
		} `json:"occurrences"`
	}
	if err := json.Unmarshal(leasedInventory.Body.Bytes(), &leasedBody); err != nil || len(leasedBody.Occurrences) != 1 {
		t.Fatal("leased occurrence inventory response was malformed")
	}
	entry := leasedBody.Occurrences[0]
	if entry.TargetAlias != "worker" || entry.Status != "leased" || entry.LeaseExpiresAt == nil || !entry.LeaseActive {
		t.Fatal("leased occurrence inventory omitted its target or active lease state")
	}
}
