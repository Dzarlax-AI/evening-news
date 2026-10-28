package authority

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestContextClientAuthenticatesAndValidates(t *testing.T) {
	const job = "10000000-0000-4000-8000-000000000001"
	e := Envelope{Version: 1, JobID: job, AttemptGeneration: 2, OwnerID: "20000000-0000-4000-8000-000000000002", Generation: 3, BackendPID: 4, ExecutorInstanceID: "30000000-0000-4000-8000-000000000003", DeadlineAt: time.Now().UTC().Add(time.Minute).Format(time.RFC3339)}
	current := ExecutionContext{Version: 1, Envelope: e, ExecutorBackendPID: 5, Kind: "news_processing", Payload: json.RawMessage(`{}`)}
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/v1/job-executions/"+job+"/2/context" || r.Header.Get("Authorization") != "Bearer fixture-token" {
			t.Error("request identity or auth mismatch")
		}
		_ = json.NewEncoder(w).Encode(current)
	}))
	defer s.Close()
	c, err := NewContextClient(s.URL, "fixture-token")
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(context.Background(), job, 2)
	if err != nil || !got.Envelope.Matches(e) {
		t.Fatalf("get: %v", err)
	}
	current.Envelope.AttemptGeneration = 3
	if _, err = c.Get(context.Background(), job, 2); err == nil {
		t.Fatal("accepted changed attempt")
	}
	current.Envelope = e
	current.Payload = json.RawMessage(`null`)
	if _, err = c.Get(context.Background(), job, 2); err == nil {
		t.Fatal("accepted null payload")
	}
}
func TestContextClientRejectsUnsafeOriginsAndDeadlines(t *testing.T) {
	for _, base := range []string{"http://u:p@example.test", "http://example.test/a", "http://example.test?x=y", "ftp://example.test", "http://example.test/#fragment", "http://example.test:abc"} {
		if _, err := NewContextClient(base, "token"); err == nil {
			t.Errorf("accepted %s", base)
		}
	}
	e := Envelope{Version: 1, JobID: "10000000-0000-4000-8000-000000000001", AttemptGeneration: 1, OwnerID: "20000000-0000-4000-8000-000000000002", Generation: 1, BackendPID: 1, ExecutorInstanceID: "30000000-0000-4000-8000-000000000003", DeadlineAt: "2026-10-28T12:00:00.123456Z"}
	other := e
	other.DeadlineAt = "2026-10-28T13:00:00.123456+01:00"
	if !e.Matches(other) {
		t.Fatal("equivalent instants did not match")
	}
	other.DeadlineAt = strings.Replace(other.DeadlineAt, "123456", "1234567", 1)
	if e.Matches(other) {
		t.Fatal("accepted sub-microsecond precision")
	}
}
