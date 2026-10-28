package domainops

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

type testGuard struct{}

func (testGuard) Check(context.Context) error { return nil }

type blockingAuthority struct {
	started chan struct{}
	once    sync.Once
}

func (a *blockingAuthority) Get(ctx context.Context, _ string, _ int) (authority.ExecutionContext, error) {
	a.once.Do(func() { close(a.started) })
	<-ctx.Done()
	return authority.ExecutionContext{}, ctx.Err()
}
func TestCancellationMapsStaleAndPublishUnknown(t *testing.T) {
	for _, op := range []string{"summary_status", "digest_publish"} {
		t.Run(op, func(t *testing.T) {
			a := &blockingAuthority{started: make(chan struct{})}
			instance := "50000000-0000-4000-8000-000000000005"
			svc := New(context.Background(), "token", instance, testGuard{}, a, nil, nil)
			input := json.RawMessage(`{}`)
			if op == "digest_publish" {
				input = json.RawMessage(`{"prepared_token":"80000000-0000-4000-8000-000000000008"}`)
			}
			sum := sha256.Sum256(input)
			req := Request{Version: 1, Operation: op, OperationID: "60000000-0000-4000-8000-000000000006", DomainInstanceID: instance, Input: input, InputFingerprint: hex.EncodeToString(sum[:]), Envelope: authority.Envelope{Version: 1, JobID: "10000000-0000-4000-8000-000000000001", AttemptGeneration: 1, OwnerID: "20000000-0000-4000-8000-000000000002", Generation: 1, BackendPID: 1, ExecutorInstanceID: "30000000-0000-4000-8000-000000000003", DeadlineAt: time.Now().UTC().Add(time.Minute).Format(time.RFC3339)}}
			wire, _ := json.Marshal(req)
			r := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(wire))
			r.Header.Set("Authorization", "Bearer token")
			r.Header.Set("X-Domain-Instance-Id", instance)
			w := httptest.NewRecorder()
			svc.Post(w, r)
			if w.Code != 202 {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
			<-a.started
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := svc.Drain(ctx); err != nil {
				t.Fatal(err)
			}
			svc.mu.Lock()
			rec := svc.records[key(req.Envelope.JobID, 1, op, req.OperationID)]
			code := ""
			if rec.code != nil {
				code = *rec.code
			}
			state := rec.state
			svc.mu.Unlock()
			want := "stale_fence"
			if op == "digest_publish" {
				want = "external_outcome_unknown"
			}
			if state != "failed" || code != want {
				t.Fatalf("state=%s code=%s", state, code)
			}
		})
	}
}
func TestExpiredActiveReceiptIsNotServed(t *testing.T) {
	instance := "50000000-0000-4000-8000-000000000005"
	svc := New(context.Background(), "token", instance, testGuard{}, nil, nil, nil)
	job := "10000000-0000-4000-8000-000000000001"
	id := "60000000-0000-4000-8000-000000000006"
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	svc.records[key(job, 1, "summary_status", id)] = &record{expires: time.Now().Add(-time.Second), cancel: cancel}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /{job}/{attempt}/{operation}/{id}", svc.Get)
	r := httptest.NewRequest(http.MethodGet, "/"+job+"/1/summary_status/"+id, nil)
	r.Header.Set("Authorization", "Bearer token")
	r.Header.Set("X-Domain-Instance-Id", instance)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, r)
	if w.Code != 404 {
		t.Fatalf("status=%d", w.Code)
	}
}
