package worker

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-worker/internal/config"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/executor"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/store"
)

func TestAcceptAndWaitResolvesLostAcceptanceAckWithoutDuplicatePost(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	envelope := executor.Envelope{
		Version: 1, JobID: "c84f11af-aab6-4b47-a32e-d225e39c0b19",
		OwnerID: "826887c4-a11f-4c8c-8b18-013e116b9691", Generation: 7,
		AttemptGeneration: 3, DeadlineAt: deadline,
	}
	var mu sync.Mutex
	state := ""
	posts := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "active_executions": 1, "executor_instance_id": "fixture-instance"})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+envelope.JobID+"/3":
			mu.Lock()
			current := state
			mu.Unlock()
			if current == "" {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(testReceipt(envelope, current))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions":
			mu.Lock()
			posts++
			state = "accepted"
			mu.Unlock()
			go func() {
				time.Sleep(70 * time.Millisecond)
				mu.Lock()
				state = "succeeded"
				mu.Unlock()
			}()
			// The durable write has happened, but the response arrives after the
			// caller's request deadline and is therefore lost to the client.
			time.Sleep(80 * time.Millisecond)
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", 25*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{
		config:   config.Config{PollInterval: 5 * time.Millisecond, RequestTimeout: 25 * time.Millisecond},
		executor: client,
	}
	now := deadline.Add(-time.Minute)
	receipt, err := worker.acceptAndWait(context.Background(), store.Job{
		ID: envelope.JobID, AttemptGeneration: envelope.AttemptGeneration,
	}, envelope, deadline, func(context.Context) (time.Time, error) { return now, nil })
	if err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	postCount := posts
	mu.Unlock()
	if receipt.State != "succeeded" {
		t.Fatalf("receipt state = %q, want succeeded", receipt.State)
	}
	if postCount != 1 {
		t.Fatalf("accept POST count = %d, want 1 after lost ACK", postCount)
	}
}

func TestAcceptAndWaitRequiresConfirmedCancellationAndInstanceIdentity(t *testing.T) {
	deadline := time.Now().UTC().Add(time.Minute)
	envelope := executor.Envelope{
		Version: 1, JobID: "5e361f10-8b0c-41c6-aed3-11db8c4e6dfa",
		OwnerID: "a39321b0-7724-443c-aaed-fd5b57e301db", Generation: 2,
		AttemptGeneration: 4, DeadlineAt: deadline,
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "active_executions": 1, "executor_instance_id": "fixture-instance"})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+envelope.JobID+"/4":
			_ = json.NewEncoder(w).Encode(testReceipt(envelope, "running"))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions/"+envelope.JobID+"/4/cancel":
			response := testReceipt(envelope, "interrupted")
			response["execution_stopped"] = true
			response["current_executor_instance_id"] = "fixture-instance"
			_ = json.NewEncoder(w).Encode(response)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{
		config:   config.Config{PollInterval: time.Millisecond, RequestTimeout: time.Second},
		executor: client,
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	receipt, err := worker.acceptAndWait(canceled, store.Job{
		ID: envelope.JobID, AttemptGeneration: envelope.AttemptGeneration,
	}, envelope, deadline, func(context.Context) (time.Time, error) {
		t.Fatal("database clock must not be read after cancellation")
		return time.Time{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if receipt.State != "interrupted" {
		t.Fatalf("cancellation receipt state = %q", receipt.State)
	}
}

func TestAcceptAndWaitCancellationDuringPollSleepUsesBoundedCancelHandshake(t *testing.T) {
	testCancellationHandshake(t, false)
}

func TestAcceptAndWaitCancellationDuringDatabaseClockUsesBoundedCancelHandshake(t *testing.T) {
	testCancellationHandshake(t, true)
}

func testCancellationHandshake(t *testing.T, cancelDuringClock bool) {
	t.Helper()
	deadline := time.Now().UTC().Add(time.Minute).Truncate(time.Microsecond)
	envelope := executor.Envelope{
		Version: 1, JobID: "6e9327d0-85d6-4a61-a816-680351b8f95f",
		OwnerID: "b54eb271-3414-4989-9ab2-f444467dcf06", Generation: 6,
		AttemptGeneration: 2, DeadlineAt: deadline,
	}
	accepted := make(chan struct{})
	var canceled atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "active_executions": 1, "executor_instance_id": "fixture-instance"})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+envelope.JobID+"/2":
			if !canceled.Load() {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(testReceipt(envelope, "running"))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions":
			close(accepted)
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions/"+envelope.JobID+"/2/cancel":
			response := testReceipt(envelope, "interrupted")
			response["execution_stopped"] = true
			response["current_executor_instance_id"] = "fixture-instance"
			_ = json.NewEncoder(w).Encode(response)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	worker := &Worker{
		config:   config.Config{PollInterval: 500 * time.Millisecond, RequestTimeout: 100 * time.Millisecond},
		executor: client,
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	databaseNow := func(callCtx context.Context) (time.Time, error) {
		if cancelDuringClock {
			canceled.Store(true)
			cancel()
			return time.Time{}, callCtx.Err()
		}
		return deadline.Add(-time.Minute), nil
	}
	done := make(chan struct {
		receipt executor.Receipt
		err     error
	}, 1)
	go func() {
		receipt, callErr := worker.acceptAndWait(ctx, store.Job{
			ID: envelope.JobID, AttemptGeneration: envelope.AttemptGeneration,
		}, envelope, deadline, databaseNow)
		done <- struct {
			receipt executor.Receipt
			err     error
		}{receipt, callErr}
	}()
	<-accepted
	if !cancelDuringClock {
		time.Sleep(20 * time.Millisecond)
		canceled.Store(true)
		cancel()
	}
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.receipt.State != "interrupted" {
			t.Fatalf("cancelled receipt state = %q, want interrupted", result.receipt.State)
		}
	case <-time.After(time.Second):
		t.Fatal("cancel handshake did not resolve within its bounded window")
	}
}

func testReceipt(envelope executor.Envelope, state string) map[string]any {
	return map[string]any{
		"job_id": envelope.JobID, "owner_id": envelope.OwnerID,
		"generation": envelope.Generation, "attempt_generation": envelope.AttemptGeneration,
		"deadline_at":          envelope.DeadlineAt.Format(time.RFC3339Nano),
		"executor_instance_id": "fixture-instance", "state": state,
		"accepted_at": time.Now().UTC().Format(time.RFC3339Nano),
		"started_at":  time.Now().UTC().Format(time.RFC3339Nano),
		"finished_at": time.Now().UTC().Format(time.RFC3339Nano),
		"result":      json.RawMessage(`{"status":"success"}`), "error_code": nil,
	}
}

type fakeQueueStore struct {
	now           time.Time
	info          store.LeaseInfo
	acquireErrors []error
	acquireGate   chan struct{}
	acquireCalls  atomic.Int32
	claimCalls    atomic.Int32
	claimErr      error
	heartbeatErr  error
	claimCalled   chan struct{}
	claimOnce     sync.Once
	releaseCount  atomic.Int32
	finishState   string
	finishResult  map[string]any
	finishCode    *string
	finishCount   int
	finishErr     error
}

func (s *fakeQueueStore) CheckSchema(context.Context) error { return nil }
func (s *fakeQueueStore) Acquire(context.Context) (store.LeaseInfo, error) {
	call := int(s.acquireCalls.Add(1))
	if s.acquireGate != nil {
		select {
		case <-s.acquireGate:
		default:
			return store.LeaseInfo{}, store.ErrLeaseHeld
		}
	}
	if call <= len(s.acquireErrors) {
		return store.LeaseInfo{}, s.acquireErrors[call-1]
	}
	return s.info, nil
}
func (s *fakeQueueStore) Release(context.Context) error { s.releaseCount.Add(1); return nil }
func (s *fakeQueueStore) Heartbeat(context.Context) error {
	if s.heartbeatErr != nil && s.claimCalled != nil {
		select {
		case <-s.claimCalled:
			return s.heartbeatErr
		default:
			return nil
		}
	}
	return s.heartbeatErr
}
func (s *fakeQueueStore) Prune(context.Context, time.Duration, int) (int64, error) { return 0, nil }
func (s *fakeQueueStore) FailExhaustedInterrupted(context.Context, int64, int) (int64, error) {
	return 0, nil
}
func (s *fakeQueueStore) Claim(context.Context, int64) (store.Job, error) {
	s.claimCalls.Add(1)
	if s.claimCalled != nil {
		s.claimOnce.Do(func() { close(s.claimCalled) })
	}
	if s.claimErr != nil {
		return store.Job{}, s.claimErr
	}
	return store.Job{}, store.ErrNoJob
}
func (s *fakeQueueStore) ActiveJobs(context.Context) ([]store.Job, error) { return nil, nil }
func (s *fakeQueueStore) TerminalJobsNeedingEvents(context.Context, int) ([]store.Job, error) {
	return nil, nil
}
func (s *fakeQueueStore) DatabaseNow(context.Context) (time.Time, error) {
	// PostgreSQL timestamp/timestamptz values are stored at microsecond
	// precision; model that boundary even when fixtures use time.Now().
	return s.now.Truncate(time.Microsecond), nil
}
func (s *fakeQueueStore) Info() store.LeaseInfo { return s.info }
func (s *fakeQueueStore) Finish(_ context.Context, _ store.Job, state string, result map[string]any, code *string) error {
	s.finishCount++
	s.finishState, s.finishResult, s.finishCode = state, result, code
	return s.finishErr
}
func (s *fakeQueueStore) FinishRecovered(ctx context.Context, job store.Job, state string, result map[string]any, code *string) error {
	return s.Finish(ctx, job, state, result, code)
}

type allowAllPolicy struct{}

func (allowAllPolicy) KnownKind(string) bool                   { return true }
func (allowAllPolicy) MayDeliver(string, json.RawMessage) bool { return false }

func fakeWorker(client *executor.Client, db *fakeQueueStore, requestTimeout time.Duration) *Worker {
	return &Worker{
		config: config.Config{PollInterval: 5 * time.Millisecond, RequestTimeout: requestTimeout,
			ShutdownDrain: 500 * time.Millisecond, MaxJobAttempts: 3},
		store: db, executor: client, policy: allowAllPolicy{}, logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestRunWaitsForHeldLeaseBeforeRecoveryOrClaims(t *testing.T) {
	db := &fakeQueueStore{
		info:        store.LeaseInfo{OwnerID: "a39321b0-7724-443c-aaed-fd5b57e301db", Generation: 6},
		acquireGate: make(chan struct{}),
	}
	recovered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/internal/v1/worker-events" {
			w.WriteHeader(http.StatusNoContent)
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			recovered <- struct{}{}
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	worker := fakeWorker(client, db, 100*time.Millisecond)
	worker.config.HeartbeatInterval = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	defer cancel()

	deadline := time.After(time.Second)
	for db.acquireCalls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("worker did not attempt lease acquisition")
		default:
			time.Sleep(time.Millisecond)
		}
	}
	select {
	case <-recovered:
		t.Fatal("worker notified executor before acquiring the lease")
	case <-time.After(40 * time.Millisecond):
	}
	if got := db.claimCalls.Load(); got != 0 {
		t.Fatalf("claim count before lease release = %d, want 0", got)
	}

	close(db.acquireGate)
	select {
	case <-recovered:
	case <-time.After(3 * time.Second):
		t.Fatal("worker did not continue after the existing owner released the lease")
	}
	time.Sleep(20 * time.Millisecond)
	if got := db.acquireCalls.Load(); got < 2 {
		t.Fatalf("acquire attempts = %d, want a held attempt followed by success", got)
	}
	cancel()
	select {
	case runErr := <-done:
		if runErr != nil {
			t.Fatalf("Run after cancellation = %v", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("worker did not stop after cancellation")
	}
}

func TestAcquireLeaseRetriesOnlyHeldAndStopsAtBound(t *testing.T) {
	db := &fakeQueueStore{acquireGate: make(chan struct{})}
	started := time.Now()
	_, err := acquireLeaseWithRetry(context.Background(), db, nil, 50*time.Millisecond, 5*time.Millisecond)
	if !errors.Is(err, store.ErrLeaseHeld) {
		t.Fatalf("acquireLeaseWithRetry = %v, want ErrLeaseHeld", err)
	}
	if elapsed := time.Since(started); elapsed > 300*time.Millisecond {
		t.Fatalf("bounded lease wait took %s, want under 300ms", elapsed)
	}
	if got := db.acquireCalls.Load(); got < 2 {
		t.Fatalf("held lease acquisition attempts = %d, want retries", got)
	}

	databaseErr := errors.New("database unavailable")
	db = &fakeQueueStore{acquireErrors: []error{databaseErr}}
	if _, err := acquireLeaseWithRetry(context.Background(), db, nil, time.Second, 5*time.Millisecond); !errors.Is(err, databaseErr) {
		t.Fatalf("non-held acquisition error = %v, want original error", err)
	}
	if got := db.acquireCalls.Load(); got != 1 {
		t.Fatalf("non-held acquisition attempts = %d, want 1", got)
	}
}

func TestAcquireLeaseCancellationStopsPromptly(t *testing.T) {
	db := &fakeQueueStore{acquireGate: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	started := time.Now()
	_, err := acquireLeaseWithRetry(ctx, db, nil, time.Second, 5*time.Millisecond)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("acquireLeaseWithRetry cancellation = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > 250*time.Millisecond {
		t.Fatalf("canceled lease wait took %s, want prompt stop", elapsed)
	}
}

func TestRunSleepCancellationReturnsLeaseLossOnlyForHeartbeatLoss(t *testing.T) {
	for _, test := range []struct {
		name          string
		claimErr      error
		heartbeat     bool
		shutdown      bool
		wantLeaseLost bool
	}{
		{name: "idle heartbeat loss", heartbeat: true, wantLeaseLost: true},
		{name: "claim error heartbeat loss", claimErr: errors.New("temporary claim failure"), heartbeat: true, wantLeaseLost: true},
		{name: "idle shutdown", shutdown: true},
		{name: "claim error shutdown", claimErr: errors.New("temporary claim failure"), shutdown: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db := &fakeQueueStore{
				info:     store.LeaseInfo{OwnerID: "a39321b0-7724-443c-aaed-fd5b57e301db", Generation: 5},
				claimErr: test.claimErr, claimCalled: make(chan struct{}),
			}
			if test.heartbeat {
				db.heartbeatErr = errors.New("fixture lease heartbeat failure")
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost && r.URL.Path == "/internal/v1/worker-events" {
					w.WriteHeader(http.StatusNoContent)
					return
				}
				http.NotFound(w, r)
			}))
			defer server.Close()
			client, err := executor.New(server.URL, "fixture-executor-token-123", 100*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			worker := fakeWorker(client, db, 100*time.Millisecond)
			worker.config.PollInterval = time.Second
			worker.config.HeartbeatInterval = 10 * time.Millisecond
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()

			select {
			case <-db.claimCalled:
			case <-time.After(time.Second):
				t.Fatal("worker did not attempt a claim")
			}
			if test.shutdown {
				// Let Claim return and the worker enter its idle/claim-error poll sleep.
				time.Sleep(20 * time.Millisecond)
				cancel()
			}
			select {
			case runErr := <-done:
				if test.wantLeaseLost && !errors.Is(runErr, ErrLeaseLost) {
					t.Fatalf("Run error = %v, want ErrLeaseLost", runErr)
				}
				if !test.wantLeaseLost && runErr != nil {
					t.Fatalf("Run error = %v, want clean shutdown", runErr)
				}
				if got := db.releaseCount.Load(); got != 1 {
					t.Fatalf("lease release count = %d, want 1", got)
				}
			case <-time.After(time.Second):
				t.Fatal("worker did not stop after idle/claim-error cancellation")
			}
		})
	}
}

func TestRunJobRetriesTransientPolicyReadWithoutChangingAttempt(t *testing.T) {
	job := store.Job{ID: "763f9954-adfe-45e0-b56d-91f971f6de32", Kind: "news_processing", AttemptGeneration: 1}
	db := &fakeQueueStore{
		now: time.Now().UTC(), info: store.LeaseInfo{OwnerID: "0a0ce350-8d79-4846-862a-9b36fb08bd29", Generation: 9},
	}
	var policyReads, posts atomic.Int32
	deadline := db.now.Add(60 * time.Second).Truncate(time.Microsecond)
	state := ""
	var mu sync.Mutex
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/policy":
			if policyReads.Add(1) == 1 {
				http.Error(w, "temporary unavailable", http.StatusServiceUnavailable)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]int{"timeout_seconds": 60})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1":
			mu.Lock()
			current := state
			mu.Unlock()
			if current == "" {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(receiptForJob(job, db.info, current, deadline))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions":
			posts.Add(1)
			mu.Lock()
			state = "succeeded"
			mu.Unlock()
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1/events":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	worker := fakeWorker(client, db, 100*time.Millisecond)
	if err := worker.runJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if policyReads.Load() != 2 || posts.Load() != 1 {
		t.Fatalf("policy reads/posts = %d/%d; want 2/1", policyReads.Load(), posts.Load())
	}
	if db.finishState != "succeeded" || db.finishCount != 1 || db.finishCode != nil {
		t.Fatalf("finish state/count/code = %q/%d/%v; want succeeded/1/NULL", db.finishState, db.finishCount, db.finishCode)
	}
}

func TestRunJobMatchesReceiptDeadlineAtPostgresTimestampPrecision(t *testing.T) {
	job := store.Job{ID: "34ceaf1d-d7b4-4320-baad-bf9a45ac33e8", Kind: "news_processing", AttemptGeneration: 1}
	// Explicitly include sub-microsecond nanoseconds to reproduce Linux CI's
	// clock behavior rather than relying on the host's wall-clock resolution.
	now := time.Date(2026, time.October, 24, 12, 0, 0, 123456789, time.UTC)
	db := &fakeQueueStore{
		now: now, info: store.LeaseInfo{OwnerID: "e3a30b7c-6f56-4e8d-a267-87a1a0d4fe5a", Generation: 12},
	}
	deadline := now.Truncate(time.Microsecond).Add(time.Minute)
	envelope := executor.Envelope{Version: 1, JobID: job.ID, OwnerID: db.info.OwnerID,
		Generation: db.info.Generation, AttemptGeneration: job.AttemptGeneration, DeadlineAt: deadline}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/policy":
			_ = json.NewEncoder(w).Encode(map[string]int{"timeout_seconds": 60})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1":
			_ = json.NewEncoder(w).Encode(testReceipt(envelope, "succeeded"))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1/events":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := db.now.Truncate(time.Microsecond), time.Date(2026, time.October, 24, 12, 0, 0, 123456000, time.UTC); !got.Equal(want) {
		t.Fatalf("fixture database time = %s, want PostgreSQL microsecond time %s", got, want)
	}
	worker := fakeWorker(client, db, 100*time.Millisecond)
	if err := worker.runJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if db.finishCount != 1 || db.finishState != "succeeded" {
		t.Fatalf("finished state/count = %q/%d, want succeeded/1", db.finishState, db.finishCount)
	}
}

func TestAcceptedReceiptAuthFailureNeverFinishesJobOrOpensQueueGate(t *testing.T) {
	job := store.Job{ID: "d2f61f58-14d7-4352-9bbd-842cbe6cae5f", Kind: "news_digest", AttemptGeneration: 1}
	db := &fakeQueueStore{
		now: time.Now().UTC(), info: store.LeaseInfo{OwnerID: "85c6a9b8-446b-4136-baa8-d5a53d6e1c83", Generation: 2},
	}
	var accepted atomic.Bool
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/policy":
			_ = json.NewEncoder(w).Encode(map[string]int{"timeout_seconds": 60})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1":
			if accepted.Load() {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			http.NotFound(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions":
			posts.Add(1)
			accepted.Store(true)
			// Simulate a committed receipt whose HTTP acknowledgement is lost.
			time.Sleep(30 * time.Millisecond)
			w.WriteHeader(http.StatusAccepted)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", 15*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	worker := fakeWorker(client, db, 15*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err = worker.runJob(ctx, job)
	if err == nil {
		t.Fatal("expected unresolved accepted execution")
	}
	if db.finishCount != 0 {
		t.Fatalf("finished %d jobs after a post-accept auth failure; want zero", db.finishCount)
	}
	if posts.Load() != 1 {
		t.Fatalf("execution POST count = %d; want one without replay after lost ACK", posts.Load())
	}
}

func TestPolicyAuthFailureStopsWorkerWithoutFailingClaimedJob(t *testing.T) {
	job := store.Job{ID: "ad31885a-8c1e-4d06-a98a-91e59ee6cb31", Kind: "news_processing", AttemptGeneration: 1}
	db := &fakeQueueStore{now: time.Now().UTC()}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/policy" {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		http.NotFound(w, r)
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	worker := fakeWorker(client, db, time.Second)
	err = worker.runJob(context.Background(), job)
	if !errors.Is(err, ErrExecutorAuth) {
		t.Fatalf("policy error = %v; want fail-closed executor auth error", err)
	}
	if db.finishCount != 0 {
		t.Fatalf("finished %d jobs after a policy authentication failure; want zero", db.finishCount)
	}
}

func TestAmbiguousPostTimeoutThenReceiptNotFoundAnd422DoesNotFailJob(t *testing.T) {
	job := store.Job{ID: "ec70f44a-06ca-43a4-ac1f-aa74a9ba47af", Kind: "news_processing", AttemptGeneration: 1}
	db := &fakeQueueStore{
		now: time.Now().UTC(), info: store.LeaseInfo{OwnerID: "a7a258fa-4ac4-4418-8ac8-64bc902a67f8", Generation: 3},
	}
	var posts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/policy":
			_ = json.NewEncoder(w).Encode(map[string]int{"timeout_seconds": 60})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1":
			http.NotFound(w, r)
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions":
			if posts.Add(1) == 1 {
				// Model a request that may commit after the client's timeout.
				time.Sleep(60 * time.Millisecond)
				w.WriteHeader(http.StatusAccepted)
				return
			}
			http.Error(w, "invalid attempt", http.StatusUnprocessableEntity)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	worker := fakeWorker(client, db, 20*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Millisecond)
	defer cancel()
	err = worker.runJob(ctx, job)
	if err == nil {
		t.Fatal("expected unresolved ambiguous acceptance")
	}
	if db.finishCount != 0 {
		t.Fatalf("finished %d jobs after timeout/404/422; want unresolved queue gate", db.finishCount)
	}
	if posts.Load() != 2 {
		t.Fatalf("execution POST count = %d; want initial ambiguous POST and one definitive rejection", posts.Load())
	}
}

func TestAcceptedReceiptFromPreviousExecutorInstanceRecoversBeforeDeadline(t *testing.T) {
	job := store.Job{ID: "5db357b3-e8c4-4d1e-b8e3-304d777893a7", Kind: "news_processing", AttemptGeneration: 1}
	db := &fakeQueueStore{
		now: time.Now().UTC(), info: store.LeaseInfo{OwnerID: "85c6a9b8-446b-4136-baa8-d5a53d6e1c83", Generation: 2},
	}
	deadline := db.now.Add(60 * time.Second).Truncate(time.Microsecond)
	envelope := executor.Envelope{Version: 1, JobID: job.ID, OwnerID: db.info.OwnerID,
		Generation: db.info.Generation, AttemptGeneration: job.AttemptGeneration, DeadlineAt: deadline}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/policy":
			_ = json.NewEncoder(w).Encode(map[string]int{"timeout_seconds": 60})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1":
			receipt := testReceipt(envelope, "running")
			receipt["executor_instance_id"] = "old-executor-instance"
			_ = json.NewEncoder(w).Encode(receipt)
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "active_executions": 0, "executor_instance_id": "new-executor-instance"})
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1/events":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", 100*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	worker := fakeWorker(client, db, 100*time.Millisecond)
	if err := worker.runJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if db.finishCount != 1 || db.finishState != "interrupted" {
		t.Fatalf("recovered state/count = %q/%d, want interrupted/1", db.finishState, db.finishCount)
	}
}

func TestRunJobWithDrainAllowsCompletionBeforeGraceExpires(t *testing.T) {
	job := store.Job{ID: "dec4a189-68cf-4d42-a69d-a74be24e91d9", Kind: "news_processing", AttemptGeneration: 1}
	db := &fakeQueueStore{
		now: time.Now().UTC(), info: store.LeaseInfo{OwnerID: "5ffcded2-7448-4d7f-a66d-e9989096c21d", Generation: 3},
	}
	accepted := make(chan struct{})
	complete := make(chan struct{})
	deadline := db.now.Add(60 * time.Second).Truncate(time.Microsecond)
	var mu sync.Mutex
	state := ""
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/policy":
			_ = json.NewEncoder(w).Encode(map[string]int{"timeout_seconds": 60})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1":
			mu.Lock()
			current := state
			mu.Unlock()
			if current == "" {
				http.NotFound(w, r)
				return
			}
			if current == "accepted" {
				select {
				case <-complete:
					mu.Lock()
					state = "succeeded"
					mu.Unlock()
					current = "succeeded"
				default:
				}
			}
			_ = json.NewEncoder(w).Encode(receiptForJob(job, db.info, current, deadline))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions":
			mu.Lock()
			state = "accepted"
			mu.Unlock()
			close(accepted)
			w.WriteHeader(http.StatusAccepted)
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/1/events":
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	worker := fakeWorker(client, db, time.Second)
	shutdownCtx, signalShutdown := context.WithCancel(context.Background())
	defer signalShutdown()
	leaseCtx := context.Background()
	finished := make(chan error, 1)
	go func() { finished <- worker.runJobWithDrain(shutdownCtx, leaseCtx, job) }()
	<-accepted
	signalShutdown()
	time.Sleep(40 * time.Millisecond)
	close(complete)
	if err := <-finished; err != nil {
		t.Fatal(err)
	}
	if db.finishState != "succeeded" {
		t.Fatalf("finished state = %q, want succeeded", db.finishState)
	}
}

func TestRecoveryDoesNotFinishWhileCancellationIsUnconfirmed(t *testing.T) {
	job := store.Job{ID: "c4a33e27-fb89-44a6-8496-ddc71991fc02", Kind: "news_processing", AttemptGeneration: 2}
	db := &fakeQueueStore{info: store.LeaseInfo{OwnerID: "a39321b0-7724-443c-aaed-fd5b57e301db", Generation: 5}}
	deadline := time.Now().UTC().Add(time.Minute)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/status":
			_ = json.NewEncoder(w).Encode(map[string]any{"ready": true, "active_executions": 1, "executor_instance_id": "same-instance"})
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/2":
			response := testReceipt(executor.Envelope{JobID: job.ID, OwnerID: db.info.OwnerID,
				Generation: 4, AttemptGeneration: 2, DeadlineAt: deadline}, "running")
			response["executor_instance_id"] = "same-instance"
			_ = json.NewEncoder(w).Encode(response)
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/job-executions/"+job.ID+"/2/cancel":
			response := testReceipt(executor.Envelope{JobID: job.ID, OwnerID: db.info.OwnerID,
				Generation: 4, AttemptGeneration: 2, DeadlineAt: deadline}, "running")
			response["execution_stopped"] = false
			response["current_executor_instance_id"] = "same-instance"
			_ = json.NewEncoder(w).Encode(response)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client, err := executor.New(server.URL, "fixture-executor-token-123", 15*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	worker := fakeWorker(client, db, 15*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	err = worker.recoverJob(ctx, job)
	if err == nil {
		t.Fatal("recovery succeeded without cancellation acknowledgement")
	}
	if db.finishCount != 0 {
		t.Fatalf("finished %d jobs despite unresolved cancellation", db.finishCount)
	}
}

func receiptForJob(job store.Job, info store.LeaseInfo, state string, deadline time.Time) map[string]any {
	return testReceipt(executor.Envelope{
		Version: 1, JobID: job.ID, OwnerID: info.OwnerID, Generation: info.Generation,
		AttemptGeneration: job.AttemptGeneration, DeadlineAt: deadline,
	}, state)
}
