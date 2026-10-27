package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-background/internal/config"
	ingestion "github.com/Dzarlax-AI/evening-news/go-ingestion/runner"
	processing "github.com/Dzarlax-AI/evening-news/go-processing/runner"
	scheduler "github.com/Dzarlax-AI/evening-news/go-scheduler/runner"
	worker "github.com/Dzarlax-AI/evening-news/go-worker/runner"
)

func testConfig() config.Config {
	return config.Config{BuildID: "test-build", HealthAddr: "127.0.0.1:0", ShutdownTimeout: time.Second, BackoffMin: 5 * time.Millisecond, BackoffMax: 20 * time.Millisecond, ProbeInterval: 15 * time.Millisecond, ProbeTimeout: 30 * time.Millisecond}
}

func TestLocalServiceURLPreservesExplicitHost(t *testing.T) {
	tests := []struct {
		addr, want string
	}{
		{"127.0.0.1:8093", "http://127.0.0.1:8093"},
		{"localhost:8093", "http://localhost:8093"},
		{"[::1]:8093", "http://[::1]:8093"},
		{"0.0.0.0:8093", "http://127.0.0.1:8093"},
		{"[::]:8093", "http://127.0.0.1:8093"},
		{"[fe80::1%lo0]:8093", "http://[fe80::1%25lo0]:8093"},
		{"bad-listener", ""},
	}
	for _, tc := range tests {
		t.Run(tc.addr, func(t *testing.T) {
			if got := localServiceURL(tc.addr); got != tc.want {
				t.Fatalf("localServiceURL(%q) = %q, want %q", tc.addr, got, tc.want)
			}
		})
	}
}

func TestLocalServiceURLIPv6LoopbackHTTP(t *testing.T) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		if errors.Is(err, syscall.EAFNOSUPPORT) || errors.Is(err, syscall.EPROTONOSUPPORT) || errors.Is(err, syscall.EADDRNOTAVAIL) {
			t.Skipf("IPv6 loopback is unavailable in this kernel: %v", err)
		}
		t.Fatalf("listen on IPv6 loopback: %v", err)
	}
	const token = "fixture-token-with-enough-bytes"
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/healthz":
			w.WriteHeader(http.StatusOK)
		case "/internal/v1/status":
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(http.StatusUnauthorized)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"executor_instance_id": "fixture-instance",
				"active_executions":    0,
				"ready":                true,
			})
		default:
			http.NotFound(w, r)
		}
	})}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	defer func() {
		_ = server.Close()
		<-serveDone
	}()

	url := localServiceURL(listener.Addr().String())
	if !strings.HasPrefix(url, "http://[::1]:") {
		t.Fatalf("IPv6 listener URL = %q", url)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := (&http.Client{Timeout: time.Second}).Do(req)
	if err != nil {
		t.Fatalf("request to preserved IPv6 listener failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("IPv6 listener response status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	if err := getHealth(ctx, listener.Addr().String(), "/healthz"); err != nil {
		t.Fatalf("health probe through shared URL builder failed: %v", err)
	}
	cfg := testConfig()
	cfg.ExecutionListenAddr = listener.Addr().String()
	cfg.Execution.Token = token
	if err := executionReady(ctx, cfg); err != nil {
		t.Fatalf("execution readiness probe through shared URL builder failed: %v", err)
	}
}

func baseDeps() dependencies {
	wait := func(ctx context.Context, ready func()) error { ready(); <-ctx.Done(); return nil }
	waitWithHealth := func(ctx context.Context, ready func(), health func(time.Time)) error {
		ready()
		health(time.Now())
		<-ctx.Done()
		return nil
	}
	return dependencies{
		runners: runnerSet{scheduler: func(c context.Context, _ scheduler.Config, r func()) error { return wait(c, r) }, ingestion: func(c context.Context, _ ingestion.Config, r func(), h func(time.Time)) error {
			return waitWithHealth(c, r, h)
		}, processing: func(c context.Context, _ processing.Config, r func()) error { return wait(c, r) }, worker: func(c context.Context, _ worker.Config, r func(), h func(time.Time)) error {
			return waitWithHealth(c, r, h)
		}, execution: func(c context.Context, _ config.Config, ready func(bool)) error { ready(true); <-c.Done(); return nil }},
		probes: probes{executor: func(context.Context, worker.Config) error { return nil }, scheduler: func(context.Context, string) error { return nil }, ingestion: func(context.Context, ingestion.Config) error { return nil }, processing: func(context.Context, processing.Config) error { return nil }, execution: func(context.Context, config.Config) error { return nil }},
		listen: net.Listen,
	}
}

func TestRuntimeStartsOnceAndWaitsForExecutorThenStops(t *testing.T) {
	cfg := testConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps := baseDeps()
	var sched, ingest, process, workers atomic.Int32
	deps.runners.scheduler = func(c context.Context, _ scheduler.Config, r func()) error {
		sched.Add(1)
		return baseDeps().runners.scheduler(c, scheduler.Config{}, r)
	}
	deps.runners.ingestion = func(c context.Context, _ ingestion.Config, r func(), h func(time.Time)) error {
		ingest.Add(1)
		return baseDeps().runners.ingestion(c, ingestion.Config{}, r, h)
	}
	deps.runners.processing = func(c context.Context, _ processing.Config, r func()) error {
		process.Add(1)
		return baseDeps().runners.processing(c, processing.Config{}, r)
	}
	deps.runners.worker = func(c context.Context, _ worker.Config, r func(), h func(time.Time)) error {
		workers.Add(1)
		h(time.Now())
		r()
		<-c.Done()
		return nil
	}
	var calls atomic.Int32
	deps.probes.executor = func(context.Context, worker.Config) error {
		if sched.Load() != 1 || ingest.Load() != 1 || process.Load() != 1 {
			t.Error("executor probe preceded component startup")
		}
		if calls.Add(1) == 1 {
			return errors.New("temporarily unavailable")
		}
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, deps) }()
	waitFor(t, func() bool { return workers.Load() == 1 })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("runtime shutdown failed")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("runtime did not stop")
	}
	if calls.Load() < 2 || sched.Load() != 1 || ingest.Load() != 1 || process.Load() != 1 || workers.Load() != 1 {
		t.Fatalf("unexpected run counts scheduler=%d ingestion=%d processing=%d worker=%d executor=%d", sched.Load(), ingest.Load(), process.Load(), workers.Load(), calls.Load())
	}
}

func TestWorkerLeaseLossRestartsOnlyWorkerAndReadinessRecovers(t *testing.T) {
	cfg := testConfig()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	deps := baseDeps()
	var sched, ingest, process, workers atomic.Int32
	deps.runners.scheduler = func(c context.Context, _ scheduler.Config, r func()) error {
		sched.Add(1)
		return baseDeps().runners.scheduler(c, scheduler.Config{}, r)
	}
	deps.runners.ingestion = func(c context.Context, _ ingestion.Config, r func(), h func(time.Time)) error {
		ingest.Add(1)
		return baseDeps().runners.ingestion(c, ingestion.Config{}, r, h)
	}
	deps.runners.processing = func(c context.Context, _ processing.Config, r func()) error {
		process.Add(1)
		return baseDeps().runners.processing(c, processing.Config{}, r)
	}
	secondReady := make(chan struct{})
	loseSecond := make(chan struct{})
	thirdStarted := make(chan struct{})
	startThird := make(chan struct{})
	deps.runners.worker = func(c context.Context, _ worker.Config, r func(), h func(time.Time)) error {
		n := workers.Add(1)
		if n == 1 {
			h(time.Now())
			r()
			return worker.ErrLeaseLost
		}
		if n == 2 {
			h(time.Now())
			r()
			close(secondReady)
			<-loseSecond
			return worker.ErrLeaseLost
		}
		close(thirdStarted)
		<-startThird
		h(time.Now())
		r()
		<-c.Done()
		return nil
	}
	var healthListener atomic.Value
	deps.listen = func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err == nil {
			healthListener.Store(ln.Addr().String())
		}
		return ln, err
	}
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, deps) }()
	select {
	case <-secondReady:
	case <-time.After(2 * time.Second):
		t.Fatal("worker did not recover from lease loss")
	}
	waitFor(t, func() bool { return readinessCode(healthListener.Load().(string)) == http.StatusOK })
	// Trigger another lease transition and confirm the health endpoint drops
	// readiness while the other three runners stay alive in this same runtime.
	close(loseSecond)
	select {
	case <-thirdStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not retry after second lease loss")
	}
	waitFor(t, func() bool { return readinessCode(healthListener.Load().(string)) == http.StatusServiceUnavailable })
	close(startThird)
	waitFor(t, func() bool { return readinessCode(healthListener.Load().(string)) == http.StatusOK })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown timed out")
	}
	if sched.Load() != 1 || ingest.Load() != 1 || process.Load() != 1 || workers.Load() != 3 {
		t.Fatalf("component restart counts changed: %d/%d/%d/%d", sched.Load(), ingest.Load(), process.Load(), workers.Load())
	}
}

func TestExecutorAuthFailureIsFatalBeforeWorkerStarts(t *testing.T) {
	cfg := testConfig()
	deps := baseDeps()
	var workers atomic.Int32
	deps.runners.worker = func(context.Context, worker.Config, func(), func(time.Time)) error { workers.Add(1); return nil }
	deps.probes.executor = func(context.Context, worker.Config) error { return ErrExecutorAuth }
	err := run(context.Background(), cfg, deps)
	if !errors.Is(err, ErrExecutorAuth) {
		t.Fatalf("runtime error = %v", err)
	}
	if workers.Load() != 0 {
		t.Fatal("worker started despite executor auth failure")
	}
}

func TestComponentFailureInterruptsExecutorStartupBackoff(t *testing.T) {
	cfg := testConfig()
	cfg.BackoffMin = 100 * time.Millisecond
	cfg.BackoffMax = 100 * time.Millisecond
	deps := baseDeps()
	fail := make(chan struct{})
	firstProbe := make(chan struct{})
	var probes atomic.Int32
	deps.runners.ingestion = func(_ context.Context, _ ingestion.Config, ready func(), health func(time.Time)) error {
		ready()
		health(time.Now())
		<-fail
		return errors.New("listener failed")
	}
	deps.probes.executor = func(context.Context, worker.Config) error {
		if probes.Add(1) == 1 {
			close(firstProbe)
		}
		return errors.New("executor unavailable")
	}
	done := make(chan error, 1)
	go func() { done <- run(context.Background(), cfg, deps) }()
	select {
	case <-firstProbe:
	case <-time.After(time.Second):
		t.Fatal("executor startup probe did not run")
	}
	close(fail)
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("component failure was ignored")
		}
	case <-time.After(time.Second):
		t.Fatal("executor backoff ignored component failure")
	}
	if probes.Load() > 1 {
		t.Fatalf("executor was retried %d times after component failure", probes.Load())
	}
}

func TestWorkerRecoveryBackoffCancelsWithoutRestart(t *testing.T) {
	cfg := testConfig()
	cfg.BackoffMin = time.Second
	cfg.BackoffMax = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	s := newState("test")
	s.phase("running")
	var attempts atomic.Int32
	done := make(chan error, 1)
	go func() {
		done <- workerLoop(ctx, cfg, func(_ context.Context, _ worker.Config, ready func(), health func(time.Time)) error {
			attempts.Add(1)
			health(time.Now())
			ready()
			return worker.ErrLeaseLost
		}, s, make(chan struct{}, 1), cfg.BackoffMin, cfg.BackoffMax)
	}()
	waitFor(t, func() bool { return s.get().Components["worker"].State == "recovering" })
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker backoff did not stop")
	}
	if attempts.Load() != 1 {
		t.Fatalf("restarted %d times after cancellation", attempts.Load())
	}
}

func TestWorkerLateHealthCallbackCannotRefreshReplacementAttempt(t *testing.T) {
	cfg := testConfig()
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = time.Millisecond
	s := newState("test")
	s.phase("running")
	s.set("scheduler", "ready", true)
	s.set("ingestion", "ready", true)
	s.set("processing", "ready", true)
	s.set("execution", "ready", true)
	s.mu.Lock()
	s.ingestionHealthAt = time.Now()
	s.ingestionAPIHealthy = true
	s.mu.Unlock()

	type callbacks struct {
		ready  func()
		health func(time.Time)
	}
	firstCallbacks := make(chan callbacks, 1)
	secondReady := make(chan time.Time, 1)
	releaseSecond := make(chan struct{})
	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- workerLoop(ctx, cfg, func(_ context.Context, _ worker.Config, ready func(), health func(time.Time)) error {
			if attempts.Add(1) == 1 {
				firstCallbacks <- callbacks{ready: ready, health: health}
				return worker.ErrLeaseLost
			}
			ready()
			at := time.Now()
			health(at)
			secondReady <- at
			<-releaseSecond
			return nil
		}, s, make(chan struct{}, 1), cfg.BackoffMin, cfg.BackoffMax)
	}()

	old := <-firstCallbacks
	var secondAt time.Time
	select {
	case secondAt = <-secondReady:
	case <-time.After(time.Second):
		t.Fatal("replacement worker attempt did not become healthy")
	}
	if !s.ready() {
		t.Fatal("runtime was not ready after replacement attempt health")
	}

	// These callbacks simulate an observer goroutine descheduled inside the old
	// runner until after its replacement has completed the ready handshake.
	old.ready()
	old.health(secondAt.Add(time.Second))
	s.mu.RLock()
	gotAt := s.workerHealthAt
	gotStatus := s.snapshot.Components["worker"]
	s.mu.RUnlock()
	if !gotAt.Equal(secondAt) {
		t.Fatalf("stale callback replaced worker health timestamp: got %s want %s", gotAt, secondAt)
	}
	if gotStatus.State != "ready" || !gotStatus.Healthy || !s.ready() {
		t.Fatalf("stale callback changed replacement readiness: status=%+v", gotStatus)
	}

	cancel()
	close(releaseSecond)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker loop did not stop")
	}
}

func TestFailedHeartbeatDropsReadinessBeforeRunExitsThenRecovers(t *testing.T) {
	cfg := testConfig()
	cfg.BackoffMin = time.Millisecond
	cfg.BackoffMax = time.Millisecond
	s := newState("test")
	s.phase("running")
	s.set("scheduler", "ready", true)
	s.set("ingestion", "ready", true)
	s.set("processing", "ready", true)
	s.set("execution", "ready", true)
	s.mu.Lock()
	s.ingestionHealthAt = time.Now()
	s.ingestionAPIHealthy = true
	s.mu.Unlock()

	firstHealth := make(chan func(time.Time), 1)
	secondReady := make(chan struct{})
	releaseFirst := make(chan struct{})
	releaseSecond := make(chan struct{})
	var attempts atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- workerLoop(ctx, cfg, func(_ context.Context, _ worker.Config, ready func(), health func(time.Time)) error {
			if attempts.Add(1) == 1 {
				ready()
				health(time.Now())
				firstHealth <- health
				<-releaseFirst
				return worker.ErrLeaseLost
			}
			ready()
			health(time.Now())
			close(secondReady)
			<-releaseSecond
			return context.Canceled
		}, s, make(chan struct{}, 1), cfg.BackoffMin, cfg.BackoffMax)
	}()

	var reportHealth func(time.Time)
	select {
	case reportHealth = <-firstHealth:
	case <-time.After(time.Second):
		t.Fatal("first worker attempt did not report health")
	}
	if !s.ready() {
		t.Fatal("runtime should be ready after initial lease health")
	}

	// A failed fenced heartbeat must invalidate readiness while Run is still
	// blocked draining its current work and before it returns ErrLeaseLost.
	reportHealth(time.Time{})
	if s.ready() || s.get().Components["worker"].State != "unhealthy" {
		t.Fatal("failed heartbeat did not immediately make runtime unready")
	}
	if attempts.Load() != 1 {
		t.Fatalf("worker restarted before the previous Run exited: attempts=%d", attempts.Load())
	}

	close(releaseFirst)
	select {
	case <-secondReady:
	case <-time.After(time.Second):
		t.Fatal("worker did not recover after the previous Run exited")
	}
	if !s.ready() {
		t.Fatal("runtime did not regain readiness after replacement lease health")
	}

	cancel()
	close(releaseSecond)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("worker loop did not stop")
	}
}

func TestExecutorStatusRequiresReadyInstanceAndTreatsAuthAsFatal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		wantAuth  bool
		wantReady bool
	}{
		{"ready", http.StatusOK, `{"executor_instance_id":"fixture","ready":true}`, false, true},
		{"not ready", http.StatusOK, `{"executor_instance_id":"fixture","ready":false}`, false, false},
		{"unauthorized", http.StatusUnauthorized, `{}`, true, false},
		{"forbidden", http.StatusForbidden, `{}`, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/internal/v1/status" || r.Header.Get("Authorization") != "Bearer test-token-123456" {
					t.Errorf("unexpected status request")
				}
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.body))
			}))
			defer srv.Close()
			cfg := worker.Config{}
			cfg.ExecutorURL = srv.URL
			cfg.ExecutorToken = "test-token-123456"
			err := executorReady(context.Background(), cfg)
			if tc.wantReady && err != nil {
				t.Fatalf("ready status error = %v", err)
			}
			if !tc.wantReady && err == nil {
				t.Fatal("unready executor was accepted")
			}
			if errors.Is(err, ErrExecutorAuth) != tc.wantAuth {
				t.Fatalf("auth classification = %v", err)
			}
		})
	}
}

func TestShutdownDrainsWorkerBeforeStoppingPrivateListeners(t *testing.T) {
	cfg := testConfig()
	ctx, cancel := context.WithCancel(context.Background())
	deps := baseDeps()
	workerStarted := make(chan struct{})
	workerDrained := make(chan struct{})
	releaseWorker := make(chan struct{})
	listenersStopped := make(chan struct{}, 2)
	deps.runners.worker = func(c context.Context, _ worker.Config, ready func(), health func(time.Time)) error {
		close(workerStarted)
		health(time.Now())
		ready()
		<-c.Done()
		<-releaseWorker
		close(workerDrained)
		return nil
	}
	listener := func(c context.Context, ready func(), health func(time.Time)) error {
		ready()
		health(time.Now())
		<-c.Done()
		select {
		case <-workerDrained:
			listenersStopped <- struct{}{}
		default:
			return errors.New("private listener stopped before worker drain")
		}
		return nil
	}
	deps.runners.ingestion = func(c context.Context, _ ingestion.Config, ready func(), health func(time.Time)) error {
		return listener(c, ready, health)
	}
	deps.runners.processing = func(c context.Context, _ processing.Config, ready func()) error {
		return listener(c, ready, func(time.Time) {})
	}
	var addr atomic.Value
	deps.listen = func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err == nil {
			addr.Store(ln.Addr().String())
		}
		return ln, err
	}
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, deps) }()
	select {
	case <-workerStarted:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	cancel()
	waitFor(t, func() bool { return readinessCode(addr.Load().(string)) == http.StatusServiceUnavailable })
	if code := endpointCode(addr.Load().(string), "/healthz"); code != http.StatusOK {
		t.Fatalf("liveness during drain = %d", code)
	}
	close(releaseWorker)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown timed out")
	}
	if len(listenersStopped) != 2 {
		t.Fatalf("stopped listeners after worker drain = %d", len(listenersStopped))
	}
}

func TestShutdownBoundWhenWorkerIgnoresCancellation(t *testing.T) {
	cfg := testConfig()
	cfg.ShutdownTimeout = 80 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	deps := baseDeps()
	started := make(chan struct{})
	release := make(chan struct{})
	runnerDone := make(chan struct{})
	deps.runners.worker = func(_ context.Context, _ worker.Config, ready func(), health func(time.Time)) error {
		close(started)
		health(time.Now())
		ready()
		<-release
		close(runnerDone)
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- run(ctx, cfg, deps) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}
	start := time.Now()
	cancel()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "phase=draining") {
			t.Fatalf("shutdown result = %v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("shutdown exceeded its overall timeout")
	}
	if elapsed := time.Since(start); elapsed > 300*time.Millisecond {
		t.Fatalf("shutdown took %s", elapsed)
	}
	close(release)
	select {
	case <-runnerDone:
	case <-time.After(time.Second):
		t.Fatal("worker goroutine did not finish after release")
	}
}

func TestHealthServerFailureIsFatalWhileRuntimeStarts(t *testing.T) {
	cfg := testConfig()
	deps := baseDeps()
	listener := make(chan net.Listener, 1)
	healthAddr := make(chan string, 1)
	probeStarted := make(chan struct{})
	releaseProbe := make(chan struct{})
	var workers atomic.Int32
	deps.listen = func(network, address string) (net.Listener, error) {
		ln, err := net.Listen(network, address)
		if err == nil {
			listener <- ln
			healthAddr <- ln.Addr().String()
		}
		return ln, err
	}
	deps.runners.worker = func(context.Context, worker.Config, func(), func(time.Time)) error { workers.Add(1); return nil }
	deps.probes.executor = func(context.Context, worker.Config) error { close(probeStarted); <-releaseProbe; return nil }
	done := make(chan error, 1)
	go func() { done <- run(context.Background(), cfg, deps) }()
	var ln net.Listener
	select {
	case ln = <-listener:
	case <-time.After(time.Second):
		t.Fatal("health listener did not open")
	}
	addr := <-healthAddr
	select {
	case <-probeStarted:
	case <-time.After(time.Second):
		t.Fatal("runtime did not reach executor readiness")
	}
	_ = ln.Close()
	waitFor(t, func() bool { return endpointCode(addr, "/healthz") == 0 })
	close(releaseProbe)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "component=health") {
			t.Fatalf("health failure = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("health failure was not monitored")
	}
	if workers.Load() != 0 {
		t.Fatal("worker started after health listener failure")
	}
}

func TestHealthHandlerSeparatesLivenessAndReadiness(t *testing.T) {
	s := newState("build-1")
	h := healthHandler(s)
	request := func(path string) (int, snapshot) {
		r := httptest.NewRecorder()
		h.ServeHTTP(r, httptest.NewRequest(http.MethodGet, path, nil))
		var out snapshot
		_ = json.Unmarshal(r.Body.Bytes(), &out)
		return r.Code, out
	}
	code, out := request("/healthz")
	if code != http.StatusOK || out.BuildID != "build-1" {
		t.Fatalf("healthz = %d %+v", code, out)
	}
	code, _ = request("/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("starting readiness = %d", code)
	}
	s.phase("running")
	for _, name := range []string{"scheduler", "ingestion", "processing", "worker", "execution"} {
		s.set(name, "ready", true)
	}
	s.workerHealthAt = time.Now()
	s.ingestionHealthAt = time.Now()
	s.ingestionAPIHealthy = true
	code, _ = request("/readyz")
	if code != http.StatusOK {
		t.Fatalf("ready readiness = %d", code)
	}
	s.phase("draining")
	code, _ = request("/readyz")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("draining readiness = %d", code)
	}
	if r := httptest.NewRecorder(); func() int { h.ServeHTTP(r, httptest.NewRequest(http.MethodPost, "/readyz", nil)); return r.Code }() != http.StatusMethodNotAllowed {
		t.Fatal("unsupported health method accepted")
	}
}

func TestIngestionProbeRecoversAfterTransientHTTPFailure(t *testing.T) {
	cfg := testConfig()
	cfg.ProbeInterval = 10 * time.Millisecond
	cfg.ProbeTimeout = 20 * time.Millisecond
	s := newState("test")
	s.phase("running")
	s.set("ingestion", "ready", false)
	s.ingestionHealth(time.Now())
	var checks atomic.Int32
	p := probes{ingestion: func(context.Context, ingestion.Config) error {
		if checks.Add(1) == 1 {
			return errors.New("temporary")
		}
		return nil
	}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); probeLoop(ctx, cfg, p, s, make(chan struct{})) }()
	waitFor(t, func() bool { return checks.Load() >= 1 && s.get().Components["ingestion"].State == "unhealthy" })
	waitFor(t, func() bool { return checks.Load() >= 2 && s.get().Components["ingestion"].Healthy })
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("probe loop did not stop")
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition did not become true")
}
func readinessCode(addr string) int {
	return endpointCode(addr, "/readyz")
}
func endpointCode(addr, path string) int {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+path, nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0
	}
	defer resp.Body.Close()
	return resp.StatusCode
}
