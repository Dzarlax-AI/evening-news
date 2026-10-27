package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-background/internal/config"
	execution "github.com/Dzarlax-AI/evening-news/go-execution"
	orchestration "github.com/Dzarlax-AI/evening-news/go-execution/orchestration"
	ingestion "github.com/Dzarlax-AI/evening-news/go-ingestion/runner"
	processing "github.com/Dzarlax-AI/evening-news/go-processing/runner"
	scheduler "github.com/Dzarlax-AI/evening-news/go-scheduler/runner"
	worker "github.com/Dzarlax-AI/evening-news/go-worker/runner"
)

var ErrExecutorAuth = errors.New("executor rejected background credentials")

type runtimeFailure struct {
	phase, component, reason string
	sentinel                 error
}

func (e runtimeFailure) Error() string {
	return fmt.Sprintf("background phase=%s component=%s reason=%s", e.phase, e.component, e.reason)
}
func (e runtimeFailure) Unwrap() error { return e.sentinel }
func failure(phase, component, reason string, sentinel error) error {
	return runtimeFailure{phase: phase, component: component, reason: reason, sentinel: sentinel}
}

type runnerSet struct {
	scheduler  func(context.Context, scheduler.Config, func()) error
	ingestion  func(context.Context, ingestion.Config, func(), func(time.Time)) error
	processing func(context.Context, processing.Config, func()) error
	worker     func(context.Context, worker.Config, func(), func(time.Time)) error
	execution  func(context.Context, config.Config, func(bool)) error
}

type probes struct {
	executor   func(context.Context, worker.Config) error
	scheduler  func(context.Context, string) error
	ingestion  func(context.Context, ingestion.Config) error
	processing func(context.Context, processing.Config) error
	execution  func(context.Context, config.Config) error
}

type dependencies struct {
	runners runnerSet
	probes  probes
	listen  func(string, string) (net.Listener, error)
}

type status struct {
	State   string `json:"state"`
	Healthy bool   `json:"healthy"`
}

type snapshot struct {
	Phase      string            `json:"phase"`
	BuildID    string            `json:"build_id"`
	Components map[string]status `json:"components"`
}

type state struct {
	mu                  sync.RWMutex
	snapshot            snapshot
	workerHealthAt      time.Time
	workerAttempt       uint64
	ingestionHealthAt   time.Time
	ingestionAPIHealthy bool
}

func newState(buildID string) *state {
	return &state{snapshot: snapshot{Phase: "starting", BuildID: buildID, Components: map[string]status{
		"scheduler": {State: "starting"}, "ingestion": {State: "starting"}, "processing": {State: "starting"}, "worker": {State: "starting"}, "execution": {State: "starting"},
	}}}
}
func (s *state) phase(phase string) { s.mu.Lock(); s.snapshot.Phase = phase; s.mu.Unlock() }
func (s *state) set(name, value string, healthy bool) {
	s.mu.Lock()
	s.snapshot.Components[name] = status{State: value, Healthy: healthy}
	if name == "worker" && value != "ready" {
		s.workerHealthAt = time.Time{}
	}
	if name == "ingestion" && value != "ready" {
		s.ingestionHealthAt = time.Time{}
		s.ingestionAPIHealthy = false
	}
	s.mu.Unlock()
}
func (s *state) health(name string, healthy bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.snapshot.Components[name]
	if name == "ingestion" {
		s.ingestionAPIHealthy = healthy
		if old.State == "ready" || old.State == "unhealthy" {
			old.State = "unhealthy"
			old.Healthy = false
			if healthy && fresh(s.ingestionHealthAt, time.Now(), 20*time.Second) {
				old.State = "ready"
				old.Healthy = true
			}
			s.snapshot.Components[name] = old
		}
		return
	}
	if old.State == "ready" || old.State == "unhealthy" {
		old.State = "unhealthy"
		old.Healthy = false
		if healthy {
			old.State = "ready"
			old.Healthy = true
		}
		s.snapshot.Components[name] = old
	}
}
func (s *state) ingestionHealth(at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingestionHealthAt = at.UTC()
	old := s.snapshot.Components["ingestion"]
	if old.State == "ready" || old.State == "unhealthy" {
		old.Healthy = s.ingestionAPIHealthy && fresh(s.ingestionHealthAt, time.Now(), 20*time.Second)
		if old.Healthy {
			old.State = "ready"
		} else {
			old.State = "unhealthy"
		}
		s.snapshot.Components["ingestion"] = old
	}
}

// beginWorkerAttempt invalidates callbacks retained by any previous runner
// invocation and starts this attempt with no inherited lease health.
func (s *state) beginWorkerAttempt() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.workerAttempt++
	s.workerHealthAt = time.Time{}
	s.snapshot.Components["worker"] = status{State: "starting"}
	return s.workerAttempt
}
func (s *state) workerReady(attempt uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if attempt != s.workerAttempt {
		return
	}
	s.snapshot.Components["worker"] = status{State: "ready"}
}
func (s *state) workerHealth(attempt uint64, at time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if attempt != s.workerAttempt {
		return
	}
	s.workerHealthAt = at.UTC()
	old := s.snapshot.Components["worker"]
	if old.State == "ready" || old.State == "unhealthy" {
		old.Healthy = fresh(s.workerHealthAt, time.Now(), 20*time.Second)
		if old.Healthy {
			old.State = "ready"
		} else {
			old.State = "unhealthy"
		}
		s.snapshot.Components["worker"] = old
	}
}
func (s *state) refreshWorker(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old := s.snapshot.Components["worker"]
	if old.State == "ready" || old.State == "unhealthy" {
		old.Healthy = fresh(s.workerHealthAt, now, 20*time.Second)
		if old.Healthy {
			old.State = "ready"
		} else {
			old.State = "unhealthy"
		}
		s.snapshot.Components["worker"] = old
	}
}
func fresh(at, now time.Time, maxAge time.Duration) bool {
	return !at.IsZero() && !at.After(now) && now.Sub(at) <= maxAge
}
func (s *state) get() snapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := snapshot{Phase: s.snapshot.Phase, BuildID: s.snapshot.BuildID, Components: make(map[string]status, len(s.snapshot.Components))}
	for k, v := range s.snapshot.Components {
		out.Components[k] = v
	}
	return out
}
func (s *state) ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.snapshot.Phase != "running" {
		return false
	}
	now := time.Now()
	for _, name := range []string{"worker", "ingestion", "execution"} {
		current := s.snapshot.Components[name]
		if current.State == "ready" || current.State == "unhealthy" {
			current.Healthy = false
			if name == "worker" {
				current.Healthy = fresh(s.workerHealthAt, now, 20*time.Second)
			} else if name == "ingestion" {
				current.Healthy = s.ingestionAPIHealthy && fresh(s.ingestionHealthAt, now, 20*time.Second)
			} else {
				current.Healthy = current.State == "ready"
			}
			if current.Healthy {
				current.State = "ready"
			} else {
				current.State = "unhealthy"
			}
			s.snapshot.Components[name] = current
		}
	}
	for _, v := range s.snapshot.Components {
		if !v.Healthy {
			return false
		}
	}
	return true
}

func productionDependencies() dependencies {
	return dependencies{
		runners: runnerSet{scheduler: scheduler.Run, ingestion: ingestion.RunWithHealth, processing: processing.Run, worker: worker.RunWithHealth, execution: runExecution},
		probes:  probes{executor: executorReady, scheduler: schedulerReady, ingestion: ingestionReady, processing: processingReady, execution: executionReady},
		listen:  net.Listen,
	}
}

func runExecution(ctx context.Context, cfg config.Config, onReady func(bool)) error {
	server, err := execution.New(cfg.Execution, execution.Handlers{})
	if err != nil {
		return errors.New("execution runner configuration is invalid")
	}
	err = server.Open(ctx)
	if err != nil {
		return errors.New("execution runner could not acquire its database guard")
	}
	closed := false
	defer func() {
		if !closed {
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = server.Close(closeCtx)
		}
	}()
	domain, err := orchestration.NewDomainClient(orchestration.DomainConfig{BaseURL: cfg.DomainServiceURL, Token: cfg.Execution.Token})
	if err != nil {
		return errors.New("domain service configuration is invalid")
	}
	delay := cfg.BackoffMin
	for {
		probeCtx, cancel := context.WithTimeout(ctx, cfg.ProbeTimeout)
		err = domain.Ready(probeCtx)
		cancel()
		if err == nil {
			break
		}
		if errors.Is(err, orchestration.ErrDomainAuth) {
			return failure("waiting_domain", "executor", "authorization rejected", ErrExecutorAuth)
		}
		if err := pause(ctx, delay); err != nil {
			return nil
		}
		delay = nextDelay(delay, cfg.BackoffMax)
	}
	pipeline, err := orchestration.NewPipeline(orchestration.Config{
		Domain:     domainConfig(cfg),
		Ingestion:  orchestration.ServiceConfig{URL: localServiceURL(cfg.Ingestion.ListenAddr), Token: cfg.Ingestion.Token},
		Processing: orchestration.ServiceConfig{URL: localServiceURL(cfg.Processing.ListenAddr), Token: cfg.Processing.Token},
	})
	if err != nil {
		return errors.New("execution pipeline configuration is invalid")
	}
	legacy, err := orchestration.NewLegacyAdapter(server.DatabasePool(), domain, server.GuardBackendPID, server.InstanceID)
	if err != nil {
		return errors.New("execution lifecycle configuration is invalid")
	}
	server.SetHandlers(execution.Handlers{Pipeline: pipeline, Finalizer: legacy, Recovery: legacy, Ready: pipeline.Ready})
	listener, err := net.Listen("tcp", cfg.ExecutionListenAddr)
	if err != nil {
		return errors.New("execution listener could not bind")
	}
	httpServer := &http.Server{Handler: server.Handler(), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	serveDone := make(chan error, 1)
	go func() {
		e := httpServer.Serve(listener)
		if errors.Is(e, http.ErrServerClosed) {
			e = nil
		}
		serveDone <- e
	}()
	monitorCtx, stopMonitor := context.WithCancel(ctx)
	defer stopMonitor()
	monitorDone := make(chan struct{})
	go func() {
		defer close(monitorDone)
		server.MonitorGuard(monitorCtx, func(ok bool) {
			if onReady != nil {
				onReady(ok)
			}
		})
	}()
	if onReady != nil {
		onReady(true)
	}
	var runErr error
	select {
	case <-ctx.Done():
	case <-monitorDone:
		if ctx.Err() == nil {
			runErr = execution.ErrNotReady
		}
	case err := <-serveDone:
		if ctx.Err() == nil {
			runErr = err
		}
	}
	stopMonitor()
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	select {
	case <-monitorDone:
	case <-shutdownCtx.Done():
		if runErr == nil && ctx.Err() == nil {
			runErr = errors.New("execution guard monitor did not stop within bound")
		}
	}
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		_ = httpServer.Close()
		if runErr == nil && ctx.Err() == nil {
			runErr = errors.New("execution listener shutdown exceeded bound")
		}
	}
	closeErr := server.Close(shutdownCtx)
	cancelShutdown()
	closed = true
	if runErr == nil && closeErr != nil && ctx.Err() == nil {
		runErr = errors.New("execution runner shutdown failed")
	}
	return runErr
}

func domainConfig(cfg config.Config) orchestration.DomainConfig {
	return orchestration.DomainConfig{BaseURL: cfg.DomainServiceURL, Token: cfg.Execution.Token}
}

func localServiceURL(addr string) string {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return ""
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return (&url.URL{Scheme: "http", Host: net.JoinHostPort(host, port)}).String()
}

// Run starts the five internal components under a single process lifecycle.
// The caller owns ctx and should provide the process signal handler.
func Run(ctx context.Context, cfg config.Config) error {
	return run(ctx, cfg, productionDependencies())
}

func run(ctx context.Context, cfg config.Config, deps dependencies) error {
	if ctx == nil {
		return errors.New("background context is required")
	}
	if err := validateDeps(deps); err != nil {
		return failure("starting", "runtime", "dependencies incomplete", nil)
	}
	if ctx.Err() != nil {
		return nil
	}

	state := newState(cfg.BuildID)
	healthLn, err := deps.listen("tcp", cfg.HealthAddr)
	if err != nil {
		return failure("starting", "health", "listener could not bind", nil)
	}
	healthServer := &http.Server{Handler: healthHandler(state), ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second, IdleTimeout: 15 * time.Second, MaxHeaderBytes: 8 << 10}
	healthDone := make(chan error, 1)
	fatal := make(chan error, 4)
	var shuttingDown atomic.Bool
	go func() {
		e := healthServer.Serve(healthLn)
		if errors.Is(e, http.ErrServerClosed) {
			e = nil
		}
		healthDone <- e
		if !shuttingDown.Load() {
			fatal <- failure(state.get().Phase, "health", "listener stopped unexpectedly", nil)
		}
	}()

	// Component lifetimes are deliberately detached from the signal context.
	// The coordinator observes the signal and stops the worker/scheduler first,
	// then lets ingestion and processing drain before cancelling their contexts.
	serviceCtx, cancelServices := context.WithCancel(context.Background())
	coreCtx, cancelCore := context.WithCancel(serviceCtx)
	listenerCtx, cancelListeners := context.WithCancel(serviceCtx)
	executionCtx, cancelExecution := context.WithCancel(serviceCtx)
	defer cancelServices()
	defer cancelCore()
	defer cancelListeners()
	defer cancelExecution()
	readyEvents := make(chan string, 5)
	probeKick := make(chan struct{}, 1)
	probeDone := make(chan struct{})
	componentDone := map[string]<-chan struct{}{}
	start := func(name string, fn func(context.Context, func()) error) {
		done := make(chan struct{})
		componentDone[name] = done
		go func() {
			componentCtx := selectCtx(name, coreCtx, listenerCtx)
			if name == "execution" {
				componentCtx = executionCtx
			}
			var once sync.Once
			onReady := func() {
				once.Do(func() {
					state.set(name, "ready", false)
					readyEvents <- name
					select {
					case probeKick <- struct{}{}:
					default:
					}
				})
			}
			e := fn(componentCtx, onReady)
			if componentCtx.Err() == nil || (e != nil && !errors.Is(e, context.Canceled)) {
				fatal <- unexpectedExit(componentExit{name: name, err: e}, state)
			}
			close(done)
		}()
	}
	start("scheduler", func(c context.Context, r func()) error { return deps.runners.scheduler(c, cfg.Scheduler, r) })
	start("ingestion", func(c context.Context, r func()) error {
		return deps.runners.ingestion(c, cfg.Ingestion, r, func(at time.Time) {
			state.ingestionHealth(at)
			select {
			case probeKick <- struct{}{}:
			default:
			}
		})
	})
	start("processing", func(c context.Context, r func()) error { return deps.runners.processing(c, cfg.Processing, r) })
	executionDone := make(chan struct{})
	componentDone["execution"] = executionDone
	go func() {
		var once sync.Once
		e := deps.runners.execution(executionCtx, cfg, func(healthy bool) {
			if healthy {
				once.Do(func() {
					state.set("execution", "ready", false)
					readyEvents <- "execution"
					select {
					case probeKick <- struct{}{}:
					default:
					}
				})
				return
			}
			state.health("execution", false)
		})
		if executionCtx.Err() == nil || (e != nil && !errors.Is(e, context.Canceled)) {
			if errors.Is(e, ErrExecutorAuth) {
				fatal <- e
			} else {
				fatal <- unexpectedExit(componentExit{name: "execution", err: e}, state)
			}
		}
		close(executionDone)
	}()
	go func() { defer close(probeDone); probeLoop(serviceCtx, cfg, deps.probes, state, probeKick) }()

	var runErr error
	readyCount := 0
	for readyCount < 4 && runErr == nil {
		select {
		case <-ctx.Done():
			state.phase("draining")
		case err := <-fatal:
			runErr = err
		case name := <-readyEvents:
			if name != "worker" {
				readyCount++
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	if runErr == nil && ctx.Err() == nil {
		if err := waitComponentHealth(ctx, state, fatal, cfg.ProbeInterval); err != nil && !errors.Is(err, context.Canceled) {
			runErr = err
		}
	}
	if runErr == nil && ctx.Err() == nil {
		state.phase("waiting_executor")
		if err := waitExecutor(ctx, cfg, deps.probes.executor, cfg.BackoffMin, cfg.BackoffMax, fatal); err != nil {
			if !errors.Is(err, context.Canceled) {
				if errors.Is(err, ErrExecutorAuth) {
					runErr = failure("waiting_executor", "executor", "authorization rejected", ErrExecutorAuth)
				} else {
					runErr = err
				}
			}
		}
	}

	workerDone := make(chan error, 1)
	workerStarted := false
	workerFinished := false
	if runErr == nil && ctx.Err() == nil {
		state.phase("running")
		workerStarted = true
		go func() {
			workerDone <- workerLoop(coreCtx, cfg, deps.runners.worker, state, probeKick, cfg.BackoffMin, cfg.BackoffMax)
		}()
	}

	if runErr == nil && ctx.Err() == nil {
		for {
			select {
			case <-ctx.Done():
				state.phase("draining")
				goto shutdown
			case err := <-workerDone:
				workerFinished = true
				if err != nil {
					runErr = err
				} else if ctx.Err() == nil {
					runErr = failure("running", "worker", "stopped unexpectedly", nil)
				}
				goto shutdown
			case err := <-fatal:
				runErr = err
				goto shutdown
			}
		}
	}

shutdown:
	state.phase("draining")
	shuttingDown.Store(true)
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	// Stop claims and the worker before removing its private API dependencies.
	cancelCore()
	if workerStarted && !workerFinished {
		if err := waitWorker(shutdownCtx, workerDone); err != nil && runErr == nil {
			runErr = err
		}
	}
	if err := waitDoneSet(shutdownCtx, []<-chan struct{}{componentDone["scheduler"]}); err != nil && runErr == nil {
		runErr = failure("draining", "scheduler", "shutdown exceeded bound", nil)
	}
	// The execution service drains while ingestion and processing remain available.
	cancelExecution()
	if err := waitDoneSet(shutdownCtx, []<-chan struct{}{componentDone["execution"]}); err != nil && runErr == nil {
		runErr = failure("draining", "execution", "shutdown exceeded bound", nil)
	}
	// Ingestion and processing continue serving until execution has drained.
	cancelListeners()
	if err := waitDoneSet(shutdownCtx, []<-chan struct{}{componentDone["ingestion"], componentDone["processing"]}); err != nil && runErr == nil {
		runErr = failure("draining", "listeners", "shutdown exceeded bound", nil)
	}
	cancelServices()
	select {
	case <-probeDone:
	case <-shutdownCtx.Done():
		if runErr == nil {
			runErr = failure("draining", "health", "probes did not stop within bound", nil)
		}
	}
	if runErr == nil {
		select {
		case err := <-fatal:
			runErr = err
		default:
		}
	}
	state.set("scheduler", "stopped", false)
	state.set("worker", "stopped", false)
	state.set("ingestion", "stopped", false)
	state.set("processing", "stopped", false)
	state.set("execution", "stopped", false)
	if err := healthServer.Shutdown(shutdownCtx); err != nil {
		_ = healthServer.Close()
		if runErr == nil {
			runErr = failure("draining", "health", "shutdown exceeded bound", nil)
		}
	}
	select {
	case err := <-healthDone:
		if err != nil && runErr == nil {
			runErr = failure("draining", "health", "listener did not stop cleanly", nil)
		}
	case <-shutdownCtx.Done():
		if runErr == nil {
			runErr = failure("draining", "runtime", "shutdown exceeded bound", nil)
		}
	}
	state.phase("stopped")
	return runErr
}

type componentExit struct {
	name string
	err  error
}

func unexpectedExit(x componentExit, s *state) error {
	reason := "stopped unexpectedly"
	if x.err != nil {
		reason = "returned an error"
	}
	return failure(s.get().Phase, x.name, reason, nil)
}
func selectCtx(name string, core, listener context.Context) context.Context {
	if name == "scheduler" {
		return core
	}
	return listener
}

func waitExecutor(ctx context.Context, cfg config.Config, probe func(context.Context, worker.Config) error, min, max time.Duration, fatal <-chan error) error {
	delay := min
	for {
		checkCtx, cancel := context.WithTimeout(ctx, cfg.ProbeTimeout)
		err := probe(checkCtx, cfg.Worker)
		cancel()
		if err == nil {
			select {
			case fatalErr := <-fatal:
				return fatalErr
			default:
				return nil
			}
		}
		if errors.Is(err, ErrExecutorAuth) {
			return err
		}
		t := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			t.Stop()
			return ctx.Err()
		case err := <-fatal:
			t.Stop()
			return err
		case <-t.C:
		}
		delay = nextDelay(delay, max)
	}
}

func waitComponentHealth(ctx context.Context, s *state, fatal <-chan error, interval time.Duration) error {
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		x := s.get()
		if x.Components["scheduler"].Healthy && x.Components["ingestion"].Healthy && x.Components["processing"].Healthy && x.Components["execution"].Healthy {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-fatal:
			return err
		case <-t.C:
		}
	}
}
func (s *state) invalidateWorkerAttempt(attempt uint64, nextState string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if attempt != s.workerAttempt {
		return
	}
	// Advancing the epoch makes delayed callbacks from this runner stale even
	// before the next attempt has begun.
	s.workerAttempt++
	s.workerHealthAt = time.Time{}
	s.snapshot.Components["worker"] = status{State: nextState}
}

func workerLoop(ctx context.Context, cfg config.Config, run func(context.Context, worker.Config, func(), func(time.Time)) error, state *state, kick chan<- struct{}, min, max time.Duration) error {
	delay := min
	for ctx.Err() == nil {
		attempt := state.beginWorkerAttempt()
		readyCh := make(chan struct{}, 1)
		var readySignal <-chan struct{} = readyCh
		done := make(chan error, 1)
		go func() {
			var once sync.Once
			done <- run(ctx, cfg.Worker,
				func() {
					once.Do(func() {
						state.workerReady(attempt)
						readyCh <- struct{}{}
						select {
						case kick <- struct{}{}:
						default:
						}
					})
				},
				func(at time.Time) {
					if ctx.Err() == nil {
						state.workerHealth(attempt, at)
					}
					select {
					case kick <- struct{}{}:
					default:
					}
				},
			)
		}()
		for {
			select {
			case <-ctx.Done():
				select {
				case err := <-done:
					state.invalidateWorkerAttempt(attempt, "stopped")
					if err != nil && !errors.Is(err, context.Canceled) {
						return workerFailure(err)
					}
					return nil
				}
			case <-readySignal:
				// The worker's callback follows its lease acquisition handshake.
				readySignal = nil
			case err := <-done:
				if ctx.Err() != nil {
					state.invalidateWorkerAttempt(attempt, "stopped")
					return nil
				}
				if !recoverableLease(err) {
					state.invalidateWorkerAttempt(attempt, "stopped")
					return workerFailure(err)
				}
				state.invalidateWorkerAttempt(attempt, "recovering")
				if err := pause(ctx, delay); err != nil {
					return nil
				}
				delay = nextDelay(delay, max)
				goto restart
			}
		}
	restart:
	}
	return nil
}

func recoverableLease(err error) bool {
	return errors.Is(err, worker.ErrLeaseLost) || errors.Is(err, worker.ErrStoreLeaseLost) || errors.Is(err, worker.ErrLeaseHeld)
}
func workerFailure(err error) error {
	if err == nil {
		return failure("running", "worker", "stopped unexpectedly", nil)
	}
	if errors.Is(err, worker.ErrExecutorAuth) {
		return failure("running", "worker", "executor authorization rejected", ErrExecutorAuth)
	}
	var httpErr *worker.ExecutorHTTPError
	if errors.As(err, &httpErr) && (httpErr.Status == http.StatusUnauthorized || httpErr.Status == http.StatusForbidden) {
		return failure("running", "worker", "executor authorization rejected", ErrExecutorAuth)
	}
	return failure("running", "worker", "execution failed", nil)
}
func pause(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
func nextDelay(current, max time.Duration) time.Duration {
	if current <= 0 {
		return max
	}
	if current >= max/2 {
		return max
	}
	return current * 2
}
func waitWorker(ctx context.Context, done <-chan error) error {
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			return workerFailure(err)
		}
		return nil
	case <-ctx.Done():
		return failure("draining", "worker", "shutdown exceeded bound", nil)
	}
}
func waitDoneSet(ctx context.Context, done []<-chan struct{}) error {
	for _, ch := range done {
		select {
		case <-ch:
		case <-ctx.Done():
			return errors.New("component shutdown exceeded bound")
		}
	}
	return nil
}

func probeLoop(ctx context.Context, cfg config.Config, p probes, s *state, kick <-chan struct{}) {
	t := time.NewTicker(cfg.ProbeInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-kick:
		case <-t.C:
		}
		checks := []struct {
			name string
			fn   func(context.Context) error
		}{
			{"scheduler", func(c context.Context) error { return p.scheduler(c, cfg.Scheduler.ListenAddr) }},
			{"ingestion", func(c context.Context) error { return p.ingestion(c, cfg.Ingestion) }},
			{"processing", func(c context.Context) error { return p.processing(c, cfg.Processing) }},
			{"execution", func(c context.Context) error { return p.execution(c, cfg) }},
		}
		type result struct {
			name string
			err  error
		}
		results := make(chan result, len(checks))
		pending := 0
		for _, item := range checks {
			componentState := s.get().Components[item.name].State
			if componentState == "ready" || componentState == "unhealthy" {
				pending++
				go func(name string, fn func(context.Context) error) {
					c, cancel := context.WithTimeout(ctx, cfg.ProbeTimeout)
					defer cancel()
					results <- result{name: name, err: fn(c)}
				}(item.name, item.fn)
			}
		}
		for i := 0; i < pending; i++ {
			select {
			case <-ctx.Done():
				return
			case r := <-results:
				s.health(r.name, r.err == nil)
			}
		}
		s.refreshWorker(time.Now())
	}
}

func validateDeps(d dependencies) error {
	if d.listen == nil || d.runners.scheduler == nil || d.runners.ingestion == nil || d.runners.processing == nil || d.runners.worker == nil || d.runners.execution == nil || d.probes.executor == nil || d.probes.scheduler == nil || d.probes.ingestion == nil || d.probes.processing == nil || d.probes.execution == nil {
		return errors.New("background runtime dependencies are incomplete")
	}
	return nil
}

func healthHandler(s *state) http.Handler {
	mux := http.NewServeMux()
	write := func(w http.ResponseWriter, code int) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(code)
		_ = json.NewEncoder(w).Encode(s.get())
	}
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		write(w, http.StatusOK)
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		code := http.StatusOK
		if !s.ready() {
			code = http.StatusServiceUnavailable
		}
		write(w, code)
	})
	return mux
}

func executorReady(ctx context.Context, cfg worker.Config) error {
	u, err := url.Parse(strings.TrimRight(cfg.ExecutorURL, "/") + "/internal/v1/status")
	if err != nil {
		return errors.New("executor unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return errors.New("executor unavailable")
	}
	req.Header.Set("Authorization", "Bearer "+cfg.ExecutorToken)
	client := http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("executor unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return ErrExecutorAuth
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("executor unavailable")
	}
	var status struct {
		InstanceID string `json:"executor_instance_id"`
		Ready      bool   `json:"ready"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 8192)).Decode(&status) != nil || status.InstanceID == "" || !status.Ready {
		return errors.New("executor unavailable")
	}
	return nil
}

func executionReady(ctx context.Context, cfg config.Config) error {
	baseURL := localServiceURL(cfg.ExecutionListenAddr)
	if baseURL == "" {
		return errors.New("execution service unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/internal/v1/status", nil)
	if err != nil {
		return errors.New("execution service unavailable")
	}
	req.Header.Set("Authorization", "Bearer "+cfg.Execution.Token)
	client := http.Client{Timeout: 2 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("execution service unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return ErrExecutorAuth
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("execution service unavailable")
	}
	var wire struct {
		InstanceID string `json:"executor_instance_id"`
		Active     int    `json:"active_executions"`
		Ready      bool   `json:"ready"`
	}
	dec := json.NewDecoder(io.LimitReader(resp.Body, 8192))
	dec.DisallowUnknownFields()
	if dec.Decode(&wire) != nil || wire.InstanceID == "" || wire.Active < 0 || !wire.Ready {
		return errors.New("execution service unavailable")
	}
	return nil
}
func schedulerReady(ctx context.Context, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.New("scheduler unavailable")
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	return scheduler.Healthcheck(ctx, net.JoinHostPort(host, port))
}
func ingestionReady(ctx context.Context, c ingestion.Config) error {
	return getHealth(ctx, c.ListenAddr, "/healthz")
}
func processingReady(ctx context.Context, c processing.Config) error {
	return getHealth(ctx, c.ListenAddr, "/healthz")
}
func getHealth(ctx context.Context, addr, path string) error {
	baseURL := localServiceURL(addr)
	if baseURL == "" {
		return errors.New("component unavailable")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, nil)
	if err != nil {
		return errors.New("component unavailable")
	}
	client := http.Client{Timeout: 2 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("component unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("component unavailable")
	}
	return nil
}
