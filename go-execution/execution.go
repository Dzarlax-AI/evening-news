// Package execution owns durable job-attempt admission, execution receipts, and
// fencing for the embedded execution runtime.
package execution

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"net/http"
	"sync"
	"time"
)

const (
	MaxEnvelopeBytes = 8192
	MaxResultBytes   = 4096
	MaxDeadline      = 24 * time.Hour
	HeartbeatStale   = 20 * time.Second
	// Python executor guard uses XOR 1145132609 and the worker uses XOR 0.
	// This stable distinct value scopes this session guard to the Go runtime.
	ExecutionGuardXOR = 0x454e4732
)

var (
	ErrInvalidConfig = errors.New("execution configuration is invalid")
	ErrLeaseLost     = errors.New("worker lease is no longer current")
	ErrNotReady      = errors.New("execution runner is not ready")
	ErrStaleAttempt  = errors.New("job attempt is no longer current")
)

type Config struct {
	DatabaseURL   string
	Schema        string // Optional PostgreSQL schema/search_path, mainly for isolated test/runtime databases.
	Token         string
	InstanceID    string // Optional canonical UUID; generated when empty.
	MaxConns      int32
	QueryTimeout  time.Duration
	GuardInterval time.Duration
	Now           func() time.Time // Optional application clock, useful for deterministic tests.
}

// ExecutionContext is the immutable authority passed to domain and lifecycle
// handlers. BackendPID is read from the locked worker status row, never from the
// worker's six-field envelope.
type ExecutionContext struct {
	Version            int    `json:"version"`
	JobID              string `json:"job_id"`
	AttemptGeneration  int64  `json:"attempt_generation"`
	OwnerID            string `json:"owner_id"`
	Generation         int64  `json:"generation"`
	BackendPID         int32  `json:"backend_pid"`
	ExecutorInstanceID string `json:"executor_instance_id"`
	DeadlineAt         string `json:"deadline_at"`
	ExecutorBackendPID int32  `json:"-"`
}

type Result struct{ Value json.RawMessage }

type Pipeline interface {
	Execute(context.Context, ExecutionContext, string, json.RawMessage) (Result, error)
}
type PipelineFunc func(context.Context, ExecutionContext, string, json.RawMessage) (Result, error)

func (f PipelineFunc) Execute(c context.Context, x ExecutionContext, k string, p json.RawMessage) (Result, error) {
	return f(c, x, k, p)
}

type Finalizer interface {
	Finalize(context.Context, ExecutionContext) (json.RawMessage, error)
}
type FinalizerFunc func(context.Context, ExecutionContext) (json.RawMessage, error)

func (f FinalizerFunc) Finalize(c context.Context, x ExecutionContext) (json.RawMessage, error) {
	return f(c, x)
}

type Recovery interface {
	Recover(context.Context, string, int64, int32) (json.RawMessage, error)
}
type RecoveryFunc func(context.Context, string, int64, int32) (json.RawMessage, error)

func (f RecoveryFunc) Recover(c context.Context, o string, g int64, p int32) (json.RawMessage, error) {
	return f(c, o, g, p)
}

type Handlers struct {
	Pipeline  Pipeline
	Finalizer Finalizer
	Recovery  Recovery
	Ready     func(context.Context) error
}

type Server struct {
	cfg        Config
	pool       *pgxpool.Pool
	guard      *pgx.Conn
	guardPID   int32
	guardMu    sync.Mutex
	instance   uuid.UUID
	handlers   Handlers
	mu         sync.Mutex
	active     map[attemptKey]context.CancelFunc
	admission  sync.Mutex
	wg         sync.WaitGroup
	ready      bool
	closed     bool
	guardLost  bool
	cancelRoot context.CancelFunc
	root       context.Context
	// beforeSetRunning is an internal deterministic test barrier for the
	// acceptance-to-start deadline boundary. Production leaves it nil.
	beforeSetRunning func()
}
type attemptKey struct {
	job     uuid.UUID
	attempt int64
}

func New(cfg Config, h Handlers) (*Server, error) {
	if cfg.DatabaseURL == "" {
		return nil, ErrInvalidConfig
	}
	if cfg.MaxConns == 0 {
		cfg.MaxConns = 2
	}
	if cfg.MaxConns < 1 {
		return nil, ErrInvalidConfig
	}
	if cfg.QueryTimeout == 0 {
		cfg.QueryTimeout = 2 * time.Second
	}
	if cfg.QueryTimeout <= 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.GuardInterval == 0 {
		cfg.GuardInterval = 2 * time.Second
	}
	if cfg.GuardInterval <= 0 {
		return nil, ErrInvalidConfig
	}
	if cfg.Now == nil {
		cfg.Now = time.Now
	}
	var instance uuid.UUID
	var err error
	if cfg.InstanceID == "" {
		instance, err = uuid.NewRandom()
	} else {
		instance, err = uuid.Parse(cfg.InstanceID)
		if err == nil && instance.String() != cfg.InstanceID {
			err = errors.New("noncanonical UUID")
		}
	}
	if err != nil {
		return nil, ErrInvalidConfig
	}
	return &Server{cfg: cfg, instance: instance, handlers: h, active: map[attemptKey]context.CancelFunc{}}, nil
}

func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/v1/status", s.status)
	mux.HandleFunc("/internal/v1/job-executions/", s.executions)
	mux.HandleFunc("/internal/v1/job-executions", s.accept)
	mux.HandleFunc("/internal/v1/worker-events", s.workerEvents)
	return mux
}

// Open initializes the dedicated pool and advisory-guard connection. Call it
// before Handler in embedded runtimes; Run is the standalone lifecycle helper.
func (s *Server) Open(ctx context.Context) error {
	if s == nil || ctx == nil {
		return ErrInvalidConfig
	}
	initCtx, cancelInit := context.WithTimeout(ctx, 10*time.Second)
	err := s.open(initCtx)
	cancelInit()
	if err != nil {
		return err
	}
	root, cancel := context.WithCancel(ctx)
	s.mu.Lock()
	s.root, s.cancelRoot, s.ready = root, cancel, true
	s.mu.Unlock()
	return nil
}

// SetHandlers installs the domain/lifecycle adapters after Open has exposed the
// runner's own pool to a composed runtime.
func (s *Server) SetHandlers(h Handlers) {
	s.mu.Lock()
	s.handlers = h
	s.mu.Unlock()
}

// DatabasePool returns the runner's dedicated execution pool for tightly
// scoped adapters. Callers must not keep transactions open across network work.
func (s *Server) DatabasePool() *pgxpool.Pool { return s.pool }

// InstanceID returns the process-scoped execution receipt identity.
func (s *Server) InstanceID() string { return s.instance.String() }

// GuardBackendPID is the physical PostgreSQL PID holding this runner's
// session advisory lock; it is intentionally not part of ExecutionContext JSON.
func (s *Server) GuardBackendPID(ctx context.Context) (int32, error) {
	s.mu.Lock()
	ready, pid := s.ready && !s.closed, s.guardPID
	s.mu.Unlock()
	if s.guard == nil || !ready || pid <= 0 {
		return 0, ErrNotReady
	}
	return pid, nil
}

// MonitorGuard checks the dedicated session lock until ctx ends and cancels
// active attempts if the lock is lost.
func (s *Server) MonitorGuard(ctx context.Context, readiness func(bool)) {
	done := make(chan struct{})
	s.watch(ctx, done, readiness)
	<-done
}

// Run acquires the isolated database guard, reports readiness, monitors the
// dedicated lock connection, and shuts down active pipelines when ownership is lost.
func Run(ctx context.Context, cfg Config, h Handlers, readiness func(bool)) error {
	s, err := New(cfg, h)
	if err != nil {
		return err
	}
	if err = s.Open(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	root := s.root
	s.mu.Unlock()
	if readiness != nil {
		readiness(true)
	}
	done := make(chan struct{})
	go s.watch(root, done, readiness)
	<-root.Done()
	<-done
	s.admission.Lock()
	s.mu.Lock()
	s.ready = false
	for _, stop := range s.active {
		stop()
	}
	s.mu.Unlock()
	s.admission.Unlock()
	closeCtx, cc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cc()
	if err := s.Close(closeCtx); err != nil {
		return err
	}
	s.mu.Lock()
	guardLost := s.guardLost
	s.mu.Unlock()
	if guardLost && ctx.Err() == nil {
		return ErrNotReady
	}
	return nil
}

func (s *Server) open(ctx context.Context) error {
	pc, err := pgxpool.ParseConfig(s.cfg.DatabaseURL)
	if err != nil {
		return ErrInvalidConfig
	}
	pc.MaxConns = s.cfg.MaxConns
	pc.MinConns = 1
	if s.cfg.Schema != "" {
		pc.ConnConfig.RuntimeParams["search_path"] = s.cfg.Schema
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return errors.New("execution database unavailable")
	}
	s.pool = pool
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return errors.New("execution database unavailable")
	}
	connCfg := pc.ConnConfig.Copy()
	guard, err := pgx.ConnectConfig(ctx, connCfg)
	if err != nil {
		pool.Close()
		return errors.New("execution guard connection unavailable")
	}
	var acquired bool
	qctx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()
	err = guard.QueryRow(qctx, `SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(current_schema()) # $1)`, ExecutionGuardXOR).Scan(&acquired)
	if err != nil || !acquired {
		closeConn(guard)
		pool.Close()
		if err != nil {
			return errors.New("execution guard unavailable")
		}
		return errors.New("another executor owns this database")
	}
	s.guard = guard
	if err = guard.QueryRow(qctx, `SELECT pg_backend_pid()`).Scan(&s.guardPID); err != nil || s.guardPID <= 0 {
		closeConn(guard)
		pool.Close()
		return errors.New("execution guard identity unavailable")
	}
	if err = s.checkSchema(ctx); err != nil {
		s.Close(context.Background())
		return err
	}
	return nil
}

func (s *Server) checkSchema(ctx context.Context) error {
	qctx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
	defer cancel()
	var ok bool
	err := s.pool.QueryRow(qctx, `SELECT to_regclass('background_jobs') IS NOT NULL AND to_regclass('background_worker_status') IS NOT NULL AND to_regclass('background_job_executions') IS NOT NULL`).Scan(&ok)
	if err != nil || !ok {
		return errors.New("execution schema is unavailable")
	}
	_, err = s.pool.Exec(qctx, `SELECT id,kind,payload,result,state,owner_generation,attempt_generation FROM background_jobs LIMIT 0`)
	if err != nil {
		return errors.New("execution job schema is incompatible")
	}
	_, err = s.pool.Exec(qctx, `SELECT id,generation,owner_id,lock_backend_pid,heartbeat_at,status FROM background_worker_status LIMIT 0`)
	if err != nil {
		return errors.New("execution worker schema is incompatible")
	}
	_, err = s.pool.Exec(qctx, `SELECT job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state,accepted_at,started_at,finished_at,result,error_code,legacy_completion_applied_at FROM background_job_executions LIMIT 0`)
	if err != nil {
		return errors.New("execution receipt schema is incompatible")
	}
	return nil
}

func (s *Server) watch(ctx context.Context, done chan<- struct{}, readiness func(bool)) {
	defer close(done)
	ticker := time.NewTicker(s.cfg.GuardInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			qctx, cancel := context.WithTimeout(ctx, s.cfg.QueryTimeout)
			var held bool
			s.guardMu.Lock()
			err := s.guard.QueryRow(qctx, `SELECT EXISTS(SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=pg_backend_pid() AND objsubid=2 AND classid=(((hashtext(current_database())::bigint+4294967296)%4294967296)::oid) AND objid=((((hashtext(current_schema())::bigint # $1)+4294967296)%4294967296)::oid) AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, ExecutionGuardXOR).Scan(&held)
			s.guardMu.Unlock()
			cancel()
			if err != nil || !held {
				s.mu.Lock()
				s.ready = false
				s.guardLost = true
				for _, stop := range s.active {
					stop()
				}
				cancelRoot := s.cancelRoot
				s.mu.Unlock()
				if readiness != nil {
					readiness(false)
				}
				if cancelRoot != nil {
					cancelRoot()
				}
				return
			}
		}
	}
}

func (s *Server) Close(ctx context.Context) error {
	s.admission.Lock()
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		s.admission.Unlock()
		return nil
	}
	s.closed = true
	s.ready = false
	for _, stop := range s.active {
		stop()
	}
	if s.cancelRoot != nil {
		s.cancelRoot()
	}
	guard := s.guard
	pool := s.pool
	s.mu.Unlock()
	s.admission.Unlock()
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		// Closing the dedicated session releases the advisory guard immediately.
		// Any cancellation-resistant handler that returns later fails its next
		// same-transaction guard fence before it can persist a terminal result.
		if guard != nil {
			s.guardMu.Lock()
			qctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			_ = guard.Close(qctx)
			cancel()
			s.guardMu.Unlock()
		}
		if pool != nil {
			go func() {
				<-done
				pool.Close()
			}()
		}
		return errors.New("execution shutdown exceeded bound")
	}
	var err error
	if guard != nil {
		s.guardMu.Lock()
		qctx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var unlocked bool
		e := guard.QueryRow(qctx, `SELECT pg_advisory_unlock(hashtext(current_database()), hashtext(current_schema()) # $1)`, ExecutionGuardXOR).Scan(&unlocked)
		cancel()
		if e != nil || !unlocked {
			_ = guard.Close(context.Background())
			err = errors.New("execution guard release failed")
		} else {
			closeConn(guard)
		}
		s.guardMu.Unlock()
	}
	if pool != nil {
		pool.Close()
	}
	return err
}
