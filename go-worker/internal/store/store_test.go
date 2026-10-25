package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testStore(t *testing.T) (*Store, context.Context, func()) {
	return testStoreWithProxy(t, nil)
}

func testStoreWithProxy(t *testing.T, proxy *commitReplyProxy) (*Store, context.Context, func()) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL lease tests")
	}
	parsed, err := url.Parse(dsn)
	if err != nil || (parsed.Hostname() != "127.0.0.1" && parsed.Hostname() != "localhost" && parsed.Hostname() != "::1") {
		t.Fatal("PostgreSQL lease tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		cancel()
		t.Fatalf("connect test database: %v", err)
	}
	schema := fmt.Sprintf("go_worker_%d", time.Now().UnixNano())
	if _, err := admin.Exec(ctx, `CREATE SCHEMA `+pgx.Identifier{schema}.Sanitize()); err != nil {
		closeConn(admin)
		cancel()
		t.Fatalf("create test schema: %v", err)
	}
	poolConfig, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test database URL: %v", err)
	}
	poolConfig.MaxConns = 4
	poolConfig.ConnConfig.RuntimeParams["search_path"] = schema
	if proxy != nil {
		poolConfig.ConnConfig.Host = "127.0.0.1"
		poolConfig.ConnConfig.Port = proxy.port
		poolConfig.ConnConfig.TLSConfig = nil
		poolConfig.ConnConfig.Fallbacks = nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		t.Fatalf("create isolated test pool: %v", err)
	}
	ddl := []string{
		`CREATE TABLE background_worker_status (
			id integer PRIMARY KEY, generation bigint NOT NULL DEFAULT 0,
			owner_id uuid, lock_backend_pid integer, heartbeat_at timestamp,
			status text NOT NULL DEFAULT 'stopped')`,
		`INSERT INTO background_worker_status (id) VALUES (1)`,
		`CREATE TABLE background_jobs (
			id uuid PRIMARY KEY, kind text NOT NULL, payload jsonb NOT NULL DEFAULT '{}',
			result jsonb, queue_position bigint NOT NULL, state text NOT NULL,
			owner_generation bigint, attempt_generation bigint NOT NULL DEFAULT 0,
			created_at timestamp NOT NULL DEFAULT clock_timestamp(), started_at timestamp,
			finished_at timestamp, error_code text, schedule_key text, schedule_revision bigint)`,
		`CREATE TABLE background_job_executions (
			job_id uuid NOT NULL REFERENCES background_jobs(id) ON DELETE CASCADE,
			attempt_generation bigint NOT NULL, envelope_sha256 text NOT NULL,
			executor_instance_id uuid NOT NULL, owner_id uuid, worker_generation bigint,
			deadline_at timestamptz, state text NOT NULL, result jsonb, error_code text,
			finished_at timestamp, legacy_completion_applied_at timestamp,
			PRIMARY KEY (job_id, attempt_generation))`,
		`CREATE TABLE scheduler_occurrence_receipts (
			occurrence_id uuid PRIMARY KEY, envelope_sha256 varchar(64) NOT NULL,
			job_id uuid NOT NULL, accepted_at timestamp NOT NULL DEFAULT clock_timestamp())`,
	}
	for _, statement := range ddl {
		if _, err := pool.Exec(ctx, statement); err != nil {
			pool.Close()
			closeConn(admin)
			cancel()
			t.Fatalf("create test schema tables: %v", err)
		}
	}
	store := &Store{Pool: pool}
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() {
			if proxy != nil {
				proxy.close()
			}
			store.Close()
			_, _ = admin.Exec(context.Background(), `DROP SCHEMA `+pgx.Identifier{schema}.Sanitize()+` CASCADE`)
			closeConn(admin)
			cancel()
		})
	}
	return store, ctx, cleanup
}

type commitReplyProxy struct {
	listener  net.Listener
	port      uint16
	target    string
	armed     atomic.Bool
	committed chan struct{}
	dropReply chan struct{}
	dropOnce  sync.Once
	activeMu  sync.Mutex
	closed    bool
	active    map[net.Conn]struct{}
	workers   sync.WaitGroup
}

func newCommitReplyProxy(t *testing.T, dsn string) *commitReplyProxy {
	t.Helper()
	parsed, err := url.Parse(dsn)
	if err != nil {
		t.Fatalf("parse PostgreSQL fixture URL for proxy: %v", err)
	}
	host := parsed.Hostname()
	port := 5432
	if parsed.Port() != "" {
		port, err = strconv.Atoi(parsed.Port())
	}
	if err != nil || host == "" || port < 1 || port > 65535 {
		t.Fatalf("PostgreSQL fixture URL has an invalid host or port")
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for local PostgreSQL response proxy: %v", err)
	}
	proxy := &commitReplyProxy{
		listener:  listener,
		port:      uint16(listener.Addr().(*net.TCPAddr).Port),
		target:    net.JoinHostPort(host, strconv.Itoa(port)),
		committed: make(chan struct{}),
		dropReply: make(chan struct{}),
		active:    make(map[net.Conn]struct{}),
	}
	proxy.workers.Add(1)
	go proxy.serve()
	t.Cleanup(proxy.close)
	return proxy
}

func (p *commitReplyProxy) serve() {
	defer p.workers.Done()
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.workers.Add(1)
		go func() {
			defer p.workers.Done()
			p.handle(client)
		}()
	}
}

func (p *commitReplyProxy) handle(client net.Conn) {
	if !p.register(client) {
		return
	}
	defer p.unregister(client)
	defer client.Close()
	server, err := net.DialTimeout("tcp", p.target, 2*time.Second)
	if err != nil {
		return
	}
	if !p.register(server) {
		return
	}
	defer p.unregister(server)
	defer server.Close()
	var startupHeader [4]byte
	if _, err := io.ReadFull(client, startupHeader[:]); err != nil {
		return
	}
	startupLength := binary.BigEndian.Uint32(startupHeader[:])
	if startupLength < 4 || startupLength > 1<<20 {
		return
	}
	startupBody := make([]byte, int(startupLength)-4)
	if _, err := io.ReadFull(client, startupBody); err != nil {
		return
	}
	if _, err := server.Write(append(startupHeader[:], startupBody...)); err != nil {
		return
	}
	clientDone := make(chan struct{})
	var pending atomic.Bool
	go func() {
		defer close(clientDone)
		for {
			kind, body, raw, err := readPGMessage(client)
			if err != nil {
				return
			}
			if isCommitMessage(kind, body) && p.armed.CompareAndSwap(true, false) {
				pending.Store(true)
			}
			if _, err := server.Write(raw); err != nil {
				return
			}
		}
	}()
	for {
		kind, _, raw, err := readPGMessage(server)
		if err != nil {
			return
		}
		if kind == 'Z' && pending.Swap(false) {
			close(p.committed)
			<-p.dropReply
			return
		}
		if _, err := client.Write(raw); err != nil {
			return
		}
		select {
		case <-clientDone:
			return
		default:
		}
	}
}

func (p *commitReplyProxy) armCommitReplyDrop() { p.armed.Store(true) }

func (p *commitReplyProxy) dropCommitReply() { p.dropOnce.Do(func() { close(p.dropReply) }) }

func (p *commitReplyProxy) register(conn net.Conn) bool {
	p.activeMu.Lock()
	defer p.activeMu.Unlock()
	if p.closed {
		_ = conn.Close()
		return false
	}
	p.active[conn] = struct{}{}
	return true
}

func (p *commitReplyProxy) unregister(conn net.Conn) {
	p.activeMu.Lock()
	delete(p.active, conn)
	p.activeMu.Unlock()
}

func (p *commitReplyProxy) close() {
	p.dropCommitReply()
	p.activeMu.Lock()
	p.closed = true
	for conn := range p.active {
		_ = conn.Close()
	}
	p.activeMu.Unlock()
	if p.listener != nil {
		_ = p.listener.Close()
	}
	p.workers.Wait()
}

func TestCommitReplyProxyRejectsConnectionsAfterClose(t *testing.T) {
	proxy := &commitReplyProxy{
		dropReply: make(chan struct{}),
		active:    make(map[net.Conn]struct{}),
	}
	proxy.close()
	client, server := net.Pipe()
	defer server.Close()
	if proxy.register(client) {
		t.Fatal("proxy accepted a connection after cleanup began")
	}
	if _, err := client.Write([]byte("probe")); err == nil {
		t.Fatal("connection remained open after registration was rejected")
	}
}

func TestPostgreSQLFixtureCleanupReleasesWithheldCommitResponse(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL lease tests")
	}
	proxy := newCommitReplyProxy(t, dsn)
	owner, _, cleanup := testStoreWithProxy(t, proxy)
	t.Cleanup(cleanup)
	if _, err := owner.Acquire(context.Background()); err != nil {
		t.Fatal(err)
	}
	proxy.armCommitReplyDrop()
	heartbeatDone := make(chan error, 1)
	go func() {
		heartbeatCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		heartbeatDone <- owner.Heartbeat(heartbeatCtx)
	}()
	select {
	case <-proxy.committed:
	case <-time.After(3 * time.Second):
		t.Fatal("proxy did not observe PostgreSQL's COMMIT completion")
	}

	cleanup()
	select {
	case err := <-heartbeatDone:
		if !errors.Is(err, ErrLeaseLost) {
			t.Fatalf("Heartbeat after fixture cleanup = %v, want fail-closed lease loss", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("fixture cleanup did not release the withheld COMMIT response")
	}
}

func readPGMessage(conn net.Conn) (byte, []byte, []byte, error) {
	var header [5]byte
	if _, err := io.ReadFull(conn, header[:]); err != nil {
		return 0, nil, nil, err
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length < 4 || length > 1<<20 {
		return 0, nil, nil, fmt.Errorf("invalid local PostgreSQL protocol message length %d", length)
	}
	body := make([]byte, int(length)-4)
	if _, err := io.ReadFull(conn, body); err != nil {
		return 0, nil, nil, err
	}
	raw := make([]byte, len(header)+len(body))
	copy(raw, header[:])
	copy(raw[len(header):], body)
	return header[0], body, raw, nil
}

func isCommitMessage(kind byte, body []byte) bool {
	var query string
	switch kind {
	case 'Q':
		query = strings.TrimRight(string(body), "\x00")
	case 'P': // Extended-protocol Parse: statement name, query string, parameter count.
		parts := strings.SplitN(string(body), "\x00", 3)
		if len(parts) > 1 {
			query = parts[1]
		}
	}
	return strings.EqualFold(strings.TrimSpace(query), "COMMIT")
}

func insertTestJob(t *testing.T, ctx context.Context, s *Store, state string, attempt int64) string {
	t.Helper()
	var id string
	err := s.Pool.QueryRow(ctx, `INSERT INTO background_jobs
		(id,kind,payload,queue_position,state,attempt_generation)
		VALUES (gen_random_uuid(),'news_processing','{}',1,$1,$2) RETURNING id::text`, state, attempt).Scan(&id)
	if err != nil {
		t.Fatalf("insert test job: %v", err)
	}
	return id
}

func TestLeaseFenceRejectsWritesAfterLeaseBackendDies(t *testing.T) {
	s, ctx, cleanup := testStore(t)
	defer cleanup()
	info, err := s.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobID := insertTestJob(t, ctx, s, "running", 1)
	job := Job{ID: jobID, AttemptGeneration: 1}
	if _, err := s.Pool.Exec(ctx, `UPDATE background_jobs SET owner_generation=$2 WHERE id=$1`, jobID, info.Generation); err != nil {
		t.Fatal(err)
	}
	var terminated bool
	if err := s.Pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, info.BackendPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate lease backend: terminated=%v err=%v", terminated, err)
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := s.Finish(writeCtx, job, "failed", map[string]any{"status": "incomplete"}, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Finish after lease backend death = %v, want ErrLeaseLost", err)
	}
}

func TestLeaseFenceRejectsStaleGenerationAfterTakeover(t *testing.T) {
	first, ctx, cleanup := testStore(t)
	defer cleanup()
	oldInfo, err := first.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobID := insertTestJob(t, ctx, first, "running", 1)
	job := Job{ID: jobID, AttemptGeneration: 1}
	if _, err := first.Pool.Exec(ctx, `UPDATE background_jobs SET owner_generation=$2 WHERE id=$1`, jobID, oldInfo.Generation); err != nil {
		t.Fatal(err)
	}
	var terminated bool
	if err := first.Pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, oldInfo.BackendPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate old lease backend: terminated=%v err=%v", terminated, err)
	}
	second := &Store{Pool: first.Pool}
	newInfo, err := second.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire replacement generation: %v", err)
	}
	if newInfo.Generation <= oldInfo.Generation {
		t.Fatalf("replacement generation %d did not advance past %d", newInfo.Generation, oldInfo.Generation)
	}
	writeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := first.Finish(writeCtx, job, "failed", map[string]any{"status": "incomplete"}, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("stale Finish = %v, want ErrLeaseLost", err)
	}
	_ = second.Release(context.Background())
}

func TestAcquireRejectsCompetingStartup(t *testing.T) {
	first, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := first.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	second := &Store{Pool: first.Pool}
	if _, err := second.Acquire(ctx); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("competing Acquire = %v, want ErrLeaseHeld", err)
	}
	_ = first.Release(context.Background())
	if _, err := second.Acquire(ctx); err != nil {
		t.Fatalf("Acquire after owner release = %v, want success", err)
	}
	_ = second.Release(context.Background())
}

func TestHeartbeatLockContentionReportsBoundedTimeoutAndReleasesLease(t *testing.T) {
	owner, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := owner.Acquire(ctx); err != nil {
		t.Fatal(err)
	}

	blockerConfig := owner.Pool.Config().ConnConfig.Copy()
	blocker, err := pgx.ConnectConfig(ctx, blockerConfig)
	if err != nil {
		t.Fatalf("connect row-lock fixture: %v", err)
	}
	defer closeConn(blocker)
	blockTx, err := blocker.Begin(ctx)
	if err != nil {
		t.Fatalf("begin row-lock fixture: %v", err)
	}
	defer rollbackTx(blockTx)
	if _, err := blockTx.Exec(ctx, `UPDATE background_worker_status SET status=status WHERE id=1`); err != nil {
		t.Fatalf("hold worker-status row lock: %v", err)
	}

	heartbeatCtx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	err = owner.Heartbeat(heartbeatCtx)
	cancel()
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Heartbeat under status-row contention = %v, want ErrLeaseLost", err)
	}
	var diagnostic *HeartbeatError
	if !errors.As(err, &diagnostic) || diagnostic.Stage != "update" || diagnostic.Reason != "timeout" || diagnostic.SQLState != "" {
		t.Fatalf("heartbeat diagnostic = %#v, want update/timeout without SQLSTATE", diagnostic)
	}
	if diagnostic.MutexWait < 0 || diagnostic.BeginDuration < 0 || diagnostic.UpdateDuration <= 0 || diagnostic.CommitDuration != 0 {
		t.Fatalf("heartbeat phase durations = %#v, want measured update wait and unstarted commit", diagnostic)
	}
	if got := err.Error(); strings.Contains(got, "UPDATE background_worker_status") || strings.Contains(got, "host=") {
		t.Fatalf("heartbeat diagnostic leaked driver or connection details: %q", got)
	}

	if err := blockTx.Rollback(ctx); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		t.Fatalf("release fixture row lock: %v", err)
	}
	replacement := &Store{Pool: owner.Pool}
	if _, err := replacement.Acquire(ctx); err != nil {
		t.Fatalf("replacement Acquire after heartbeat invalidation = %v", err)
	}
	_ = replacement.Release(context.Background())
}

func TestHeartbeatSeparatesLeaseMutexWaitFromBeginTimeout(t *testing.T) {
	owner, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := owner.Acquire(ctx); err != nil {
		t.Fatal(err)
	}

	owner.leaseMu.Lock()
	heartbeatCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		result <- owner.Heartbeat(heartbeatCtx)
	}()
	<-started
	<-heartbeatCtx.Done()
	owner.leaseMu.Unlock()

	err := <-result
	var diagnostic *HeartbeatError
	if !errors.Is(err, ErrLeaseLost) || !errors.As(err, &diagnostic) {
		t.Fatalf("Heartbeat after mutex wait exhausted context = %v, want bounded lease-loss diagnostic", err)
	}
	if diagnostic.Stage != "begin" || diagnostic.Reason != "timeout" || diagnostic.MutexWait <= 0 || diagnostic.BeginDuration < 0 || diagnostic.UpdateDuration != 0 || diagnostic.CommitDuration != 0 {
		t.Fatalf("heartbeat phase durations = %#v, want mutex wait separated from timed-out begin", diagnostic)
	}
}

func TestConcurrentIdleClaimAndHeartbeatKeepLeaseCurrent(t *testing.T) {
	owner, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := owner.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	interruptedID := insertTestJob(t, ctx, owner, "interrupted", 3)

	results := make(chan error, 2)
	go func() {
		for range 40 {
			if err := owner.Heartbeat(ctx); err != nil {
				results <- fmt.Errorf("idle heartbeat: %w", err)
				return
			}
		}
		results <- nil
	}()
	go func() {
		for range 40 {
			if _, err := owner.Claim(ctx, 3); !errors.Is(err, ErrNoJob) {
				results <- fmt.Errorf("idle claim = %v, want ErrNoJob", err)
				return
			}
		}
		if _, err := owner.FailExhaustedInterrupted(ctx, 3, 10); err != nil {
			results <- fmt.Errorf("idle exhausted-job recovery: %w", err)
			return
		}
		if _, err := owner.Prune(ctx, 0, 10); err != nil {
			results <- fmt.Errorf("idle retention pass: %w", err)
			return
		}
		results <- nil
	}()
	for range 2 {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := owner.Claim(ctx, 3); !errors.Is(err, ErrNoJob) {
		t.Fatalf("final idle claim = %v, want lease to remain current", err)
	}
	var recoveredState, recoveredCode string
	if err := owner.Pool.QueryRow(ctx, `SELECT state,error_code FROM background_jobs WHERE id=$1`, interruptedID).Scan(&recoveredState, &recoveredCode); err != nil {
		t.Fatal(err)
	}
	if recoveredState != "failed" || recoveredCode != "worker_recovery_exhausted" {
		t.Fatalf("idle recovery result = %s/%s, want failed/worker_recovery_exhausted", recoveredState, recoveredCode)
	}
}

func TestHeartbeatDelayedCommitTimeoutReportsCommitDurationAndReleasesLease(t *testing.T) {
	owner, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := owner.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	_, err := owner.Pool.Exec(ctx, `CREATE FUNCTION delay_worker_heartbeat_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_sleep(0.3); RETURN NEW; END $$`)
	if err != nil {
		t.Fatalf("create deferred commit delay fixture: %v", err)
	}
	_, err = owner.Pool.Exec(ctx, `CREATE CONSTRAINT TRIGGER delay_worker_heartbeat_commit
		AFTER UPDATE ON background_worker_status DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION delay_worker_heartbeat_commit()`)
	if err != nil {
		t.Fatalf("install deferred commit delay fixture: %v", err)
	}

	heartbeatCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	err = owner.Heartbeat(heartbeatCtx)
	cancel()
	var diagnostic *HeartbeatError
	if !errors.Is(err, ErrLeaseLost) || !errors.As(err, &diagnostic) {
		t.Fatalf("Heartbeat with delayed commit = %v, want fail-closed lease-loss diagnostic", err)
	}
	if diagnostic.Stage != "commit" || diagnostic.Reason != "timeout" || diagnostic.CommitDuration <= 0 || diagnostic.MutexWait < 0 || diagnostic.BeginDuration < 0 || diagnostic.UpdateDuration < 0 {
		t.Fatalf("heartbeat phase durations = %#v, want delayed commit timing", diagnostic)
	}
	if got := err.Error(); strings.Contains(got, "pg_sleep") || strings.Contains(got, "CREATE FUNCTION") {
		t.Fatalf("heartbeat diagnostic leaked fixture details: %q", got)
	}

	replacement := &Store{Pool: owner.Pool}
	if _, err := replacement.Acquire(ctx); err != nil {
		t.Fatalf("replacement Acquire after delayed commit timeout = %v", err)
	}
	_ = replacement.Release(context.Background())
}

func TestHeartbeatCommittedUpdateWithLostReplyFencesOldWriter(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for PostgreSQL lease tests")
	}
	proxy := newCommitReplyProxy(t, dsn)
	owner, ctx, cleanup := testStoreWithProxy(t, proxy)
	defer cleanup()
	info, err := owner.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	jobID := insertTestJob(t, ctx, owner, "running", 1)
	job := Job{ID: jobID, AttemptGeneration: 1}
	if _, err := owner.Pool.Exec(ctx, `UPDATE background_jobs SET owner_generation=$2 WHERE id=$1`, jobID, info.Generation); err != nil {
		t.Fatal(err)
	}
	var heartbeatBefore time.Time
	if err := owner.Pool.QueryRow(ctx, `SELECT heartbeat_at FROM background_worker_status WHERE id=1`).Scan(&heartbeatBefore); err != nil {
		t.Fatal(err)
	}

	proxy.armCommitReplyDrop()
	heartbeatDone := make(chan error, 1)
	go func() {
		heartbeatCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		heartbeatDone <- owner.Heartbeat(heartbeatCtx)
	}()
	select {
	case <-proxy.committed:
	case <-time.After(3 * time.Second):
		t.Fatal("proxy did not observe PostgreSQL's COMMIT completion")
	}
	var heartbeatAfter time.Time
	if err := owner.Pool.QueryRow(ctx, `SELECT heartbeat_at FROM background_worker_status WHERE id=1`).Scan(&heartbeatAfter); err != nil {
		t.Fatalf("read heartbeat after independently observed COMMIT: %v", err)
	}
	if !heartbeatAfter.After(heartbeatBefore) {
		t.Fatalf("heartbeat timestamp after server-confirmed COMMIT = %s, want later than %s", heartbeatAfter, heartbeatBefore)
	}
	proxy.dropCommitReply()

	err = <-heartbeatDone
	var diagnostic *HeartbeatError
	if !errors.Is(err, ErrLeaseLost) || !errors.As(err, &diagnostic) || diagnostic.Stage != "commit" {
		t.Fatalf("Heartbeat after dropped COMMIT reply = %v, want fail-closed commit diagnostic", err)
	}
	if diagnostic.CommitDuration < 0 {
		t.Fatalf("commit duration = %s, want nonnegative measured duration", diagnostic.CommitDuration)
	}

	replacement := &Store{Pool: owner.Pool}
	newInfo, err := replacement.Acquire(ctx)
	if err != nil {
		t.Fatalf("replacement Acquire after committed heartbeat reply loss = %v", err)
	}
	if newInfo.Generation <= info.Generation {
		t.Fatalf("replacement generation %d did not advance past %d", newInfo.Generation, info.Generation)
	}
	if err := owner.Finish(ctx, job, "failed", map[string]any{"status": "incomplete"}, nil); !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("old writer Finish after lost COMMIT reply = %v, want ErrLeaseLost", err)
	}
	_ = replacement.Release(context.Background())
}

func TestHeartbeatReportsLeaseConnectionFailureAndAllowsReplacement(t *testing.T) {
	owner, ctx, cleanup := testStore(t)
	defer cleanup()
	info, err := owner.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var terminated bool
	if err := owner.Pool.QueryRow(ctx, `SELECT pg_terminate_backend($1)`, info.BackendPID).Scan(&terminated); err != nil || !terminated {
		t.Fatalf("terminate lease backend: terminated=%v err=%v", terminated, err)
	}

	heartbeatCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err = owner.Heartbeat(heartbeatCtx)
	cancel()
	if !errors.Is(err, ErrLeaseLost) {
		t.Fatalf("Heartbeat after backend termination = %v, want ErrLeaseLost", err)
	}
	var diagnostic *HeartbeatError
	if !errors.As(err, &diagnostic) || diagnostic.Stage != "begin" {
		t.Fatalf("heartbeat diagnostic = %#v, want begin stage", diagnostic)
	}
	switch diagnostic.Reason {
	case "sqlstate":
		if diagnostic.SQLState != "57P01" {
			t.Fatalf("heartbeat SQLSTATE = %q, want 57P01", diagnostic.SQLState)
		}
	case "connection_closed", "other":
		if diagnostic.SQLState != "" {
			t.Fatalf("heartbeat SQLSTATE = %q, want empty when classifying %s", diagnostic.SQLState, diagnostic.Reason)
		}
	default:
		t.Fatalf("heartbeat diagnostic reason = %q, want bounded connection failure classification", diagnostic.Reason)
	}
	if strings.Contains(err.Error(), "ERROR:") || strings.Contains(err.Error(), "postgres://") {
		t.Fatalf("heartbeat diagnostic leaked driver or connection details: %q", err.Error())
	}

	replacement := &Store{Pool: owner.Pool}
	if _, err := replacement.Acquire(ctx); err != nil {
		t.Fatalf("replacement Acquire after dead lease backend = %v", err)
	}
	_ = replacement.Release(context.Background())
}

func TestHeartbeatDiagnosticClassifiesSQLStateWithoutDriverMessage(t *testing.T) {
	err := heartbeatError("commit", &pgconn.PgError{Code: "40001", Message: "sensitive SQL and endpoint details"})
	if err.Stage != "commit" || err.Reason != "sqlstate" || err.SQLState != "40001" {
		t.Fatalf("heartbeat diagnostic = %#v", err)
	}
	if strings.Contains(err.Error(), "sensitive") || strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("diagnostic leaked driver message: %q", err.Error())
	}
}

func TestHeartbeatErrorFormatsBoundedPhaseTimings(t *testing.T) {
	diagnostic := &HeartbeatError{
		Stage:          "commit",
		Reason:         "timeout",
		SQLState:       "57014",
		MutexWait:      1200 * time.Millisecond,
		BeginDuration:  2300 * time.Millisecond,
		UpdateDuration: 3400 * time.Millisecond,
		CommitDuration: 4500 * time.Millisecond,
	}
	got := diagnostic.Error()
	for _, want := range []string{
		"stage=commit", "reason=timeout", "mutex_wait_ms=1200", "begin_ms=2300",
		"update_ms=3400", "commit_ms=4500", "sqlstate=57014",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("HeartbeatError = %q, missing %q", got, want)
		}
	}
	if strings.Contains(got, "postgres://") || strings.Contains(got, "UPDATE background_worker_status") {
		t.Fatalf("HeartbeatError leaked SQL or connection details: %q", got)
	}
}

func TestHeartbeatDiagnosticsUseFiniteSafeReasons(t *testing.T) {
	cases := []struct {
		name   string
		err    error
		reason string
	}{
		{name: "timeout", err: context.DeadlineExceeded, reason: "timeout"},
		{name: "canceled", err: context.Canceled, reason: "canceled"},
		{name: "transaction closed", err: pgx.ErrTxClosed, reason: "transaction_closed"},
		{name: "connection closed", err: pgconn.ErrConnClosed, reason: "connection_closed"},
		{name: "other", err: errors.New("private driver details"), reason: "other"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			diagnostic := heartbeatError("begin", tc.err)
			if diagnostic.Reason != tc.reason || diagnostic.SQLState != "" {
				t.Fatalf("heartbeat diagnostic = %#v, want reason %q", diagnostic, tc.reason)
			}
			if strings.Contains(diagnostic.Error(), "private") {
				t.Fatalf("diagnostic leaked original error: %q", diagnostic.Error())
			}
			if !errors.Is(diagnostic, ErrLeaseLost) {
				t.Fatalf("diagnostic does not unwrap to ErrLeaseLost: %v", diagnostic)
			}
		})
	}
}

func TestAttemptCapAndPendingCompletionRetention(t *testing.T) {
	s, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	jobID := insertTestJob(t, ctx, s, "interrupted", 3)
	if _, err := s.FailExhaustedInterrupted(ctx, 3, 10); err != nil {
		t.Fatal(err)
	}
	var state, errorCode string
	if err := s.Pool.QueryRow(ctx, `SELECT state,error_code FROM background_jobs WHERE id=$1`, jobID).Scan(&state, &errorCode); err != nil {
		t.Fatal(err)
	}
	if state != "failed" || errorCode != "worker_recovery_exhausted" {
		t.Fatalf("capped interrupted job = %s/%s", state, errorCode)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE background_jobs SET finished_at=clock_timestamp()-interval '2 days' WHERE id=$1`, jobID); err != nil {
		t.Fatal(err)
	}
	var occurrenceID string
	if err := s.Pool.QueryRow(ctx, `INSERT INTO scheduler_occurrence_receipts
		(occurrence_id,envelope_sha256,job_id) VALUES (gen_random_uuid(),repeat('a',64),$1)
		RETURNING occurrence_id::text`, jobID).Scan(&occurrenceID); err != nil {
		t.Fatal(err)
	}
	removed, err := s.Prune(ctx, time.Hour, 10)
	if err != nil || removed != 0 {
		t.Fatalf("prune with pending completion receipt = %d, %v; want 0", removed, err)
	}
	if _, err := s.Pool.Exec(ctx, `UPDATE background_job_executions SET legacy_completion_applied_at=clock_timestamp()`); err != nil {
		t.Fatal(err)
	}
	removed, err = s.Prune(ctx, time.Hour, 10)
	if err != nil || removed != 1 {
		t.Fatalf("prune after completion event = %d, %v; want 1", removed, err)
	}
	var receiptCount int
	if err := s.Pool.QueryRow(ctx, `SELECT count(*) FROM scheduler_occurrence_receipts WHERE occurrence_id=$1`, occurrenceID).Scan(&receiptCount); err != nil || receiptCount != 1 {
		t.Fatalf("scheduler occurrence receipt count = %d, err=%v; want 1", receiptCount, err)
	}
}

func TestClaimResumesInterruptedAttemptBelowCap(t *testing.T) {
	s, ctx, cleanup := testStore(t)
	defer cleanup()
	if _, err := s.Acquire(ctx); err != nil {
		t.Fatal(err)
	}
	jobID := insertTestJob(t, ctx, s, "interrupted", 1)
	changed, err := s.FailExhaustedInterrupted(ctx, 3, 10)
	if err != nil || changed != 0 {
		t.Fatalf("prematurely exhaust interrupted attempt: changed=%d err=%v", changed, err)
	}
	job, err := s.Claim(ctx, 3)
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != jobID || job.State != "running" || job.AttemptGeneration != 2 {
		t.Fatalf("resumed job = %#v; want same job running at attempt 2", job)
	}
}
