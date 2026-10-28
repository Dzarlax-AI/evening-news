package authority

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const ExecutorGuardXOR int32 = 1145132609

var ErrGuardUnavailable = errors.New("executor instance is not the database owner")

// Guard reserves one connection from the domain pool for the singleton lock.
// A failed probe permanently revokes the guard; the caller must cancel active
// operations and stop serving. A replacement process must acquire a new guard.
type Guard struct {
	mu   sync.Mutex
	conn *pgxpool.Conn
	pid  int32
}

func AcquireGuard(ctx context.Context, pool *pgxpool.Pool) (*Guard, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	var acquired bool
	var pid int32
	err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext(current_database()), hashtext(current_schema()) # $1::integer), pg_backend_pid()`, ExecutorGuardXOR).Scan(&acquired, &pid)
	if err != nil {
		discard(conn)
		return nil, err
	}
	if !acquired {
		conn.Release()
		return nil, ErrGuardUnavailable
	}
	return &Guard{conn: conn, pid: pid}, nil
}

// Check serializes probes on the dedicated session and fails closed.
func (g *Guard) Check(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn == nil {
		return ErrGuardUnavailable
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var pid int32
	var held bool
	err := g.conn.QueryRow(ctx, `SELECT pg_backend_pid(), EXISTS (
 SELECT 1 FROM pg_locks WHERE locktype='advisory' AND granted AND pid=pg_backend_pid() AND objsubid=2
 AND classid=(((hashtext(current_database())::bigint + 4294967296) % 4294967296)::oid)
 AND objid=((((hashtext(current_schema()) # $1::integer)::bigint + 4294967296) % 4294967296)::oid)
 AND database=(SELECT oid FROM pg_database WHERE datname=current_database()))`, ExecutorGuardXOR).Scan(&pid, &held)
	if err != nil || !held || pid != g.pid {
		conn := g.conn
		g.conn = nil
		g.pid = 0
		discard(conn)
		return ErrGuardUnavailable
	}
	return nil
}

func (g *Guard) BackendPID() int32 { g.mu.Lock(); defer g.mu.Unlock(); return g.pid }

func (g *Guard) Close(ctx context.Context) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.conn == nil {
		return nil
	}
	conn := g.conn
	g.conn = nil
	g.pid = 0
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	var unlocked bool
	err := conn.QueryRow(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()), hashtext(current_schema()) # $1::integer)`, ExecutorGuardXOR).Scan(&unlocked)
	if err != nil || !unlocked {
		discard(conn)
		if err != nil {
			return err
		}
		return ErrGuardUnavailable
	}
	conn.Release()
	return nil
}

func discard(conn *pgxpool.Conn) {
	// Never return a session with uncertain lock state to the pool.
	raw := conn.Hijack()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = raw.Close(ctx)
}
