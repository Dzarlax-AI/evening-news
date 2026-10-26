// Package runner exposes the durable worker lifecycle for standalone and composed runtimes.
package runner

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-worker/internal/config"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/executor"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/newsadapter"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/store"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/worker"
)

var ErrLeaseLost = worker.ErrLeaseLost
var ErrLeaseHeld = store.ErrLeaseHeld
var ErrStoreLeaseLost = store.ErrLeaseLost
var ErrExecutorAuth = worker.ErrExecutorAuth

type HeartbeatError = store.HeartbeatError
type ExecutorHTTPError = executor.HTTPError

var ErrInvalidConfig = errors.New("component configuration is invalid")

type Config struct {
	config.Config
	DBMaxConns int32
}

func LoadConfig(getenv func(string) string) (Config, error) {
	c, e := config.LoadFrom(getenv)
	if e != nil {
		return Config{}, fmt.Errorf("%w: %s", ErrInvalidConfig, e)
	}
	return Config{Config: c, DBMaxConns: 4}, nil
}
func ConfigFromEnv() (Config, error) { return LoadConfig(os.Getenv) }
func Healthcheck(ctx context.Context, cfg Config) error {
	db, e := store.Open(ctx, cfg.DatabaseURL, cfg.DBMaxConns)
	if e != nil {
		return e
	}
	defer db.Close()
	if e = db.CheckSchema(ctx); e != nil {
		return e
	}
	return db.CheckWorkerHealth(ctx)
}
func Run(ctx context.Context, cfg Config, onReady func()) error {
	return RunWithHealth(ctx, cfg, onReady, nil)
}

// RunWithHealth reports lease health from the worker's existing store pool.
// The callback runs after recovery completes, after each successful fenced
// heartbeat, and with a zero timestamp when a heartbeat fails. It never
// creates a second database pool.
func RunWithHealth(ctx context.Context, cfg Config, onReady func(), onLeaseHealth func(time.Time)) error {
	if ctx == nil {
		return errors.New("worker context is required")
	}
	if cfg.DBMaxConns < 1 || cfg.DatabaseURL == "" || cfg.ExecutorURL == "" || cfg.ExecutorToken == "" || cfg.PollInterval <= 0 || cfg.RequestTimeout <= 0 {
		return ErrInvalidConfig
	}
	if ctx.Err() != nil {
		return nil
	}
	openCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	db, e := store.Open(openCtx, cfg.DatabaseURL, cfg.DBMaxConns)
	cancel()
	if e != nil {
		if ctx.Err() != nil && errors.Is(e, context.Canceled) {
			return nil
		}
		return e
	}
	defer db.Close()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	var queue worker.QueueStore = db
	if onLeaseHealth != nil {
		queue = heartbeatObserver{QueueStore: db, onHealth: onLeaseHealth}
	}
	var ready func()
	if onReady != nil || onLeaseHealth != nil {
		ready = func() {
			if onLeaseHealth != nil {
				onLeaseHealth(time.Now().UTC())
			}
			if onReady != nil {
				onReady()
			}
		}
	}
	instance, e := worker.NewWithReady(cfg.Config, queue, newsadapter.Adapter{}, logger, ready)
	if e != nil {
		return e
	}
	e = instance.Run(ctx)
	if ctx.Err() != nil && errors.Is(e, context.Canceled) {
		return nil
	}
	return e
}

type heartbeatObserver struct {
	worker.QueueStore
	onHealth func(time.Time)
}

func (s heartbeatObserver) Heartbeat(ctx context.Context) error {
	err := s.QueueStore.Heartbeat(ctx)
	if s.onHealth != nil {
		if err != nil {
			s.onHealth(time.Time{})
			return err
		}
		s.onHealth(time.Now().UTC())
	}
	return err
}
