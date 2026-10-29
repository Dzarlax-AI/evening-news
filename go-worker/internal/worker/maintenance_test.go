package worker

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-worker/internal/config"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/store"
)

type maintenanceQueue struct {
	fakeQueueStore
	recoveryReads atomic.Int32
	heartbeats    atomic.Int32
}

func (q *maintenanceQueue) ActiveJobs(context.Context) ([]store.Job, error) {
	q.recoveryReads.Add(1)
	return nil, nil
}
func (q *maintenanceQueue) TerminalJobsNeedingEvents(context.Context, int) ([]store.Job, error) {
	q.recoveryReads.Add(1)
	return nil, nil
}
func (q *maintenanceQueue) Heartbeat(context.Context) error { q.heartbeats.Add(1); return nil }

func TestMaintenanceGatePrecedesRecoveryReplayAndClaims(t *testing.T) {
	for _, release := range []bool{false, true} {
		name := "cancel_while_paused"
		if release {
			name = "resume"
		}
		t.Run(name, func(t *testing.T) {
			var released atomic.Bool
			entered := make(chan struct{}, 1)
			var externalCalls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { externalCalls.Add(1); w.WriteHeader(204) }))
			defer server.Close()
			queue := &maintenanceQueue{fakeQueueStore: fakeQueueStore{info: store.LeaseInfo{OwnerID: "a39321b0-7724-443c-aaed-fd5b57e301db", Generation: 1}}}
			cfg := config.Config{ExecutorURL: server.URL, ExecutorToken: "synthetic-purpose-token", RequestTimeout: time.Second, PollInterval: time.Millisecond, HeartbeatInterval: time.Millisecond, MaxJobAttempts: 3, ShutdownDrain: time.Second}
			cfg.BeforeRecovery = func(context.Context) error {
				select {
				case entered <- struct{}{}:
				default:
				}
				if !released.Load() {
					return errors.New("operator confirmation required")
				}
				return nil
			}
			ready := make(chan struct{})
			worker, err := NewWithReady(cfg, queue, allowAllPolicy{}, slog.New(slog.NewTextHandler(io.Discard, nil)), func() { close(ready) })
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("gate not reached")
			}
			deadline := time.After(time.Second)
			for queue.heartbeats.Load() == 0 {
				select {
				case <-deadline:
					t.Fatal("paused worker lost heartbeat")
				case <-time.After(time.Millisecond):
				}
			}
			if queue.recoveryReads.Load() != 0 || queue.claimCalls.Load() != 0 || externalCalls.Load() != 0 {
				t.Fatal("paused installation replayed work or emitted an event")
			}
			if release {
				released.Store(true)
				select {
				case <-ready:
				case <-time.After(time.Second):
					t.Fatal("worker did not resume after operator release")
				}
			}
			cancel()
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("paused worker did not stop")
			}
			if !release && (queue.recoveryReads.Load() != 0 || externalCalls.Load() != 0) {
				t.Fatal("cancellation bypassed maintenance gate")
			}
		})
	}
}
