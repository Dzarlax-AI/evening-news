package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-worker/internal/worker"
)

type heartbeatTestStore struct {
	worker.QueueStore
	err error
}

func (s heartbeatTestStore) Heartbeat(context.Context) error { return s.err }

func TestHeartbeatObserverReportsOnlySuccessfulExistingPoolHeartbeat(t *testing.T) {
	var observed time.Time
	observer := heartbeatObserver{QueueStore: heartbeatTestStore{}, onHealth: func(at time.Time) { observed = at }}
	if err := observer.Heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if observed.IsZero() {
		t.Fatal("successful heartbeat did not refresh lease health")
	}

	want := errors.New("heartbeat failed")
	observed = time.Time{}
	observer.QueueStore = heartbeatTestStore{err: want}
	if err := observer.Heartbeat(context.Background()); !errors.Is(err, want) {
		t.Fatalf("heartbeat error = %v", err)
	}
	if !observed.IsZero() {
		t.Fatal("failed heartbeat refreshed lease health")
	}
}
