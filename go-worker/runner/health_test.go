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

func TestHeartbeatObserverReportsSuccessAndImmediateFailureFromExistingPool(t *testing.T) {
	var observed []time.Time
	observer := heartbeatObserver{QueueStore: heartbeatTestStore{}, onHealth: func(at time.Time) { observed = append(observed, at) }}
	if err := observer.Heartbeat(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(observed) != 1 || observed[0].IsZero() {
		t.Fatal("successful heartbeat did not refresh lease health")
	}

	want := errors.New("heartbeat failed")
	observer.QueueStore = heartbeatTestStore{err: want}
	if err := observer.Heartbeat(context.Background()); !errors.Is(err, want) {
		t.Fatalf("heartbeat error = %v", err)
	}
	if len(observed) != 2 || !observed[1].IsZero() {
		t.Fatalf("failed heartbeat did not invalidate health immediately: %v", observed)
	}
}
