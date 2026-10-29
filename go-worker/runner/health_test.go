package runner

import (
	"context"
	"errors"
	"sync"
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

type notifyingHeartbeatStore struct {
	worker.QueueStore
	entered chan struct{}
	release chan struct{}
}

func (s notifyingHeartbeatStore) HeartbeatWithFailure(_ context.Context, notify func()) error {
	notify()
	close(s.entered)
	<-s.release
	return errors.New("lost")
}

func TestHeartbeatObserverInvalidatesHealthBeforeCleanup(t *testing.T) {
	queue := notifyingHeartbeatStore{entered: make(chan struct{}), release: make(chan struct{})}
	observed := make(chan time.Time, 1)
	lost := make(chan struct{})
	observer := heartbeatObserver{QueueStore: queue, onHealth: func(at time.Time) { observed <- at }}
	done := make(chan error, 1)
	go func() { done <- observer.HeartbeatWithFailure(context.Background(), func() { close(lost) }) }()
	select {
	case <-queue.entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup not reached")
	}
	select {
	case <-lost:
	default:
		t.Fatal("worker was not notified")
	}
	select {
	case at := <-observed:
		if !at.IsZero() {
			t.Fatal("health was not invalidated")
		}
	case <-time.After(time.Second):
		t.Fatal("health was not invalidated before cleanup")
	}
	close(queue.release)
	<-done
}

// A health callback may acquire its own locks or reenter the store. It must
// not delay cleanup by running inside the lease callback's critical section.
type lockedHeartbeatStore struct {
	worker.QueueStore
	mu sync.Mutex
}

func (s *lockedHeartbeatStore) HeartbeatWithFailure(_ context.Context, notify func()) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	notify()
	return errors.New("lost")
}
func TestHeartbeatObserverDoesNotCallHealthUnderLeaseLock(t *testing.T) {
	queue := &lockedHeartbeatStore{}
	observer := heartbeatObserver{QueueStore: queue, onHealth: func(time.Time) { queue.mu.Lock(); queue.mu.Unlock() }}
	done := make(chan error, 1)
	go func() { done <- observer.HeartbeatWithFailure(context.Background(), nil) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("health callback deadlocked heartbeat cleanup")
	}
}
