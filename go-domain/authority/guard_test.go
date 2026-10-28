package authority

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestGuardCancellationWhileWaitingDoesNotRevoke(t *testing.T) {
	g := &Guard{lock: make(chan struct{}, 1), pid: 42}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- g.Check(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("got %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled waiter blocked")
	}
	g.lock <- struct{}{}
	if got := g.BackendPID(); got != 42 {
		t.Fatalf("lost owner PID: %d", got)
	}
}
func TestZeroGuardIsUnavailable(t *testing.T) {
	var g Guard
	if err := g.Check(context.Background()); !errors.Is(err, ErrGuardUnavailable) {
		t.Fatalf("got %v", err)
	}
	if g.BackendPID() != 0 {
		t.Fatal("zero guard has PID")
	}
	if err := g.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
