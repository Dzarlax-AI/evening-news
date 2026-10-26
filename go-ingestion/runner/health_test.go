package runner

import (
	"context"
	"testing"
	"time"
)

func TestWatchPoolHealthUsesBoundedContextAndStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	refreshed := make(chan time.Time, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		watchPoolHealth(ctx, time.Millisecond, 20*time.Millisecond, func(c context.Context) error {
			if _, ok := c.Deadline(); !ok {
				t.Error("pool ping has no deadline")
			}
			return nil
		}, func(at time.Time) {
			select {
			case refreshed <- at:
			default:
			}
		})
	}()
	select {
	case at := <-refreshed:
		if at.IsZero() {
			t.Fatal("empty health timestamp")
		}
	case <-time.After(time.Second):
		t.Fatal("pool health was not refreshed")
	}
	cancel()
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("health observer did not stop")
	}
}
