package airuntime

import (
	"context"
	"sync"
	"time"
)

// SlidingWindow admits at most limit operations in each rolling window. Waiting
// occurs outside the lock and honors both cancellation and the caller deadline.
type SlidingWindow struct {
	limit  int
	window time.Duration
	now    func() time.Time
	wait   func(context.Context, time.Duration) error
	mu     sync.Mutex
	calls  []time.Time
}

func NewSlidingWindow(limit int) *SlidingWindow {
	if limit < 1 {
		limit = 1
	}
	return &SlidingWindow{limit: limit, window: time.Second, now: time.Now, wait: waitContext}
}

func (w *SlidingWindow) Acquire(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		w.mu.Lock()
		now := w.now()
		cutoff := now.Add(-w.window)
		first := 0
		for first < len(w.calls) && !w.calls[first].After(cutoff) {
			first++
		}
		if first > 0 {
			w.calls = append([]time.Time(nil), w.calls[first:]...)
		}
		if len(w.calls) < w.limit {
			w.calls = append(w.calls, now)
			w.mu.Unlock()
			return nil
		}
		wait := w.calls[0].Add(w.window).Sub(now)
		w.mu.Unlock()
		if wait < 0 {
			wait = 0
		}
		if err := w.wait(ctx, wait); err != nil {
			return err
		}
	}
}

func waitContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
