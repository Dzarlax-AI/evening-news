package airuntime

import (
	"context"
	"errors"
	"fmt"
	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"sync"
	"time"
)

type breakerState uint8

const (
	closed breakerState = iota
	open
	halfOpen
)

type Breaker struct {
	mu          sync.Mutex
	state       breakerState
	failures    int
	lastFailure time.Time
	threshold   int
	recovery    time.Duration
	now         func() time.Time
}

func NewBreaker() *Breaker { return &Breaker{threshold: 5, recovery: 60 * time.Second, now: time.Now} }

// Do shares failure state among native AI operations. A retry series is one
// breaker call, matching the previous Python wrapper around tenacity.
func (b *Breaker) Do(ctx context.Context, fn func() (any, error)) (any, error) {
	b.mu.Lock()
	if b.state == open {
		if b.now().Sub(b.lastFailure) >= b.recovery {
			b.state = halfOpen
		} else {
			remaining := b.recovery - b.now().Sub(b.lastFailure)
			b.mu.Unlock()
			return nil, fmt.Errorf("breaker_open:%0.1f", remaining.Seconds())
		}
	}
	b.mu.Unlock()
	value, err := fn()
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		if b.state == halfOpen {
			b.state, b.failures = closed, 0
		} else if b.state == closed {
			b.failures = 0
		}
		return value, nil
	}
	if ctx.Err() != nil {
		return nil, err
	}
	var providerErr *ai.Error
	if errors.As(err, &providerErr) && !providerErr.Retryable {
		return nil, err
	}
	b.failures++
	b.lastFailure = b.now()
	if b.failures >= b.threshold {
		b.state = open
	}
	return nil, err
}

func (b *Breaker) Snapshot() (string, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	names := []string{"closed", "open", "half_open"}
	return names[b.state], b.failures
}
