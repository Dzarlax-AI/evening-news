// Package terminalevents handles Go-finalized operational and source alerts.
package terminalevents

import (
	"context"
	"fmt"
	"sync"
	"time"
)

type AlertSender func(context.Context, string, string) bool
type alertState struct {
	active     bool
	last       time.Time
	suppressed int
}
type Alerts struct {
	mu       sync.Mutex
	states   map[string]*alertState
	send     AlertSender
	now      func() time.Time
	interval time.Duration
}

func NewAlerts(send AlertSender) *Alerts {
	return &Alerts{states: map[string]*alertState{}, send: send, now: time.Now, interval: time.Hour}
}
func (a *Alerts) Active(key string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	v := a.states[key]
	return v != nil && v.active
}
func truncate(s string, n int) string {
	v := []rune(s)
	if len(v) > n {
		return string(v[:n])
	}
	return s
}
func (a *Alerts) Failure(ctx context.Context, key, title, message string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.states[key]
	if state == nil {
		state = &alertState{}
		a.states[key] = state
	}
	now := a.now()
	send := !state.active || now.Sub(state.last) >= a.interval
	state.active = true
	if !send {
		state.suppressed++
		return false
	}
	if state.suppressed > 0 {
		message += fmt.Sprintf("\n\nПовторов с прошлого уведомления: %d", state.suppressed)
	}
	sent := a.safeSend(ctx, truncate(title, 200), truncate(message, 3200))
	if sent {
		state.last = now
		state.suppressed = 0
	} else {
		state.active = false
	}
	return sent
}
func (a *Alerts) Recovery(ctx context.Context, key, title, message string) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	state := a.states[key]
	if state == nil || !state.active {
		return false
	}
	if state.suppressed > 0 {
		message += fmt.Sprintf("\n\nПодавлено повторов: %d", state.suppressed)
	}
	sent := a.safeSend(ctx, truncate(title, 200), truncate(message, 3200))
	if sent {
		state.active = false
		state.last = a.now()
		state.suppressed = 0
	}
	return sent
}

func (a *Alerts) safeSend(ctx context.Context, title, message string) (sent bool) {
	defer func() {
		if recover() != nil {
			sent = false
		}
	}()
	if a.send == nil {
		return false
	}
	return a.send(ctx, title, message)
}
