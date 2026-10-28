// Package callbacks contains bounded in-process state for private article callbacks.
package callbacks

import (
	"container/list"
	"errors"
	"sync"
)

var (
	ErrAlreadyRunning = errors.New("Article AI callback is already running")
	ErrAdmissionFull  = errors.New("Article AI admission is full")
)

// Admission tracks active callbacks and completed responses. Keys must contain
// the entire callback identity, including its input fingerprint and operation.
// Cached responses are copied so callers cannot mutate replayed data.
type Admission struct {
	mu                                     sync.Mutex
	active                                 map[string]struct{}
	cached                                 map[string]*list.Element
	lru                                    *list.List
	bytes, maxActive, maxEntries, maxBytes int
}
type entry struct {
	key      string
	response []byte
}

func NewAdmission(maxActive, maxEntries, maxBytes int) *Admission {
	return &Admission{active: make(map[string]struct{}), cached: make(map[string]*list.Element), lru: list.New(), maxActive: maxActive, maxEntries: maxEntries, maxBytes: maxBytes}
}

// Begin returns a cached response, or reserves an active slot for new work.
// A successful reservation must be paired with Finish, including on failure.
func (a *Admission) Begin(key string) ([]byte, bool, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if item := a.cached[key]; item != nil {
		a.lru.MoveToBack(item)
		return append([]byte(nil), item.Value.(entry).response...), true, nil
	}
	if _, ok := a.active[key]; ok {
		return nil, false, ErrAlreadyRunning
	}
	if len(a.active) >= a.maxActive {
		return nil, false, ErrAdmissionFull
	}
	a.active[key] = struct{}{}
	return nil, false, nil
}

// Finish releases a reservation. A nil response means failure and is not cached.
func (a *Admission) Finish(key string, response []byte) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if _, ok := a.active[key]; !ok {
		return
	}
	delete(a.active, key)
	if response == nil || len(response) > a.maxBytes || a.maxEntries <= 0 {
		return
	}
	if old := a.cached[key]; old != nil {
		a.remove(old)
	}
	item := a.lru.PushBack(entry{key, append([]byte(nil), response...)})
	a.cached[key] = item
	a.bytes += len(response)
	for a.lru.Len() > a.maxEntries || a.bytes > a.maxBytes {
		a.remove(a.lru.Front())
	}
}
func (a *Admission) remove(item *list.Element) {
	e := item.Value.(entry)
	delete(a.cached, e.key)
	a.bytes -= len(e.response)
	a.lru.Remove(item)
}
