package cloakfixture

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type Peer interface{ AssertClean(context.Context) error }
type Profiles struct {
	Worker   *WorkerClient
	StateDir string
	Peer     Peer
}
type ProfileResult struct {
	Limit           int             `json:"limit"`
	MaxOriginActive int             `json:"max_origin_active"`
	Requests        []RequestResult `json:"requests"`
}
type resultOrError struct {
	result RequestResult
	err    error
}

func (p *Profiles) state() (snapshot, error) {
	var s snapshot
	path := filepath.Join(p.StateDir, "origin-state.json")
	i, e := os.Lstat(path)
	if e != nil || !i.Mode().IsRegular() || i.Size() > 65536 {
		return s, errors.New("origin_state_unavailable")
	}
	b, e := os.ReadFile(path)
	if e != nil || json.Unmarshal(b, &s) != nil || s.Version != 1 || s.Active < 0 || s.MaxActive < 0 || len(s.Arrivals) > 128 {
		return s, errors.New("origin_state_unavailable")
	}
	return s, nil
}
func (p *Profiles) wait(ctx context.Context, caseID string, n int) (snapshot, error) {
	tick := time.NewTicker(25 * time.Millisecond)
	defer tick.Stop()
	for {
		s, e := p.state()
		if e == nil && countCase(s, caseID) >= n {
			return s, nil
		}
		select {
		case <-ctx.Done():
			return s, errors.New("fixture_arrival_timeout")
		case <-tick.C:
		}
	}
}
func countCase(s snapshot, c string) int {
	n := 0
	for _, a := range s.Arrivals {
		if a.Case == c {
			n++
		}
	}
	return n
}
func (p *Profiles) release(c string) error {
	if !fixtureID.MatchString(c) {
		return errors.New("fixture_release_invalid")
	}
	dir := filepath.Join(p.StateDir, "release")
	if err := os.MkdirAll(dir, 0700); err != nil {
		return errors.New("fixture_release_failed")
	}
	f, e := os.CreateTemp(dir, ".release-")
	if e != nil {
		return errors.New("fixture_release_failed")
	}
	name := f.Name()
	defer os.Remove(name)
	if _, e = f.WriteString("release\n"); e == nil {
		e = f.Sync()
	}
	if x := f.Close(); e == nil {
		e = x
	}
	if e == nil {
		e = os.Rename(name, filepath.Join(dir, c))
	}
	if e != nil {
		return errors.New("fixture_release_failed")
	}
	return nil
}
func (p *Profiles) start(ctx context.Context, name, c string, budget int) <-chan resultOrError {
	ch := make(chan resultOrError, 1)
	go func() { r, e := p.Worker.Request(ctx, name, c, budget, ""); ch <- resultOrError{r, e} }()
	return ch
}
func receive(ctx context.Context, ch <-chan resultOrError) (RequestResult, error) {
	select {
	case r := <-ch:
		return r.result, r.err
	case <-ctx.Done():
		return RequestResult{}, errors.New("client_timeout")
	}
}
func (p *Profiles) Profile(ctx context.Context, limit int) (ProfileResult, error) {
	var out ProfileResult
	if p.Worker == nil || p.Peer == nil || (limit != 1 && limit != 2) {
		return out, errors.New("client_configuration_invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, 40*time.Second)
	defer cancel()
	c := "two"
	if limit == 1 {
		c = "one-first"
	}
	first := p.start(ctx, "one", c, 25000)
	if _, e := p.wait(ctx, c, 1); e != nil {
		return out, e
	}
	var second, third <-chan resultOrError
	if limit == 1 {
		second = p.start(ctx, "two", "one-second", 25000)
	} else {
		second = p.start(ctx, "two", "two", 25000)
		if _, e := p.wait(ctx, "two", 2); e != nil {
			return out, e
		}
		third = p.start(ctx, "three", "two", 25000)
	}
	select {
	case <-ctx.Done():
		return out, errors.New("client_timeout")
	case <-time.After(600 * time.Millisecond):
	}
	s, e := p.state()
	if e != nil {
		return out, e
	}
	if limit == 1 {
		n := 0
		for _, a := range s.Arrivals {
			if strings.HasPrefix(a.Case, "one-") {
				n++
			}
		}
		if s.Active != 1 || n != 1 {
			return out, errors.New("one_slot_overlap_detected")
		}
	} else if s.Active != 2 || countCase(s, "two") != 2 {
		return out, errors.New("two_slot_barrier_or_third_waiter_invalid")
	}
	if e = p.release(c); e != nil {
		return out, e
	}
	if limit == 1 {
		if _, e = p.wait(ctx, "one-second", 1); e != nil {
			return out, e
		}
		if e = p.release("one-second"); e != nil {
			return out, e
		}
	}
	channels := []<-chan resultOrError{first, second}
	if third != nil {
		channels = append(channels, third)
	}
	for _, ch := range channels {
		r, err := receive(ctx, ch)
		if err != nil {
			return out, err
		}
		out.Requests = append(out.Requests, r)
	}
	if e = ensureSuccess(out.Requests...); e != nil {
		return out, e
	}
	s, e = p.state()
	if e != nil {
		return out, e
	}
	count := countCase(s, "two")
	if limit == 1 {
		count = countCase(s, "one-first") + countCase(s, "one-second")
	}
	wantCount := limit + 1
	if s.MaxActive != limit || count != wantCount || s.Active != 0 {
		return out, errors.New("fixture_evidence_invalid")
	}
	if e = p.Peer.AssertClean(ctx); e != nil {
		return out, e
	}
	out.Limit = limit
	out.MaxOriginActive = s.MaxActive
	return out, nil
}

func (p *Profiles) Cancellation(ctx context.Context) ([]RequestResult, error) {
	ctx, cancel := context.WithTimeout(ctx, 35*time.Second)
	defer cancel()
	first := p.start(ctx, "cancelled", "cancel", 6000)
	second := p.start(ctx, "survivor", "cancel", 25000)
	if _, err := p.wait(ctx, "cancel", 2); err != nil {
		return nil, err
	}
	a, err := receive(ctx, first)
	if err != nil {
		return nil, err
	}
	if err = p.release("cancel"); err != nil {
		return nil, err
	}
	b, err := receive(ctx, second)
	if err != nil {
		return nil, err
	}
	if (a.Outcome != "failed" && a.Outcome != "rejected") || (a.Error != "native_browser_timeout" && a.Error != "cancelled") || ensureSuccess(b) != nil {
		return nil, errors.New("cancellation_isolation_failed")
	}
	if err = p.Peer.AssertClean(ctx); err != nil {
		return nil, err
	}
	return []RequestResult{a, b}, nil
}

func (p *Profiles) UpstreamError(ctx context.Context) (RequestResult, error) {
	r, err := p.Worker.Request(ctx, "upstream-error", "error", 25000, "error/error")
	if err != nil {
		return r, err
	}
	if r.Outcome != "rejected" || r.Error != "native_browser_http_rejected" || r.UpstreamStatus == nil || *r.UpstreamStatus != 503 {
		return r, errors.New("upstream_error_classification_invalid")
	}
	if err = p.Peer.AssertClean(ctx); err != nil {
		return r, err
	}
	return r, nil
}
