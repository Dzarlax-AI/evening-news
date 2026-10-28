package telegramdelivery

import (
	"context"
	"errors"
	"testing"
)

type fakeStore struct {
	reservations []Reservation
	finishErr    error
	finishes     int
}

func (s *fakeStore) Reserve(context.Context, Part) (Reservation, error) {
	r := s.reservations[0]
	s.reservations = s.reservations[1:]
	return r, nil
}
func (s *fakeStore) Finish(context.Context, Part, Result) error { s.finishes++; return s.finishErr }

type fakeSender struct {
	results []State
	calls   int
}

func (s *fakeSender) Send(context.Context, string, any) Result {
	v := s.results[s.calls]
	s.calls++
	return Result{State: v}
}

type fakeAuthority struct{}

func (fakeAuthority) Check(context.Context) error { return nil }
func TestPublishStopsOnAmbiguousPart(t *testing.T) {
	store := &fakeStore{reservations: []Reservation{New, New, New}}
	sender := &fakeSender{results: []State{Rejected, Sent, Unknown}}
	d := Delivery{store, sender, fakeAuthority{}}
	count, err := d.Publish(context.Background(), Part{}, []Part{{}, {}, {}})
	if count != 1 || !errors.Is(err, ErrUnknown) || sender.calls != 3 {
		t.Fatalf("count=%d err=%v calls=%d", count, err, sender.calls)
	}
}
func TestPublishNeverReplaysSendingOrAfterLostCommit(t *testing.T) {
	for _, tc := range []struct {
		reservation Reservation
		finishErr   error
		calls       int
	}{{InFlight, nil, 0}, {New, errors.New("commit unknown"), 1}} {
		store := &fakeStore{reservations: []Reservation{tc.reservation}, finishErr: tc.finishErr}
		sender := &fakeSender{results: []State{Sent}}
		_, err := (Delivery{store, sender, fakeAuthority{}}).Publish(context.Background(), Part{}, nil)
		if !errors.Is(err, ErrUnknown) || sender.calls != tc.calls {
			t.Fatalf("err=%v calls=%d", err, sender.calls)
		}
	}
}

func TestPublishRateLimitDoesNotFallback(t *testing.T) {
	store := &fakeStore{reservations: []Reservation{RateLimited}}
	sender := &fakeSender{}
	_, err := (Delivery{store, sender, fakeAuthority{}}).Publish(context.Background(), Part{}, []Part{{}})
	if !errors.Is(err, ErrRejected) || sender.calls != 0 {
		t.Fatalf("err=%v calls=%d", err, sender.calls)
	}
}

func TestRichRejectionFallsBackButUnknownNeverDoes(t *testing.T) {
	for _, tc := range []struct {
		rich      State
		wantCalls int
		wantError bool
	}{
		{Rejected, 2, false}, {Unknown, 1, true},
	} {
		store := &fakeStore{reservations: []Reservation{New, New}}
		sender := &fakeSender{results: []State{tc.rich, Sent}}
		count, err := (Delivery{store, sender, fakeAuthority{}}).Publish(context.Background(), Part{}, []Part{{}})
		if sender.calls != tc.wantCalls || (err != nil) != tc.wantError || !tc.wantError && count != 1 {
			t.Fatalf("count=%d calls=%d err=%v", count, sender.calls, err)
		}
	}
}
