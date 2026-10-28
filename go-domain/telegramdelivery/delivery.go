package telegramdelivery

import (
	"context"
	"errors"
)

// Part identifies an immutable payload within a stable edition/destination intent.
// The store must enforce uniqueness of (intent, method, index) and reject a
// different payload hash. It must commit sending before Reserve returns New.
type Part struct {
	Intent, Method, PayloadHash string
	Index                       int
	Payload                     any
}
type Reservation int

const (
	New Reservation = iota
	AlreadySent
	InFlight
	PreviouslyRejected
	RateLimited
)

type Store interface {
	Reserve(context.Context, Part) (Reservation, error)
	Finish(context.Context, Part, Result) error
}
type Sender interface {
	Send(context.Context, string, any) Result
}
type Authority interface{ Check(context.Context) error }
type Delivery struct {
	Store     Store
	Sender    Sender
	Authority Authority
}

var ErrUnknown = errors.New("delivery_unknown")
var ErrRejected = errors.New("delivery_rejected")

// Publish never retries a possibly accepted request. Fallback is used only
// after a definite rich rejection; a partially sent fallback stops on unknown.
func (d Delivery) Publish(ctx context.Context, rich Part, plain []Part) (int, error) {
	r, err := d.part(ctx, rich)
	if err != nil {
		return 0, err
	}
	if r == Sent {
		return 1, nil
	}
	sent := 0
	for _, p := range plain {
		state, err := d.part(ctx, p)
		if err != nil {
			return sent, err
		}
		if state != Sent {
			return sent, ErrRejected
		}
		sent++
	}
	if len(plain) == 0 {
		return 0, ErrRejected
	}
	return sent, nil
}
func (d Delivery) part(ctx context.Context, p Part) (State, error) {
	if err := d.Authority.Check(ctx); err != nil {
		return "", err
	}
	reservation, err := d.Store.Reserve(ctx, p)
	// A failed reservation COMMIT must never lead to a network request.
	if err != nil {
		return "", ErrUnknown
	}
	switch reservation {
	case AlreadySent:
		return Sent, nil
	case InFlight:
		return "", ErrUnknown
	case PreviouslyRejected:
		return Rejected, nil
	case RateLimited:
		return "", ErrRejected
	case New:
	default:
		return "", ErrUnknown
	}
	// A lost lease after the reservation leaves sending as a possible outcome.
	if err = d.Authority.Check(ctx); err != nil {
		return "", ErrUnknown
	}
	result := d.Sender.Send(ctx, p.Method, p.Payload)
	if err = d.Authority.Check(ctx); err != nil {
		return "", ErrUnknown
	}
	if err = d.Store.Finish(ctx, p, result); err != nil {
		return "", ErrUnknown
	}
	if result.State == Rejected && result.StatusCode == 429 {
		return "", ErrRejected
	}
	if result.State == Unknown {
		return "", ErrUnknown
	}
	if result.State != Sent && result.State != Rejected {
		return "", ErrUnknown
	}
	return result.State, nil
}

// SendPart exposes one receipt-backed part for callers that need to report
// whether rich or plain delivery completed.
func (d Delivery) SendPart(ctx context.Context, p Part) (State, error) { return d.part(ctx, p) }
