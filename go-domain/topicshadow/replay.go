package topicshadow

import (
	"context"
	"errors"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Replay runs explicit release cutoffs against an isolated loopback fixture.
// Populate the fixture with immutable article versions and real readiness
// receipts before using it for evaluation. Replay records logical times.
func (r Runner) Replay(ctx context.Context, cutoffs []time.Time) ([]PassReport, error) {
	if r.Pool == nil || !strings.HasPrefix(r.Version, "replay-") || len(cutoffs) == 0 {
		return nil, errors.New("topic shadow replay requires an isolated version and cutoffs")
	}
	if !isLoopbackReplayDatabase(r.Pool.Config().ConnConfig) {
		return nil, errors.New("topic shadow replay requires an isolated loopback database")
	}
	for i, cutoff := range cutoffs {
		if cutoff.IsZero() || (i > 0 && !cutoff.After(cutoffs[i-1])) {
			return nil, errors.New("topic shadow replay cutoffs must strictly increase")
		}
	}
	r.replay = true
	reports := make([]PassReport, 0, len(cutoffs))
	for _, cutoff := range cutoffs {
		var aggregate PassReport
		for {
			pass, err := r.RunPass(ctx, cutoff, 50)
			aggregate.Scanned = pass.Scanned
			aggregate.Processed += pass.Processed
			aggregate.Reviewed += pass.Reviewed
			aggregate.Deferred = pass.Deferred
			if err != nil {
				return append(reports, aggregate), err
			}
			if pass.Scanned && !pass.Deferred {
				break
			}
			if pass.Processed == 0 {
				return append(reports, aggregate), errors.New("topic shadow replay cannot complete the cutoff")
			}
		}
		reports = append(reports, aggregate)
	}
	return reports, nil
}

// pgx may add a non-TLS fallback on the same host under sslmode=prefer.
func isLoopbackReplayDatabase(config *pgx.ConnConfig) bool {
	if !net.ParseIP(config.Host).IsLoopback() {
		return false
	}
	for _, fallback := range config.Fallbacks {
		if fallback == nil || !net.ParseIP(fallback.Host).IsLoopback() {
			return false
		}
	}
	return true
}
