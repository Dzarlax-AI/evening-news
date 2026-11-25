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
	normalized := make([]time.Time, len(cutoffs))
	for i, cutoff := range cutoffs {
		normalized[i] = cutoff.UTC().Truncate(time.Microsecond)
		if normalized[i].IsZero() || (i > 0 && !normalized[i].After(normalized[i-1])) {
			return nil, errors.New("topic shadow replay cutoffs must strictly increase")
		}
	}
	r.replay = true
	reports := make([]PassReport, 0, len(cutoffs))
	for _, cutoff := range normalized {
		var aggregate PassReport
		for {
			pass, err := r.RunPass(ctx, cutoff, 50)
			aggregate.Scanned = pass.Scanned
			aggregate.Processed += pass.Processed
			aggregate.Reviewed += pass.Reviewed
			aggregate.Deferred = pass.Deferred
			aggregate.DeferredReason = pass.DeferredReason
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

var (
	errReplayBackfill   = errors.New("topic shadow replay backfill requires a fresh policy version")
	errReplayInProgress = errors.New("topic shadow replay must finish its pending cutoff before moving forward")
)

// checkReplayCutoff runs under the policy lease. Saved cutoffs can be exported
// again, but inserting an earlier edition would require replaying topic state.
func (r Runner) checkReplayCutoff(ctx context.Context, cutoff time.Time) (bool, error) {
	var saved bool
	err := r.fenced(ctx, func(tx pgx.Tx) error {
		var inProgress *time.Time
		var behind bool
		err := tx.QueryRow(ctx, `SELECT EXISTS (
			SELECT 1 FROM topic_shadow_editions WHERE policy_version=$1 AND cutoff_at=$2
		), coalesce(greatest(c.started_at, c.covered_through, c.scan_after, (
			SELECT max(e.cutoff_at) FROM topic_shadow_editions e WHERE e.policy_version=$1
		)) > $2, false), c.replay_in_progress_at
		FROM topic_shadow_checkpoints c WHERE c.policy_version=$1`, r.Version, cutoff).Scan(&saved, &behind, &inProgress)
		if err != nil || saved {
			return err
		}
		if behind {
			return errReplayBackfill
		}
		if inProgress != nil && !inProgress.Equal(cutoff) {
			return errReplayInProgress
		}
		_, err = tx.Exec(ctx, `UPDATE topic_shadow_checkpoints SET replay_in_progress_at=$2 WHERE policy_version=$1`, r.Version, cutoff)
		return err
	})
	return saved, err
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
