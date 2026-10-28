package callbacks

import (
	"context"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/jackc/pgx/v5"
)

// Claim identifies a processing item. Fence must be obtained from the trusted
// execution service and checked against the authenticated callback envelope.
// Call Validate before compute and again after compute, including on replay.
type Claim struct {
	Fence                                        authority.Fence
	RunID, EnvelopeDigest, InputFingerprint, URL string
	ArticleID                                    int64
	ArticleAttempt                               int
}

// Validate takes a short transaction only for the checks; no network or model
// call may run while it is open. The caller must perform a fresh validation
// before returning any computed or cached response.
func (c Claim) Validate(ctx context.Context) error {
	f := c.Fence
	if f.Pool == nil || c.RunID == "" || c.EnvelopeDigest == "" || c.InputFingerprint == "" || c.URL == "" || c.ArticleID <= 0 || c.ArticleAttempt <= 0 {
		return authority.ErrStale
	}
	tx, err := f.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	return c.ValidateTx(ctx, tx, false)
}

// ValidateTx retains row locks through a fenced learning write when requested.
// The caller owns the transaction and must never perform network work inside it.
func (c Claim) ValidateTx(ctx context.Context, tx pgx.Tx, lockItem bool) error {
	f := c.Fence
	if f.Pool == nil || c.RunID == "" || c.EnvelopeDigest == "" || c.InputFingerprint == "" || c.URL == "" || c.ArticleID <= 0 || c.ArticleAttempt <= 0 {
		return authority.ErrStale
	}
	if err := f.Validate(ctx, tx); err != nil {
		return err
	}
	var id, state string
	err := tx.QueryRow(ctx, `SELECT run_id::text,run_state FROM fence_article_processing_attempt($1::uuid,$2::uuid,$3,$4,$5::uuid,$6,$7,$8::uuid,$9::timestamp)`, c.RunID, f.JobID, f.AttemptGeneration, c.EnvelopeDigest, f.OwnerID, f.Generation, f.WorkerBackendPID, f.ExecutorInstanceID, f.Deadline.UTC().Truncate(time.Microsecond)).Scan(&id, &state)
	if err != nil || id != c.RunID || state != "running" {
		return authority.ErrStale
	}
	var attempt, processingAttempts int
	var fingerprint, itemState, articleURL string
	query := `SELECT i.article_attempt,i.input_fingerprint,i.state,a.processing_attempts,a.url
 FROM article_processing_items i JOIN articles a ON a.id=i.article_id
 WHERE i.run_id=$1::uuid AND i.article_id=$2`
	if lockItem {
		query += ` FOR UPDATE OF i,a`
	}
	err = tx.QueryRow(ctx, query, c.RunID, c.ArticleID).Scan(&attempt, &fingerprint, &itemState, &processingAttempts, &articleURL)
	if err != nil || attempt != c.ArticleAttempt || fingerprint != c.InputFingerprint || itemState != "running" || processingAttempts != c.ArticleAttempt || articleURL != c.URL {
		return authority.ErrStale
	}
	return nil
}
