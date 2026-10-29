package digestworkflow

import (
	"context"
	"encoding/json"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
)

// Generate persists a complete immutable edition. No transaction or pooled
// connection remains held during the provider call.
func (w Workflow) Generate(ctx context.Context, job string, attempt int, operation string, cutoff time.Time) (int, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	if err = w.Fence.Validate(ctx, tx); err != nil {
		tx.Rollback(ctx)
		return 0, err
	}
	var exists bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM editorial_digest_editions WHERE job_id=$1 AND attempt_generation=$2 AND locale='ru')`, job, attempt).Scan(&exists)
	if err != nil {
		tx.Rollback(ctx)
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	if exists {
		return 1, nil
	}
	id := EditionID(job, attempt, operation)
	snap, snapshot, err := w.Snapshot(ctx, cutoff, id)
	if err != nil {
		return 0, err
	}
	if len(snap.Articles) == 0 {
		return 0, nil
	}
	provider := w.Provider
	if w.ResolveProvider != nil {
		provider, err = w.ResolveProvider(ctx)
		if err != nil {
			return 0, err
		}
	}
	providerCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	generation, err := editorial.Generate(providerCtx, snap, "ru", provider)
	cancel()
	if err != nil {
		return 0, err
	}
	rendered, err := editorial.Render(snap, "ru", generation.Edition)
	if err != nil {
		return 0, err
	}
	editionJSON, err := json.Marshal(generation.Edition)
	if err != nil {
		return 0, ErrInvalid
	}
	renderJSON, err := json.Marshal(rendered)
	if err != nil {
		return 0, ErrInvalid
	}
	if w.PriceUsage != nil {
		generation.Usage.Cost = w.PriceUsage(generation.Usage)
	}
	usageJSON, err := json.Marshal(generation.Usage)
	if err != nil {
		return 0, ErrInvalid
	}
	tx, err = w.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return 0, err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO editorial_digest_editions (id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,provider_usage,job_id,attempt_generation)
 VALUES ($1,$2::date,'ru','ru-v2-summaries',$3,$4::jsonb,$5::jsonb,$6::jsonb,$7::jsonb,$8,$9)
 ON CONFLICT (job_id,attempt_generation,locale) DO NOTHING`, id, snap.Date, generation.Edition.SourceSnapshotHash, snapshot, editionJSON, renderJSON, usageJSON, job, attempt)
	if err != nil {
		return 0, err
	}
	if tag.RowsAffected() == 1 && w.RecordUsage != nil {
		if err = w.RecordUsage(ctx, tx, generation.Usage); err != nil {
			return 0, err
		}
	}
	if tag.RowsAffected() == 0 {
		var found bool
		if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM editorial_digest_editions WHERE job_id=$1 AND attempt_generation=$2 AND locale='ru')`, job, attempt).Scan(&found); err != nil {
			return 0, err
		}
		if !found {
			return 0, ErrInvalid
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return 1, nil
}
