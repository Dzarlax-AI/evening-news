package topicshadow

import (
	"context"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

// BackfillCursor is a stable (fetch time, article ID) pagination position.
// Persist both the initial upper bound and the last fully indexed position.
type BackfillCursor struct {
	FetchedAt time.Time
	ArticleID int64
}
type BackfillArticle struct {
	ArticleID      int64
	Claim          *string
	Title, Summary string
	FetchedAt      time.Time
	ReadyAt        *time.Time
}
type BackfillQueryer interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// BackfillUpperBound captures the archive horizon; a separate readiness-tail
// pass is necessary for commits and summary changes concurrent with the scan.
func BackfillUpperBound(ctx context.Context, db BackfillQueryer) (BackfillCursor, error) {
	var c BackfillCursor
	err := db.QueryRow(ctx, `SELECT fetched_at AT TIME ZONE 'UTC', id FROM articles WHERE fetched_at IS NOT NULL ORDER BY fetched_at DESC,id DESC LIMIT 1`).Scan(&c.FetchedAt, &c.ArticleID)
	if errors.Is(err, pgx.ErrNoRows) {
		return BackfillCursor{}, nil
	}
	return c, err
}

// BackfillPage reads the retained eligible archive directly from articles,
// including legacy records without an article_processing_items receipt. A nil
// ReadyAt identifies snapshot-only history, not evidence of live availability.
// The caller persists the cursor only after all returned points are durable.
func BackfillPage(ctx context.Context, db BackfillQueryer, after, upper BackfillCursor, limit int) ([]BackfillArticle, error) {
	if limit < 1 || limit > 256 || upper.FetchedAt.IsZero() || upper.ArticleID <= 0 || after.ArticleID < 0 {
		return nil, errors.New("invalid backfill bounds")
	}
	rows, err := db.Query(ctx, `SELECT a.id, i.claim_token::text, a.title, a.summary, a.fetched_at AT TIME ZONE 'UTC', i.ready_at
 FROM articles a LEFT JOIN topic_shadow_inputs i ON i.article_id=a.id AND i.claim_token=a.completed_processing_claim
 WHERE a.fetched_at IS NOT NULL AND a.summary_processed IS TRUE
 AND a.is_advertisement IS NOT TRUE AND nullif(btrim(a.title),'') IS NOT NULL
 AND nullif(btrim(a.summary),'') IS NOT NULL
 AND (a.fetched_at,a.id) > ($1,$2) AND (a.fetched_at,a.id) <= ($3,$4)
 ORDER BY a.fetched_at,a.id LIMIT $5`, after.FetchedAt, after.ArticleID, upper.FetchedAt, upper.ArticleID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]BackfillArticle, 0, limit)
	for rows.Next() {
		var a BackfillArticle
		if err := rows.Scan(&a.ArticleID, &a.Claim, &a.Title, &a.Summary, &a.FetchedAt, &a.ReadyAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}
