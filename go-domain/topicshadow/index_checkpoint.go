package topicshadow

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type IndexCheckpoint struct {
	Pool                      *pgxpool.Pool
	Policy, Collection, Owner string
	EmbeddingLimit            int
}

type IndexProgress struct {
	StartedAt time.Time
	Complete  bool
}

var ErrEmbeddingBudget = errors.New("topic shadow embedding daily limit reached")

// ReserveEmbedding charges each input, so batching keeps the existing daily
// allowance for article embeddings independent of the number of HTTP calls.
func (c IndexCheckpoint) ReserveEmbedding(ctx context.Context, count int) error {
	tag, err := c.Pool.Exec(ctx, `WITH current_day AS (SELECT (clock_timestamp() AT TIME ZONE 'UTC')::date AS d)
		UPDATE topic_shadow_semantic_index SET embedding_day=d,
		embedding_requests=CASE WHEN embedding_day=d THEN embedding_requests+$4::integer ELSE $4::integer END
		FROM current_day WHERE policy_version=$1 AND collection=$2
		AND $4::integer BETWEEN 1 AND $3::integer
		AND (embedding_day IS DISTINCT FROM d OR embedding_requests <= $3::integer-$4::integer)`, c.Policy, c.Collection, c.EmbeddingLimit, count)
	if err == nil && tag.RowsAffected() != 1 {
		return ErrEmbeddingBudget
	}
	return err
}

func (c IndexCheckpoint) RecordEmbeddingTokens(ctx context.Context, tokens int) error {
	_, err := c.Pool.Exec(ctx, `UPDATE topic_shadow_semantic_index SET embedding_tokens=embedding_tokens+$3
		WHERE policy_version=$1 AND collection=$2`, c.Policy, c.Collection, tokens)
	return err
}

// Advance indexes a bounded part of a fixed archive. Index writes are versioned;
// recording the cursor only after successful writes makes retries idempotent.
func (c IndexCheckpoint) Advance(ctx context.Context, index *SemanticIndex, limit int) (IndexProgress, error) {
	var progress IndexProgress
	if c.Pool == nil || c.EmbeddingLimit < 1 || c.Owner == "" || limit < 1 || limit > 250 {
		return progress, errors.New("invalid semantic checkpoint configuration")
	}
	if err := index.CheckCollection(ctx); err != nil {
		return progress, err
	}
	var upper, after BackfillCursor
	var completed *time.Time
	load := func() error {
		return c.Pool.QueryRow(ctx, `SELECT started_at,upper_fetched_at,upper_article_id,cursor_fetched_at,
		cursor_article_id,completed_at FROM topic_shadow_semantic_index WHERE policy_version=$1 AND collection=$2`,
			c.Policy, c.Collection).Scan(&progress.StartedAt, &upper.FetchedAt, &upper.ArticleID, &after.FetchedAt, &after.ArticleID, &completed)
	}
	err := load()
	if errors.Is(err, pgx.ErrNoRows) {
		upper, err = BackfillUpperBound(ctx, c.Pool)
		if err != nil {
			return progress, err
		}
		if upper.ArticleID == 0 {
			upper.FetchedAt = time.Now().UTC()
		}
		_, err = c.Pool.Exec(ctx, `INSERT INTO topic_shadow_semantic_index(policy_version,collection,upper_fetched_at,upper_article_id,cursor_fetched_at)
			VALUES ($1,$2,$3,$4,'0001-01-01 00:00:00+00') ON CONFLICT DO NOTHING`, c.Policy, c.Collection, upper.FetchedAt, upper.ArticleID)
		if err != nil {
			return progress, err
		}
		err = load()
	}
	if err != nil {
		return progress, err
	}
	if completed != nil {
		progress.Complete = true
		return progress, nil
	}
	tag, err := c.Pool.Exec(ctx, `UPDATE topic_shadow_semantic_index SET lease_owner=$3,lease_until=clock_timestamp()+interval '13 minutes'
		WHERE policy_version=$1 AND collection=$2 AND completed_at IS NULL
		AND (lease_until IS NULL OR lease_until<=clock_timestamp())`, c.Policy, c.Collection, c.Owner)
	if err != nil || tag.RowsAffected() == 0 {
		return progress, err
	}
	defer func() {
		clean, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_, _ = c.Pool.Exec(clean, `UPDATE topic_shadow_semantic_index SET lease_owner=NULL,lease_until=NULL
			WHERE policy_version=$1 AND collection=$2 AND lease_owner=$3`, c.Policy, c.Collection, c.Owner)
	}()
	if err = load(); err != nil {
		return progress, err
	}
	var articles []BackfillArticle
	if upper.ArticleID > 0 {
		articles, err = BackfillPage(ctx, c.Pool, after, upper, limit)
		if err != nil {
			return progress, err
		}
	}
	save := func(count int, complete bool) error {
		tag, err := c.Pool.Exec(ctx, `UPDATE topic_shadow_semantic_index SET cursor_fetched_at=$4,cursor_article_id=$5,
			scanned_articles=scanned_articles+$6,completed_at=CASE WHEN $7 THEN clock_timestamp() END
			WHERE policy_version=$1 AND collection=$2 AND lease_owner=$3 AND lease_until>clock_timestamp()`,
			c.Policy, c.Collection, c.Owner, after.FetchedAt, after.ArticleID, count, complete)
		if err == nil && tag.RowsAffected() != 1 {
			return errLeaseLost
		}
		return err
	}
	for chunk := range slices.Chunk(articles, min(semanticEmbeddingBatchSize, c.EmbeddingLimit)) {
		batch := make([]IndexedArticle, 0, len(chunk))
		for _, article := range chunk {
			available := progress.StartedAt
			claim := semanticSnapshotClaim
			if article.ReadyAt != nil {
				available = *article.ReadyAt
				if article.Claim != nil {
					claim = *article.Claim
				}
			}
			batch = append(batch, IndexedArticle{ArticleID: article.ArticleID, Claim: claim,
				Title: truncateUTF8(article.Title, 1024), Summary: truncateUTF8(article.Summary, 12000),
				FetchedAt: article.FetchedAt, AvailableAt: available})
		}
		if _, err = index.indexBatch(ctx, batch); err != nil {
			return progress, err
		}
		last := chunk[len(chunk)-1]
		after = BackfillCursor{FetchedAt: last.FetchedAt, ArticleID: last.ArticleID}
		if err = save(len(chunk), false); err != nil {
			return progress, err
		}
	}
	if len(articles) < limit {
		err = save(0, true)
		progress.Complete = err == nil
	}
	return progress, err
}

// Reconcile removes points whose source has disappeared from the eligible
// corpus. Matching also validates the exact current version before Jev sees it.
func (c IndexCheckpoint) Reconcile(ctx context.Context, index *SemanticIndex, limit int) error {
	if limit < 1 || limit > 200 {
		return errors.New("invalid reconciliation limit")
	}
	var offset string
	if err := c.Pool.QueryRow(ctx, `SELECT coalesce(prune_cursor,'') FROM topic_shadow_semantic_index
		WHERE policy_version=$1 AND collection=$2 AND completed_at IS NOT NULL`,
		c.Policy, c.Collection).Scan(&offset); err != nil {
		return err
	}
	points, next, err := index.reconciliationPage(ctx, offset, limit)
	if err != nil {
		return err
	}
	if len(points) > 0 {
		articleIDs := make([]int64, 0, len(points))
		for _, point := range points {
			articleIDs = append(articleIDs, point.ArticleID)
		}
		rows, err := c.Pool.Query(ctx, `SELECT id FROM articles WHERE id=ANY($1) AND fetched_at IS NOT NULL
			AND summary_processed IS TRUE AND is_advertisement IS NOT TRUE AND nullif(btrim(summary),'') IS NOT NULL
			AND nullif(btrim(title),'') IS NOT NULL`, articleIDs)
		if err != nil {
			return err
		}
		presentIDs, err := pgx.CollectRows(rows, pgx.RowTo[int64])
		if err != nil {
			return err
		}
		present := make(map[int64]bool, len(presentIDs))
		for _, id := range presentIDs {
			present[id] = true
		}
		var obsolete []string
		for _, point := range points {
			if !present[point.ArticleID] {
				obsolete = append(obsolete, point.VersionID)
			}
		}
		if err = index.removePoints(ctx, obsolete); err != nil {
			return err
		}
	}
	_, err = c.Pool.Exec(ctx, `UPDATE topic_shadow_semantic_index SET prune_cursor=nullif($3,'')
		WHERE policy_version=$1 AND collection=$2 AND coalesce(prune_cursor,'')=$4`, c.Policy, c.Collection, next, offset)
	return err
}
