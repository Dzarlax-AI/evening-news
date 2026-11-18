package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

type DedupObservation struct {
	Version   int16
	ExactHash []byte
}

type DedupArticle struct {
	ID          int64
	SourceID    *int64
	FetchedAt   *time.Time
	SourceTitle string
	Title       string
	Summary     string
	Russian     DedupLocalizedText
	Serbian     DedupLocalizedText
}

type DedupLocalizedText struct {
	Title   string
	Summary string
}

const (
	DedupCandidatePool = 128
	maxDedupWindow     = 7 * 24 * time.Hour
	dedupEligibleSQL   = `a.processing_state='completed' AND a.processed IS TRUE
		AND a.is_advertisement IS NOT TRUE AND a.summary_processed IS TRUE AND btrim(a.summary)<>''`
)

// RecordDedupObservation records an already committed, admitted attempt. The
// returned article ID is the current exact representative, or zero when no
// exact fingerprint is available. The result has no effect on visibility.
func (s *Store) RecordDedupObservation(ctx context.Context, f Fence, articleID int64, attempt int, token, inputFingerprint string, observation DedupObservation) (int64, bool, error) {
	if observation.Version < 1 || (observation.ExactHash != nil && len(observation.ExactHash) != sha256.Size) {
		return 0, false, errors.New("invalid dedup observation")
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, false, errors.New("dedup observation transaction unavailable")
	}
	defer rollback(tx)
	if _, state, err := s.FenceTx(ctx, tx, f); err != nil {
		return 0, false, err
	} else if state != "running" {
		return 0, false, ErrCancelled
	}
	if observation.ExactHash != nil {
		// Serialize first registration for a fingerprint across replicas. The
		// two-key advisory lock is shared only by this version and exact hash.
		if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext('article-dedup-trial-' || $1::smallint::text), hashtext(encode($2::bytea,'hex')))`,
			observation.Version, observation.ExactHash); err != nil {
			return 0, false, errors.New("dedup fingerprint lock unavailable")
		}
	}
	var eligibleID int64
	err = tx.QueryRow(ctx, `SELECT a.id FROM articles a JOIN article_processing_items i ON i.article_id=a.id
		WHERE a.id=$1 AND a.processing_attempts=$2 AND a.completed_processing_claim=$4::uuid AND `+dedupEligibleSQL+`
		AND i.run_id=$3::uuid AND i.article_attempt=$2 AND i.claim_token=$4::uuid AND i.input_fingerprint=$5
		AND i.state='completed' FOR SHARE OF a, i`, articleID, attempt, f.RunID, token, inputFingerprint).Scan(&eligibleID)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, errors.New("dedup admission check failed")
	}
	_, err = tx.Exec(ctx, `INSERT INTO article_dedup_observations(article_id, version, article_attempt, claim_token, exact_hash)
		VALUES ($1,$2,$3,$4::uuid,$5)
		ON CONFLICT (article_id,version) DO UPDATE SET
			article_attempt=EXCLUDED.article_attempt, claim_token=EXCLUDED.claim_token, exact_hash=EXCLUDED.exact_hash,
			registered_at=CASE WHEN article_dedup_observations.exact_hash IS NOT DISTINCT FROM EXCLUDED.exact_hash
				THEN article_dedup_observations.registered_at ELSE EXCLUDED.registered_at END
		WHERE article_dedup_observations.claim_token <> EXCLUDED.claim_token`, eligibleID, observation.Version, attempt, token, observation.ExactHash)
	if err != nil {
		return 0, false, errors.New("dedup observation write failed")
	}
	// Use the stored hash even on replay, where the current attempt's immutable
	// observation may have already been recorded.
	var representative int64
	err = tx.QueryRow(ctx, `SELECT candidate.article_id
		FROM article_dedup_observations subject
		JOIN article_dedup_observations candidate ON candidate.version=subject.version AND candidate.exact_hash=subject.exact_hash
		JOIN articles a ON a.id=candidate.article_id AND a.completed_processing_claim=candidate.claim_token
		WHERE subject.article_id=$1 AND subject.version=$2 AND subject.claim_token=$3::uuid AND `+dedupEligibleSQL+`
		ORDER BY candidate.registered_at, candidate.article_id LIMIT 1`, eligibleID, observation.Version, token).Scan(&representative)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, errors.New("dedup representative lookup failed")
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, false, errors.New("dedup observation commit unconfirmed")
	}
	return representative, true, nil
}

// LoadDedupTrial selects a bounded set of eligible articles around the subject's
// ingestion time. The search spans sources and considers both supported locales.
func (s *Store) LoadDedupTrial(ctx context.Context, articleID int64, token string, version int16, window time.Duration) (DedupArticle, []DedupArticle, bool, error) {
	if window < time.Second || window > maxDedupWindow {
		return DedupArticle{}, nil, false, errors.New("invalid dedup trial window")
	}
	rows, err := s.Pool.Query(ctx, `WITH subject AS (
		SELECT a.id, a.source_id, a.fetched_at, a.title, a.source_title, a.summary
		FROM articles a JOIN article_dedup_observations d ON d.article_id=a.id AND d.version=$3 AND d.claim_token=a.completed_processing_claim
		WHERE a.id=$1 AND a.completed_processing_claim=$2::uuid AND `+dedupEligibleSQL+`
	), candidates AS (
		SELECT a.id, a.source_id, a.fetched_at, a.title, a.source_title, a.summary
		FROM articles a CROSS JOIN subject s
		WHERE a.id<>s.id AND `+dedupEligibleSQL+`
		AND a.fetched_at BETWEEN s.fetched_at-make_interval(secs => $4) AND s.fetched_at+make_interval(secs => $4)
		ORDER BY a.fetched_at DESC, a.id DESC LIMIT $5
	), selected AS (
		SELECT * FROM subject UNION ALL SELECT * FROM candidates
	)
	SELECT a.id, a.source_id, a.fetched_at, COALESCE(a.source_title,''),
		left(a.title,500), left(a.summary,1800),
		left(COALESCE(ru.title,''),500), left(COALESCE(ru.summary,''),1800),
		left(COALESCE(sr.title,''),500), left(COALESCE(sr.summary,''),1800)
	FROM selected a
	LEFT JOIN article_localizations ru ON ru.article_id=a.id AND ru.locale='ru' AND ru.status='completed'
	LEFT JOIN article_localizations sr ON sr.article_id=a.id AND sr.locale='sr-Latn' AND sr.status='completed'
	ORDER BY (a.id=$1) DESC, a.fetched_at DESC, a.id DESC`, articleID, token, version, window.Seconds(), DedupCandidatePool+1)
	if err != nil {
		return DedupArticle{}, nil, false, errors.New("dedup trial lookup failed")
	}
	defer rows.Close()
	var current DedupArticle
	var candidates []DedupArticle
	truncated := false
	for rows.Next() {
		var a DedupArticle
		if err = rows.Scan(&a.ID, &a.SourceID, &a.FetchedAt, &a.SourceTitle, &a.Title, &a.Summary,
			&a.Russian.Title, &a.Russian.Summary, &a.Serbian.Title, &a.Serbian.Summary); err != nil {
			return DedupArticle{}, nil, false, errors.New("dedup candidate read failed")
		}
		if a.ID == articleID {
			current = a
		} else if len(candidates) < DedupCandidatePool {
			candidates = append(candidates, a)
		} else {
			truncated = true
		}
	}
	if rows.Err() != nil {
		return DedupArticle{}, nil, false, errors.New("dedup candidate read failed")
	}
	return current, candidates, truncated, nil
}
