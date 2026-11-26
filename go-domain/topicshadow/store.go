package topicshadow

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var (
	errLeaseLost   = errors.New("topic shadow lease lost")
	errDailyLimit  = errors.New("topic shadow daily request limit reached")
	errArticleBusy = errors.New("topic shadow article busy")
)

type queuedArticle struct {
	ArticleInput
	Claim          string
	ReadyAt        time.Time
	Attempts       int
	SnapshotAnchor bool
}

type shadowTopic struct {
	TopicCard
	State         LifecycleState
	Revision      int64
	FirstSourceAt time.Time
}

func (r Runner) acquire(ctx context.Context, now time.Time) (bool, error) {
	tag, err := r.Pool.Exec(ctx, `INSERT INTO topic_shadow_checkpoints
		(policy_version, started_at, scan_after, lease_owner, lease_until)
		VALUES ($1,$2,$3,$4,clock_timestamp() + interval '13 minutes')
		ON CONFLICT (policy_version) DO UPDATE SET lease_owner=EXCLUDED.lease_owner,lease_until=EXCLUDED.lease_until
		WHERE topic_shadow_checkpoints.lease_until IS NULL OR topic_shadow_checkpoints.lease_until <= clock_timestamp()`,
		r.Version, now, r.scanStart(now), r.Owner)
	return err == nil && tag.RowsAffected() == 1, err
}

func (r Runner) scanStart(now time.Time) time.Time {
	if r.Semantic != nil && !r.WarmupFrom.IsZero() {
		return r.WarmupFrom
	}
	return scanStart(now, r.replay)
}

func scanStart(now time.Time, replay bool) time.Time {
	if replay {
		return now.AddDate(0, 0, -30)
	}
	return now
}

func (r Runner) fenced(ctx context.Context, action func(pgx.Tx) error) error {
	tx, err := r.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var locked bool
	err = tx.QueryRow(ctx, `SELECT true FROM topic_shadow_checkpoints
		WHERE policy_version=$1 AND lease_owner=$2 AND lease_until > clock_timestamp() FOR UPDATE`, r.Version, r.Owner).Scan(&locked)
	if errors.Is(err, pgx.ErrNoRows) {
		return errLeaseLost
	}
	if err != nil {
		return err
	}
	if err = action(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// enqueue scans the forward cursor and reconciles late commits before it. The
// cursor and work are durable together. Only eligible public projection fields
// enter the decision input, and the text is bounded at the database boundary.
func (r Runner) enqueue(ctx context.Context, cutoff time.Time, limit int) (bool, error) {
	complete := false
	err := r.fenced(ctx, func(tx pgx.Tx) error {
		var after, started time.Time
		var article int64
		var claim string
		if err := tx.QueryRow(ctx, `SELECT scan_after,scan_article_id,scan_claim::text,started_at
			FROM topic_shadow_checkpoints WHERE policy_version=$1`, r.Version).Scan(&after, &article, &claim, &started); err != nil {
			return err
		}
		const projection = `SELECT i.article_id,i.claim_token::text,i.ready_at,i.source_id,left(i.title,240),left(i.summary,650),
			i.published_at,left(coalesce(i.serbian_title,''),240),left(coalesce(i.serbian_summary,''),650)
			FROM topic_shadow_inputs i`
		for _, reconciliation := range []bool{false, true} {
			var rows pgx.Rows
			var err error
			if reconciliation {
				rows, err = tx.Query(ctx, projection+` WHERE ready_at >= $6 AND ready_at <= $7
					AND (ready_at,article_id,claim_token) <= ($1,$2,$3::uuid)
					AND NOT EXISTS (SELECT 1 FROM topic_shadow_decisions d WHERE d.policy_version=$4 AND d.article_id=i.article_id AND d.claim_token=i.claim_token)
					ORDER BY ready_at,article_id,claim_token LIMIT $5`, after, article, claim, r.Version, limit+1, r.scanStart(started), cutoff)
			} else {
				rows, err = tx.Query(ctx, projection+` WHERE (ready_at,article_id,claim_token) > ($1,$2,$3::uuid)
					AND ready_at <= $4 ORDER BY ready_at,article_id,claim_token LIMIT $5`, after, article, claim, cutoff, limit+1)
			}
			if err != nil {
				return err
			}
			items, err := pgx.CollectRows(rows, scanInput)
			if err != nil {
				return err
			}
			drained := len(items) <= limit
			for _, item := range items[:min(len(items), limit)] {
				if _, err = tx.Exec(ctx, `INSERT INTO topic_shadow_decisions(policy_version,article_id,claim_token,ready_at,input)
					VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING`, r.Version, item.ArticleID, item.Claim, item.ReadyAt, item.ArticleInput); err != nil {
					return err
				}
				if !reconciliation {
					after, article, claim = item.ReadyAt, item.ArticleID, item.Claim
				}
			}
			if reconciliation {
				complete = complete && drained
			} else {
				complete = drained
			}
		}
		_, err := tx.Exec(ctx, `UPDATE topic_shadow_checkpoints SET scan_after=$2,scan_article_id=$3,scan_claim=$4,
			covered_through=CASE WHEN $5 THEN greatest(covered_through,$6) ELSE covered_through END WHERE policy_version=$1`,
			r.Version, after, article, claim, complete, cutoff)
		return err
	})
	return complete, err
}

func scanInput(row pgx.CollectableRow) (queuedArticle, error) {
	var item queuedArticle
	err := row.Scan(&item.ArticleID, &item.Claim, &item.ReadyAt, &item.SourceID, &item.Title, &item.Summary,
		&item.PublishedAt, &item.SerbianTitle, &item.SerbianSummary)
	return item, err
}

func (r Runner) next(ctx context.Context, cutoff, retryClock time.Time) (queuedArticle, bool, error) {
	var item queuedArticle
	var nextAttempt *time.Time
	var input json.RawMessage
	err := r.Pool.QueryRow(ctx, `SELECT article_id,claim_token::text,ready_at,input,attempts,next_attempt_at
		FROM topic_shadow_decisions WHERE policy_version=$1 AND state IN ('queued','retry') AND ready_at <= $2
		ORDER BY ready_at,article_id,claim_token LIMIT 1`, r.Version, cutoff).Scan(&item.ArticleID, &item.Claim, &item.ReadyAt, &input, &item.Attempts, &nextAttempt)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && nextAttempt != nil && nextAttempt.After(retryClock)) {
		return item, false, nil
	}
	if err != nil {
		return item, false, err
	}
	err = json.Unmarshal(input, &item.ArticleInput)
	return item, err == nil, err
}

// rankedTopic keeps the activity visible at the article's cutoff alongside
// the current revision, which finish uses for the write fence.
type rankedTopic struct {
	shadowTopic
	activity time.Time
	status   LifecycleStatus
}

func (r Runner) topics(ctx context.Context, at time.Time) ([]rankedTopic, error) {
	rows, err := r.Pool.Query(ctx, `SELECT t.topic_id::text,t.revision,t.state,t.card,t.first_source_at,t.first_available_at,
		t.opened_at,t.last_material_at,t.quieted_at,t.material_count, activity.at,
		CASE WHEN t.opened_at IS NULL OR t.opened_at > $2 THEN 'candidate'
		     WHEN activity.at <= $2 - interval '168 hours' THEN 'dormant'
		     ELSE 'active' END
		FROM topic_shadow_topics t
		LEFT JOIN LATERAL (
			SELECT max(e.available_at) AS at FROM topic_shadow_events e
			WHERE e.policy_version=t.policy_version AND e.topic_id=t.topic_id
			  AND e.kind IN ('opened','updated','returned') AND e.available_at <= $2
		) history ON t.last_material_at > $2
		CROSS JOIN LATERAL (SELECT coalesce(
			CASE WHEN t.last_material_at <= $2 THEN t.last_material_at ELSE history.at END,
			t.first_available_at) AS at) activity
		WHERE t.policy_version=$1 AND t.first_available_at <= $2
		  AND coalesce(t.last_material_at,t.first_available_at) >= $2 - interval '30 days'
		  AND activity.at >= $2 - interval '30 days'
		ORDER BY activity.at DESC,t.topic_id LIMIT 3000`, r.Version, at)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(row pgx.CollectableRow) (rankedTopic, error) {
		var topic rankedTopic
		var err error
		topic.shadowTopic, err = scanTopicFields(row, &topic.activity, &topic.status)
		return topic, err
	})
}

func scanTopic(row pgx.CollectableRow) (shadowTopic, error) {
	return scanTopicFields(row)
}

func scanTopicFields(row pgx.CollectableRow, extra ...any) (shadowTopic, error) {
	var t shadowTopic
	var status string
	var card json.RawMessage
	var opened, last, quieted *time.Time
	fields := []any{&t.State.CandidateID, &t.Revision, &status, &card, &t.FirstSourceAt, &t.State.FirstAvailableAt,
		&opened, &last, &quieted, &t.State.MaterialCount}
	err := row.Scan(append(fields, extra...)...)
	if err == nil {
		err = json.Unmarshal(card, &t.TopicCard)
	}
	t.ID = t.State.CandidateID
	t.State.Status = LifecycleStatus(status)
	if opened != nil {
		t.State.OpenedAt = *opened
	}
	if last != nil {
		t.State.LastMaterialAvailableAt = *last
	}
	if quieted != nil {
		t.State.QuietedAt = *quieted
	}
	return t, err
}

// Old decisions predate the applied flag. Recover only historical decisions
// that were not explicitly excluded by the late-arrival guard.
const appliedMaterial = `coalesce((decision->>'material_applied')::boolean,
	decision->>'novelty'='material_update' AND NOT (decision ? 'late_historical'))`

func (r Runner) facts(ctx context.Context, topic string, item queuedArticle) ([]string, error) {
	rows, err := r.Pool.Query(ctx, `SELECT coalesce(decision->>'fact','') FROM topic_shadow_decisions
		WHERE policy_version=$1 AND topic_id=$2 AND state='accepted' AND (ready_at,article_id,claim_token) < ($3,$4,$5::uuid)
		AND `+appliedMaterial+` AND coalesce(decision->>'fact','') <> ''
		ORDER BY ready_at DESC,article_id DESC LIMIT 24`, r.Version, topic, item.ReadyAt, item.ArticleID, item.Claim)
	if err != nil {
		return nil, err
	}
	facts, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil || len(facts) <= 6 {
		return facts, err
	}
	terms := tokenize(articleText(item.ArticleInput))
	best, overlap := 5, -1
	for i := 5; i < len(facts); i++ {
		score := 0
		for term := range tokenize(facts[i]) {
			if _, present := terms[term]; present {
				score++
			}
		}
		if score > overlap {
			best, overlap = i, score
		}
	}
	return append(facts[:5], facts[best]), nil
}

func (r Runner) compareMaterial(ctx context.Context, tx pgx.Tx, topic string, item queuedArticle, fact string) (repeat, distinctSource bool, err error) {
	rows, err := tx.Query(ctx, `SELECT decision->>'fact', (input->>'SourceID')::bigint
		FROM topic_shadow_decisions
		WHERE policy_version=$1 AND topic_id=$2 AND state='accepted' AND `+appliedMaterial+`
		  AND ready_at BETWEEN $3::timestamptz - interval '30 days' AND $3
		  AND coalesce(decision->>'fact','') <> ''`, r.Version, topic, item.ReadyAt)
	if err != nil {
		return false, false, err
	}
	defer rows.Close()
	fingerprint := normalizedPhrase(fact)
	for rows.Next() {
		var known string
		var source *int64
		if err := rows.Scan(&known, &source); err != nil {
			return false, false, err
		}
		if fingerprint != "" && fingerprint == normalizedPhrase(known) {
			return true, false, nil
		}
		if source != nil && item.SourceID != nil && *source != *item.SourceID {
			distinctSource = true
		}
	}
	return false, distinctSource, rows.Err()
}

func (r Runner) current(ctx context.Context, tx pgx.Tx, item queuedArticle) (bool, error) {
	var current bool
	err := tx.QueryRow(ctx, `SELECT true FROM articles a
		WHERE a.id=$1 AND a.completed_processing_claim=$2 AND EXISTS (
			SELECT 1 FROM topic_shadow_inputs i WHERE i.article_id=a.id AND i.claim_token=a.completed_processing_claim
		) FOR SHARE OF a NOWAIT`, item.ArticleID, item.Claim).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "55P03" {
		return false, errArticleBusy
	}
	return current, err
}

func nullableTime(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	return &t
}

func (r Runner) persistTopic(ctx context.Context, tx pgx.Tx, t shadowTopic, next LifecycleResult, item *queuedArticle, recordedAt time.Time) error {
	firstAvailable := next.State.FirstAvailableAt
	if firstAvailable.IsZero() && item != nil {
		firstAvailable = item.ReadyAt
	}
	var tag int64
	if t.Revision == 0 {
		out, err := tx.Exec(ctx, `INSERT INTO topic_shadow_topics(policy_version,topic_id,revision,state,card,first_source_at,
			first_available_at,opened_at,last_material_at,quieted_at,material_count)
			VALUES ($1,$2,1,$3,$4,$5,$6,$7,$8,$9,$10) ON CONFLICT DO NOTHING`,
			r.Version, t.ID, next.State.Status, t.TopicCard, t.FirstSourceAt, firstAvailable, nullableTime(next.State.OpenedAt),
			nullableTime(next.State.LastMaterialAvailableAt), nullableTime(next.State.QuietedAt), next.State.MaterialCount)
		if err != nil {
			return err
		}
		tag = out.RowsAffected()
	} else {
		out, err := tx.Exec(ctx, `UPDATE topic_shadow_topics SET revision=revision+1,state=$4,first_source_at=$5,
			opened_at=$6,last_material_at=$7,quieted_at=$8,material_count=$9
			WHERE policy_version=$1 AND topic_id=$2 AND revision=$3`,
			r.Version, t.ID, t.Revision, next.State.Status, t.FirstSourceAt, nullableTime(next.State.OpenedAt),
			nullableTime(next.State.LastMaterialAvailableAt), nullableTime(next.State.QuietedAt), next.State.MaterialCount)
		if err != nil {
			return err
		}
		tag = out.RowsAffected()
	}
	if tag != 1 {
		return errLeaseLost
	}
	for _, change := range next.Journal {
		var articleID any
		var claim any
		if item != nil && change.Kind != "quieted" {
			articleID, claim = item.ArticleID, item.Claim
		}
		key := change.Kind + ":" + change.AvailableAt.UTC().Format(time.RFC3339Nano) + ":" + change.FactID
		if _, err := tx.Exec(ctx, `INSERT INTO topic_shadow_events(policy_version,topic_id,topic_revision,event_key,kind,
			event_at,available_at,recorded_at,article_id,claim_token,evidence)
			VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)`, r.Version, t.ID, t.Revision+1, key, change.Kind,
			nullableTime(change.EventAt), change.AvailableAt, recordedAt, articleID, claim, change); err != nil {
			return err
		}
	}
	return nil
}
