package topicshadow

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
)

var (
	errLeaseLost  = errors.New("topic shadow lease lost")
	errDailyLimit = errors.New("topic shadow daily request limit reached")
)

type queuedArticle struct {
	ArticleInput
	Claim    string
	ReadyAt  time.Time
	Attempts int
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
		VALUES ($1,$2::timestamptz,$2::timestamptz - interval '30 days',$3,clock_timestamp() + interval '13 minutes')
		ON CONFLICT (policy_version) DO UPDATE SET lease_owner=EXCLUDED.lease_owner,lease_until=EXCLUDED.lease_until
		WHERE topic_shadow_checkpoints.lease_until IS NULL OR topic_shadow_checkpoints.lease_until <= clock_timestamp()`,
		r.Version, now, r.Owner)
	return err == nil && tag.RowsAffected() == 1, err
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
		var after time.Time
		var article int64
		var claim string
		if err := tx.QueryRow(ctx, `SELECT scan_after,scan_article_id,scan_claim::text
			FROM topic_shadow_checkpoints WHERE policy_version=$1`, r.Version).Scan(&after, &article, &claim); err != nil {
			return err
		}
		const projection = `SELECT i.article_id,i.claim_token::text,i.ready_at,i.source_id,left(i.title,240),left(i.summary,650),
			i.published_at,left(coalesce(i.serbian_title,''),240),left(coalesce(i.serbian_summary,''),650)
			FROM topic_shadow_inputs i`
		for _, reconciliation := range []bool{false, true} {
			var rows pgx.Rows
			var err error
			if reconciliation {
				rows, err = tx.Query(ctx, projection+` WHERE ready_at >= $1::timestamptz - interval '1 day'
					AND (ready_at,article_id,claim_token) <= ($1,$2,$3::uuid)
					AND NOT EXISTS (SELECT 1 FROM topic_shadow_decisions d WHERE d.policy_version=$4 AND d.article_id=i.article_id AND d.claim_token=i.claim_token)
					ORDER BY ready_at,article_id,claim_token LIMIT $5`, after, article, claim, r.Version, limit+1)
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

func (r Runner) next(ctx context.Context, now time.Time) (queuedArticle, bool, error) {
	var item queuedArticle
	var nextAttempt *time.Time
	var input json.RawMessage
	err := r.Pool.QueryRow(ctx, `SELECT article_id,claim_token::text,ready_at,input,attempts,next_attempt_at
		FROM topic_shadow_decisions WHERE policy_version=$1 AND state IN ('queued','retry') AND ready_at <= $2
		ORDER BY ready_at,article_id,claim_token LIMIT 1`, r.Version, now).Scan(&item.ArticleID, &item.Claim, &item.ReadyAt, &input, &item.Attempts, &nextAttempt)
	if errors.Is(err, pgx.ErrNoRows) || (err == nil && nextAttempt != nil && nextAttempt.After(now)) {
		return item, false, nil
	}
	if err != nil {
		return item, false, err
	}
	err = json.Unmarshal(input, &item.ArticleInput)
	return item, err == nil, err
}

func (r Runner) topics(ctx context.Context, at time.Time) ([]shadowTopic, error) {
	rows, err := r.Pool.Query(ctx, `SELECT topic_id::text,revision,state,card,first_source_at,first_available_at,
		opened_at,last_material_at,quieted_at,material_count
		FROM topic_shadow_topics WHERE policy_version=$1 AND coalesce(last_material_at,first_available_at) >= $2::timestamptz - interval '30 days'
		AND first_available_at <= $2 ORDER BY coalesce(last_material_at,first_available_at) DESC,topic_id LIMIT 3000`, r.Version, at)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, scanTopic)
}

func scanTopic(row pgx.CollectableRow) (shadowTopic, error) {
	var t shadowTopic
	var status string
	var card json.RawMessage
	var opened, last, quieted *time.Time
	err := row.Scan(&t.State.CandidateID, &t.Revision, &status, &card, &t.FirstSourceAt, &t.State.FirstAvailableAt,
		&opened, &last, &quieted, &t.State.MaterialCount)
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

func (r Runner) facts(ctx context.Context, topic string, item queuedArticle) ([]string, error) {
	rows, err := r.Pool.Query(ctx, `SELECT coalesce(decision->>'fact','') FROM topic_shadow_decisions
		WHERE policy_version=$1 AND topic_id=$2 AND state='accepted' AND ready_at <= $3
		AND decision->>'novelty' = 'material_update' AND coalesce(decision->>'fact','') <> ''
		ORDER BY ready_at DESC,article_id DESC LIMIT 24`, r.Version, topic, item.ReadyAt)
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

func (r Runner) current(ctx context.Context, tx pgx.Tx, item queuedArticle) (bool, error) {
	var current bool
	err := tx.QueryRow(ctx, `SELECT true FROM articles a
		WHERE a.id=$1 AND a.completed_processing_claim=$2 AND EXISTS (
			SELECT 1 FROM topic_shadow_inputs i WHERE i.article_id=a.id AND i.claim_token=a.completed_processing_claim
		) FOR SHARE OF a NOWAIT`, item.ArticleID, item.Claim).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
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
