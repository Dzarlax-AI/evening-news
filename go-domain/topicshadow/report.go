package topicshadow

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (r Runner) freezeEditions(ctx context.Context, now time.Time) error {
	return r.fenced(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT DISTINCT e.snapshot->>'cutoff_utc' FROM editorial_digest_editions e
			CROSS JOIN topic_shadow_checkpoints c
			WHERE c.policy_version=$1 AND e.locale='ru'
			AND e.created_at >= (c.started_at AT TIME ZONE 'UTC')
			AND nullif(e.snapshot->>'cutoff_utc','') IS NOT NULL
			ORDER BY 1 DESC LIMIT 45`, r.Version)
		if err != nil {
			return err
		}
		cutoffs, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		slices.Reverse(cutoffs)
		for _, value := range cutoffs {
			cutoff, err := time.Parse(time.RFC3339Nano, value)
			if err != nil || cutoff.After(now) {
				continue
			}
			if err = freezeCutoff(ctx, tx, r.Version, cutoff); err != nil {
				return err
			}
		}
		return nil
	})
}

func freezeCutoff(ctx context.Context, tx pgx.Tx, version string, cutoff time.Time) error {
	_, err := tx.Exec(ctx, `WITH boundary AS (
		SELECT c.covered_through,
			coalesce((SELECT max(e.cutoff_at) FROM topic_shadow_editions e WHERE e.policy_version=$1
				AND e.cutoff_at < $2),c.started_at) AS previous
		FROM topic_shadow_checkpoints c WHERE c.policy_version=$1
	), info AS (
		SELECT b.*,
			b.covered_through >= $2 AND NOT EXISTS (
				SELECT 1 FROM topic_shadow_decisions d WHERE d.policy_version=$1 AND d.ready_at <= $2
				AND (d.state IN ('queued','retry') OR d.decided_at > $2)
			) AS complete
		FROM boundary b
	), transitions AS (
		SELECT e.topic_id, t.card->>'Title' AS topic, e.kind, e.available_at, e.event_at, e.article_id,
			e.evidence->>'Reason' AS reason, d.decision->>'fact' AS fact
		FROM topic_shadow_events e
		JOIN topic_shadow_topics t USING (policy_version,topic_id)
		LEFT JOIN topic_shadow_decisions d
			ON d.policy_version=e.policy_version AND d.article_id=e.article_id AND d.claim_token=e.claim_token
		CROSS JOIN info i
		WHERE e.policy_version=$1 AND e.recorded_at <= $2
		  AND (greatest(e.recorded_at,e.available_at) > i.previous OR i.previous >= $2)
	), digest_preview AS (
		SELECT topic_id,max(topic) AS topic,
			jsonb_agg(jsonb_build_object('article_id',article_id,'fact',fact,'kind',kind)
				ORDER BY available_at,article_id) AS updates
		FROM transitions WHERE kind IN ('opened','updated','returned') GROUP BY topic_id
	), reviews AS (
		SELECT d.article_id,d.ready_at,d.decided_at,d.decision->>'review_reason' AS reason
		FROM topic_shadow_decisions d CROSS JOIN info i
		WHERE d.policy_version=$1 AND d.state='review' AND d.decided_at <= $2
		  AND (d.decided_at > i.previous OR i.previous >= $2)
	), single_source AS (
		SELECT count(*) AS eligible, count(*) FILTER (WHERE EXISTS (
			SELECT 1 FROM topic_shadow_events quiet
			WHERE quiet.policy_version=opened.policy_version AND quiet.topic_id=opened.topic_id
			  AND quiet.kind='quieted' AND quiet.available_at=opened.available_at+interval '168 hours'
			  AND quiet.recorded_at <= $2
		)) AS quiet_without_update
		FROM topic_shadow_events opened
		WHERE opened.policy_version=$1 AND opened.kind='opened' AND opened.evidence->>'Reason'='one_material'
		  AND opened.recorded_at <= $2 AND opened.available_at <= $2-interval '168 hours'
	), catchup AS (
		SELECT count(*) FILTER (WHERE state IN ('queued','retry') OR decided_at > $2) AS pending_at_cutoff,
			count(*) FILTER (WHERE decided_at <= $2 AND state='review') AS reviewed_to_date
		FROM topic_shadow_decisions WHERE policy_version=$1 AND ready_at <= $2
	)
	INSERT INTO topic_shadow_editions(policy_version,cutoff_at,revision,created_at,complete,snapshot)
	SELECT $1,$2,1,clock_timestamp(),coalesce(i.complete,false),
		jsonb_build_object('previous_cutoff',i.previous,'covered_through',i.covered_through,
			'single_source', (SELECT to_jsonb(s) FROM single_source s),
			'catchup', (SELECT to_jsonb(c) FROM catchup c),
			'transitions',coalesce((SELECT jsonb_agg(to_jsonb(t) ORDER BY t.available_at,t.topic_id,t.kind) FROM transitions t),'[]'::jsonb),
			'digest_preview',coalesce((SELECT jsonb_agg(to_jsonb(p) ORDER BY p.topic_id) FROM digest_preview p),'[]'::jsonb),
			'review',coalesce((SELECT jsonb_agg(to_jsonb(q) ORDER BY q.decided_at,q.article_id) FROM reviews q),'[]'::jsonb))
	FROM info i ON CONFLICT DO NOTHING`, version, cutoff)
	return err
}

// WriteReport exports a saved shadow edition. A zero cutoff selects the newest
// edition. A report never rebuilds an existing edition or invokes the provider.
func WriteReport(ctx context.Context, pool *pgxpool.Pool, version string, cutoff time.Time, out io.Writer) error {
	if pool == nil || out == nil {
		return errors.New("invalid topic shadow report configuration")
	}
	if strings.TrimSpace(version) == "" {
		version = DefaultPolicyVersion
	}
	var envelope struct {
		PolicyVersion string          `json:"policy_version"`
		Cutoff        time.Time       `json:"cutoff_at"`
		Revision      int             `json:"revision"`
		Complete      bool            `json:"complete"`
		Snapshot      json.RawMessage `json:"snapshot"`
	}
	err := pool.QueryRow(ctx, `SELECT policy_version,cutoff_at,revision,complete,snapshot FROM topic_shadow_editions
		WHERE policy_version=$1 AND ($2::timestamptz IS NULL OR cutoff_at=$2)
		ORDER BY cutoff_at DESC,revision DESC LIMIT 1`, version, nullableTime(cutoff)).Scan(
		&envelope.PolicyVersion, &envelope.Cutoff, &envelope.Revision, &envelope.Complete, &envelope.Snapshot)
	if errors.Is(err, pgx.ErrNoRows) {
		return errors.New("topic shadow edition not found")
	}
	if err != nil {
		return errors.New("topic shadow report unavailable")
	}
	encoder := json.NewEncoder(out)
	encoder.SetIndent("", "  ")
	return encoder.Encode(envelope)
}
