package topicshadow

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
)

const SemanticPolicyVersion = "qdrant-jev-topics-v1"
const semanticSnapshotClaim = "legacy-shadow-snapshot"

type SemanticSearch interface {
	Index(context.Context, IndexedArticle) (bool, error)
	Neighbors(context.Context, IndexedArticle, int) ([]SemanticNeighbor, error)
}

type semanticDecisionKey struct{}

type semanticDecision struct {
	CandidateIDs []string
	Trace        JevTrace
	Anchor       *queuedArticle
}

func articleSourceIDs(id *int64) []string {
	if id == nil {
		return nil
	}
	return []string{strconv.FormatInt(*id, 10)}
}

func semanticSeed(item queuedArticle) shadowTopic {
	sourceAt := item.ReadyAt
	if item.PublishedAt != nil && !item.PublishedAt.After(sourceAt) {
		sourceAt = *item.PublishedAt
	}
	card := TopicCard{ID: item.Claim, Title: truncateUTF8(item.Title, maxCardTextBytes), Context: truncateUTF8(item.Summary, 400)}
	if item.SerbianTitle != "" && item.SerbianTitle != item.Title {
		card.Aliases = []string{truncateUTF8(item.SerbianTitle, maxCardTextBytes)}
	}
	return shadowTopic{TopicCard: card, FirstSourceAt: sourceAt,
		State: LifecycleState{CandidateID: item.Claim, Status: LifecycleCandidate, FirstAvailableAt: item.ReadyAt}}
}

func (r Runner) evaluateSemantic(ctx context.Context, item queuedArticle, cutoff time.Time) (bool, error) {
	var fetched time.Time
	var title, summary string
	err := r.Pool.QueryRow(ctx, `SELECT coalesce(fetched_at, ready_at),title,summary FROM topic_shadow_inputs
		WHERE article_id=$1 AND claim_token=$2`, item.ArticleID, item.Claim).Scan(&fetched, &title, &summary)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, r.finish(ctx, item, shadowTopic{}, MatchResult{}, NoveltyResult{}, "superseded")
	}
	if err != nil {
		return false, err
	}
	indexed := IndexedArticle{ArticleID: item.ArticleID, Claim: item.Claim, Title: truncateUTF8(title, 1024), Summary: truncateUTF8(summary, 12000), FetchedAt: fetched, AvailableAt: item.ReadyAt}
	if _, err := r.Semantic.Index(ctx, indexed); err != nil {
		return false, err
	}
	neighbors, err := r.Semantic.Neighbors(ctx, indexed, 32)
	if err != nil {
		return false, err
	}
	cards, topics, anchors, err := r.semanticCandidates(ctx, item, neighbors)
	if err != nil {
		return false, err
	}
	metadata := semanticDecision{CandidateIDs: make([]string, 0, len(cards))}
	for _, c := range cards {
		metadata.CandidateIDs = append(metadata.CandidateIDs, c.Card.ID)
	}
	observed := WithJevTrace(ctx, func(trace JevTrace) { metadata.Trace = trace })
	match, novelty, err := ClassifyJev(observed, budgetedDecisions{runner: r, cutoff: cutoff}, item.ArticleInput, cards)
	if err != nil {
		var provider *DecisionsError
		if errors.As(err, &provider) && provider.Code == "invalid_response" && metadata.Trace.RequestSHA256 != "" {
			return true, r.finish(context.WithValue(ctx, semanticDecisionKey{}, metadata), item, shadowTopic{},
				MatchResult{Outcome: MatchOutcomeReview}, NoveltyResult{Outcome: NoveltyOutcomeReview}, "review")
		}
		return false, err
	}
	var topic shadowTopic
	status := "review"
	switch match.Outcome {
	case MatchOutcomeNewCandidate:
		topic = semanticSeed(item)
		status = "accepted"
	case MatchOutcomeMatch:
		topic = topics[match.TopicID]
		if topic.ID != "" {
			metadata.Anchor = anchors[topic.ID]
			if novelty.Outcome != NoveltyOutcomeReview {
				status = "accepted"
			}
		}
	}
	return status == "review", r.finish(context.WithValue(ctx, semanticDecisionKey{}, metadata), item, topic, match, novelty, status)
}

func (r Runner) semanticCandidates(ctx context.Context, current queuedArticle, neighbors []SemanticNeighbor) ([]JevCandidate, map[string]shadowTopic, map[string]*queuedArticle, error) {
	cards := make([]JevCandidate, 0, 8)
	topics := make(map[string]shadowTopic)
	anchors := make(map[string]*queuedArticle)
	for _, neighbor := range neighbors {
		if len(cards) == 8 {
			break
		}
		anchor, linked, err := r.resolveSemanticNeighbor(ctx, current, neighbor)
		if errors.Is(err, pgx.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, nil, nil, err
		}
		id := linked
		if id == "" {
			id = anchor.Claim
		}
		if _, seen := topics[id]; seen {
			continue
		}
		rows, err := r.Pool.Query(ctx, `SELECT topic_id::text,revision,state,card,first_source_at,first_available_at,
			opened_at,last_material_at,quieted_at,material_count FROM topic_shadow_topics
			WHERE policy_version=$1 AND topic_id=$2 AND first_available_at <= $3`, r.Version, id, current.ReadyAt)
		if err != nil {
			return nil, nil, nil, err
		}
		t, err := pgx.CollectExactlyOneRow(rows, scanTopic)
		if errors.Is(err, pgx.ErrNoRows) && linked == "" {
			t = semanticSeed(anchor)
			anchors[id] = &anchor
		} else if errors.Is(err, pgx.ErrNoRows) {
			continue
		} else if err != nil {
			return nil, nil, nil, err
		}
		facts, err := r.facts(ctx, id, current)
		if err != nil {
			return nil, nil, nil, err
		}
		if len(facts) > 0 {
			facts = facts[:min(3, len(facts))]
			for i := range facts {
				facts[i] = truncateUTF8(facts[i], 360)
			}
		}
		cards = append(cards, JevCandidate{Card: t.TopicCard, Facts: facts})
		topics[id] = t
	}
	return cards, topics, anchors, nil
}

func (r Runner) resolveSemanticNeighbor(ctx context.Context, current queuedArticle, neighbor SemanticNeighbor) (queuedArticle, string, error) {
	var anchor queuedArticle
	var linked, fingerprintTitle, fingerprintSummary string
	var fetched time.Time
	var err error
	if neighbor.Claim == semanticSnapshotClaim {
		if neighbor.AvailableAt.After(current.ReadyAt) || neighbor.ArticleID == current.ArticleID {
			return anchor, "", pgx.ErrNoRows
		}
		anchor.Claim = neighbor.VersionID
		anchor.ReadyAt = neighbor.AvailableAt
		err = r.Pool.QueryRow(ctx, `SELECT a.id,a.source_id,a.title,a.summary,a.published_at AT TIME ZONE 'UTC',
			a.fetched_at AT TIME ZONE 'UTC',coalesce(d.topic_id::text,'')
			FROM articles a LEFT JOIN topic_shadow_decisions d ON d.policy_version=$1 AND d.article_id=a.id AND d.claim_token=$2
			WHERE a.id=$3 AND a.summary_processed IS TRUE AND a.is_advertisement IS NOT TRUE
			AND NOT EXISTS (SELECT 1 FROM topic_shadow_inputs i WHERE i.article_id=a.id)`,
			r.Version, anchor.Claim, neighbor.ArticleID).Scan(&anchor.ArticleID, &anchor.SourceID, &fingerprintTitle, &fingerprintSummary,
			&anchor.PublishedAt, &fetched, &linked)
		anchor.SnapshotAnchor = true
	} else {
		err = r.Pool.QueryRow(ctx, `SELECT i.article_id,i.claim_token::text,i.ready_at,i.source_id,i.title,i.summary,
			i.published_at,left(coalesce(i.serbian_title,''),240),left(coalesce(i.serbian_summary,''),650),coalesce(d.topic_id::text,''),
			coalesce(i.fetched_at,i.ready_at)
			FROM topic_shadow_inputs i LEFT JOIN topic_shadow_decisions d
			  ON d.policy_version=$1 AND d.article_id=i.article_id AND d.claim_token=i.claim_token AND d.state IN ('accepted','review')
			WHERE i.article_id=$2 AND i.claim_token::text=$3 AND (i.ready_at,i.article_id,i.claim_token) < ($4,$5,$6::uuid)`,
			r.Version, neighbor.ArticleID, neighbor.Claim, current.ReadyAt, current.ArticleID, current.Claim).Scan(
			&anchor.ArticleID, &anchor.Claim, &anchor.ReadyAt, &anchor.SourceID, &fingerprintTitle, &fingerprintSummary,
			&anchor.PublishedAt, &anchor.SerbianTitle, &anchor.SerbianSummary, &linked, &fetched)
	}
	if err != nil {
		return anchor, "", err
	}
	version := IndexedArticle{ArticleID: anchor.ArticleID, Claim: neighbor.Claim, Title: truncateUTF8(fingerprintTitle, 1024), Summary: truncateUTF8(fingerprintSummary, 12000)}
	if version.PointID() != neighbor.VersionID || !fetched.Equal(neighbor.FetchedAt) || !anchor.ReadyAt.Equal(neighbor.AvailableAt) {
		return anchor, "", pgx.ErrNoRows
	}
	anchor.Title = truncateUTF8(fingerprintTitle, 512)
	anchor.Summary = truncateUTF8(fingerprintSummary, 1600)
	return anchor, linked, nil
}

// persistSemanticAnchor attaches a previously unassigned, still current item
// as the seed in the same journal transaction as the incoming decision.
func (r Runner) persistSemanticAnchor(ctx context.Context, tx pgx.Tx, anchor queuedArticle, topic shadowTopic, recordedAt time.Time) error {
	var current bool
	var err error
	if anchor.SnapshotAnchor {
		var title, summary string
		err = tx.QueryRow(ctx, `SELECT a.title,a.summary FROM articles a WHERE id=$1 AND summary_processed IS TRUE
		   AND is_advertisement IS NOT TRUE AND NOT EXISTS (SELECT 1 FROM topic_shadow_inputs i WHERE i.article_id=a.id)
		   FOR SHARE OF a NOWAIT`, anchor.ArticleID).Scan(&title, &summary)
		if errors.Is(err, pgx.ErrNoRows) {
			return errArticleBusy
		}
		current = (IndexedArticle{ArticleID: anchor.ArticleID, Claim: semanticSnapshotClaim, Title: truncateUTF8(title, 1024), Summary: truncateUTF8(summary, 12000)}).PointID() == anchor.Claim
	} else {
		current, err = r.current(ctx, tx, anchor)
	}
	if err != nil {
		return err
	}
	if !current {
		return errArticleBusy
	}
	if err = r.persistTopic(ctx, tx, topic, LifecycleResult{State: topic.State}, &anchor, recordedAt); err != nil {
		return err
	}
	decision, err := json.Marshal(map[string]any{"candidate_seed": true, "fact": topic.Context, "material_applied": false})
	if err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO topic_shadow_decisions(policy_version,article_id,claim_token,ready_at,input,state,topic_id,decided_at,decision)
		VALUES($1,$2,$3,$4,$5,'accepted',$6,$7,$8)
		ON CONFLICT (policy_version,article_id,claim_token) DO UPDATE
		SET topic_id=excluded.topic_id, decision=coalesce(topic_shadow_decisions.decision,'{}'::jsonb)||excluded.decision
		WHERE topic_shadow_decisions.state='review' AND topic_shadow_decisions.topic_id IS NULL`,
		r.Version, anchor.ArticleID, anchor.Claim, anchor.ReadyAt, anchor.ArticleInput, topic.ID, recordedAt, decision)
	if err == nil && tag.RowsAffected() != 1 {
		return errArticleBusy
	}
	return err
}
