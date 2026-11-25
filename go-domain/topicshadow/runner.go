package topicshadow

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultPolicyVersion = "luna-topics-v1"

type Runner struct {
	Pool              *pgxpool.Pool
	Version           string
	Owner             string
	Client            DecisionsTransport
	DailyRequestLimit int
	replay            bool
}

type PassReport struct {
	Scanned        bool
	Processed      int
	Reviewed       int
	Deferred       bool
	DeferredReason string
}

func (r Runner) RunPass(ctx context.Context, now time.Time, limit int) (PassReport, error) {
	var report PassReport
	if r.Pool == nil || r.Client == nil || r.Owner == "" || r.DailyRequestLimit < 2 || limit < 1 || limit > 50 || now.IsZero() {
		return report, errors.New("invalid topic shadow runner configuration")
	}
	if r.Version == "" {
		r.Version = DefaultPolicyVersion
	}
	now = now.UTC()
	ctx, cancel := context.WithTimeout(ctx, 12*time.Minute)
	defer cancel()
	acquired, err := r.acquire(ctx, now)
	if err != nil {
		return report, err
	}
	if !acquired {
		report.Deferred = true
		report.DeferredReason = "lease_busy"
		return report, nil
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
		defer cancel()
		_, _ = r.Pool.Exec(cleanup, `UPDATE topic_shadow_checkpoints SET lease_owner=NULL,lease_until=NULL
			WHERE policy_version=$1 AND lease_owner=$2`, r.Version, r.Owner)
	}()

	if r.replay {
		saved, err := r.checkReplayCutoff(ctx, now)
		if err != nil || saved {
			report.Scanned = saved
			return report, err
		}
	} else {
		var exhausted bool
		err = r.Pool.QueryRow(ctx, `SELECT api_day IS NOT DISTINCT FROM (clock_timestamp() AT TIME ZONE 'UTC')::date
			AND api_requests >= $2 FROM topic_shadow_checkpoints WHERE policy_version=$1`,
			r.Version, r.DailyRequestLimit).Scan(&exhausted)
		if err != nil {
			return report, err
		}
		if exhausted {
			report.Deferred = true
			report.DeferredReason = "daily_request_limit"
			return report, r.freezeEditions(ctx, now)
		}
	}

	report.Scanned, err = r.enqueue(ctx, now, limit)
	if err != nil {
		return report, err
	}
	for report.Processed < limit {
		item, found, err := r.next(ctx, now, r.retryTime(now))
		if err != nil {
			return report, err
		}
		if !found {
			break
		}
		if err = r.expire(ctx, item.ReadyAt); err != nil {
			return report, err
		}
		reviewed, err := r.evaluate(ctx, item, now)
		if err != nil {
			if errors.Is(err, errArticleBusy) || errors.Is(err, errDailyLimit) {
				report.Deferred = true
				report.DeferredReason = "article_busy"
				if errors.Is(err, errDailyLimit) {
					report.DeferredReason = "daily_request_limit"
				}
				break
			}
			var providerErr *DecisionsError
			if !errors.As(err, &providerErr) || ctx.Err() != nil {
				return report, err
			}
			if providerErr.StatusCode == 401 || providerErr.StatusCode == 403 {
				report.Deferred = true
				report.DeferredReason = "provider_authentication"
				break
			}
			terminal, saveErr := r.deferArticle(ctx, item, r.retryTime(now), providerErr)
			if saveErr != nil {
				return report, saveErr
			}
			if !terminal {
				report.Deferred = true
				report.DeferredReason = "provider_retry"
				break
			}
			reviewed = true
		}
		report.Processed++
		if reviewed {
			report.Reviewed++
		}
	}
	var pending bool
	err = r.Pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM topic_shadow_decisions
		WHERE policy_version=$1 AND ready_at <= $2 AND state IN ('queued','retry'))`, r.Version, now).Scan(&pending)
	if err != nil {
		return report, err
	}
	report.Deferred = report.Deferred || pending || !report.Scanned
	if report.Deferred && report.DeferredReason == "" {
		report.DeferredReason = "pending_input"
	}
	if !report.Deferred {
		if err = r.expire(ctx, now); err != nil {
			return report, err
		}
	}
	if r.replay {
		if report.Deferred {
			return report, nil
		}
		return report, r.fenced(ctx, func(tx pgx.Tx) error {
			if err := freezeCutoff(ctx, tx, r.Version, now); err != nil {
				return err
			}
			_, err := tx.Exec(ctx, `UPDATE topic_shadow_checkpoints SET replay_in_progress_at=NULL WHERE policy_version=$1`, r.Version)
			return err
		})
	}
	return report, r.freezeEditions(ctx, now)
}

func (r Runner) retryTime(cutoff time.Time) time.Time {
	if r.replay {
		return time.Now().UTC()
	}
	return cutoff
}

type budgetedDecisions struct {
	runner Runner
	cutoff time.Time
}

func (b budgetedDecisions) Decide(ctx context.Context, input string, questions []ChoiceQuestion) (DecisionsResult, error) {
	if _, err := marshalDecisionsRequest(input, questions); err != nil {
		return DecisionsResult{}, err
	}
	r := b.runner
	var replayCutoff *time.Time
	if r.replay {
		replayCutoff = &b.cutoff
	}
	err := r.fenced(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `WITH daily AS (SELECT (coalesce($3::timestamptz,clock_timestamp()) AT TIME ZONE 'UTC')::date AS today)
			UPDATE topic_shadow_checkpoints SET api_day=daily.today,
			api_requests=CASE WHEN api_day=daily.today THEN api_requests+1 ELSE 1 END
			FROM daily WHERE policy_version=$1 AND (api_day IS DISTINCT FROM daily.today OR api_requests < $2)`,
			r.Version, r.DailyRequestLimit, replayCutoff)
		if err == nil && tag.RowsAffected() == 0 {
			return errDailyLimit
		}
		return err
	})
	if err != nil {
		return DecisionsResult{}, err
	}
	requestCtx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	return r.Client.Decide(requestCtx, input, questions)
}

func (r Runner) evaluate(ctx context.Context, item queuedArticle, cutoff time.Time) (bool, error) {
	var current bool
	if err := r.fenced(ctx, func(tx pgx.Tx) error {
		var err error
		current, err = r.current(ctx, tx, item)
		return err
	}); err != nil {
		return false, err
	}
	if !current {
		return false, r.finish(ctx, item, shadowTopic{}, MatchResult{}, NoveltyResult{}, "superseded")
	}
	topics, err := r.topics(ctx, item.ReadyAt)
	if err != nil {
		return false, err
	}
	candidates := make([]CandidateTopic, 0, len(topics))
	for _, t := range topics {
		candidates = append(candidates, CandidateTopic{Card: t.TopicCard, Status: t.status, LastMaterialAt: t.activity})
	}
	ranked := RankCandidates(item.ArticleInput, candidates, item.ReadyAt, 8)
	classifier := Classifier{Client: budgetedDecisions{runner: r, cutoff: cutoff}}
	match, err := classifier.Match(ctx, item.ArticleInput, ranked)
	if err != nil {
		return false, err
	}
	if match.Outcome == MatchOutcomeReview {
		return true, r.finish(ctx, item, shadowTopic{}, match, NoveltyResult{}, "review")
	}
	var topic shadowTopic
	if match.Outcome == MatchOutcomeMatch {
		for _, known := range topics {
			if known.ID == match.TopicID {
				topic = known.shadowTopic
				break
			}
		}
		if topic.ID == "" {
			return true, r.finish(ctx, item, shadowTopic{}, match, NoveltyResult{}, "review")
		}
	} else {
		sourceAt := item.ReadyAt
		if item.PublishedAt != nil && !item.PublishedAt.After(item.ReadyAt) {
			sourceAt = *item.PublishedAt
		}
		card := TopicCard{ID: item.Claim, Title: truncateUTF8(item.Title, maxCardTextBytes), Context: truncateUTF8(item.Summary, 400)}
		if item.SerbianTitle != "" && item.SerbianTitle != item.Title {
			card.Aliases = []string{truncateUTF8(item.SerbianTitle, maxCardTextBytes)}
		}
		topic = shadowTopic{TopicCard: card,
			FirstSourceAt: sourceAt,
			State:         LifecycleState{CandidateID: item.Claim, Status: LifecycleCandidate, FirstAvailableAt: item.ReadyAt}}
	}
	facts, err := r.facts(ctx, topic.ID, item)
	if err != nil {
		return false, err
	}
	novelty, err := classifier.Novelty(ctx, item.ArticleInput, topic.TopicCard, facts)
	if err != nil {
		return false, err
	}
	if novelty.Outcome == NoveltyOutcomeReview {
		return true, r.finish(ctx, item, shadowTopic{}, match, novelty, "review")
	}
	return false, r.finish(ctx, item, topic, match, novelty, "accepted")
}

func (r Runner) finish(ctx context.Context, item queuedArticle, topic shadowTopic, match MatchResult, novelty NoveltyResult, status string) error {
	return r.fenced(ctx, func(tx pgx.Tx) error {
		current, err := r.current(ctx, tx, item)
		if err != nil {
			return err
		}
		if !current {
			status = "superseded"
		}
		recordedAt := r.recordedAt(item.ReadyAt)
		report := map[string]any{"match": match, "assessment": novelty, "novelty": novelty.Outcome}
		if status == "review" {
			switch {
			case hasAmbiguousReferent(articleText(item.ArticleInput)):
				report["review_reason"] = "unresolved_referent"
			case match.Outcome == MatchOutcomeReview:
				report["review_reason"] = "match_uncertain"
			case novelty.Outcome == NoveltyOutcomeReview:
				report["review_reason"] = "novelty_uncertain"
			default:
				report["review_reason"] = "model_error"
			}
		}
		var topicID any
		if status == "accepted" && topic.Revision == 0 &&
			(!novelty.SubjectCandidate || novelty.Outcome != NoveltyOutcomeMaterialUpdate) {
			report["topic_skipped"] = "no_material_topic"
		} else if status == "accepted" {
			topicID = topic.ID
			fact := novelty.EvidenceSpan
			if fact == "" {
				fact = match.EvidenceSpan
			}
			if fact == "" {
				fact = item.Title
			}
			report["fact"] = fact
			alreadyCounted := false
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM topic_shadow_decisions WHERE policy_version=$1
				AND topic_id=$2 AND article_id=$3 AND state='accepted' AND `+appliedMaterial+`)`,
				r.Version, topic.ID, item.ArticleID).Scan(&alreadyCounted); err != nil {
				return err
			}
			material := novelty.Outcome == NoveltyOutcomeMaterialUpdate && !alreadyCounted
			if item.ReadyAt.Before(topic.State.LastMaterialAvailableAt) || item.ReadyAt.Before(topic.State.QuietedAt) {
				material = false
				report["late_historical"] = true
			}
			if item.PublishedAt != nil && !item.PublishedAt.After(item.ReadyAt) && item.PublishedAt.Before(topic.FirstSourceAt) {
				topic.FirstSourceAt = *item.PublishedAt
			}
			independent := 1
			var sources []string
			if item.SourceID != nil {
				sources = []string{strconv.FormatInt(*item.SourceID, 10)}
			}
			if material {
				repeat, distinctSource, err := r.compareMaterial(ctx, tx, topic.ID, item, fact)
				if err != nil {
					return err
				}
				material = !repeat
				if distinctSource {
					independent = 2
				}
			}
			eventAt := novelty.EventAt
			if eventAt.After(item.ReadyAt) {
				eventAt = time.Time{}
			}
			next := ApplyLifecycle(topic.State, LifecycleDecision{
				CandidateID: topic.ID, AvailableAt: item.ReadyAt, EventAt: eventAt,
				FactID: strconv.FormatInt(item.ArticleID, 10), SourceIDs: sources, MaterialUpdate: material,
				Repeat: novelty.Outcome == NoveltyOutcomeRepeat, Context: novelty.Outcome == NoveltyOutcomeContext,
				ConcreteStatusChange: novelty.ConcreteStatusChange, IndependentMaterialCount: independent,
			}, DefaultQuietAfter)
			report["material_applied"] = next.State.MaterialCount > topic.State.MaterialCount
			if err := r.persistTopic(ctx, tx, topic, next, &item, recordedAt); err != nil {
				return err
			}
		}
		tag, err := tx.Exec(ctx, `UPDATE topic_shadow_decisions SET state=$4,topic_id=$5,decided_at=$6,
			decision=$7,next_attempt_at=NULL,attempts=attempts+1
			WHERE policy_version=$1 AND article_id=$2 AND claim_token=$3 AND state IN ('queued','retry')`,
			r.Version, item.ArticleID, item.Claim, status, topicID, recordedAt, report)
		if err == nil && tag.RowsAffected() != 1 {
			return errLeaseLost
		}
		return err
	})
}

func (r Runner) expire(ctx context.Context, at time.Time) error {
	return r.fenced(ctx, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, `SELECT topic_id::text,revision,state,card,first_source_at,first_available_at,
			opened_at,last_material_at,quieted_at,material_count FROM topic_shadow_topics
			WHERE policy_version=$1 AND state='active' AND last_material_at <= $2
			ORDER BY last_material_at,topic_id`, r.Version, at.Add(-DefaultQuietAfter))
		if err != nil {
			return err
		}
		topics, err := pgx.CollectRows(rows, scanTopic)
		if err != nil {
			return err
		}
		for _, topic := range topics {
			if err = r.persistTopic(ctx, tx, topic, ExpireLifecycle(topic.State, at, DefaultQuietAfter), nil, r.recordedAt(at)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (r Runner) deferArticle(ctx context.Context, item queuedArticle, now time.Time, failure *DecisionsError) (bool, error) {
	retryable := (failure.StatusCode == 0 && failure.Code == "transport") || failure.StatusCode == 429 || failure.StatusCode >= 500
	if !retryable || (item.Attempts >= 2 && failure.StatusCode != 429) {
		return true, r.finish(ctx, item, shadowTopic{}, MatchResult{}, NoveltyResult{}, "review")
	}
	wait := max(2*time.Minute, failure.RetryAfter)
	if failure.StatusCode == 429 {
		wait = max(wait, 15*time.Minute)
	}
	err := r.fenced(ctx, func(tx pgx.Tx) error {
		tag, err := tx.Exec(ctx, `UPDATE topic_shadow_decisions SET state='retry',attempts=attempts+1,
			next_attempt_at=$4,decision=jsonb_build_object('error_code',$5::text)
			WHERE policy_version=$1 AND article_id=$2 AND claim_token=$3 AND state IN ('queued','retry')`,
			r.Version, item.ArticleID, item.Claim, now.Add(wait), failure.Code)
		if err == nil && tag.RowsAffected() != 1 {
			return errLeaseLost
		}
		return err
	})
	return false, err
}

func (r Runner) recordedAt(available time.Time) time.Time {
	if r.replay {
		return available.UTC()
	}
	return time.Now().UTC()
}

// PassErrorCode describes a pass failure without exposing database diagnostics.
func PassErrorCode(err error) string {
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline"
	case errors.Is(err, context.Canceled):
		return "cancelled"
	case errors.Is(err, errLeaseLost):
		return "lease_lost"
	case errors.As(err, &pgErr):
		switch pgErr.Code {
		case "42501":
			return "database_permission"
		case "42P01", "42703":
			return "database_schema"
		case "40001", "40P01":
			return "database_transaction"
		default:
			return "database_error"
		}
	default:
		return "pass_error"
	}
}
