package digestworkflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
	"github.com/jackc/pgx/v5"
)

// EditionIDLocale preserves every existing Russian ID byte-for-byte.
func EditionIDLocale(job string, attempt int, operation, locale string) string {
	if locale == editorial.RussianLocale {
		return EditionID(job, attempt, operation)
	}
	return EditionID(job, attempt, operation+":"+locale)
}
func serbianEditionID(russianID string) string { return EditionID(russianID, 1, "sr-Latn") }

func (w Workflow) localizedSnapshot(ctx context.Context, ru editorial.Snapshot, id string) (editorial.Snapshot, error) {
	sr := ru
	sr.Articles = append([]editorial.Article(nil), ru.Articles...)
	sr.EditionURL = fmt.Sprintf("%s/digest/%s?locale=sr-Latn&edition=%s", ru.SiteBaseURL, ru.Date, id)
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return sr, err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return sr, err
	}
	for i, article := range ru.Articles {
		aid, err := strconv.Atoi(article.ID)
		if err != nil {
			return sr, ErrInvalid
		}
		cid, err := strconv.Atoi(article.CategoryID)
		if err != nil {
			return sr, ErrInvalid
		}
		var title, summary, currentTitle, currentSummary, label string
		err = tx.QueryRow(ctx, `SELECT l.title,l.summary,COALESCE(a.title,''),COALESCE(a.summary,''),COALESCE(c.display_name,'') FROM article_localizations l JOIN articles a ON a.id=l.article_id LEFT JOIN category_localizations c ON c.category_id=$2 AND c.locale='sr-Latn' WHERE l.article_id=$1 AND l.locale='sr-Latn' AND l.status='completed'`, aid, cid).Scan(&title, &summary, &currentTitle, &currentSummary, &label)
		if errors.Is(err, pgx.ErrNoRows) {
			return sr, ErrNotReady
		}
		if err != nil {
			return sr, err
		}
		if currentTitle != article.Title || currentSummary != article.Summary || title == "" || summary == "" || label == "" {
			return sr, ErrNotReady
		}
		sr.Articles[i].Title = title
		sr.Articles[i].Summary = summary
		sr.Articles[i].Content = ""
		sr.Articles[i].CategoryName = label
	}
	if err = tx.Commit(ctx); err != nil {
		return sr, err
	}
	if _, err = editorial.ValidateSnapshot(sr, "sr-Latn"); err != nil {
		return sr, ErrNotReady
	}
	return sr, nil
}

func (w Workflow) persistLocalizedEdition(ctx context.Context, id, job string, attempt int, snapshot editorial.Snapshot, e editorial.Edition, usage editorial.ProviderUsage, account bool) error {
	rendered, err := editorial.Render(snapshot, e.Locale, e)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	editionRaw, _ := json.Marshal(e)
	renderRaw, _ := json.Marshal(rendered)
	if w.PriceUsage != nil {
		usage.Cost = w.PriceUsage(usage)
	}
	usageRaw, _ := json.Marshal(usage)
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return err
	}
	tag, err := tx.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,provider_usage,job_id,attempt_generation) VALUES($1,$2::date,$3,'bilingual-v1',$4,$5::jsonb,$6::jsonb,$7::jsonb,$8::jsonb,$9,$10) ON CONFLICT DO NOTHING`, id, snapshot.Date, e.Locale, e.SourceSnapshotHash, raw, editionRaw, renderRaw, usageRaw, job, attempt)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 1 && account && w.RecordUsage != nil {
		if err = w.RecordUsage(ctx, tx, usage); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

func (w Workflow) loadRussian(ctx context.Context, date, job string, attempt int, expectedID string) (string, editorial.Snapshot, editorial.Edition, error) {
	var id string
	var snapshotRaw, editionRaw []byte
	var snapshot editorial.Snapshot
	var edition editorial.Edition
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return id, snapshot, edition, err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return id, snapshot, edition, err
	}
	err = tx.QueryRow(ctx, `SELECT id::text,snapshot,edition FROM editorial_digest_editions WHERE edition_date=$1::date AND locale='ru' AND (($2<>'' AND id=NULLIF($2,'')::uuid) OR ($2='' AND job_id=NULLIF($3,'')::uuid AND attempt_generation=$4)) ORDER BY created_at DESC,id DESC LIMIT 1`, date, expectedID, job, attempt).Scan(&id, &snapshotRaw, &editionRaw)
	if err != nil {
		return id, snapshot, edition, err
	}
	if json.Unmarshal(snapshotRaw, &snapshot) != nil || json.Unmarshal(editionRaw, &edition) != nil || editorial.ValidateEdition(snapshot, "ru", edition) != nil {
		return id, snapshot, edition, ErrInvalid
	}
	return id, snapshot, edition, tx.Commit(ctx)
}

// freezeInputs writes each localized snapshot once. A delayed locale uses the
// original Russian composition and cannot silently reread a changed summary.
func (w Workflow) freezeInputs(ctx context.Context, id string, ru editorial.Snapshot, sr *editorial.Snapshot) (editorial.Snapshot, *editorial.Snapshot, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return ru, sr, err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return ru, sr, err
	}
	ruRaw, _ := json.Marshal(ru)
	var srRaw []byte
	if sr != nil {
		srRaw, _ = json.Marshal(sr)
	}
	_, err = tx.Exec(ctx, `INSERT INTO editorial_locale_inputs(russian_edition_id,russian_snapshot,serbian_snapshot,last_error) VALUES($1,$2::jsonb,$3::jsonb,CASE WHEN $3::jsonb IS NULL THEN 'serbian_summaries_missing' ELSE NULL END) ON CONFLICT(russian_edition_id) DO NOTHING`, id, ruRaw, srRaw)
	if err != nil {
		return ru, sr, err
	}
	if sr != nil {
		_, err = tx.Exec(ctx, `UPDATE editorial_locale_inputs SET serbian_snapshot=$2::jsonb,last_error=NULL WHERE russian_edition_id=$1 AND serbian_snapshot IS NULL`, id, srRaw)
		if err != nil {
			return ru, sr, err
		}
	}
	err = tx.QueryRow(ctx, `SELECT russian_snapshot,serbian_snapshot FROM editorial_locale_inputs WHERE russian_edition_id=$1`, id).Scan(&ruRaw, &srRaw)
	if err != nil {
		return ru, sr, err
	}
	if json.Unmarshal(ruRaw, &ru) != nil {
		return ru, sr, ErrInvalid
	}
	sr = nil
	if len(srRaw) > 0 {
		var value editorial.Snapshot
		if json.Unmarshal(srRaw, &value) != nil {
			return ru, nil, ErrInvalid
		}
		sr = &value
	}
	return ru, sr, tx.Commit(ctx)
}
func (w Workflow) reserveSerbian(ctx context.Context, id string) (bool, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return false, err
	}
	tag, err := tx.Exec(ctx, `UPDATE editorial_locale_inputs SET attempts=attempts+1,next_attempt_at=now()+interval '5 minutes',last_error='serbian_generation_interrupted' WHERE russian_edition_id=$1 AND serbian_snapshot IS NOT NULL AND attempts<3 AND (next_attempt_at IS NULL OR next_attempt_at<=now())`, id)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, tx.Commit(ctx)
}
func (w Workflow) serbianDone(ctx context.Context, id string, ready bool) error {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return err
	}
	code := "serbian_editorial_not_ready"
	if ready {
		code = ""
	}
	_, err = tx.Exec(ctx, `UPDATE editorial_locale_inputs SET last_error=NULLIF($2,''),next_attempt_at=CASE WHEN $2='' THEN NULL ELSE next_attempt_at END WHERE russian_edition_id=$1`, id, code)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (w Workflow) hasSerbian(ctx context.Context, id string) (bool, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return false, err
	}
	var found bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM editorial_digest_editions WHERE id=$1 AND locale='sr-Latn')`, id).Scan(&found)
	if err != nil {
		return false, err
	}
	return found, tx.Commit(ctx)
}

// GenerateLocalized never delays or regenerates a ready Russian edition for SR.
func (w Workflow) GenerateLocalized(ctx context.Context, job string, attempt int, operation string, cutoff time.Time) (int, error) {
	return w.generateLocalized(ctx, job, attempt, operation, cutoff, "")
}

// RetrySerbian targets an already frozen bilingual edition; it never backfills
// an edition created before the localization policy was enabled.
func (w Workflow) RetrySerbian(ctx context.Context, job string, attempt int, operation string, cutoff time.Time, russianID string) (int, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return 0, err
	}
	var found bool
	err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM editorial_locale_inputs WHERE russian_edition_id=$1)`, russianID).Scan(&found)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	if !found {
		return 0, nil
	}
	return w.generateLocalized(ctx, job, attempt, operation, cutoff, russianID)
}
func (w Workflow) generateLocalized(ctx context.Context, job string, attempt int, operation string, cutoff time.Time, expectedID string) (int, error) {
	id, ru, plan, err := w.loadRussian(ctx, cutoff.UTC().Format("2006-01-02"), job, attempt, expectedID)
	existing := err == nil
	if expectedID != "" && !existing {
		return 0, ErrNotReady
	}
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return 0, err
	}
	if !existing {
		id = EditionID(job, attempt, operation)
		ru, _, err = w.Snapshot(ctx, cutoff, id)
		if err != nil {
			return 0, err
		}
		if len(ru.Articles) == 0 {
			return 0, nil
		}
	}
	srID := serbianEditionID(id)
	if existing {
		ready, e := w.hasSerbian(ctx, srID)
		if e != nil {
			return 1, e
		}
		if ready {
			return 1, nil
		}
	}
	srValue, srErr := w.localizedSnapshot(ctx, ru, srID)
	var sr *editorial.Snapshot
	if srErr == nil {
		sr = &srValue
	} else if !errors.Is(srErr, ErrNotReady) {
		return 0, srErr
	}
	ru, sr, err = w.freezeInputs(ctx, id, ru, sr)
	if err != nil {
		return 0, err
	}
	if existing && sr == nil {
		return 1, nil
	}
	provider := w.Provider
	if w.ResolveProvider != nil {
		provider, err = w.ResolveProvider(ctx)
		if err != nil {
			return 0, err
		}
	}
	admitted := false
	if sr != nil {
		admitted, err = w.reserveSerbian(ctx, id)
		if err != nil {
			return 0, err
		}
	}
	if existing && !admitted {
		return 1, nil
	}
	providerCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	if !existing && sr != nil && admitted {
		result, e := editorial.GenerateBilingual(providerCtx, ru, *sr, provider)
		if e != nil {
			return 0, e
		}
		if e = w.persistLocalizedEdition(ctx, id, job, attempt, ru, result.Russian, result.Usage, true); e != nil {
			return 0, e
		}
		if result.Serbian != nil {
			if e = w.persistLocalizedEdition(ctx, srID, job, attempt, *sr, *result.Serbian, editorial.ProviderUsage{}, false); e != nil {
				return 1, e
			}
		}
		if e = w.serbianDone(ctx, id, result.Serbian != nil); e != nil {
			return 1, e
		}
		return 1, nil
	}
	if !existing {
		result, e := editorial.Generate(providerCtx, ru, "ru", provider)
		if e != nil {
			return 0, e
		}
		if e = w.persistLocalizedEdition(ctx, id, job, attempt, ru, result.Edition, result.Usage, true); e != nil {
			return 0, e
		}
		return 1, nil
	}
	result, e := editorial.GenerateForPlan(providerCtx, *sr, plan, provider)
	if e != nil {
		if err = w.serbianDone(ctx, id, false); err != nil {
			return 1, err
		}
		return 1, nil
	}
	// The Serbian edition is paired with the original Russian job identity.
	var originalJob *string
	var originalAttempt *int
	if err = w.Pool.QueryRow(ctx, `SELECT job_id::text,attempt_generation FROM editorial_digest_editions WHERE id=$1`, id).Scan(&originalJob, &originalAttempt); err != nil {
		return 1, err
	}
	if originalJob == nil || originalAttempt == nil {
		return 1, ErrInvalid
	}
	if err = w.persistLocalizedEdition(ctx, srID, *originalJob, *originalAttempt, *sr, result.Edition, result.Usage, true); err != nil {
		return 1, err
	}
	return 1, w.serbianDone(ctx, id, true)
}
