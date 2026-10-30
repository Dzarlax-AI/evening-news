package store

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"time"
)

// ClaimLocalizationRetry uses the existing processing claim and callback fence,
// but leaves the article's processing attempt and state untouched.
func (s *Store) ClaimLocalizationRetry(ctx context.Context, f Fence, maxAttempts int, delay time.Duration) (Article, string, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Article{}, "", false, errors.New("localization claim unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return Article{}, "", false, e
	} else if state != "running" {
		return Article{}, "", false, ErrCancelled
	}
	if _, err = tx.Exec(ctx, `UPDATE article_localizations SET status='exhausted',next_retry_at=NULL,last_error='retry_interrupted' WHERE locale='sr-Latn' AND status='retry_wait' AND attempts >= $1 AND next_retry_at<=now()`, maxAttempts); err != nil {
		return Article{}, "", false, errors.New("localization recovery failed")
	}
	var a Article
	err = tx.QueryRow(ctx, `SELECT a.id,COALESCE(a.title,''),COALESCE(a.url,''),COALESCE(a.content,''),COALESCE(a.summary,''),COALESCE(a.summary_processed,false),COALESCE(a.category_processed,false),COALESCE(a.ad_processed,false),a.is_advertisement,COALESCE(a.processing_attempts,0) FROM article_localizations l JOIN articles a ON a.id=l.article_id WHERE l.locale='sr-Latn' AND l.status='retry_wait' AND l.attempts<$1 AND l.next_retry_at<=now() AND a.processing_state='completed' AND a.processing_attempts>0 AND NOT EXISTS (SELECT 1 FROM article_processing_items i WHERE i.article_id=a.id AND (i.state='running' OR i.run_id=$2::uuid)) ORDER BY l.next_retry_at,a.id FOR UPDATE OF l,a SKIP LOCKED LIMIT 1`, maxAttempts, f.RunID).Scan(&a.ID, &a.Title, &a.URL, &a.Content, &a.Summary, &a.SummaryProcessed, &a.CategoryProcessed, &a.AdProcessed, &a.IsAdvertisement, &a.ProcessingAttempts)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			if err = tx.Commit(ctx); err != nil {
				return Article{}, "", false, errors.New("localization recovery commit failed")
			}
			return Article{}, "", false, nil
		}
		return Article{}, "", false, errors.New("localization selection failed")
	}
	var raw [16]byte
	if _, err = rand.Read(raw[:]); err != nil {
		return Article{}, "", false, errors.New("localization token unavailable")
	}
	a.ClaimToken = fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
	input := map[string]any{"title": a.Title, "content": a.Summary, "url": a.URL, "locales": []string{"sr-Latn"}}
	fp := localizationFingerprint(input)
	receipt, _ := json.Marshal(map[string]string{"snapshot_fingerprint": sourceSnapshotFingerprint(a)})
	_, err = tx.Exec(ctx, `INSERT INTO article_processing_items(run_id,article_id,article_attempt,input_fingerprint,claim_token,state,result) VALUES($1::uuid,$2,$3,$4,$5::uuid,'running',$6::jsonb)`, f.RunID, a.ID, a.ProcessingAttempts, fp, a.ClaimToken, receipt)
	if err != nil {
		return Article{}, "", false, errors.New("localization claim receipt failed")
	}
	_, err = tx.Exec(ctx, `UPDATE article_localizations SET attempts=attempts+1,next_retry_at=$2,last_error='retry_interrupted',updated_at=now() WHERE article_id=$1 AND locale='sr-Latn'`, a.ID, time.Now().UTC().Add(delay))
	if err != nil {
		return Article{}, "", false, errors.New("localization claim update failed")
	}
	if err = tx.Commit(ctx); err != nil {
		return Article{}, "", false, errors.New("localization claim commit failed")
	}
	return a, fp, true, nil
}

func localizationFingerprint(input map[string]any) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(input)
	sum := sha256.Sum256(bytes.TrimSuffix(b.Bytes(), []byte("\n")))
	return hex.EncodeToString(sum[:])
}

func (s *Store) SaveLocalizationRetry(ctx context.Context, f Fence, a Article, fp, title, summary, prompt, model string, valid bool, maxAttempts int) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("localization outcome unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return e
	} else if state != "running" {
		return ErrCancelled
	}
	var token, storedFP, state string
	var receipt []byte
	var attempt int
	err = tx.QueryRow(ctx, `SELECT claim_token::text,input_fingerprint,state,result,article_attempt FROM article_processing_items WHERE run_id=$1::uuid AND article_id=$2 FOR UPDATE`, f.RunID, a.ID).Scan(&token, &storedFP, &state, &receipt, &attempt)
	if err != nil || token != a.ClaimToken || storedFP != fp || state != "running" || attempt != a.ProcessingAttempts {
		return errors.New("localization claim changed")
	}
	var current Article
	var articleAttempt int
	err = tx.QueryRow(ctx, `SELECT COALESCE(title,''),COALESCE(url,''),COALESCE(content,''),COALESCE(summary,''),COALESCE(summary_processed,false),COALESCE(category_processed,false),COALESCE(ad_processed,false),is_advertisement,processing_attempts FROM articles WHERE id=$1 FOR UPDATE`, a.ID).Scan(&current.Title, &current.URL, &current.Content, &current.Summary, &current.SummaryProcessed, &current.CategoryProcessed, &current.AdProcessed, &current.IsAdvertisement, &articleAttempt)
	var data map[string]string
	_ = json.Unmarshal(receipt, &data)
	if err != nil || articleAttempt != attempt || sourceSnapshotFingerprint(current) != data["snapshot_fingerprint"] {
		return errors.New("article changed during localization")
	}
	status := "retry_wait"
	if valid {
		status = "completed"
	}
	_, err = tx.Exec(ctx, `UPDATE article_localizations SET title=CASE WHEN $2 THEN $3 ELSE title END,summary=CASE WHEN $2 THEN $4 ELSE summary END,input_fingerprint=CASE WHEN $2 THEN $5 ELSE input_fingerprint END,prompt_version=CASE WHEN $2 THEN $6 ELSE prompt_version END,model=CASE WHEN $2 THEN $7 ELSE model END,status=CASE WHEN $2 THEN 'completed' WHEN attempts >= $8 THEN 'exhausted' ELSE 'retry_wait' END,next_retry_at=CASE WHEN $2 OR attempts >= $8 THEN NULL ELSE next_retry_at END,last_error=CASE WHEN $2 THEN NULL ELSE 'invalid_localization' END,updated_at=now() WHERE article_id=$1 AND locale='sr-Latn' AND status='retry_wait'`, a.ID, valid, title, summary, fp, prompt, model, maxAttempts)
	if err != nil {
		return errors.New("localization outcome update failed")
	}
	tag, err := tx.Exec(ctx, `UPDATE article_processing_items SET state=$3,updated_at=now() WHERE run_id=$1::uuid AND article_id=$2 AND state='running' AND claim_token=$4::uuid AND input_fingerprint=$5`, f.RunID, a.ID, status, a.ClaimToken, fp)
	if err != nil || tag.RowsAffected() != 1 {
		return errors.New("localization receipt update failed")
	}
	return tx.Commit(ctx)
}
