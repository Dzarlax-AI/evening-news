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
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrStaleFence = errors.New("processing execution fence is no longer active")
var ErrRunConflict = errors.New("processing run envelope conflicts with its receipt")
var ErrCancelled = errors.New("processing run is cancelled")
var ErrCategoryMapping = errors.New("article category mapping target unavailable")

type Envelope struct {
	Version            int       `json:"version"`
	JobID              string    `json:"job_id"`
	AttemptGeneration  int32     `json:"attempt_generation"`
	OwnerID            string    `json:"owner_id"`
	Generation         int32     `json:"generation"`
	BackendPID         int32     `json:"backend_pid"`
	ExecutorInstanceID string    `json:"executor_instance_id"`
	DeadlineAt         time.Time `json:"deadline_at"`
}
type Fence struct {
	RunID          string
	Envelope       Envelope
	EnvelopeSHA256 string
}
type Category struct {
	Name            string  `json:"name"`
	Confidence      float64 `json:"confidence"`
	AICategory      string  `json:"ai_category"`
	DisplayCategory string  `json:"display_category"`
}
type Persisted struct {
	State         string
	Fields        map[string]any
	Attempts      int
	NextAttemptAt *time.Time
	Error         *string
	Processed     bool
}
type Article struct {
	ID                 int64           `json:"id"`
	Title              string          `json:"title"`
	URL                string          `json:"url"`
	Content            string          `json:"content"`
	Summary            string          `json:"summary"`
	RawDescription     string          `json:"raw_description"`
	Description        string          `json:"description"`
	SourceID           *int64          `json:"source_id"`
	SourceType         string          `json:"source_type"`
	SourceName         string          `json:"source_name"`
	PublishedAt        *time.Time      `json:"published_at"`
	SummaryProcessed   bool            `json:"summary_processed"`
	CategoryProcessed  bool            `json:"category_processed"`
	AdProcessed        bool            `json:"ad_processed"`
	IsAdvertisement    *bool           `json:"is_advertisement"`
	ProcessingAttempts int             `json:"processing_attempts"`
	ProcessingState    string          `json:"processing_state"`
	ImageURL           string          `json:"image_url"`
	MediaFiles         json.RawMessage `json:"media_files"`
	Processed          bool            `json:"processed"`
	ClaimToken         string          `json:"-"`
}
type Store struct {
	Pool     *pgxpool.Pool
	lockMu   sync.Mutex
	lockConn map[string]*pgx.Conn
}

func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 1
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("database pool unavailable")
	}
	if err = pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("database unavailable")
	}
	return &Store{Pool: pool, lockConn: map[string]*pgx.Conn{}}, nil
}
func (s *Store) Close() {
	s.lockMu.Lock()
	for _, c := range s.lockConn {
		_ = c.Close(context.Background())
	}
	s.lockConn = map[string]*pgx.Conn{}
	s.lockMu.Unlock()
	s.Pool.Close()
}

// AcquireRunLock prevents duplicate execution across processor replicas. The dedicated
// session connection avoids deadlocking a single-connection work pool.
func (s *Store) AcquireRunLock(ctx context.Context, runID string) (func(), bool, error) {
	conn, err := pgx.ConnectConfig(ctx, s.Pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, false, errors.New("processing run lock unavailable")
	}
	var ok bool
	if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('article-processing-v1'),hashtext($1))`, runID).Scan(&ok); err != nil {
		_ = conn.Close(ctx)
		return nil, false, errors.New("processing run lock unavailable")
	}
	if !ok {
		_ = conn.Close(ctx)
		return nil, false, nil
	}
	var once sync.Once
	release := func() {
		once.Do(func() {
			cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, _ = conn.Exec(cctx, `SELECT pg_advisory_unlock(hashtext('article-processing-v1'),hashtext($1))`, runID)
			_ = conn.Close(cctx)
		})
	}
	return release, true, nil
}
func RunID(job string, attempt int32) string {
	sum := sha256.Sum256([]byte("evening-news-processing-v1\x00" + job + "\x00" + fmt.Sprint(attempt)))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
func EnvelopeHash(e Envelope) string {
	b, _ := json.Marshal(e)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}
func NewFence(e Envelope) Fence {
	return Fence{RunID: RunID(e.JobID, e.AttemptGeneration), Envelope: e, EnvelopeSHA256: EnvelopeHash(e)}
}
func (s *Store) ExistingRun(ctx context.Context, f Fence) (map[string]any, bool, error) {
	var id, job, state, digest string
	var attempt int32
	var result []byte
	var code *string
	err := s.Pool.QueryRow(ctx, `SELECT id::text,job_id::text,attempt_generation,state,result,error_code,envelope_sha256 FROM article_processing_runs WHERE job_id=$1 AND attempt_generation=$2`, f.Envelope.JobID, f.Envelope.AttemptGeneration).Scan(&id, &job, &attempt, &state, &result, &code, &digest)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, errors.New("processing run lookup failed")
	}
	var r map[string]any
	_ = json.Unmarshal(result, &r)
	return map[string]any{"id": id, "job_id": job, "attempt_generation": attempt, "state": state, "result": r, "error_code": code, "envelope_sha256": digest}, true, nil
}
func (s *Store) FenceTx(ctx context.Context, tx pgx.Tx, f Fence) (string, string, error) {
	var id, state string
	e := f.Envelope
	err := tx.QueryRow(ctx, `SELECT run_id::text,run_state FROM fence_article_processing_attempt($1::uuid,$2::uuid,$3,$4,$5::uuid,$6,$7,$8::uuid,$9::timestamp)`, f.RunID, e.JobID, e.AttemptGeneration, f.EnvelopeSHA256, e.OwnerID, e.Generation, e.BackendPID, e.ExecutorInstanceID, e.DeadlineAt.UTC()).Scan(&id, &state)
	if err != nil {
		if pgCode(err) == "P0001" {
			return "", "", ErrStaleFence
		}
		return "", "", errors.New("processing fence check failed")
	}
	if id != f.RunID {
		return "", "", ErrRunConflict
	}
	return id, state, nil
}
func pgCode(err error) string {
	var p *pgconn.PgError
	if errors.As(err, &p) {
		return p.Code
	}
	return ""
}
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func (s *Store) GetRun(ctx context.Context, id string) (map[string]any, error) {
	var job, state string
	var attempt int32
	var result []byte
	var code *string
	err := s.Pool.QueryRow(ctx, `SELECT job_id::text,attempt_generation,state,result,error_code FROM article_processing_runs WHERE id=$1::uuid`, id).Scan(&job, &attempt, &state, &result, &code)
	if err != nil {
		return nil, errors.New("processing run unavailable")
	}
	var r map[string]any
	_ = json.Unmarshal(result, &r)
	return map[string]any{"id": id, "job_id": job, "attempt_generation": attempt, "state": state, "result": r, "error_code": code}, nil
}
func (s *Store) Cancel(ctx context.Context, id string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("processing cancellation unavailable")
	}
	defer rollback(tx)
	var state string
	err = tx.QueryRow(ctx, `SELECT state FROM article_processing_runs WHERE id=$1::uuid FOR UPDATE`, id).Scan(&state)
	if err != nil {
		return errors.New("processing run unavailable")
	}
	if state == "running" {
		if _, err = tx.Exec(ctx, `UPDATE article_processing_runs SET state='cancelled',updated_at=now(),finished_at=now() WHERE id=$1::uuid AND state='running'`, id); err != nil {
			return errors.New("processing cancellation failed")
		}
		if _, err = tx.Exec(ctx, `UPDATE article_processing_items SET state='cancelled',updated_at=now() WHERE run_id=$1::uuid AND state='running'`, id); err != nil {
			return errors.New("processing item cancellation failed")
		}
	}
	return tx.Commit(ctx)
}
func (s *Store) FailRun(ctx context.Context, f Fence, code string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("processing result transaction unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return e
	} else if state != "running" {
		return ErrCancelled
	}
	_, err = tx.Exec(ctx, `UPDATE article_processing_runs SET state='failed',error_code=$2,updated_at=now(),finished_at=now() WHERE id=$1::uuid AND state='running'`, f.RunID, code)
	if err != nil {
		return errors.New("processing run update failed")
	}
	return tx.Commit(ctx)
}
func (s *Store) FinishRun(ctx context.Context, f Fence, receipt map[string]any) error {
	b, _ := json.Marshal(receipt)
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("processing result transaction unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return e
	} else if state != "running" {
		return ErrCancelled
	}
	_, err = tx.Exec(ctx, `UPDATE article_processing_runs SET state='succeeded',result=$2::jsonb,updated_at=now(),finished_at=now() WHERE id=$1::uuid AND state='running'`, f.RunID, b)
	if err != nil {
		return errors.New("processing run update failed")
	}
	return tx.Commit(ctx)
}
func (s *Store) CanonicalJob(ctx context.Context, f Fence) (string, json.RawMessage, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return "", nil, errors.New("processing transaction unavailable")
	}
	defer rollback(tx)
	if _, _, err = s.FenceTx(ctx, tx, f); err != nil {
		return "", nil, err
	}
	var kind string
	var payload []byte
	err = tx.QueryRow(ctx, `SELECT kind,payload FROM background_jobs WHERE id=$1::uuid`, f.Envelope.JobID).Scan(&kind, &payload)
	if err != nil {
		return "", nil, errors.New("canonical processing job unavailable")
	}
	if err = tx.Commit(ctx); err != nil {
		return "", nil, errors.New("processing transaction failed")
	}
	return kind, payload, nil
}

func (s *Store) RecoverClaims(ctx context.Context, f Fence) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("processing claim recovery unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return e
	} else if state != "running" {
		return ErrCancelled
	}
	_, err = tx.Exec(ctx, `UPDATE article_processing_items i SET state='cancelled',updated_at=now() FROM article_processing_runs r WHERE i.run_id=r.id AND i.state='running' AND (r.state<>'running' OR r.deadline_at<=now() OR NOT EXISTS(SELECT 1 FROM background_jobs j WHERE j.id=r.job_id AND j.state='running' AND j.attempt_generation=r.attempt_generation))`)
	if err != nil {
		return errors.New("processing claim recovery failed")
	}
	return tx.Commit(ctx)
}
func (s *Store) RecoverExhausted(ctx context.Context, f Fence, maxAttempts int, now time.Time) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("processing exhaustion recovery unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return e
	} else if state != "running" {
		return ErrCancelled
	}
	_, err = tx.Exec(ctx, `UPDATE articles SET processing_state='exhausted',next_attempt_at=NULL,processed=true WHERE processing_state IN ('pending','retry_wait') AND processing_attempts >= $1 AND (next_attempt_at IS NULL OR next_attempt_at <= $2)`, maxAttempts, now)
	if err != nil {
		return errors.New("processing exhaustion recovery failed")
	}
	return tx.Commit(ctx)
}
func (s *Store) SelectDue(ctx context.Context, f Fence, limit int, cursor int64, selected []int64, dateFrom, dateTo *time.Time, maxAttempts int) ([]Article, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, errors.New("article selection transaction unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return nil, e
	} else if state != "running" {
		return nil, ErrCancelled
	}
	var rows pgx.Rows
	if len(selected) > 0 {
		rows, err = tx.Query(ctx, `SELECT a.id,COALESCE(a.title,''),COALESCE(a.url,''),COALESCE(a.content,''),COALESCE(a.summary,''),a.source_id,COALESCE(s.source_type,'rss'),COALESCE(s.name,'Unknown'),a.published_at,COALESCE(a.summary_processed,false),COALESCE(a.category_processed,false),COALESCE(a.ad_processed,false),a.is_advertisement,COALESCE(a.processing_attempts,0),COALESCE(a.processing_state,'pending'),COALESCE(a.image_url,''),COALESCE(a.media_files::jsonb,'[]'::jsonb),COALESCE(a.processed,false) FROM articles a LEFT JOIN sources s ON s.id=a.source_id WHERE a.id=ANY($1) AND a.id>$2 AND COALESCE(a.processing_attempts,0)<$3 AND (COALESCE(a.processing_state,'pending')='pending' OR (a.processing_state='retry_wait' AND a.next_attempt_at<=now())) ORDER BY a.id LIMIT $4`, selected, cursor, maxAttempts, limit)
	} else {
		rows, err = tx.Query(ctx, `SELECT a.id,COALESCE(a.title,''),COALESCE(a.url,''),COALESCE(a.content,''),COALESCE(a.summary,''),a.source_id,COALESCE(s.source_type,'rss'),COALESCE(s.name,'Unknown'),a.published_at,COALESCE(a.summary_processed,false),COALESCE(a.category_processed,false),COALESCE(a.ad_processed,false),a.is_advertisement,COALESCE(a.processing_attempts,0),COALESCE(a.processing_state,'pending'),COALESCE(a.image_url,''),COALESCE(a.media_files::jsonb,'[]'::jsonb),COALESCE(a.processed,false) FROM articles a LEFT JOIN sources s ON s.id=a.source_id WHERE a.id>$1 AND COALESCE(a.processing_attempts,0)<$2 AND (COALESCE(a.processing_state,'pending')='pending' OR (a.processing_state='retry_wait' AND a.next_attempt_at<=now())) AND ($3::timestamp IS NULL OR a.published_at >= $3) AND ($4::timestamp IS NULL OR a.published_at <= $4) ORDER BY a.id LIMIT $5`, cursor, maxAttempts, dateFrom, dateTo, limit)
	}
	if err != nil {
		return nil, errors.New("article selection failed")
	}
	defer rows.Close()
	out := []Article{}
	for rows.Next() {
		var a Article
		if err = rows.Scan(&a.ID, &a.Title, &a.URL, &a.Content, &a.Summary, &a.SourceID, &a.SourceType, &a.SourceName, &a.PublishedAt, &a.SummaryProcessed, &a.CategoryProcessed, &a.AdProcessed, &a.IsAdvertisement, &a.ProcessingAttempts, &a.ProcessingState, &a.ImageURL, &a.MediaFiles, &a.Processed); err != nil {
			return nil, errors.New("article decode failed")
		}
		out = append(out, a)
	}
	if rows.Err() != nil {
		return nil, errors.New("article selection failed")
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, errors.New("article selection commit failed")
	}
	return out, nil
}

func fingerprint(title, content, url string, original *string) string {
	v := map[string]string{"title": title, "content": content, "url": url}
	if original != nil {
		v["original_context"] = *original
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	buf := bytes.TrimSuffix(out.Bytes(), []byte("\n"))
	h := sha256.Sum256(buf)
	return hex.EncodeToString(h[:])
}
func (s *Store) ClaimArticle(ctx context.Context, f Fence, a Article, force bool, timeout, retryDelay time.Duration) (int, string, string, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, "", "", errors.New("article claim transaction unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return 0, "", "", e
	} else if state != "running" {
		return 0, "", "", ErrCancelled
	}
	var currentAttempt int
	var state string
	var title, url, content, summary string
	var summaryProcessed, categoryProcessed, adProcessed bool
	var advertisement *bool
	err = tx.QueryRow(ctx, `SELECT COALESCE(processing_attempts,0),COALESCE(processing_state,'pending'),COALESCE(title,''),COALESCE(url,''),COALESCE(content,''),COALESCE(summary,''),COALESCE(summary_processed,false),COALESCE(category_processed,false),COALESCE(ad_processed,false),is_advertisement FROM articles WHERE id=$1 FOR UPDATE`, a.ID).Scan(&currentAttempt, &state, &title, &url, &content, &summary, &summaryProcessed, &categoryProcessed, &adProcessed, &advertisement)
	if err != nil {
		return 0, "", "", errors.New("article claim target unavailable")
	}
	if currentAttempt != a.ProcessingAttempts || !(state == "pending" || state == "retry_wait") {
		return 0, "", "", errors.New("article claim no longer eligible")
	}
	if title != a.Title || url != a.URL || content != a.Content || summary != a.Summary || summaryProcessed != a.SummaryProcessed || categoryProcessed != a.CategoryProcessed || adProcessed != a.AdProcessed || !sameBool(advertisement, a.IsAdvertisement) {
		return 0, "", "", errors.New("article changed before processing claim")
	}
	attempt := currentAttempt + 1
	delay := max(timeout+60*time.Second, retryDelay)
	if delay <= 0 {
		delay = 30 * time.Minute
	}
	claimDeadline := time.Now().UTC().Add(delay)
	tag, err := tx.Exec(ctx, `UPDATE articles SET processing_attempts=$2,processing_state='retry_wait',next_attempt_at=$3,last_processing_error='Attempt interrupted before outcome was saved' WHERE id=$1 AND processing_attempts=$4`, a.ID, attempt, claimDeadline, currentAttempt)
	if err != nil || tag.RowsAffected() != 1 {
		return 0, "", "", errors.New("article claim lost")
	}
	srcfp := sourceSnapshotFingerprint(a)
	var raw [16]byte
	if _, err = rand.Read(raw[:]); err != nil {
		return 0, "", "", errors.New("article claim token unavailable")
	}
	token := fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16])
	var currentInput string
	original := (*string)(nil)
	if a.SourceType == "telegram" && len([]rune(strings.TrimSpace(content))) >= 30 {
		original = &content
	}
	currentInput = fingerprint(title, content, url, original)
	initial, _ := json.Marshal(map[string]string{"snapshot_fingerprint": srcfp})
	_, err = tx.Exec(ctx, `INSERT INTO article_processing_items(run_id,article_id,article_attempt,input_fingerprint,claim_token,state,result) VALUES($1::uuid,$2,$3,$4,$5::uuid,'running',$6::jsonb)`, f.RunID, a.ID, attempt, currentInput, token, initial)
	if err != nil {
		return 0, "", "", errors.New("article claim receipt unavailable")
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, "", "", errors.New("article claim commit failed")
	}
	return attempt, currentInput, token, nil
}
func (s *Store) UpdateItemFingerprint(ctx context.Context, f Fence, articleID int64, attempt int, claimToken, fingerprint string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("article claim update unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return e
	} else if state != "running" {
		return ErrCancelled
	}
	tag, err := tx.Exec(ctx, `UPDATE article_processing_items SET input_fingerprint=$5,updated_at=now() WHERE run_id=$1::uuid AND article_id=$2 AND article_attempt=$3 AND claim_token=$4::uuid AND state='running'`, f.RunID, articleID, attempt, claimToken, fingerprint)
	if err != nil || tag.RowsAffected() != 1 {
		return errors.New("article claim changed")
	}
	return tx.Commit(ctx)
}
func (s *Store) SaveOutcome(ctx context.Context, f Fence, a Article, attempt int, claimToken, inputFP string, p Persisted) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("article outcome transaction unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return e
	} else if state != "running" {
		return ErrCancelled
	}
	var claimFP, storedToken string
	var claimState string
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT input_fingerprint,claim_token::text,state,result FROM article_processing_items WHERE run_id=$1::uuid AND article_id=$2 AND article_attempt=$3 FOR UPDATE`, f.RunID, a.ID, attempt).Scan(&claimFP, &storedToken, &claimState, &raw)
	if err != nil || claimFP != inputFP || storedToken != claimToken || claimState != "running" {
		return errors.New("article outcome claim changed")
	}
	var receipt map[string]string
	_ = json.Unmarshal(raw, &receipt)
	var articleAttempt int
	var dbTitle, dbURL, dbContent, dbSummary string
	var dbSummaryProcessed, dbCategoryProcessed, dbAdProcessed bool
	var dbAdvertisement *bool
	err = tx.QueryRow(ctx, `SELECT COALESCE(processing_attempts,0),COALESCE(title,''),COALESCE(url,''),COALESCE(content,''),COALESCE(summary,''),COALESCE(summary_processed,false),COALESCE(category_processed,false),COALESCE(ad_processed,false),is_advertisement FROM articles WHERE id=$1 FOR UPDATE`, a.ID).Scan(&articleAttempt, &dbTitle, &dbURL, &dbContent, &dbSummary, &dbSummaryProcessed, &dbCategoryProcessed, &dbAdProcessed, &dbAdvertisement)
	dbArticle := a
	dbArticle.Title, dbArticle.URL, dbArticle.Content, dbArticle.Summary = dbTitle, dbURL, dbContent, dbSummary
	dbArticle.SummaryProcessed, dbArticle.CategoryProcessed, dbArticle.AdProcessed, dbArticle.IsAdvertisement = dbSummaryProcessed, dbCategoryProcessed, dbAdProcessed, dbAdvertisement
	if err != nil || articleAttempt != attempt || sourceSnapshotFingerprint(dbArticle) != receipt["snapshot_fingerprint"] {
		return errors.New("article changed during processing")
	}
	fields := p.Fields
	sets := []string{"processing_attempts=$2", "processing_state=$3", "next_attempt_at=$4", "last_processing_error=$5", "processed=$6"}
	args := []any{a.ID, p.Attempts, p.State, p.NextAttemptAt, p.Error, p.Processed}
	allowed := map[string]bool{"title": true, "content": true, "summary": true, "summary_processed": true, "category_processed": true, "ad_processed": true, "is_advertisement": true, "ad_confidence": true, "ad_type": true, "ad_reasoning": true, "image_url": true, "media_files": true, "published_at": true}
	for _, key := range []string{"title", "content", "summary", "summary_processed", "category_processed", "ad_processed", "is_advertisement", "ad_confidence", "ad_type", "ad_reasoning", "image_url", "media_files", "published_at"} {
		v, ok := fields[key]
		if !ok || !allowed[key] {
			continue
		}
		if key == "media_files" {
			b, _ := json.Marshal(v)
			v = b
		}
		args = append(args, v)
		sets = append(sets, fmt.Sprintf("%s=$%d", key, len(args)))
	}
	if _, err = tx.Exec(ctx, `UPDATE articles SET `+strings.Join(sets, ",")+` WHERE id=$1`, args...); err != nil {
		return errors.New("article update failed")
	}
	if cats, ok := fields["categories"].([]Category); ok {
		if _, err = tx.Exec(ctx, `DELETE FROM article_categories WHERE article_id=$1`, a.ID); err != nil {
			return errors.New("category replacement failed")
		}
		for _, cat := range cats {
			var categoryID int
			// Python's category service first accepts a configured fixed name
			// directly. Only unknown names are eligible for user mappings.
			err = tx.QueryRow(ctx, `SELECT id FROM categories WHERE lower(name)=lower($1) LIMIT 1`, cat.Name).Scan(&categoryID)
			if errors.Is(err, pgx.ErrNoRows) {
				fixed, found, lookupErr := lookupFixedCategory(ctx, tx, cat.Name)
				if lookupErr != nil {
					return errors.New("category mapping lookup failed")
				}
				if !found {
					fixed = cat.DisplayCategory
				}
				if fixed == "" {
					fixed = "Other"
				}
				err = tx.QueryRow(ctx, `SELECT id FROM categories WHERE lower(name)=lower($1) LIMIT 1`, fixed).Scan(&categoryID)
				if errors.Is(err, pgx.ErrNoRows) && !strings.EqualFold(fixed, "Other") {
					err = tx.QueryRow(ctx, `SELECT id FROM categories WHERE lower(name)='other' LIMIT 1`).Scan(&categoryID)
				}
			}
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					return ErrCategoryMapping
				}
				return errors.New("category target lookup failed")
			}
			_, err = tx.Exec(ctx, `INSERT INTO article_categories(article_id,category_id,confidence,ai_category,created_at) VALUES($1,$2,$3,$4,now()) ON CONFLICT(article_id,category_id) DO NOTHING`, a.ID, categoryID, cat.Confidence, cat.AICategory)
			if err != nil {
				return errors.New("category insert failed")
			}
		}
	}
	result, _ := json.Marshal(map[string]any{"state": p.State, "error": p.Error})
	tag, err := tx.Exec(ctx, `UPDATE article_processing_items SET state=$4,result=$5::jsonb,updated_at=now() WHERE run_id=$1::uuid AND article_id=$2 AND article_attempt=$3 AND claim_token=$6::uuid AND input_fingerprint=$7 AND state='running'`, f.RunID, a.ID, attempt, p.State, result, claimToken, inputFP)
	if err != nil || tag.RowsAffected() != 1 {
		return errors.New("article item update failed")
	}
	return tx.Commit(ctx)
}

func lookupFixedCategory(ctx context.Context, tx pgx.Tx, name string) (string, bool, error) {
	var fixed, mappingName string
	err := tx.QueryRow(ctx, `SELECT ai_category,fixed_category FROM category_mapping WHERE ai_category=$1 AND is_active IS TRUE LIMIT 1 FOR UPDATE`, name).Scan(&mappingName, &fixed)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `SELECT ai_category,fixed_category FROM category_mapping WHERE lower(ai_category)=lower($1) AND is_active IS TRUE LIMIT 1 FOR UPDATE`, name).Scan(&mappingName, &fixed)
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	if _, err = tx.Exec(ctx, `UPDATE category_mapping SET usage_count=COALESCE(usage_count,0)+1,last_used=now() WHERE ai_category=$1`, mappingName); err != nil {
		return "", false, err
	}
	return fixed, true, nil
}

// RecordCategoryMappingFailure preserves the already-committed claim and its
// finite recovery deadline after the category transaction has rolled back.
func (s *Store) RecordCategoryMappingFailure(ctx context.Context, f Fence, a Article, attempt int, claimToken, inputFP string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("category failure receipt unavailable")
	}
	defer rollback(tx)
	if _, state, e := s.FenceTx(ctx, tx, f); e != nil {
		return e
	} else if state != "running" {
		return ErrCancelled
	}
	var storedToken, claimFP, itemState string
	var raw []byte
	if err = tx.QueryRow(ctx, `SELECT claim_token::text,input_fingerprint,state,result FROM article_processing_items WHERE run_id=$1::uuid AND article_id=$2 AND article_attempt=$3 FOR UPDATE`, f.RunID, a.ID, attempt).Scan(&storedToken, &claimFP, &itemState, &raw); err != nil || storedToken != claimToken || claimFP != inputFP || itemState != "running" {
		return errors.New("category failure claim changed")
	}
	var receipt map[string]string
	_ = json.Unmarshal(raw, &receipt)
	var current Article
	if err = tx.QueryRow(ctx, `SELECT id,COALESCE(title,''),COALESCE(url,''),COALESCE(content,''),COALESCE(summary,''),COALESCE(summary_processed,false),COALESCE(category_processed,false),COALESCE(ad_processed,false),is_advertisement FROM articles WHERE id=$1 FOR UPDATE`, a.ID).Scan(&current.ID, &current.Title, &current.URL, &current.Content, &current.Summary, &current.SummaryProcessed, &current.CategoryProcessed, &current.AdProcessed, &current.IsAdvertisement); err != nil {
		return errors.New("category failure article unavailable")
	}
	if sourceSnapshotFingerprint(current) != receipt["snapshot_fingerprint"] {
		return errors.New("article changed during category failure handling")
	}
	if _, err = tx.Exec(ctx, `UPDATE articles SET last_processing_error='category_mapping_failed' WHERE id=$1 AND processing_attempts=$2`, a.ID, attempt); err != nil {
		return errors.New("category failure article update unavailable")
	}
	result, _ := json.Marshal(map[string]any{"state": "retry_wait", "error": "category_mapping_failed"})
	tag, err := tx.Exec(ctx, `UPDATE article_processing_items SET state='retry_wait',result=$4::jsonb,updated_at=now() WHERE run_id=$1::uuid AND article_id=$2 AND article_attempt=$3 AND claim_token=$5::uuid AND input_fingerprint=$6 AND state='running'`, f.RunID, a.ID, attempt, result, claimToken, inputFP)
	if err != nil || tag.RowsAffected() != 1 {
		return errors.New("category failure item update unavailable")
	}
	return tx.Commit(ctx)
}

func sourceSnapshotFingerprint(a Article) string {
	v := map[string]any{
		"title": a.Title, "url": a.URL, "content": a.Content, "summary": a.Summary,
		"summary_processed": a.SummaryProcessed, "category_processed": a.CategoryProcessed,
		"ad_processed": a.AdProcessed, "is_advertisement": a.IsAdvertisement,
	}
	var out bytes.Buffer
	enc := json.NewEncoder(&out)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	h := sha256.Sum256(bytes.TrimSuffix(out.Bytes(), []byte("\n")))
	return hex.EncodeToString(h[:])
}

func sameBool(a, b *bool) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	return *a == *b
}

type ArticleOutcome struct {
	ArticleID     int64      `json:"article_id"`
	State         string     `json:"state"`
	Reason        *string    `json:"reason"`
	NextAttemptAt *time.Time `json:"next_attempt_at"`
}

var articleOutcomeReasonCodes = map[string]struct{}{
	"advertisement": {}, "category_mapping_failed": {}, "content_refresh_unavailable": {},
	"extraction_challenge": {}, "extraction_unavailable": {}, "extractor_failed": {},
	"http_error": {}, "invalid_response": {}, "missing_ad_classification": {},
	"missing_categories": {}, "missing_summary": {}, "provider_error": {},
	"provider_timeout": {}, "provider_unavailable": {}, "resource_limit": {},
	"smart_filter_rejected": {}, "summary_describes_challenge": {}, "timeout": {},
	"processing_error": {}, "busy": {}, "cancelled": {},
}

func sanitizeArticleOutcomeReason(raw *string) *string {
	if raw == nil || strings.TrimSpace(*raw) == "" {
		return nil
	}
	code := strings.TrimSpace(*raw)
	if _, ok := articleOutcomeReasonCodes[code]; !ok {
		code = "other"
	}
	return &code
}

func (s *Store) ArticleOutcomes(ctx context.Context, ids []int64) ([]ArticleOutcome, error) {
	if len(ids) == 0 {
		return []ArticleOutcome{}, nil
	}
	rows, err := s.Pool.Query(ctx, `SELECT id,processing_state,last_processing_error,next_attempt_at FROM articles WHERE id=ANY($1) ORDER BY id`, ids)
	if err != nil {
		return nil, errors.New("article outcome read failed")
	}
	defer rows.Close()
	out := []ArticleOutcome{}
	for rows.Next() {
		var v ArticleOutcome
		if err = rows.Scan(&v.ArticleID, &v.State, &v.Reason, &v.NextAttemptAt); err != nil {
			return nil, errors.New("article outcome decode failed")
		}
		v.Reason = sanitizeArticleOutcomeReason(v.Reason)
		out = append(out, v)
	}
	if rows.Err() != nil {
		return nil, errors.New("article outcome read failed")
	}
	return out, nil
}
func (s *Store) HasDuplicateHash(ctx context.Context, hash string, articleID int64) (bool, error) {
	var exists bool
	err := s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM articles WHERE hash_content=$1 AND id<>$2)`, hash, articleID).Scan(&exists)
	if err != nil {
		return false, errors.New("duplicate lookup failed")
	}
	return exists, nil
}
func (s *Store) String() string { return strings.TrimSpace("article-processing-store") }
