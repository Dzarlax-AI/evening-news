package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrStaleFence   = errors.New("ingestion execution fence is no longer active")
	ErrRunConflict  = errors.New("ingestion run envelope conflicts with its receipt")
	ErrRunCancelled = errors.New("ingestion run is cancelled")
)

type Fence struct {
	RunID             string
	JobID             string
	AttemptGeneration int32
	EnvelopeSHA256    string
	OwnerID           string
	WorkerGeneration  int32
	BackendPID        int32
	ExecutorInstance  string
	DeadlineAt        time.Time
}

type Source struct {
	ID          int32
	Name        string
	Type        string
	URL         string
	Config      map[string]any
	ConfigJSON  json.RawMessage
	ConfigText  string
	Fingerprint string
}

type Article struct {
	Title       string          `json:"title"`
	URL         string          `json:"url"`
	Content     *string         `json:"content"`
	Summary     *string         `json:"summary"`
	ImageURL    *string         `json:"image_url"`
	MediaFiles  json.RawMessage `json:"media_files"`
	PublishedAt *time.Time      `json:"published_at"`
	SourceType  string          `json:"source_type"`
	SourceName  string          `json:"source_name"`
	RawData     map[string]any  `json:"raw_data"`
}

type PageState struct {
	ScanLimit          int
	CursorOffset       int
	ScanActive         bool
	PreviousHashes     map[string]string
	ScanHashes         map[string]string
	AcknowledgedHashes map[string]string
	SnapshotAt         *time.Time
	ContentSHA256      string
	CandidateIndexes   []int `json:"-"`
	SegmentOffset      int   `json:"-"`
}

type CommitCounts struct{ Candidates, Saved, Updated, Skipped int }
type Store struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, dsn string, maxConns int32) (*Store, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	probe, err := pgx.Connect(ctx, dsn)
	if err != nil {
		return nil, errors.New("database unavailable")
	}
	var schema string
	err = probe.QueryRow(ctx, `SELECT current_schema()`).Scan(&schema)
	_ = probe.Close(ctx)
	if err != nil || schema == "" || strings.ContainsAny(schema, "\x00,\"") {
		return nil, errors.New("database schema unavailable")
	}
	cfg.MaxConns, cfg.MinConns = maxConns, 1
	if cfg.ConnConfig.RuntimeParams == nil {
		cfg.ConnConfig.RuntimeParams = map[string]string{}
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("database pool unavailable")
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, errors.New("database unavailable")
	}
	return &Store{Pool: pool}, nil
}
func (s *Store) Close() { s.Pool.Close() }

// AcquireRunLock holds a session advisory lock for the full execution window.
// The dedicated connection prevents two ingestion processes from collecting
// the same resumed run at once; the database fence remains the commit guard.
func (s *Store) AcquireRunLock(ctx context.Context, runID string) (func(), bool, error) {
	// Keep the advisory lock on a dedicated connection so a pool configured
	// with one ingestion connection can still perform fenced work.
	conn, err := pgx.ConnectConfig(ctx, s.Pool.Config().ConnConfig.Copy())
	if err != nil {
		return nil, false, errors.New("ingestion execution lock unavailable")
	}
	var acquired bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtext('article-ingestion-v1'),hashtext($1))`, runID).Scan(&acquired); err != nil {
		_ = conn.Close(ctx)
		return nil, false, errors.New("ingestion execution lock unavailable")
	}
	if !acquired {
		_ = conn.Close(ctx)
		return nil, false, nil
	}
	release := func() {
		unlockCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = conn.Exec(unlockCtx, `SELECT pg_advisory_unlock(hashtext('article-ingestion-v1'),hashtext($1))`, runID)
		_ = conn.Close(unlockCtx)
	}
	return release, true, nil
}

// SourceFingerprint is shared with the Python selector callback. Each field is
// encoded as byte-length, colon, then the UTF-8 bytes. ConfigJSON is obtained
// from PostgreSQL's config::text so both runtimes hash PostgreSQL's canonical
// JSONB spelling instead of language-specific JSON encodings.
func SourceFingerprint(id int32, name, sourceType, url string, enabled bool, configText string) string {
	parts := []string{fmt.Sprint(id), name, sourceType, url, fmt.Sprint(enabled), configText}
	var b strings.Builder
	for _, part := range parts {
		b.WriteString(fmt.Sprint(len([]byte(part))))
		b.WriteByte(':')
		b.WriteString(part)
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

func RunID(jobID string, attempt int32) string {
	sum := sha256.Sum256([]byte("evening-news-ingestion-v1\x00" + jobID + "\x00" + fmt.Sprint(attempt)))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (s *Store) ExistingRun(ctx context.Context, f Fence) (string, string, bool, error) {
	var id, digest, state string
	err := s.Pool.QueryRow(ctx, `SELECT id::text,envelope_sha256,state FROM article_ingestion_runs WHERE job_id=$1 AND attempt_generation=$2`, f.JobID, f.AttemptGeneration).Scan(&id, &digest, &state)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", "", false, nil
	}
	if err != nil {
		return "", "", false, errors.New("ingestion run lookup failed")
	}
	return id, digest, true, nil
}

func (s *Store) FenceTx(ctx context.Context, tx pgx.Tx, f Fence) (string, string, error) {
	var id, state string
	err := tx.QueryRow(ctx, `SELECT run_id,run_state FROM fence_article_ingestion_attempt($1::uuid,$2::uuid,$3,$4,$5::uuid,$6,$7,$8::uuid,$9::timestamp)`,
		f.RunID, f.JobID, f.AttemptGeneration, f.EnvelopeSHA256, f.OwnerID, f.WorkerGeneration, f.BackendPID, f.ExecutorInstance, f.DeadlineAt.UTC()).Scan(&id, &state)
	if err != nil {
		if pgCode(err) == "P0001" {
			return "", "", ErrStaleFence
		}
		return "", "", errors.New("ingestion fence check failed")
	}
	if id != f.RunID {
		return "", "", ErrRunConflict
	}
	return id, state, nil
}

func (s *Store) EnabledSources(ctx context.Context, f Fence) ([]Source, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return nil, errors.New("source read transaction unavailable")
	}
	defer rollback(tx)
	if _, _, err = s.FenceTx(ctx, tx, f); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT id,name,source_type,url,config,config::text FROM sources WHERE enabled IS TRUE AND source_type IN ('rss','telegram','custom') ORDER BY id`)
	if err != nil {
		return nil, errors.New("enabled source read failed")
	}
	items := make([]Source, 0)
	for rows.Next() {
		var src Source
		if err := rows.Scan(&src.ID, &src.Name, &src.Type, &src.URL, &src.ConfigJSON, &src.ConfigText); err != nil {
			rows.Close()
			return nil, errors.New("enabled source decode failed")
		}
		if len(src.ConfigJSON) == 0 {
			src.ConfigJSON = json.RawMessage(`{}`)
		}
		if json.Unmarshal(src.ConfigJSON, &src.Config) != nil {
			src.Config = map[string]any{}
		}
		if src.Config == nil {
			src.Config = map[string]any{}
		}
		src.Fingerprint = SourceFingerprint(src.ID, src.Name, src.Type, src.URL, true, src.ConfigText)
		items = append(items, src)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, errors.New("enabled source read failed")
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return nil, errors.New("source read transaction failed")
	}
	return items, nil
}

func (s *Store) BeginSource(ctx context.Context, f Fence, src Source) (PageState, bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return PageState{}, false, errors.New("source progress transaction unavailable")
	}
	defer rollback(tx)
	if _, state, err := s.FenceTx(ctx, tx, f); err != nil {
		return PageState{}, false, err
	} else if state != "running" {
		return PageState{}, false, ErrRunCancelled
	}
	var enabled bool
	var name, url, configText string
	var configRaw []byte
	err = tx.QueryRow(ctx, `SELECT enabled,name,url,config,config::text FROM sources WHERE id=$1 AND source_type=$2 FOR UPDATE`, src.ID, src.Type).Scan(&enabled, &name, &url, &configRaw, &configText)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := markSourceSkipped(ctx, tx, f, src, 0, time.Now().UTC()); err != nil {
			return PageState{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return PageState{}, false, errors.New("source skip commit failed")
		}
		return PageState{}, false, nil
	}
	if err != nil {
		return PageState{}, false, errors.New("source state check failed")
	}
	if !enabled {
		if err := markSourceSkipped(ctx, tx, f, src, 0, time.Now().UTC()); err != nil {
			return PageState{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return PageState{}, false, errors.New("source skip commit failed")
		}
		return PageState{}, false, nil
	}
	fp := SourceFingerprint(src.ID, name, src.Type, url, enabled, configText)
	if fp != src.Fingerprint {
		if err := markSourceSkipped(ctx, tx, f, src, 0, time.Now().UTC()); err != nil {
			return PageState{}, false, err
		}
		if err := tx.Commit(ctx); err != nil {
			return PageState{}, false, errors.New("source skip commit failed")
		}
		return PageState{}, false, nil
	}
	command, err := tx.Exec(ctx, `INSERT INTO article_ingestion_source_runs(run_id,source_id,source_type,config_sha256,state)
		VALUES($1,$2,$3,$4,'running') ON CONFLICT(run_id,source_id) DO UPDATE SET state='running',error_code=NULL
		WHERE article_ingestion_source_runs.config_sha256=EXCLUDED.config_sha256
		AND article_ingestion_source_runs.state NOT IN ('succeeded','partial','skipped')`, f.RunID, src.ID, src.Type, src.Fingerprint)
	if err != nil {
		return PageState{}, false, errors.New("source progress update failed")
	}
	if command.RowsAffected() == 0 {
		if err := tx.Commit(ctx); err != nil {
			return PageState{}, false, errors.New("source progress transaction failed")
		}
		return PageState{}, false, nil
	}
	page := PageState{ScanLimit: 20, PreviousHashes: map[string]string{}, ScanHashes: map[string]string{}, AcknowledgedHashes: map[string]string{}}
	if src.Type == "custom" {
		page, err = readPageState(ctx, tx, src)
		if err != nil {
			return PageState{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return PageState{}, false, errors.New("source progress transaction failed")
	}
	return page, true, nil
}

func readPageState(ctx context.Context, tx pgx.Tx, src Source) (PageState, error) {
	defaultState := func() PageState {
		return PageState{ScanLimit: intConfig(src.Config, "max_articles_per_check", 20, 1, 100),
			PreviousHashes: map[string]string{}, ScanHashes: map[string]string{}, AcknowledgedHashes: map[string]string{}}
	}
	p := defaultState()
	var fp string
	var loaded PageState
	var a, b, c []byte
	err := tx.QueryRow(ctx, `SELECT config_sha256,scan_limit,cursor_offset,scan_active,previous_hashes,scan_hashes,acknowledged_hashes,snapshot_at,COALESCE(content_sha256,'') FROM source_collection_state WHERE source_id=$1`, src.ID).Scan(&fp, &loaded.ScanLimit, &loaded.CursorOffset, &loaded.ScanActive, &a, &b, &c, &loaded.SnapshotAt, &loaded.ContentSHA256)
	if errors.Is(err, pgx.ErrNoRows) {
		return p, nil
	}
	if err != nil {
		return p, errors.New("page monitor state read failed")
	}
	if fp != src.Fingerprint {
		// A URL or config edit starts a fresh scan. Never carry the old cursor
		// into a new snapshot: its acknowledged prefix belongs to another source.
		return defaultState(), nil
	}
	p.ScanLimit, p.CursorOffset, p.ScanActive = loaded.ScanLimit, loaded.CursorOffset, loaded.ScanActive
	p.SnapshotAt, p.ContentSHA256 = loaded.SnapshotAt, loaded.ContentSHA256
	_ = json.Unmarshal(a, &p.PreviousHashes)
	_ = json.Unmarshal(b, &p.ScanHashes)
	_ = json.Unmarshal(c, &p.AcknowledgedHashes)
	if p.PreviousHashes == nil {
		p.PreviousHashes = map[string]string{}
	}
	if p.ScanHashes == nil {
		p.ScanHashes = map[string]string{}
	}
	if p.AcknowledgedHashes == nil {
		p.AcknowledgedHashes = map[string]string{}
	}
	if p.ScanLimit < 1 || p.ScanLimit > 100 {
		p.ScanLimit = 20
	}
	if p.CursorOffset >= p.ScanLimit {
		p.CursorOffset = 0
		p.ScanActive = false
	}
	return p, nil
}

func (s *Store) CommitSource(ctx context.Context, f Fence, src Source, articles []Article, outcome, errorCode string, page *PageState) (CommitCounts, error) {
	return s.commitSource(ctx, f, src, articles, outcome, errorCode, page, "")
}

// CommitSourceWithSelectorStudy atomically commits the Page Monitor prefix,
// cursor, source receipt, and the digest of the captured HTML used for a
// successful selector-study reparse.
func (s *Store) CommitSourceWithSelectorStudy(ctx context.Context, f Fence, src Source, articles []Article, outcome, errorCode string, page *PageState, studyHTMLSHA256 string) (CommitCounts, error) {
	if !validSHA256Hex(studyHTMLSHA256) {
		return CommitCounts{}, errors.New("selector study digest invalid")
	}
	return s.commitSource(ctx, f, src, articles, outcome, errorCode, page, studyHTMLSHA256)
}

func (s *Store) commitSource(ctx context.Context, f Fence, src Source, articles []Article, outcome, errorCode string, page *PageState, studyHTMLSHA256 string) (CommitCounts, error) {
	c := CommitCounts{Candidates: len(articles)}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return c, errors.New("article commit transaction unavailable")
	}
	defer rollback(tx)
	if _, state, err := s.FenceTx(ctx, tx, f); err != nil {
		return c, err
	} else if state != "running" {
		return c, ErrRunCancelled
	}
	var receiptState string
	err = tx.QueryRow(ctx, `SELECT state FROM article_ingestion_source_runs WHERE run_id=$1 AND source_id=$2 FOR UPDATE`, f.RunID, src.ID).Scan(&receiptState)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, errors.New("source progress receipt missing")
	}
	if err != nil {
		return c, errors.New("source progress receipt read failed")
	}
	if receiptState == "succeeded" || receiptState == "partial" || receiptState == "skipped" || receiptState == "failed" || receiptState == "resource_limit" || receiptState == "unavailable" {
		if err := tx.Commit(ctx); err != nil {
			return c, errors.New("source idempotency check failed")
		}
		return CommitCounts{}, nil
	}
	var enabled bool
	var name, url, configText string
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT enabled,name,url,config,config::text FROM sources WHERE id=$1 AND source_type=$2 FOR UPDATE`, src.ID, src.Type).Scan(&enabled, &name, &url, &raw, &configText)
	if errors.Is(err, pgx.ErrNoRows) {
		c.Skipped = len(articles)
		if e := markSourceSkipped(ctx, tx, f, src, c.Candidates, time.Now().UTC()); e != nil {
			return c, e
		}
		if err := tx.Commit(ctx); err != nil {
			return c, errors.New("source skip commit failed")
		}
		return c, nil
	}
	if err != nil {
		return c, errors.New("source state check failed")
	}
	if !enabled {
		c.Skipped = len(articles)
		if e := saveSourceRun(ctx, tx, f, src, c, "skipped", "source_disabled", time.Now().UTC(), ""); e != nil {
			return c, e
		}
		if e := updateRunProgress(ctx, tx, f, src.ID, c); e != nil {
			return c, e
		}
		if err := tx.Commit(ctx); err != nil {
			return c, errors.New("source skip commit failed")
		}
		return c, nil
	}
	fp := SourceFingerprint(src.ID, name, src.Type, url, enabled, configText)
	if fp != src.Fingerprint {
		c.Skipped = len(articles)
		if e := saveSourceRun(ctx, tx, f, src, c, "skipped", "source_changed", time.Now().UTC(), ""); e != nil {
			return c, e
		}
		if e := updateRunProgress(ctx, tx, f, src.ID, c); e != nil {
			return c, e
		}
		if err := tx.Commit(ctx); err != nil {
			return c, errors.New("source skip commit failed")
		}
		return c, nil
	}
	now := time.Now().UTC()
	seenURLs := map[string]bool{}
	seenTitles := map[string]bool{}
	for _, a := range articles {
		urls := candidateURLs(a)
		title := strings.ToLower(strings.TrimSpace(a.Title))
		duplicate := false
		for _, u := range urls {
			if u != "" && seenURLs[u] {
				duplicate = true
				break
			}
		}
		if !duplicate && title != "" && seenTitles[title] {
			duplicate = true
		}
		if duplicate {
			c.Skipped++
			continue
		}
		old, e := findURLDuplicate(ctx, tx, urls)
		if e != nil {
			return c, e
		}
		if old != nil {
			if old.Processed && strings.TrimSpace(old.Summary) != "" {
				c.Skipped++
				recordSeen(urls, title, seenURLs, seenTitles)
				continue
			}
			updated, updateErr := updateExisting(ctx, tx, *old, a)
			if updateErr != nil {
				e = updateErr
				return c, e
			}
			if updated {
				c.Updated++
			} else {
				c.Skipped++
			}
			recordSeen(urls, title, seenURLs, seenTitles)
			continue
		}
		dup, e := titleDuplicate(ctx, tx, src.ID, title, now.Add(-7*24*time.Hour))
		if e != nil {
			return c, e
		}
		if dup {
			c.Skipped++
			continue
		}
		e = insertArticle(ctx, tx, src.ID, a, now)
		if e != nil {
			if pgCode(e) == "23505" {
				c.Skipped++
				recordSeen(urls, title, seenURLs, seenTitles)
				continue
			}
			return c, errors.New("article insert failed")
		}
		c.Saved++
		recordSeen(urls, title, seenURLs, seenTitles)
	}
	if _, err = tx.Exec(ctx, `UPDATE sources SET last_fetch=$2 WHERE id=$1`, src.ID, now); err != nil {
		return c, errors.New("source fetch metadata update failed")
	}
	if err = saveSourceRun(ctx, tx, f, src, c, outcome, errorCode, now, studyHTMLSHA256); err != nil {
		return c, err
	}
	if page != nil {
		if err = savePageState(ctx, tx, src, *page, now); err != nil {
			return c, err
		}
	}
	if err = updateRunProgress(ctx, tx, f, src.ID, c); err != nil {
		return c, err
	}
	if err = tx.Commit(ctx); err != nil {
		return c, errors.New("article commit outcome unknown")
	}
	return c, nil
}

func (s *Store) UpdateRun(ctx context.Context, f Fence, state string, result map[string]any, errorCode string) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return errors.New("run update transaction unavailable")
	}
	defer rollback(tx)
	if _, current, err := s.FenceTx(ctx, tx, f); err != nil {
		return err
	} else if current != "running" {
		return ErrRunCancelled
	}
	b, _ := json.Marshal(result)
	if len(b) > 4096 {
		return errors.New("run progress exceeds limit")
	}
	_, err = tx.Exec(ctx, `UPDATE article_ingestion_runs SET state=$2::varchar,result=$3::jsonb,error_code=$4,updated_at=clock_timestamp() AT TIME ZONE 'UTC',finished_at=CASE WHEN $2::varchar='running' THEN NULL ELSE clock_timestamp() AT TIME ZONE 'UTC' END WHERE id=$1 AND state='running'`, f.RunID, state, string(b), nullableString(errorCode))
	if err != nil {
		return errors.New("run progress update failed")
	}
	return tx.Commit(ctx)
}

// SourceTotals reconstructs durable run metrics after a process restart. The
// run row is updated alongside each source receipt, so in-memory counters are
// only a convenience while collecting and never the final authority.
func (s *Store) SourceTotals(ctx context.Context, f Fence) (int, int, map[string]int, int, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return 0, 0, nil, 0, errors.New("source totals transaction unavailable")
	}
	defer rollback(tx)
	if _, state, err := s.FenceTx(ctx, tx, f); err != nil {
		return 0, 0, nil, 0, err
	} else if state != "running" {
		return 0, 0, nil, 0, ErrRunCancelled
	}
	var completed, fetched int
	err = tx.QueryRow(ctx, `SELECT COUNT(*) FILTER (WHERE state IN ('succeeded','partial','failed','skipped','busy','deferred','resource_limit','unavailable')),
		COALESCE(SUM(saved+updated),0) FROM article_ingestion_source_runs WHERE run_id=$1`, f.RunID).Scan(&completed, &fetched)
	if err != nil {
		return 0, 0, nil, 0, errors.New("source totals read failed")
	}
	var failedSources int
	err = tx.QueryRow(ctx, `SELECT COUNT(*) FROM article_ingestion_source_runs
		WHERE run_id=$1 AND state IN ('partial','failed','busy','deferred','resource_limit','unavailable')
		AND saved+updated=0 AND skipped=0`, f.RunID).Scan(&failedSources)
	if err != nil {
		return 0, 0, nil, 0, errors.New("source failure totals read failed")
	}
	rows, err := tx.Query(ctx, `SELECT error_code,COUNT(*) FROM article_ingestion_source_runs
		WHERE run_id=$1 AND error_code IS NOT NULL GROUP BY error_code`, f.RunID)
	if err != nil {
		return 0, 0, nil, 0, errors.New("source outcome totals read failed")
	}
	outcomes := map[string]int{}
	for rows.Next() {
		var code string
		var count int
		if err := rows.Scan(&code, &count); err != nil {
			rows.Close()
			return 0, 0, nil, 0, errors.New("source outcome totals decode failed")
		}
		outcomes[code] = count
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return 0, 0, nil, 0, errors.New("source outcome totals read failed")
	}
	rows.Close()
	if err := tx.Commit(ctx); err != nil {
		return 0, 0, nil, 0, errors.New("source totals transaction failed")
	}
	return completed, fetched, outcomes, failedSources, nil
}

func (s *Store) GetRun(ctx context.Context, id string) (map[string]any, error) {
	var job, state, code string
	var attempt int32
	var raw []byte
	var started, updated time.Time
	var finished *time.Time
	err := s.Pool.QueryRow(ctx, `SELECT job_id::text,attempt_generation,state,COALESCE(error_code,''),result,started_at,updated_at,finished_at FROM article_ingestion_runs WHERE id=$1`, id).Scan(&job, &attempt, &state, &code, &raw, &started, &updated, &finished)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, pgx.ErrNoRows
	}
	if err != nil {
		return nil, errors.New("run status read failed")
	}
	var result map[string]any
	_ = json.Unmarshal(raw, &result)
	if result == nil {
		result = map[string]any{}
	}
	return map[string]any{"run_id": id, "job_id": job, "attempt_generation": attempt, "state": state, "error_code": nilIfEmpty(code), "result": result, "started_at": started.UTC().Format(time.RFC3339Nano), "updated_at": updated.UTC().Format(time.RFC3339Nano), "finished_at": formatTime(finished)}, nil
}

func (s *Store) LoadFence(ctx context.Context, id string) (Fence, error) {
	var f Fence
	var runID, jobID, ownerID, executorID string
	var digest string
	err := s.Pool.QueryRow(ctx, `SELECT id::text,job_id::text,attempt_generation,envelope_sha256,
		owner_id::text,worker_generation,backend_pid,executor_instance_id::text,deadline_at
		FROM article_ingestion_runs WHERE id=$1 AND state='running'`, id).Scan(
		&runID, &jobID, &f.AttemptGeneration, &digest, &ownerID, &f.WorkerGeneration,
		&f.BackendPID, &executorID, &f.DeadlineAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Fence{}, pgx.ErrNoRows
		}
		return Fence{}, errors.New("ingestion recovery fence read failed")
	}
	f.RunID, f.JobID, f.OwnerID, f.ExecutorInstance, f.EnvelopeSHA256 = runID, jobID, ownerID, executorID, digest
	f.DeadlineAt = f.DeadlineAt.UTC()
	return f, nil
}
func (s *Store) Cancel(ctx context.Context, id string) error {
	_, err := s.Pool.Exec(ctx, `UPDATE article_ingestion_runs SET state='cancelled',error_code='cancelled',updated_at=clock_timestamp() AT TIME ZONE 'UTC',finished_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=$1 AND state='running'`, id)
	if err != nil {
		return errors.New("run cancellation failed")
	}
	return nil
}

type dbArticle struct {
	ID                                     int64
	Title, URL, Content, Summary, ImageURL string
	Processed                              bool
}

func findURLDuplicate(ctx context.Context, tx pgx.Tx, urls []string) (*dbArticle, error) {
	if len(urls) == 0 {
		return nil, nil
	}
	var a dbArticle
	err := tx.QueryRow(ctx, `SELECT id,title,url,COALESCE(content,''),COALESCE(summary,''),COALESCE(image_url,''),processed FROM articles WHERE url=ANY($1::text[]) ORDER BY array_position($1::text[],url),id LIMIT 1 FOR UPDATE`, urls).Scan(&a.ID, &a.Title, &a.URL, &a.Content, &a.Summary, &a.ImageURL, &a.Processed)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, errors.New("article duplicate lookup failed")
	}
	return &a, nil
}
func updateExisting(ctx context.Context, tx pgx.Tx, old dbArticle, a Article) (bool, error) {
	var published any
	if a.PublishedAt != nil {
		published = a.PublishedAt.UTC()
	}
	sp, err := tx.Begin(ctx)
	if err != nil {
		return false, errors.New("article metadata savepoint unavailable")
	}
	defer rollback(sp)
	result, err := sp.Exec(ctx, `UPDATE articles SET title=COALESCE(NULLIF($2,''),title),content=COALESCE($3,content),url=COALESCE(NULLIF($4,''),url),image_url=COALESCE($5,image_url),published_at=COALESCE($6,published_at),hash_content=COALESCE(NULLIF($7,''),hash_content) WHERE id=$1 AND NOT (processed IS TRUE AND COALESCE(summary,'')<>'')`, old.ID, a.Title, nullableNonempty(a.Content), a.URL, nullableNonempty(a.ImageURL), published, contentHash(a.Title, a.URL))
	if err != nil {
		if pgCode(err) == "23505" {
			if rollbackErr := sp.Rollback(ctx); rollbackErr != nil {
				return false, errors.New("article metadata savepoint rollback failed")
			}
			return false, nil
		}
		return false, errors.New("article metadata update failed")
	}
	if err := sp.Commit(ctx); err != nil {
		return false, errors.New("article metadata update failed")
	}
	return result.RowsAffected() == 1, nil
}
func titleDuplicate(ctx context.Context, tx pgx.Tx, source int32, title string, since time.Time) (bool, error) {
	if title == "" {
		return false, nil
	}
	var yes bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM articles WHERE source_id=$1 AND lower(title)=$2 AND fetched_at >= $3)`, source, title, since).Scan(&yes)
	if err != nil {
		return false, errors.New("article title duplicate lookup failed")
	}
	return yes, nil
}
func insertArticle(ctx context.Context, tx pgx.Tx, source int32, a Article, now time.Time) error {
	media := []byte(a.MediaFiles)
	if len(media) == 0 || !json.Valid(media) {
		media = []byte(`[]`)
	}
	ad := adFields(a.RawData)
	admarkers, _ := json.Marshal(ad.markers)
	var published any
	if a.PublishedAt != nil {
		published = a.PublishedAt.UTC()
	}
	sp, err := tx.Begin(ctx)
	if err != nil {
		return errors.New("article savepoint unavailable")
	}
	defer rollback(sp)
	_, err = sp.Exec(ctx, `INSERT INTO articles(source_id,title,url,content,summary,image_url,media_files,published_at,fetched_at,processed,summary_processed,category_processed,processing_state,processing_attempts,next_attempt_at,last_processing_error,hash_content,is_advertisement,ad_confidence,ad_type,ad_reasoning,ad_markers,ad_processed) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,FALSE,FALSE,FALSE,'pending',0,NULL,NULL,$10,$11,$12,$13,$14,$15::jsonb,$16)`, source, a.Title, a.URL, nullableNonempty(a.Content), nullableNonempty(a.Summary), nullableNonempty(a.ImageURL), string(media), published, now, contentHash(a.Title, a.URL), ad.isAd, ad.confidence, nullableString(ad.typ), nullableString(ad.reason), string(admarkers), ad.processed)
	if err != nil {
		return err
	}
	return sp.Commit(ctx)
}

type adMeta struct {
	isAd        bool
	confidence  float64
	typ, reason string
	markers     []any
	processed   bool
}

func adFields(raw map[string]any) adMeta {
	v := adMeta{markers: []any{}}
	if raw == nil {
		return v
	}
	_, a := raw["advertising_detection"]
	_, b := raw["is_advertisement"]
	if !a && !b {
		return v
	}
	v.processed = true
	v.isAd, _ = raw["is_advertisement"].(bool)
	v.confidence, _ = raw["ad_confidence"].(float64)
	v.typ, _ = raw["ad_type"].(string)
	v.reason, _ = raw["ad_reasoning"].(string)
	if x, ok := raw["ad_markers"].([]any); ok {
		v.markers = x
	}
	return v
}
func candidateURLs(a Article) []string {
	r := []string{}
	seen := map[string]bool{}
	add := func(x string) {
		if x != "" && !seen[x] {
			seen[x] = true
			r = append(r, x)
		}
	}
	add(a.URL)
	for _, k := range []string{"telegram_url", "message_url", "original_link"} {
		if x, ok := a.RawData[k].(string); ok {
			add(x)
		}
	}
	return r
}
func recordSeen(urls []string, title string, su, st map[string]bool) {
	for _, u := range urls {
		su[u] = true
	}
	if title != "" {
		st[title] = true
	}
}
func contentHash(title, url string) string {
	sum := sha256.Sum256([]byte(title + ":" + url))
	return hex.EncodeToString(sum[:])
}
func validSHA256Hex(value string) bool {
	if len(value) != sha256.Size*2 {
		return false
	}
	decoded, err := hex.DecodeString(value)
	return err == nil && hex.EncodeToString(decoded) == value
}
func nullableNonempty(s *string) any {
	if s == nil || *s == "" {
		return nil
	}
	return *s
}
func saveSourceRun(ctx context.Context, tx pgx.Tx, f Fence, src Source, c CommitCounts, outcome, code string, now time.Time, studyHTMLSHA256 string) error {
	state := outcome
	if state == "success" {
		state = "succeeded"
	}
	if state != "succeeded" && state != "partial" && state != "failed" && state != "skipped" &&
		state != "busy" && state != "deferred" && state != "resource_limit" && state != "unavailable" {
		state = "failed"
	}
	_, err := tx.Exec(ctx, `INSERT INTO article_ingestion_source_runs(run_id,source_id,source_type,config_sha256,state,candidates,saved,updated,skipped,fetched_at,error_code,selector_study_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(run_id,source_id) DO UPDATE SET state=EXCLUDED.state,candidates=article_ingestion_source_runs.candidates+EXCLUDED.candidates,saved=article_ingestion_source_runs.saved+EXCLUDED.saved,updated=article_ingestion_source_runs.updated+EXCLUDED.updated,skipped=article_ingestion_source_runs.skipped+EXCLUDED.skipped,fetched_at=EXCLUDED.fetched_at,error_code=EXCLUDED.error_code,selector_study_sha256=COALESCE(EXCLUDED.selector_study_sha256,article_ingestion_source_runs.selector_study_sha256)`, f.RunID, src.ID, src.Type, src.Fingerprint, state, c.Candidates, c.Saved, c.Updated, c.Skipped, now, nullableString(code), nullableString(studyHTMLSHA256))
	if err != nil {
		return errors.New("source run result update failed")
	}
	return nil
}
func markSourceSkipped(ctx context.Context, tx pgx.Tx, f Fence, src Source, candidates int, now time.Time) error {
	c := CommitCounts{Candidates: candidates, Skipped: candidates}
	if err := saveSourceRun(ctx, tx, f, src, c, "skipped", "source_changed", now, ""); err != nil {
		return err
	}
	return updateRunProgress(ctx, tx, f, src.ID, c)
}
func updateRunProgress(ctx context.Context, tx pgx.Tx, f Fence, _ int32, c CommitCounts) error {
	_, err := tx.Exec(ctx, `UPDATE article_ingestion_runs SET result=jsonb_set(jsonb_set(COALESCE(result,'{}'::jsonb),'{sources_completed}',to_jsonb(COALESCE((result->>'sources_completed')::integer,0)+1),true),'{articles_fetched}',to_jsonb(COALESCE((result->>'articles_fetched')::integer,0)+$2+$3),true),updated_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=$1 AND state='running'`, f.RunID, c.Saved, c.Updated)
	if err != nil {
		return errors.New("run progress write failed")
	}
	return nil
}
func savePageState(ctx context.Context, tx pgx.Tx, src Source, p PageState, now time.Time) error {
	a, _ := json.Marshal(p.PreviousHashes)
	b, _ := json.Marshal(p.ScanHashes)
	c, _ := json.Marshal(p.AcknowledgedHashes)
	_, err := tx.Exec(ctx, `INSERT INTO source_collection_state(source_id,config_sha256,scan_limit,cursor_offset,scan_active,previous_hashes,scan_hashes,acknowledged_hashes,snapshot_at,content_sha256,updated_at) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb,$9,NULLIF($10,''),$11) ON CONFLICT(source_id) DO UPDATE SET config_sha256=EXCLUDED.config_sha256,scan_limit=EXCLUDED.scan_limit,cursor_offset=EXCLUDED.cursor_offset,scan_active=EXCLUDED.scan_active,previous_hashes=EXCLUDED.previous_hashes,scan_hashes=EXCLUDED.scan_hashes,acknowledged_hashes=EXCLUDED.acknowledged_hashes,snapshot_at=EXCLUDED.snapshot_at,content_sha256=EXCLUDED.content_sha256,updated_at=EXCLUDED.updated_at`, src.ID, src.Fingerprint, p.ScanLimit, p.CursorOffset, p.ScanActive, string(a), string(b), string(c), p.SnapshotAt, p.ContentSHA256, now)
	if err != nil {
		return errors.New("page monitor cursor commit failed")
	}
	return nil
}
func pgCode(err error) string {
	var p *pgconn.PgError
	if errors.As(err, &p) {
		return p.Code
	}
	return ""
}
func rollback(tx pgx.Tx) {
	if tx != nil {
		c, x := context.WithTimeout(context.Background(), 5*time.Second)
		defer x()
		_ = tx.Rollback(c)
	}
}
func nullableString(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func nilIfEmpty(s string) any {
	if s == "" {
		return nil
	}
	return s
}
func formatTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return t.UTC().Format(time.RFC3339Nano)
}
func intConfig(m map[string]any, key string, fallback, min, max int) int {
	v, ok := m[key].(float64)
	if !ok || v < float64(min) || v > float64(max) {
		return fallback
	}
	return int(v)
}
