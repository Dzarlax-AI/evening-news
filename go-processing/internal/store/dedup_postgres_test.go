package store

import (
	"context"
	"crypto/sha256"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresDedupTrialUsesCurrentAdmittedCopies(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL is required for dedup PostgreSQL tests")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1") {
		t.Fatal("dedup PostgreSQL tests require a loopback-only TEST_DATABASE_URL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal("connect to isolated dedup database failed")
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("dedup_test_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	role := schema + "_writer"
	qRole := pgx.Identifier{role}.Sanitize()
	exec := func(command string, args ...any) {
		t.Helper()
		if _, err := admin.Exec(ctx, command, args...); err != nil {
			t.Fatalf("dedup fixture setup failed: SQLSTATE %s", pgCode(err))
		}
	}
	exec("CREATE SCHEMA " + q)
	defer func() {
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE")
		_, _ = admin.Exec(context.Background(), "DROP ROLE "+qRole)
	}()
	exec("CREATE ROLE " + qRole + " NOLOGIN")
	exec("SET search_path TO " + q)
	exec(`CREATE TABLE articles(id integer primary key, source_id integer, title text not null, summary text, telegram_context jsonb,
		fetched_at timestamp, summary_processed boolean, is_advertisement boolean,
		processing_attempts integer not null, processing_state text not null, processed boolean not null);
		CREATE TABLE article_processing_items(run_id uuid, article_id integer, article_attempt integer,
		input_fingerprint text, claim_token uuid, state text, primary key(run_id,article_id));
		CREATE TABLE article_localizations(article_id integer, locale text, status text, title text, summary text,
		primary key(article_id,locale));`)
	exec(`CREATE FUNCTION fence_article_processing_attempt(uuid,uuid,integer,text,uuid,integer,integer,uuid,timestamp)
		RETURNS TABLE(run_id uuid,run_state text) LANGUAGE sql AS $$ SELECT $1,'running'::text $$`)
	migration, err := os.ReadFile("../../../go-domain/migration/sql/022.sql")
	if err != nil {
		t.Fatal("read dedup migration failed")
	}
	exec(strings.ReplaceAll(string(migration), "__SCHEMA_Q__", q))
	exec("GRANT USAGE ON SCHEMA " + q + " TO " + qRole)
	exec("GRANT SELECT (id, source_id, title, source_title, summary, fetched_at, summary_processed, is_advertisement, processing_attempts, processing_state, processed, completed_processing_claim), UPDATE(processed) ON articles TO " + qRole)
	exec("GRANT SELECT, UPDATE ON article_processing_items TO " + qRole)
	exec("GRANT SELECT ON article_localizations TO " + qRole)
	exec("GRANT SELECT, INSERT, UPDATE ON article_dedup_observations TO " + qRole)

	const token = "40000000-0000-4000-8000-000000000001"
	const reprocessToken = "40000000-0000-4000-8000-000000000002"
	const fingerprint = "fixture-claim-fingerprint"
	fence := func(id int64) Fence {
		return Fence{
			RunID: fmt.Sprintf("10000000-0000-4000-8000-%012d", id),
			Envelope: Envelope{
				JobID: "20000000-0000-4000-8000-000000000001", AttemptGeneration: 1,
				OwnerID: "30000000-0000-4000-8000-000000000001", Generation: 1,
				BackendPID: 1, ExecutorInstanceID: token, DeadlineAt: time.Now().Add(time.Hour),
			},
		}
	}
	base := time.Date(2026, 11, 17, 22, 5, 0, 0, time.UTC)
	for _, fixture := range []struct {
		id       int64
		state    string
		age      time.Duration
		ad       bool
		finished bool
	}{
		{1, "rejected", 0, false, true},
		{2, "completed", time.Minute, false, true},
		{3, "completed", 2 * time.Minute, false, true},
		{4, "completed", 3 * time.Minute, false, true},
		{5, "completed", 4 * time.Minute, false, true},
		{6, "completed", -49 * time.Hour, false, true},
		{7, "completed", 5 * time.Minute, true, true},
		{8, "retry_wait", 6 * time.Minute, false, false},
	} {
		exec(`INSERT INTO articles(id,source_id,title,summary,telegram_context,fetched_at,summary_processed,is_advertisement,processing_attempts,processing_state,processed,source_title,completed_processing_claim)
			VALUES ($1,$2,'Common headline','Fixture summary',NULL,$3,true,$4,1,$5,$6,'Original title',$7)`,
			fixture.id, fixture.id%2+1, base.Add(fixture.age), fixture.ad, fixture.state, fixture.finished, token)
		exec(`INSERT INTO article_processing_items VALUES ($1,$2,1,$3,$4,$5)`, fence(fixture.id).RunID, fixture.id, fingerprint, token, fixture.state)
	}
	exec(`INSERT INTO article_localizations VALUES (3,'sr-Latn','completed','Objava','Kratak izveštaj')`)

	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("dedup fixture pool configuration failed")
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.RuntimeParams["role"] = role
	cfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal("dedup fixture pool creation failed")
	}
	defer pool.Close()
	s := &Store{Pool: pool}
	digest := sha256.Sum256([]byte("A copy of the same synthetic original"))
	observation := DedupObservation{Version: 1, ExactHash: digest[:]}
	record := func(id int64, attempt int, obs DedupObservation) (int64, bool, error) {
		return s.RecordDedupObservation(ctx, fence(id), id, attempt, token, fingerprint, obs)
	}
	for _, id := range []int64{1, 7, 8} {
		if rep, recorded, err := record(id, 1, observation); err != nil || recorded || rep != 0 {
			t.Fatalf("inadmissible copy registered: article=%d rep=%d recorded=%v err=%v", id, rep, recorded, err)
		}
	}
	type result struct {
		id, rep  int64
		recorded bool
		err      error
	}
	start := make(chan struct{})
	results := make(chan result, 3)
	for _, id := range []int64{2, 3, 4} {
		go func() {
			<-start
			rep, recorded, err := record(id, 1, observation)
			results <- result{id, rep, recorded, err}
		}()
	}
	close(start)
	var canonical int64
	for range 3 {
		got := <-results
		if got.err != nil || !got.recorded || got.rep < 2 || got.rep > 4 {
			t.Fatalf("eligible copy observation failed: %+v", got)
		}
		if canonical != 0 && canonical != got.rep {
			t.Fatalf("concurrent exact copies returned different representatives: %d and %d", canonical, got.rep)
		}
		canonical = got.rep
	}
	if rep, recorded, err := record(3, 1, observation); err != nil || !recorded || rep != canonical {
		t.Fatalf("same attempt replay changed representative: rep=%d recorded=%v err=%v", rep, recorded, err)
	}
	updated := observation
	newDigest := sha256.Sum256([]byte("The decision from the earlier original was reversed"))
	updated.ExactHash = newDigest[:]
	if rep, recorded, err := record(5, 1, updated); err != nil || !recorded || rep != 5 {
		t.Fatalf("updated source text incorrectly matched the old fingerprint: rep=%d recorded=%v err=%v", rep, recorded, err)
	}
	subject, candidates, truncated, err := s.LoadDedupTrial(ctx, 5, token, 1, 48*time.Hour)
	if err != nil || truncated || subject.ID != 5 || subject.SourceTitle != "Original title" || len(candidates) != 3 {
		t.Fatalf("candidate trial visibility/window failed: len=%d truncated=%v err=%v", len(candidates), truncated, err)
	}
	if candidates[1].ID != 3 || candidates[1].Serbian.Title == "" || *subject.SourceID == *candidates[0].SourceID {
		t.Fatal("dedup candidate trial must preserve both current localizations and cross-source peers")
	}

	exec("UPDATE articles SET processing_attempts=2, processing_state='rejected' WHERE id=$1", canonical)
	if rep, recorded, err := record(canonical, 1, observation); err != nil || recorded || rep != 0 {
		t.Fatalf("obsolete attempt re-registered: rep=%d recorded=%v err=%v", rep, recorded, err)
	}
	remaining := int64(2)
	if canonical == remaining {
		remaining = 3
	}
	if rep, recorded, err := record(remaining, 1, observation); err != nil || !recorded || rep == canonical || rep == 0 {
		t.Fatalf("stale representative did not fall out of selection: rep=%d recorded=%v err=%v", rep, recorded, err)
	}
	var count, admitted int
	if err := admin.QueryRow(ctx, `SELECT (SELECT count(*) FROM article_dedup_observations),
		count(*) FILTER (WHERE processing_state='completed' AND is_advertisement IS NOT TRUE) FROM articles`).Scan(&count, &admitted); err != nil || count != 4 || admitted != 4 {
		t.Fatalf("dedup observations changed article admission: observations=%d admitted=%d", count, admitted)
	}

	// A separate reprocess can finish on attempt 1 again. The old claim
	// must stay ineligible even though both receipts use the same counter.
	exec("UPDATE articles SET fetched_at=NULL,completed_processing_claim=$1 WHERE id=5", reprocessToken)
	exec(`INSERT INTO article_processing_items VALUES ($1,5,1,$2,$3,'completed')`, fence(9).RunID, fingerprint, reprocessToken)
	if _, recorded, err := record(5, 1, updated); err != nil || recorded {
		t.Fatalf("old claim with reused attempt number was accepted: recorded=%v err=%v", recorded, err)
	}
	rep, recorded, err := s.RecordDedupObservation(ctx, fence(9), 5, 1, reprocessToken, fingerprint, observation)
	if err != nil || !recorded || rep == 0 || rep == 5 || rep == canonical {
		t.Fatalf("new claim with reused attempt number did not replace the old hash: rep=%d recorded=%v err=%v", rep, recorded, err)
	}
	subject, candidates, truncated, err = s.LoadDedupTrial(ctx, 5, reprocessToken, 1, 48*time.Hour)
	if err != nil || subject.ID != 5 || subject.FetchedAt != nil || len(candidates) != 0 || truncated {
		t.Fatalf("nullable date changed exact observation or produced candidates: id=%d len=%d err=%v", subject.ID, len(candidates), err)
	}
	exec("UPDATE articles SET fetched_at=$1 WHERE id=5", base)

	exec(`INSERT INTO articles(id,source_id,title,summary,fetched_at,summary_processed,is_advertisement,processing_attempts,processing_state,processed)
		SELECT n,1,'Synthetic headline','Fixture summary',$1,true,false,1,'completed',true
		FROM generate_series(1000,$2::integer) n`, base, 1000+DedupCandidatePool)
	_, candidates, truncated, err = s.LoadDedupTrial(ctx, 5, reprocessToken, 1, 48*time.Hour)
	if err != nil || !truncated || len(candidates) != DedupCandidatePool {
		t.Fatalf("candidate pool limit not applied: len=%d truncated=%v err=%v", len(candidates), truncated, err)
	}

}
