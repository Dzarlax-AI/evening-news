package topicshadow

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type testTopicTransport struct {
	calls    int
	failOnce bool
}

func (f *testTopicTransport) Decide(_ context.Context, input string, questions []ChoiceQuestion) (DecisionsResult, error) {
	f.calls++
	if f.failOnce {
		f.failOnce = false
		return DecisionsResult{}, &DecisionsError{Code: "http_error", StatusCode: 503}
	}
	if strings.Contains(input, "Invalid request fixture") {
		return DecisionsResult{}, &DecisionsError{Code: "invalid_request"}
	}
	result := DecisionsResult{Model: DecisionsModel}
	for _, q := range questions {
		choice := q.Choices[0].Value
		switch q.Name {
		case "decision":
			if strings.Contains(input, "Article ID: 35\n") || strings.Contains(input, "Article ID: 36\n") {
				choice = MatchOutcomeNewCandidate
			}
		case "subject_kind":
			if strings.Contains(input, "One-off fixture") {
				choice = NoveltySubjectOrdinary
			}
		case "official_change":
			if strings.Contains(input, "proposal only") {
				choice = "no"
			}
		case "event_date":
			choice = q.Choices[len(q.Choices)-1].Value
		case "disagreement":
			choice = "no"
		case "update":
			if strings.Contains(input, "repeat bulletin") {
				choice = NoveltyOutcomeRepeat
			}
		}
		result.Answers = append(result.Answers, ans(q.Name, choice, .99))
	}
	return result, nil
}

func TestReplayDatabaseChecksEveryHost(t *testing.T) {
	for _, tt := range []struct {
		hosts   string
		allowed bool
	}{
		{"127.0.0.1", true},
		{"127.0.0.1,127.0.0.2", true},
		{"127.0.0.1,192.0.2.10", false},
		{"192.0.2.10,127.0.0.1", false},
	} {
		config, err := pgx.ParseConfig("host=" + tt.hosts + " sslmode=prefer")
		if err != nil {
			t.Fatal(err)
		}
		if got := isLoopbackReplayDatabase(config); got != tt.allowed {
			t.Errorf("allowed for %s = %t, want %t", tt.hosts, got, tt.allowed)
		}
	}
}

func TestRunnerKeepsHistoryAndDeduplicatesPasses(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not configured")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid isolated PostgreSQL configuration")
	}
	if !isLoopbackReplayDatabase(cfg.ConnConfig) {
		t.Fatal("topic shadow test requires a loopback-only PostgreSQL database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "topic_shadow_test_" + hex.EncodeToString(nonce[:])
	admin, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	quoted := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, "CREATE SCHEMA "+quoted); err != nil {
		t.Fatal(err)
	}
	defer admin.Exec(context.WithoutCancel(ctx), "DROP SCHEMA "+quoted+" CASCADE")
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err = migration.Run(ctx, pool, true, false); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state)
		VALUES ('20000000-0000-4000-8000-000000000001','news_processing','{}','topic-shadow-fixture','succeeded');
		INSERT INTO article_processing_runs(id,job_id,attempt_generation,envelope_sha256,owner_id,
			worker_generation,backend_pid,executor_instance_id,deadline_at)
		VALUES ('20000000-0000-4000-8000-000000000002','20000000-0000-4000-8000-000000000001',
			1,repeat('a',64),'20000000-0000-4000-8000-000000000003',1,1,
			'20000000-0000-4000-8000-000000000004',timezone('UTC',now()))`); err != nil {
		t.Fatal(err)
	}
	add := func(id int, claim, title, summary string, ready time.Time) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO articles
			(id,title,summary,url,summary_processed,processing_state,is_advertisement,completed_processing_claim,published_at)
			VALUES ($1,$2,$3,'https://fixture.invalid/news/'||($1::integer)::text,true,'completed',false,$4,timezone('UTC',$5::timestamptz))`,
			id, title, summary, claim, ready.UTC()); err != nil {
			t.Fatal(err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO article_processing_items(run_id,article_id,article_attempt,
			input_fingerprint,claim_token,state,updated_at)
			VALUES ('20000000-0000-4000-8000-000000000002',$1,1,repeat('a',64),$2,'completed',$3)`,
			id, claim, ready.UTC()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO sources(id,name,source_type,url)
		VALUES (1,'Fixture One','rss','https://one.invalid/rss'),(2,'Fixture Two','rss','https://two.invalid/rss')`); err != nil {
		t.Fatal(err)
	}
	setSource := func(article, source int) {
		t.Helper()
		if _, err := pool.Exec(ctx, `UPDATE articles SET source_id=$2 WHERE id=$1`, article, source); err != nil {
			t.Fatal(err)
		}
	}
	transport := &testTopicTransport{}
	r := Runner{Pool: pool, Version: "fixture", Owner: "20000000-0000-4000-8000-000000000005", Client: transport, DailyRequestLimit: 20}
	base := time.Now().UTC().Truncate(time.Second).Add(-8 * 24 * time.Hour)
	add(31, "20000000-0000-4000-8000-000000000031", "Belgrade bridge project opens", "The Belgrade bridge project officially opened.", base)
	pass, err := r.RunPass(ctx, base, 50)
	if err != nil || pass.Processed != 1 || pass.Reviewed != 0 || transport.calls != 2 {
		t.Fatalf("opening: %+v calls=%d err=%v", pass, transport.calls, err)
	}
	pass, err = r.RunPass(ctx, base.Add(DefaultQuietAfter), 50)
	if err != nil || pass.Processed != 0 || transport.calls != 2 {
		t.Fatalf("quiet: %+v calls=%d err=%v", pass, transport.calls, err)
	}
	add(32, "20000000-0000-4000-8000-000000000032", "Belgrade bridge project adds transit", "The Belgrade bridge project adds transit service.", base.Add(DefaultQuietAfter+time.Minute))
	add(33, "20000000-0000-4000-8000-000000000033", "Belgrade bridge project repeat bulletin", "A repeat bulletin about the new transit service.", base.Add(DefaultQuietAfter+2*time.Minute))
	now := base.Add(DefaultQuietAfter + 3*time.Minute)
	pass, err = r.RunPass(ctx, now, 50)
	if err != nil || pass.Processed != 2 || pass.Reviewed != 0 || transport.calls != 6 {
		t.Fatalf("return: %+v calls=%d err=%v", pass, transport.calls, err)
	}
	pass, err = r.RunPass(ctx, now, 50)
	if err != nil || pass.Processed != 0 || transport.calls != 6 {
		t.Fatalf("rerun: %+v calls=%d err=%v", pass, transport.calls, err)
	}
	var transitions, active int
	err = pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM topic_shadow_events WHERE policy_version='fixture'),
		(SELECT count(*) FROM topic_shadow_topics WHERE policy_version='fixture' AND state='active')`).Scan(&transitions, &active)
	if err != nil || transitions != 3 || active != 1 {
		t.Fatalf("journal: %d %d %v", transitions, active, err)
	}
	cutoff := time.Now().UTC().Add(time.Second).Truncate(time.Microsecond)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err = freezeCutoff(ctx, tx, r.Version, cutoff); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	var output bytes.Buffer
	if err = WriteReport(ctx, pool, r.Version, cutoff, &output); err != nil {
		t.Fatal(err)
	}
	var report struct {
		Snapshot struct {
			DigestPreview []json.RawMessage `json:"digest_preview"`
		} `json:"snapshot"`
	}
	if err = json.Unmarshal(output.Bytes(), &report); err != nil || len(report.Snapshot.DigestPreview) != 1 {
		t.Fatalf("saved preview: topics=%d err=%v", len(report.Snapshot.DigestPreview), err)
	}

	replayTransport := &testTopicTransport{}
	r.Version, r.Client = "replay-fixture", replayTransport
	cutoffs := []time.Time{base.Add(time.Second), base.Add(DefaultQuietAfter), now}
	passes, err := r.Replay(ctx, cutoffs)
	if err != nil || len(passes) != 3 || replayTransport.calls != 6 {
		t.Fatalf("replay: passes=%+v calls=%d err=%v", passes, replayTransport.calls, err)
	}
	for index, cutoff := range cutoffs {
		var snapshot struct {
			Complete bool `json:"complete"`
			Snapshot struct {
				Transitions []struct {
					Kind string `json:"kind"`
				} `json:"transitions"`
			} `json:"snapshot"`
		}
		output.Reset()
		if err := WriteReport(ctx, pool, r.Version, cutoff, &output); err != nil {
			t.Fatal(err)
		}
		want := []string{"opened", "quieted", "returned"}[index]
		err := json.Unmarshal(output.Bytes(), &snapshot)
		if err != nil || !snapshot.Complete || len(snapshot.Snapshot.Transitions) != 1 || snapshot.Snapshot.Transitions[0].Kind != want {
			t.Fatalf("replay cutoff %d failed: %+v err=%v", index, snapshot, err)
		}
	}

	if _, err = r.Replay(ctx, []time.Time{base.Add(24 * time.Hour)}); !errors.Is(err, errReplayBackfill) {
		t.Fatalf("replay accepted backfill after a later edition: %v", err)
	}
	if _, err = r.Replay(ctx, cutoffs); err != nil || replayTransport.calls != 6 {
		t.Fatalf("saved cutoffs should rerun without provider calls: %v", err)
	}

	retryTransport := &testTopicTransport{failOnce: true}
	retry := r
	retry.Version, retry.Client, retry.DailyRequestLimit = "replay-retry", retryTransport, 3
	if passes, err = retry.Replay(ctx, cutoffs[:1]); err == nil || len(passes) != 1 || passes[0].DeferredReason != "provider_retry" {
		t.Fatalf("replay did not retain the transient failure: %+v err=%v", passes, err)
	}
	if _, err = retry.Replay(ctx, cutoffs[1:2]); !errors.Is(err, errReplayInProgress) {
		t.Fatalf("replay advanced despite the unfinished cutoff: %v", err)
	}
	var retryIsWallClock bool
	err = pool.QueryRow(ctx, `SELECT next_attempt_at > clock_timestamp()
		FROM topic_shadow_decisions WHERE policy_version=$1 AND article_id=31`, retry.Version).Scan(&retryIsWallClock)
	if err != nil || !retryIsWallClock {
		t.Fatalf("replay retry should wait on wall time: %v", err)
	}
	if _, err = pool.Exec(ctx, `UPDATE topic_shadow_decisions SET next_attempt_at=clock_timestamp() - interval '1 second'
		WHERE policy_version=$1 AND article_id=31`, retry.Version); err != nil {
		t.Fatal(err)
	}
	retry.DailyRequestLimit = 4
	if passes, err = retry.Replay(ctx, cutoffs); err != nil || len(passes) != len(cutoffs) || retryTransport.calls != 7 {
		t.Fatalf("replay should resume the saved retry and reset quota on the next logical day: %+v, calls=%d err=%v", passes, retryTransport.calls, err)
	}

	// A permanent local error and an ordinary event must not block the next
	// article or create topics, even if the event otherwise looks material.
	add(34, "20000000-0000-4000-8000-000000000034", "Invalid request fixture", "Terminal failure.", now.Add(time.Minute))
	add(35, "20000000-0000-4000-8000-000000000035", "One-off fixture", "An official result that does not merit a topic.", now.Add(2*time.Minute))
	add(36, "20000000-0000-4000-8000-000000000036", "Local library opens", "The district opened a public library.", now.Add(3*time.Minute))
	r.Version, r.Client = "fixture", transport
	pass, err = r.RunPass(ctx, now.Add(4*time.Minute), 50)
	if err != nil || pass.Processed != 3 || pass.Reviewed != 1 || pass.Deferred {
		t.Fatalf("queue through invalid and one-off articles: %+v err=%v", pass, err)
	}
	var terminal, ordinary, attached bool
	err = pool.QueryRow(ctx, `SELECT
		(SELECT state = 'review' AND attempts = 1 FROM topic_shadow_decisions WHERE policy_version='fixture' AND article_id=34),
		(SELECT state = 'accepted' AND topic_id IS NULL FROM topic_shadow_decisions WHERE policy_version='fixture' AND article_id=35),
		(SELECT topic_id IS NOT NULL FROM topic_shadow_decisions WHERE policy_version='fixture' AND article_id=36)`).
		Scan(&terminal, &ordinary, &attached)
	if err != nil || !terminal || !ordinary || !attached {
		t.Fatalf("terminal=%v ordinary=%v attached=%v err=%v", terminal, ordinary, attached, err)
	}

	// A receipt committed behind the forward cursor should still attach to the
	// topic visible at its readiness time without advancing the current state.
	add(37, "20000000-0000-4000-8000-000000000037", "Belgrade bridge project opens ahead of schedule", "Late historical material.", base.Add(8*time.Hour))
	pass, err = r.RunPass(ctx, now.Add(5*time.Minute), 50)
	if err != nil || pass.Processed != 1 || pass.Reviewed != 0 {
		t.Fatalf("late input reconciliation: %+v err=%v", pass, err)
	}
	var attachedLate bool
	err = pool.QueryRow(ctx, `SELECT topic_id='20000000-0000-4000-8000-000000000031'::uuid
		AND decision->>'late_historical'='true' AND decision->>'material_applied'='false'
		FROM topic_shadow_decisions WHERE policy_version=$1 AND article_id=37`, r.Version).Scan(&attachedLate)
	if err != nil || !attachedLate {
		t.Fatalf("late receipt must attach without entering applied history: %v", err)
	}

	live := r
	live.Version = "fixture-live"
	live.DailyRequestLimit = 30
	liveStart := now.Add(time.Hour)
	if pass, err = live.RunPass(ctx, liveStart, 50); err != nil || pass.Processed != 0 {
		t.Fatalf("live startup should not enqueue the archive: %+v err=%v", pass, err)
	}
	add(41, "20000000-0000-4000-8000-000000000041", "City library renovation proposal only", "A proposal was published.", liveStart.Add(time.Minute))
	add(42, "20000000-0000-4000-8000-000000000042", "City library renovation proposal only gets a budget", "The same source disclosed budget details.", liveStart.Add(2*time.Minute))
	add(43, "20000000-0000-4000-8000-000000000043", "City library renovation proposal only gets a budget", "Another source copies the same bulletin.", liveStart.Add(3*time.Minute))
	add(44, "20000000-0000-4000-8000-000000000044", "City library renovation proposal only adds a reading room", "A second source disclosed a new fact.", liveStart.Add(4*time.Minute))
	setSource(41, 1)
	setSource(42, 1)
	setSource(43, 2)
	setSource(44, 2)
	for _, minute := range []int{2, 3, 4} {
		pass, err = live.RunPass(ctx, liveStart.Add(time.Duration(minute)*time.Minute), 50)
		if err != nil || pass.Reviewed != 0 {
			t.Fatalf("candidate independence pass %d: %+v err=%v", minute, pass, err)
		}
		var opened bool
		err = pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM topic_shadow_events WHERE policy_version=$1 AND kind='opened')`, live.Version).Scan(&opened)
		if err != nil || opened != (minute == 4) {
			t.Fatalf("independent candidate opening at %d: opened=%v err=%v", minute, opened, err)
		}
	}
	eventDay := liveStart.AddDate(0, 0, -1).Format(time.DateOnly)
	lockedAt := liveStart.Add(5 * time.Minute)
	add(45, "20000000-0000-4000-8000-000000000045", "City library renovation approved on "+eventDay, "The city approved the renovation on "+eventDay+".", lockedAt)
	if _, err = pool.Exec(ctx, `INSERT INTO editorial_digest_editions
		(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered)
		SELECT gen_random_uuid(),current_date,'ru','fixture',repeat('a',64),
			jsonb_build_object('cutoff_utc',$1::timestamptz+n*interval '1 minute','articles',jsonb_build_array(1)),
			jsonb_build_object('version','1','locale','ru','source_snapshot_hash',repeat('a',64),'stories',jsonb_build_array()),
			jsonb_build_object('rich_html','','plain_parts',jsonb_build_array(),'preview_text','')
		FROM generate_series(0,45) n`, lockedAt); err != nil {
		t.Fatal(err)
	}
	setSource(45, 2)
	lock, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Rollback(ctx)
	if _, err = lock.Exec(ctx, `SELECT id FROM articles WHERE id=45 FOR UPDATE`); err != nil {
		t.Fatal(err)
	}
	pass, err = live.RunPass(ctx, lockedAt, 50)
	if err != nil || pass.DeferredReason != "article_busy" {
		t.Fatalf("row lock must defer safely: %+v err=%v", pass, err)
	}
	if err = lock.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	pass, err = live.RunPass(ctx, lockedAt, 50)
	if err != nil || pass.Processed != 1 {
		t.Fatalf("row should process after lock release: %+v err=%v", pass, err)
	}
	var eventDate, actualSource bool
	err = pool.QueryRow(ctx, `SELECT (event_at AT TIME ZONE 'UTC')::date::text=$2, evidence->'SourceIDs'='["2"]'::jsonb
		FROM topic_shadow_events WHERE policy_version=$1 AND article_id=45`, live.Version, eventDay).Scan(&eventDate, &actualSource)
	if err != nil || !eventDate || !actualSource {
		t.Fatalf("event date and source provenance missing: date=%v source=%v err=%v", eventDate, actualSource, err)
	}

	var provisional, completed int
	err = pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE revision=1 AND NOT complete),
		count(*) FILTER (WHERE revision=2 AND complete)
		FROM topic_shadow_editions WHERE policy_version=$1 AND cutoff_at=$2`, live.Version, lockedAt).Scan(&provisional, &completed)
	if err != nil || provisional != 1 || completed != 1 {
		t.Fatalf("expected immutable provisional and completed snapshots: %d/%d err=%v", provisional, completed, err)
	}
	for _, minute := range []int{45, 46} {
		if _, err = live.RunPass(ctx, lockedAt.Add(time.Duration(minute)*time.Minute), 50); err != nil {
			t.Fatal(err)
		}
	}
	var savedEditions int
	err = pool.QueryRow(ctx, `SELECT count(DISTINCT cutoff_at) FROM topic_shadow_editions
		WHERE policy_version=$1 AND complete`, live.Version).Scan(&savedEditions)
	if err != nil || savedEditions != 46 {
		t.Fatalf("edition catchup should cover all 46 cutoffs: got %d err=%v", savedEditions, err)
	}

	// At representative volume the bounded scan should be able to start from
	// recent receipt keys instead of sorting the complete article history.
	if _, err := pool.Exec(ctx, `INSERT INTO articles (id,title,summary,url,summary_processed,processing_state,is_advertisement,completed_processing_claim)
		SELECT i,'Fixture topic','Fixture summary','https://fixture.invalid/volume/'||i,true,'completed',false,
			('30000000-0000-4000-8000-'||lpad(i::text,12,'0'))::uuid
		FROM generate_series(1000,12999) i;
		INSERT INTO article_processing_items(run_id,article_id,article_attempt,input_fingerprint,claim_token,state,updated_at)
		SELECT '20000000-0000-4000-8000-000000000002',id,1,repeat('a',64),completed_processing_claim,'completed',
			timestamp '2026-10-01' + id*interval '1 minute'
		FROM articles WHERE id BETWEEN 1000 AND 12999;
		ANALYZE articles;
		ANALYZE article_processing_items`); err != nil {
		t.Fatal(err)
	}
	explain, err := pool.Query(ctx, `EXPLAIN SELECT article_id,claim_token,ready_at,title,summary FROM topic_shadow_inputs
		WHERE (ready_at,article_id,claim_token) > ('2026-10-09','12490','00000000-0000-0000-0000-000000000000')
		AND ready_at <= '2026-10-10' ORDER BY ready_at,article_id,claim_token LIMIT 51`)
	if err != nil {
		t.Fatal(err)
	}
	lines, err := pgx.CollectRows(explain, pgx.RowTo[string])
	if err != nil || !strings.Contains(strings.Join(lines, " "), "idx_article_processing_items_topic_ready") {
		t.Fatalf("readiness cursor index was not chosen: %v", err)
	}

}
