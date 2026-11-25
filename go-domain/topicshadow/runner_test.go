package topicshadow

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type testTopicTransport struct{ calls int }

func (f *testTopicTransport) Decide(_ context.Context, input string, questions []ChoiceQuestion) (DecisionsResult, error) {
	f.calls++
	result := DecisionsResult{Model: DecisionsModel}
	for _, q := range questions {
		choice := q.Choices[0].Value
		switch q.Name {
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

func TestRunnerKeepsHistoryAndDeduplicatesPasses(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not configured")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal("invalid isolated PostgreSQL configuration")
	}
	ip := net.ParseIP(cfg.ConnConfig.Host)
	if ip == nil || !ip.IsLoopback() || len(cfg.ConnConfig.Fallbacks) != 0 {
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
	transport := &testTopicTransport{}
	r := Runner{Pool: pool, Version: "fixture", Owner: "20000000-0000-4000-8000-000000000005", Client: transport, DailyRequestLimit: 20}
	base := time.Now().UTC().Truncate(time.Second).Add(-8 * 24 * time.Hour)
	add(31, "20000000-0000-4000-8000-000000000031", "Belgrade bridge project opens", "The Belgrade bridge project officially opened.", base)
	pass, err := r.RunPass(ctx, base.Add(time.Second), 50)
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
