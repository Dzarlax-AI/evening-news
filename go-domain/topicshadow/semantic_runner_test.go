package topicshadow

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type semanticFixtureIndex struct{ prior []IndexedArticle }

func (s *semanticFixtureIndex) Index(_ context.Context, a IndexedArticle) (bool, error) {
	s.prior = append(s.prior, a)
	return true, nil
}

func (s *semanticFixtureIndex) Neighbors(_ context.Context, a IndexedArticle, limit int) ([]SemanticNeighbor, error) {
	var out []SemanticNeighbor
	for _, p := range s.prior {
		if !p.FetchedAt.Before(a.FetchedAt) || len(out) == limit {
			continue
		}
		out = append(out, SemanticNeighbor{ArticleID: p.ArticleID, Claim: p.Claim, VersionID: p.PointID(), FetchedAt: p.FetchedAt, AvailableAt: p.AvailableAt, Score: 1})
	}
	return out, nil
}

type semanticFixtureJev struct{}

func (semanticFixtureJev) Decide(_ context.Context, input string, questions []ChoiceQuestion) (DecisionsResult, error) {
	result := DecisionsResult{Model: "fixture"}
	for _, q := range questions {
		value := q.Choices[0].Value
		switch q.Name {
		case "decision":
			value = MatchOutcomeNewCandidate
			for _, c := range q.Choices {
				if strings.HasPrefix(c.Value, topicChoicePrefix) {
					value = c.Value
					break
				}
			}
		case "relation":
			value = NoveltyOutcomeMaterialUpdate
			if strings.Contains(input, "second report") {
				value = NoveltyOutcomeRepeat
			}
		case "official_change":
			value = "yes"
		}
		var probabilities []ChoiceProbability
		for _, c := range q.Choices {
			p := 0.0
			if c.Value == value {
				p = 1
			}
			probabilities = append(probabilities, ChoiceProbability{Value: c.Value, Probability: p})
		}
		result.Answers = append(result.Answers, ChoiceAnswer{Name: q.Name, Choice: value, Probabilities: probabilities})
	}
	return result, nil
}

func TestSemanticRunnerProtectsLifecycle(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not configured")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil || !isLoopbackReplayDatabase(cfg.ConnConfig) {
		t.Fatal("requires loopback test database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	admin, err := pgxpool.NewWithConfig(ctx, cfg.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()
	var nonce [8]byte
	if _, err = rand.Read(nonce[:]); err != nil {
		t.Fatal(err)
	}
	schema := "semantic_shadow_test_" + hex.EncodeToString(nonce[:])
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
		VALUES ('20000000-0000-4000-8000-000000000001','news_processing','{}','semantic-shadow-fixture','succeeded');
		INSERT INTO article_processing_runs(id,job_id,attempt_generation,envelope_sha256,owner_id,
			worker_generation,backend_pid,executor_instance_id,deadline_at)
		VALUES ('20000000-0000-4000-8000-000000000002','20000000-0000-4000-8000-000000000001',
			1,repeat('a',64),'20000000-0000-4000-8000-000000000003',1,1,
			'20000000-0000-4000-8000-000000000004',timezone('UTC',now()))`); err != nil {
		t.Fatal(err)
	}
	base := time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	items := []struct {
		id                    int
		claim, title, summary string
		available             time.Time
	}{
		{1, "20000000-0000-4000-8000-000000000011", "ALTA loses banking license", "The central bank revoked the banking license of ALTA Banka.", base},
		{2, "20000000-0000-4000-8000-000000000012", "ALTA Banka license revoked", "A second report confirms the banking license of ALTA Banka was revoked.", base.Add(time.Hour)},
		{3, "20000000-0000-4000-8000-000000000013", "Rumor about ALTA Banka", "Rumor says OTP Bank could take over ALTA Banka.", base.Add(2 * time.Hour)},
		{4, "20000000-0000-4000-8000-000000000014", "ALTA deposit payouts begin", "The deposit insurer began paying insured ALTA Banka depositors.", base.Add(8 * 24 * time.Hour)},
	}
	for _, i := range items {
		if _, err = pool.Exec(ctx, `INSERT INTO articles(id,title,summary,url,summary_processed,processing_state,is_advertisement,
		completed_processing_claim,fetched_at,published_at)
		VALUES ($1,$2,$3,'https://fixture.invalid/news/'||($1::integer)::text,true,'completed',false,$4,timezone('UTC',$5::timestamptz),timezone('UTC',$5::timestamptz))`,
			i.id, i.title, i.summary, i.claim, i.available); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO article_processing_items(run_id,article_id,article_attempt,input_fingerprint,claim_token,state,updated_at)
		VALUES ('20000000-0000-4000-8000-000000000002',$1,1,repeat('a',64),$2,'completed',$3)`, i.id, i.claim, i.available); err != nil {
			t.Fatal(err)
		}
	}
	r := Runner{Pool: pool, Version: "replay-semantic-pilot", Owner: "20000000-0000-4000-8000-000000000005", Client: semanticFixtureJev{},
		DailyRequestLimit: 20, Semantic: &semanticFixtureIndex{}, WarmupFrom: base.Add(-time.Microsecond)}
	if _, err = r.Replay(ctx, []time.Time{base.Add(3 * time.Hour), base.Add(7 * 24 * time.Hour), base.Add(9 * 24 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	var events []string
	if err = pool.QueryRow(ctx, `SELECT array_agg(kind ORDER BY available_at,topic_revision) FROM topic_shadow_events WHERE policy_version=$1`, r.Version).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(events, ","); got != "opened,quieted,returned" {
		t.Fatalf("unexpected timeline %s", got)
	}
	var duplicate, rumor string
	if err = pool.QueryRow(ctx, `SELECT (SELECT decision->>'novelty' FROM topic_shadow_decisions WHERE policy_version=$1 AND article_id=2),
		(SELECT state FROM topic_shadow_decisions WHERE policy_version=$1 AND article_id=3)`, r.Version).Scan(&duplicate, &rumor); err != nil {
		t.Fatal(err)
	}
	if duplicate != "repeat" || rumor != "review" {
		t.Fatalf("unexpected duplicate and rumor: %s / %s", duplicate, rumor)
	}
}
