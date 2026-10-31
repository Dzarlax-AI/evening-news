package airuntime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/digestworkflow"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-domain/telegramdelivery"
	"github.com/Dzarlax-AI/evening-news/go-domain/viberdelivery"
	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type viberFixtureRoundTrip func(*http.Request) (*http.Response, error)

func (f viberFixtureRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func TestViberRuntimeWithRealFence(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL unavailable")
	}
	if validateLoopbackTestDatabaseDSN(dsn) != nil {
		t.Fatal("loopback required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, e := pgx.Connect(ctx, dsn)
	if e != nil {
		t.Fatal("fixture database unavailable")
	}
	defer admin.Close(context.Background())
	schema := fmt.Sprintf("viber_runtime_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, e = admin.Exec(ctx, "CREATE SCHEMA "+q); e != nil {
		t.Fatal(e)
	}
	defer admin.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE")
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.MaxConns = 5
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	defer pool.Close()
	if _, e = migration.Run(ctx, pool, true, false); e != nil {
		t.Fatal("migration fixture", e)
	}
	connect := func() *pgx.Conn {
		c, e := pgx.ConnectConfig(ctx, cfg.ConnConfig)
		if e != nil {
			t.Fatal(e)
		}
		return c
	}
	worker := connect()
	defer worker.Close(context.Background())
	executor := connect()
	defer executor.Close(context.Background())
	var wp, ep int
	if e = worker.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema())),pg_backend_pid()`).Scan(new(any), &wp); e != nil {
		t.Fatal(e)
	}
	if e = executor.QueryRow(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # 1162757938),pg_backend_pid()`).Scan(new(any), &ep); e != nil {
		t.Fatal(e)
	}
	const job = "10000000-0000-4000-8000-000000000001"
	const owner = "10000000-0000-4000-8000-000000000002"
	const execID = "10000000-0000-4000-8000-000000000003"
	const domain = "10000000-0000-4000-8000-000000000004"
	const ruID = "10000000-0000-4000-8000-000000000005"
	const srID = "10000000-0000-4000-8000-000000000006"
	deadline := time.Now().UTC().Add(3 * time.Minute).Truncate(time.Microsecond)
	if _, e = pool.Exec(ctx, `UPDATE background_worker_status SET owner_id=$1,generation=1,status='running',lock_backend_pid=$2,heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC' WHERE id=1`, owner, wp); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES($1,'news_digest','{}','publication-fixture','running',1,1)`, job); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, job, strings.Repeat("a", 64), execID, owner, deadline); e != nil {
		t.Fatal(e)
	}
	fence := authority.Fence{Pool: pool, JobID: job, OwnerID: owner, ExecutorInstanceID: execID, Kind: "news_digest", AttemptGeneration: 1, Generation: 1, WorkerBackendPID: wp, ExecutorBackendPID: ep, Deadline: deadline, Payload: json.RawMessage(`{}`)}
	if e = fence.Check(ctx); e != nil {
		t.Fatal("real fence rejected", e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO installation_state(id,installation_id) VALUES(1,'viber-test')`); e != nil {
		t.Fatal(e)
	}
	credentials := &credentialstore.Store{Pool: pool, Key: bytes.Repeat([]byte{4}, 32), KeyID: "fixture-key", InstallationID: "viber-test"}
	if _, e = credentials.Put(ctx, "viber", 0, []byte("fixture-token")); e != nil {
		t.Fatal(e)
	}
	settings := appsettings.Store{Pool: pool}
	verified := time.Now().UTC()
	_, e = settings.UpdateViber(ctx, appsettings.ViberUpdate{ChannelID: "fixture-channel", ChannelName: "fixture", SenderID: "fixture-sender", CredentialRevision: 1, VerifiedCredentialRevision: 1, WebhookReady: true, VerifiedAt: &verified, DeliveryEnabled: true})
	if e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES('operational_serbian_enabled','true',now())`); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES('telegram_serbian_delivery_enabled','true',now()),('telegram_serbian_chat_id','"fixture-sr"',now())`); e != nil {
		t.Fatal(e)
	}
	pinned, e := settings.PinOperational(ctx, job, 1)
	if e != nil || pinned.Viber == nil || !pinned.Viber.DeliveryEnabled || pinned.Viber.EnabledFrom == nil {
		t.Fatal("failed to pin Viber settings", e)
	}
	date := time.Now().UTC().Format("2006-01-02")
	for _, item := range []struct{ id, locale, text string }{{ruID, "ru", "Подтвержденная новость"}, {srID, "sr-Latn", "Potvrđena vest"}} {
		snap := editorial.Snapshot{Date: date, CutoffUTC: time.Now().UTC().Truncate(time.Second), SiteBaseURL: "https://news.example", EditionURL: "https://news.example/digest/fixture", Articles: []editorial.Article{{ID: "101", Title: item.text, Summary: item.text, Content: item.text, SourceName: "Fixture", URL: "https://source.example/101", CategoryID: "4", CategoryName: "Tech"}}}
		story := editorial.Story{Kind: "main", Title: item.text, Summary: item.text, PrimaryArticleID: "101", ArticleIDs: []string{"101"}, Evidence: []editorial.Evidence{{ArticleID: "101", Quote: item.text, Locale: item.locale}}}
		edition, err := editorial.FinalizeEdition(snap, item.locale, []editorial.Story{story}, nil)
		if err != nil {
			t.Fatal("synthetic edition invalid", err)
		}
		snapshotJSON, _ := json.Marshal(snap)
		editionJSON, _ := json.Marshal(edition)
		rendered, err := editorial.Render(snap, item.locale, edition)
		if err != nil {
			t.Fatal(err)
		}
		renderedJSON, _ := json.Marshal(rendered)
		if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,job_id,attempt_generation) VALUES($1,$2,$3,'test',$4,$5,$6,$7,$8,1)`, item.id, date, item.locale, edition.SourceSnapshotHash, snapshotJSON, editionJSON, renderedJSON, job); e != nil {
			t.Fatal("save edition", e)
		}
	}
	var posts atomic.Int32
	var failAt atomic.Int32
	var unknownAt atomic.Int32
	var sentTexts []string
	server := &Server{db: pool, credentials: credentials, client: &http.Client{Transport: viberFixtureRoundTrip(func(r *http.Request) (*http.Response, error) {
		postNumber := posts.Add(1)
		body, _ := io.ReadAll(r.Body)
		var posted struct {
			Text string `json:"text"`
		}
		if json.Unmarshal(body, &posted) != nil {
			t.Error("invalid request body")
		}
		sentTexts = append(sentTexts, posted.Text)
		if r.URL.String() != "https://chatapi.viber.com/pa/post" {
			t.Errorf("unexpected network target")
		}
		if postNumber == unknownAt.Load() {
			return &http.Response{StatusCode: 500, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
		}
		if postNumber == failAt.Load() {
			return &http.Response{StatusCode: 429, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`)), Request: r}, nil
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":0,"message_token":12345678901234567890}`)), Request: r}, nil
	})}}
	v := digestInput{DomainInstanceID: domain, Envelope: digestEnvelope{JobID: job, Attempt: 1}}
	// The full prepare path must persist Viber before rejecting incomplete Telegram settings.
	if pinned.Telegram == nil || pinned.Telegram.ChatID != "" {
		t.Fatal("unexpected Telegram configuration")
	}
	v.Operation = "digest_prepare"
	v.Kind = "news_digest"
	v.Payload = json.RawMessage(`{}`)
	v.Envelope.Deadline = deadline.Format(time.RFC3339Nano)
	if _, e = server.executeDigest(ctx, v, fence); !errors.Is(e, digestworkflow.ErrDestination) {
		t.Fatalf("expected Telegram destination failure: %v", e)
	}
	if posts.Load() != 0 {
		t.Fatal("prepare unexpectedly sent network request")
	}
	var pending int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM viber_publication_requests WHERE russian_edition_id=$1`, ruID).Scan(&pending); e != nil || pending != 1 {
		t.Fatal("prepare failed to durably request Viber", e)
	}
	if e = server.bindViberPublication(ctx, v, fence, &pinned, ruID); e != nil {
		t.Fatal("bind", e)
	}
	var deliveryID, paired string
	if e = pool.QueryRow(ctx, `SELECT id::text,serbian_edition_id::text FROM viber_digest_deliveries WHERE russian_edition_id=$1`, ruID).Scan(&deliveryID, &paired); e != nil || paired != srID {
		t.Fatal("wrong frozen pairing", e)
	}
	if e = server.bindViberPublication(ctx, v, fence, &pinned, ruID); e != nil {
		t.Fatal("idempotent bind", e)
	}
	if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='succeeded' WHERE id=$1`, job); e != nil {
		t.Fatal(e)
	}
	var enqueued bool
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatal("enqueue", e)
	}
	var deliveryJob string
	if e = pool.QueryRow(ctx, `SELECT last_job_id::text FROM viber_digest_deliveries WHERE id=$1`, deliveryID).Scan(&deliveryJob); e != nil {
		t.Fatal(e)
	}
	payload, _ := json.Marshal(map[string]string{"delivery_id": deliveryID})
	if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='running',owner_generation=1,attempt_generation=1 WHERE id=$1`, deliveryJob); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, deliveryJob, strings.Repeat("b", 64), execID, owner, deadline); e != nil {
		t.Fatal(e)
	}
	deliveryFence := fence
	deliveryFence.JobID = deliveryJob
	deliveryFence.Kind = "viber_digest_delivery"
	deliveryFence.Payload = payload
	v.Envelope.JobID = deliveryJob
	v.Payload = payload
	wrong := deliveryFence
	wrong.AttemptGeneration = 2
	if _, e = server.executeViberDelivery(ctx, v, wrong); e == nil || posts.Load() != 0 {
		t.Fatal("stale fence sent network request")
	}
	if _, e = executor.Exec(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()),hashtext(current_schema()) # 1162757938)`); e != nil {
		t.Fatal(e)
	}
	if _, e = server.executeViberDelivery(ctx, v, deliveryFence); e == nil || posts.Load() != 0 {
		t.Fatal("lost executor lock allowed a request")
	}
	if _, e = executor.Exec(ctx, `SELECT pg_advisory_lock(hashtext(current_database()),hashtext(current_schema()) # 1162757938)`); e != nil {
		t.Fatal(e)
	}
	result, e := server.executeViberDelivery(ctx, v, deliveryFence)
	if e != nil {
		t.Fatal("delivery", e)
	}
	if result["success"] != true || posts.Load() != 1 {
		t.Fatalf("delivery outcome %#v posts=%d", result, posts.Load())
	}
	var state, token string
	if e = pool.QueryRow(ctx, `SELECT d.state,p.message_token FROM viber_digest_deliveries d JOIN viber_digest_parts p ON p.delivery_id=d.id WHERE d.id=$1`, deliveryID).Scan(&state, &token); e != nil || state != "completed" || token != "12345678901234567890" {
		t.Fatal("receipt not completed", e)
	}
	if _, e = server.executeViberDelivery(ctx, v, deliveryFence); e != nil || posts.Load() != 1 {
		t.Fatal("completed delivery resent", e)
	}
	// Bind a new Russian edition before its Serbian pair is available.
	const job2 = "20000000-0000-4000-8000-000000000001"
	const ru2 = "20000000-0000-4000-8000-000000000002"
	const sr2 = "20000000-0000-4000-8000-000000000003"
	if _, e = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES($1,'news_digest','{}','second-publication','running',1,1)`, job2); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, job2, strings.Repeat("c", 64), execID, owner, deadline); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,job_id,attempt_generation,created_at) SELECT $1,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,$2,1,clock_timestamp() AT TIME ZONE 'UTC' FROM editorial_digest_editions WHERE id=$3`, ru2, job2, ruID); e != nil {
		t.Fatal(e)
	}
	pinned2, e := settings.PinOperational(ctx, job2, 1)
	if e != nil {
		t.Fatal(e)
	}
	fence2 := fence
	fence2.JobID = job2
	v.Envelope.JobID = job2
	v.Payload = json.RawMessage(`{}`)
	if e = server.bindViberPublication(ctx, v, fence2, &pinned2, ru2); e != nil {
		t.Fatal("late bind", e)
	}
	var id2, state2 string
	if e = pool.QueryRow(ctx, `SELECT id::text,state FROM viber_digest_deliveries WHERE russian_edition_id=$1`, ru2).Scan(&id2, &state2); e != nil || state2 != "waiting_edition" {
		t.Fatal("missing waiting binding", e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || enqueued {
		t.Fatal("delivery enqueued before SR", e)
	}
	// Simulate the localized edition arriving on the next day; generation stays exact.
	if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,job_id,attempt_generation,created_at) SELECT $1,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,$2,1,(clock_timestamp()+interval '1 day') AT TIME ZONE 'UTC' FROM editorial_digest_editions WHERE id=$3`, sr2, job2, srID); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatal("late SR not dispatched", e)
	}
	var deliveryJob2, paired2 string
	if e = pool.QueryRow(ctx, `SELECT last_job_id::text,serbian_edition_id::text FROM viber_digest_deliveries WHERE id=$1`, id2).Scan(&deliveryJob2, &paired2); e != nil || paired2 != sr2 {
		t.Fatal("late SR paired incorrectly", e)
	}
	runDelivery := func(jobID string) (map[string]any, error) {
		if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='running',owner_generation=1,attempt_generation=1 WHERE id=$1`, jobID); e != nil {
			t.Fatal(e)
		}
		if _, e = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, jobID, strings.Repeat("d", 64), execID, owner, deadline); e != nil {
			t.Fatal(e)
		}
		raw, _ := json.Marshal(map[string]string{"delivery_id": id2})
		current := fence
		current.JobID = jobID
		current.Kind = "viber_digest_delivery"
		current.Payload = raw
		v.Envelope.JobID = jobID
		v.Payload = raw
		if jobID == deliveryJob2 {
			store := viberdelivery.PostgresStore{Pool: pool, Fence: current, JobID: jobID, AttemptGeneration: 1, DomainInstanceID: domain}
			if e = store.Materialize(ctx, id2, "fixture", []viberdelivery.PartInput{{Text: "fixture first part"}, {Text: "fixture second part"}}); e != nil {
				t.Fatal(e)
			}
		}
		return server.executeViberDelivery(ctx, v, current)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_channel_leases SET next_post_at=now()-interval '1 second'`); e != nil {
		t.Fatal(e)
	}
	failAt.Store(3)
	result, e = runDelivery(deliveryJob2)
	if e != nil || result["status"] != "deferred" || posts.Load() != 3 {
		t.Fatalf("rate limit result %#v posts=%d err=%v", result, posts.Load(), e)
	}
	if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='succeeded' WHERE id=$1`, deliveryJob2); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE viber_digest_deliveries SET next_dispatch_at=now()-interval '1 second' WHERE id=$1; UPDATE viber_channel_leases SET cooldown_until=now()-interval '1 second',next_post_at=now()-interval '1 second'`, pgx.QueryExecModeSimpleProtocol, id2); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatal("retry enqueue", e)
	}
	var retryJob string
	if e = pool.QueryRow(ctx, `SELECT last_job_id::text FROM viber_digest_deliveries WHERE id=$1`, id2).Scan(&retryJob); e != nil {
		t.Fatal(e)
	}
	result, e = runDelivery(retryJob)
	if e != nil || result["success"] != true || posts.Load() != 4 {
		t.Fatalf("retry result %#v posts=%d err=%v", result, posts.Load(), e)
	}
	if len(sentTexts) != 4 || sentTexts[1] != "fixture first part" || sentTexts[2] != "fixture second part" || sentTexts[3] != "fixture second part" {
		t.Fatal("retry did not skip the confirmed part")
	}
	// An indeterminate HTTP failure must persist unknown and never resend.
	const job3 = "30000000-0000-4000-8000-000000000001"
	const ru3 = "30000000-0000-4000-8000-000000000002"
	const sr3 = "30000000-0000-4000-8000-000000000003"
	if _, e = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES($1,'news_digest','{}','third-publication','running',1,1)`, job3); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, job3, strings.Repeat("e", 64), execID, owner, deadline); e != nil {
		t.Fatal(e)
	}
	for _, ed := range []struct{ id, source string }{{ru3, ruID}, {sr3, srID}} {
		if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,job_id,attempt_generation,created_at) SELECT $1,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,$2,1,clock_timestamp() AT TIME ZONE 'UTC' FROM editorial_digest_editions WHERE id=$3`, ed.id, job3, ed.source); e != nil {
			t.Fatal(e)
		}
	}
	pinned3, e := settings.PinOperational(ctx, job3, 1)
	if e != nil {
		t.Fatal(e)
	}
	fence3 := fence
	fence3.JobID = job3
	v.Envelope.JobID = job3
	v.Payload = json.RawMessage(`{}`)
	if e = server.bindViberPublication(ctx, v, fence3, &pinned3, ru3); e != nil {
		t.Fatal(e)
	}
	var id3 string
	if e = pool.QueryRow(ctx, `SELECT id::text FROM viber_digest_deliveries WHERE russian_edition_id=$1`, ru3).Scan(&id3); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatal("unknown fixture enqueue", e)
	}
	var dj3 string
	if e = pool.QueryRow(ctx, `SELECT last_job_id::text FROM viber_digest_deliveries WHERE id=$1`, id3).Scan(&dj3); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='running',owner_generation=1,attempt_generation=1 WHERE id=$1; UPDATE viber_channel_leases SET next_post_at=now()-interval '1 second'`, pgx.QueryExecModeSimpleProtocol, dj3); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, dj3, strings.Repeat("f", 64), execID, owner, deadline); e != nil {
		t.Fatal(e)
	}
	raw3, _ := json.Marshal(map[string]string{"delivery_id": id3})
	fd3 := fence
	fd3.JobID = dj3
	fd3.Kind = "viber_digest_delivery"
	fd3.Payload = raw3
	v.Envelope.JobID = dj3
	v.Payload = raw3
	unknownAt.Store(5)
	if _, e = server.executeViberDelivery(ctx, v, fd3); !errors.Is(e, telegramdelivery.ErrUnknown) || posts.Load() != 5 {
		t.Fatal("unknown response not handled", e)
	}
	if e = pool.QueryRow(ctx, `SELECT state FROM viber_digest_deliveries WHERE id=$1`, id3).Scan(&state); e != nil || state != "delivery_unknown" {
		t.Fatal("unknown not persisted", e)
	}
	if _, e = server.executeViberDelivery(ctx, v, fd3); e != nil || posts.Load() != 5 {
		t.Fatal("unknown delivery resent", e)
	}

	// Viber-only storage failure does not block Telegram preparation; the worker
	// reconstructs the exact shared selection once storage recovers.
	const job4 = "40000000-0000-4000-8000-000000000001"
	const ru4 = "40000000-0000-4000-8000-000000000002"
	const sr4 = "40000000-0000-4000-8000-000000000003"
	if _, e = pool.Exec(ctx, `INSERT INTO background_jobs(id,kind,payload,idempotency_key,state,owner_generation,attempt_generation) VALUES($1,'news_digest','{}','fourth-publication','running',1,1)`, job4); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `INSERT INTO background_job_executions(job_id,attempt_generation,envelope_sha256,executor_instance_id,owner_id,worker_generation,deadline_at,state) VALUES($1,1,$2,$3,$4,1,$5,'running')`, job4, strings.Repeat("a", 64), execID, owner, deadline); e != nil {
		t.Fatal(e)
	}
	for _, ed := range []struct{ id, source string }{{ru4, ruID}, {sr4, srID}} {
		if _, e = pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,job_id,attempt_generation,created_at) SELECT $1,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,$2,1,clock_timestamp() AT TIME ZONE 'UTC' FROM editorial_digest_editions WHERE id=$3`, ed.id, job4, ed.source); e != nil {
			t.Fatal(e)
		}
	}
	if _, e = settings.UpdateTelegram(ctx, appsettings.TelegramUpdate{ChatID: "@fixture_ru"}); e != nil {
		t.Fatal(e)
	}
	if _, e = settings.PinOperational(ctx, job4, 1); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `CREATE FUNCTION deny_viber_request_fixture() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture'; END $$; CREATE TRIGGER deny_viber_request_fixture BEFORE INSERT ON viber_publication_requests FOR EACH ROW EXECUTE FUNCTION deny_viber_request_fixture()`); e != nil {
		t.Fatal(e)
	}
	server.digest = &digestRegistry{}
	fence4 := fence
	fence4.JobID = job4
	v.Envelope.JobID = job4
	v.Envelope.Deadline = deadline.Format(time.RFC3339Nano)
	v.Operation = "digest_prepare"
	v.Kind = "news_digest"
	v.Payload = json.RawMessage(`{}`)
	result, e = server.executeDigest(ctx, v, fence4)
	if e != nil || result["viber_binding_status"] != "retry_pending" || result["prepared_token"] == "" {
		t.Fatalf("Telegram preparation failed after Viber write failure: %#v %v", result, e)
	}
	var selected string
	if e = pool.QueryRow(ctx, `SELECT value->>'russian_edition_id' FROM settings WHERE key=$1`, "managed_digest_publication:"+job4+":1").Scan(&selected); e != nil || selected != ru4 {
		t.Fatal("shared selection missing", e)
	}
	var before int
	if e = pool.QueryRow(ctx, `SELECT count(*) FROM viber_publication_requests WHERE russian_edition_id=$1`, ru4).Scan(&before); e != nil || before != 0 {
		t.Fatal("fault injection failed", e)
	}
	if _, e = pool.Exec(ctx, `DROP TRIGGER deny_viber_request_fixture ON viber_publication_requests; DROP FUNCTION deny_viber_request_fixture()`); e != nil {
		t.Fatal(e)
	}
	if _, e = pool.Exec(ctx, `UPDATE background_jobs SET state='failed' WHERE id=$1`, job4); e != nil {
		t.Fatal(e)
	}
	if e = pool.QueryRow(ctx, `SELECT enqueue_viber_digest_delivery()`).Scan(&enqueued); e != nil || !enqueued {
		t.Fatal("shared selection was not recovered", e)
	}
	var recoveredPair string
	if e = pool.QueryRow(ctx, `SELECT serbian_edition_id::text FROM viber_digest_deliveries WHERE russian_edition_id=$1`, ru4).Scan(&recoveredPair); e != nil || recoveredPair != sr4 {
		t.Fatal("wrong recovered exact edition", e)
	}
	if posts.Load() != 5 {
		t.Fatal("prepare or reconstruction caused a network request")
	}
	if _, e = executor.Exec(ctx, `SELECT pg_advisory_unlock(hashtext(current_database()),hashtext(current_schema()) # 1162757938)`); e != nil {
		t.Fatal(e)
	}
	if e = deliveryFence.Check(ctx); !errors.Is(e, authority.ErrStale) {
		t.Fatal("executor fence loss not detected", e)
	}
}
