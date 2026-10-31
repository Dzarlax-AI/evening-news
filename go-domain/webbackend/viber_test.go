package webbackend

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-domain/viberdelivery"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type viberTestRT func(*http.Request) (*http.Response, error)

func (f viberTestRT) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }
func viberBackendFixture(t *testing.T) (*Server, *credentialstore.Store) {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL required")
	}
	u, e := url.Parse(dsn)
	if e != nil || (u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost") {
		t.Fatal("loopback fixture required")
	}
	ctx := context.Background()
	root, e := pgxpool.New(ctx, dsn)
	if e != nil {
		t.Fatal(e)
	}
	schema := fmt.Sprintf("viber_web_%d", time.Now().UnixNano())
	q := pgx.Identifier{schema}.Sanitize()
	if _, e = root.Exec(ctx, "CREATE SCHEMA "+q); e != nil {
		t.Fatal(e)
	}
	cfg, e := pgxpool.ParseConfig(dsn)
	if e != nil {
		t.Fatal(e)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { pool.Close(); root.Exec(context.Background(), "DROP SCHEMA "+q+" CASCADE"); root.Close() })
	result, e := migration.Run(ctx, pool, true, false)
	if e != nil || len(result.Errors) > 0 {
		t.Fatalf("migration: %v %v", e, result.Errors)
	}
	installation := strings.Repeat("a", 32)
	if _, e = pool.Exec(ctx, `INSERT INTO installation_state(id,installation_id) VALUES(1,$1)`, installation); e != nil {
		t.Fatal(e)
	}
	credentials := &credentialstore.Store{Pool: pool, Key: bytes.Repeat([]byte{7}, 32), KeyID: "viber-fixture", InstallationID: installation}
	if _, e = credentials.Put(ctx, "viber", 0, []byte("synthetic-viber-token")); e != nil {
		t.Fatal(e)
	}
	return &Server{Pool: pool, Credentials: credentials, Guard: testGuard{}, ViberSiteBase: "https://news.fixture"}, credentials
}
func viberBackendRequest(t *testing.T, s *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, e := json.Marshal(body)
	if e != nil {
		t.Fatal(e)
	}
	w := httptest.NewRecorder()
	s.Handler().ServeHTTP(w, httptest.NewRequest(method, path, bytes.NewReader(raw)))
	return w
}
func viberSignedEvent(body []byte, token string) string {
	m := hmac.New(sha256.New, []byte(token))
	m.Write(body)
	return hex.EncodeToString(m.Sum(nil))
}

func TestViberBackendConnectRequiresSignedHandshakeAndCAS(t *testing.T) {
	s, _ := viberBackendFixture(t)
	ctx := context.Background()
	signed := false
	calls := 0
	s.ViberClient = &http.Client{Transport: viberTestRT(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.Host != "chatapi.viber.com" {
			t.Fatal("unexpected external host")
		}
		data, _ := io.ReadAll(r.Body)
		var payload map[string]any
		if json.Unmarshal(data, &payload) != nil || payload["auth_token"] != "synthetic-viber-token" {
			t.Fatal("missing transport credential")
		}
		response := `{"status":0}`
		switch r.URL.Path {
		case "/pa/get_account_info":
			response = `{"status":0,"Id":"pa:fixture","chat_hostname":"Fixture channel","members":[{"id":"sender","name":"Fixture admin","role":"superadmin"}]}`
		case "/pa/set_webhook":
			if payload["url"] != "https://news.fixture/integrations/viber/webhook" {
				t.Fatal("unexpected callback origin")
			}
			body, _ := json.Marshal(map[string]any{"event": "webhook", "timestamp": time.Now().UnixMilli(), "message_token": 1})
			signature := strings.Repeat("0", 64)
			if signed {
				signature = viberSignedEvent(body, "synthetic-viber-token")
			}
			got := viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/webhook", map[string]any{"body": body, "signature": signature})
			if signed && got.Code != 200 || !signed && got.Code != 401 {
				t.Fatalf("callback status %d", got.Code)
			}
		default:
			t.Fatalf("unexpected Viber method: %s", r.URL.Path)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(response))}, nil
	})}
	verified := viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/verify", map[string]any{"expected_revision": 0})
	if verified.Code != 200 || strings.Contains(verified.Body.String(), "synthetic-viber-token") {
		t.Fatalf("verification status %d", verified.Code)
	}
	v, e := (appsettings.Store{Pool: s.Pool}).Viber(ctx)
	if e != nil || v.ChannelID != "pa:fixture" || v.WebhookReady || v.Revision != 1 {
		t.Fatalf("verification configuration: %v", e)
	}
	stale := viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/connect", map[string]any{"expected_revision": 0})
	if stale.Code != 409 || calls != 1 {
		t.Fatal("stale connect performed external mutation")
	}
	unverified := viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/connect", map[string]any{"expected_revision": 1})
	if unverified.Code != 502 {
		t.Fatalf("unsigned connect status %d", unverified.Code)
	}
	v, _ = (appsettings.Store{Pool: s.Pool}).Viber(ctx)
	if v.WebhookReady {
		t.Fatal("unsigned callback enabled delivery")
	}
	signed = true
	connected := viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/connect", map[string]any{"expected_revision": 1})
	if connected.Code != 200 {
		t.Fatalf("signed connect status %d: %s", connected.Code, connected.Body.String())
	}
	if _, e = s.Pool.Exec(ctx, `INSERT INTO settings(key,value) VALUES('operational_serbian_enabled','true') ON CONFLICT(key) DO UPDATE SET value=excluded.value`); e != nil {
		t.Fatal(e)
	}
	enable := viberBackendRequest(t, s, "PUT", "/internal/web/v1/configuration/viber", map[string]any{"expected_revision": 2, "delivery_enabled": true, "sender_id": "sender"})
	if enable.Code != 200 {
		t.Fatalf("enable status %d: %s", enable.Code, enable.Body.String())
	}
	v, _ = (appsettings.Store{Pool: s.Pool}).Viber(ctx)
	if !v.DeliveryEnabled || !v.WebhookReady || v.EnabledFrom == nil {
		t.Fatal("connected delivery was not enabled")
	}
	if calls != 3 {
		t.Fatalf("configuration actions unexpectedly posted a message; calls=%d", calls)
	}
}

func TestViberBackendRejectsInvalidCallbackBeforePersistence(t *testing.T) {
	s, _ := viberBackendFixture(t)
	for _, tc := range []struct {
		body      []byte
		signature string
		want      int
	}{
		{[]byte(`{"event":"webhook"}`), strings.Repeat("f", 64), 401},
		{[]byte(`{"event":"webhook","timestamp":1}`), viberSignedEvent([]byte(`{"event":"webhook","timestamp":1}`), "synthetic-viber-token"), 401},
		{bytes.Repeat([]byte("x"), 16385), strings.Repeat("f", 64), 400},
	} {
		w := viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/webhook", map[string]any{"body": tc.body, "signature": tc.signature})
		if w.Code != tc.want {
			t.Fatalf("callback got=%d want=%d", w.Code, tc.want)
		}
	}
	var n int
	if e := s.Pool.QueryRow(context.Background(), `SELECT count(*) FROM settings WHERE key='viber_webhook_ack'`).Scan(&n); e != nil || n != 0 {
		t.Fatal("invalid callback persisted evidence")
	}
}
func TestViberCallbackOriginIsFixedHTTPS(t *testing.T) {
	for _, base := range []string{"http://localhost:8000", "https://u:p@host", "https://host/path", "https://host?override=1"} {
		if _, e := viberCallbackURL(base); e == nil {
			t.Fatalf("accepted unsafe origin %s", base)
		}
	}
	got, e := viberCallbackURL("https://news.fixture/")
	if e != nil || got != "https://news.fixture/integrations/viber/webhook" {
		t.Fatal("invalid callback URL")
	}
}

func TestViberManualTestReceiptAndChannelLease(t *testing.T) {
	s, credentials := viberBackendFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	v, err := (appsettings.Store{Pool: s.Pool}).UpdateViber(ctx, appsettings.ViberUpdate{ChannelID: "channel", ChannelName: "Fixture", SenderID: "sender", CredentialRevision: 1, VerifiedCredentialRevision: 1, WebhookReady: true, VerifiedAt: &now, DeliveryEnabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Pool.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES('operational_delivery_enabled','false',now())`); err != nil {
		t.Fatal(err)
	}
	calls := 0
	responseCode := 200
	s.ViberClient = &http.Client{Transport: viberTestRT(func(r *http.Request) (*http.Response, error) {
		calls++
		if r.URL.String() != "https://chatapi.viber.com/pa/post" {
			t.Fatal("wrong Viber endpoint")
		}
		raw, _ := io.ReadAll(r.Body)
		var body struct {
			From string `json:"from"`
			Text string `json:"text"`
			Auth string `json:"auth_token"`
		}
		if json.Unmarshal(raw, &body) != nil || body.From != "sender" || body.Auth != "synthetic-viber-token" || body.Text != "Probna poruka · Evening News\nViber kanal je povezan. Ovo je test, nije dnevni pregled.\nhttps://news.fixture/?locale=sr-Latn" {
			t.Fatal("unexpected test payload")
		}
		data := `{"status":0,"message_token":12345678901234567890}`
		if responseCode != 200 {
			data = `{}`
		}
		return &http.Response{StatusCode: responseCode, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(data)), Request: r}, nil
	})}
	request := func(id string, revision int64) *httptest.ResponseRecorder {
		return viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/test", map[string]any{"request_id": id, "expected_revision": revision})
	}
	const id = "10000000-0000-4000-8000-000000000001"
	const id2 = "10000000-0000-4000-8000-000000000002"
	const id3 = "10000000-0000-4000-8000-000000000003"
	if got := request(id, v.Revision+1); got.Code != 409 || calls != 0 {
		t.Fatal("stale settings allowed", got.Code)
	}
	got := request(id, v.Revision)
	if got.Code != 200 || calls != 1 || !strings.Contains(got.Body.String(), `"state":"sent"`) || !strings.Contains(got.Body.String(), `"message_token":"12345678901234567890"`) || strings.Contains(got.Body.String(), "synthetic-viber-token") {
		t.Fatal("test result", got.Code, got.Body.String())
	}
	got = request(id, v.Revision)
	if got.Code != 200 || calls != 1 || !strings.Contains(got.Body.String(), `"state":"sent"`) {
		t.Fatal("duplicate resent", got.Code)
	}
	got = viberBackendRequest(t, s, "GET", "/internal/web/v1/configuration/viber", nil)
	if got.Code != 200 || !strings.Contains(got.Body.String(), `"last_test"`) || !strings.Contains(got.Body.String(), id) {
		t.Fatal("last test missing", got.Code)
	}
	// Active digest lease excludes a manual test even when automatic delivery is disabled.
	const ru = "20000000-0000-4000-8000-000000000001"
	const delivery = "20000000-0000-4000-8000-000000000002"
	_, err = s.Pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered) VALUES($1,current_date,'ru','test',repeat('a',64),'{"articles":[{}]}',jsonb_build_object('version','1','locale','ru','source_snapshot_hash',repeat('a',64),'stories','[]'::jsonb),'{"rich_html":"","plain_parts":[],"preview_text":""}'); INSERT INTO viber_digest_deliveries(id,russian_edition_id,channel_id,sender_id,settings_revision,credential_revision,enabled_from,state) VALUES($2,$1,'channel','sender',1,1,now(),'ready'); UPDATE viber_channel_leases SET delivery_id=$2,test_request_id=NULL,lease_until=now()+interval '1 minute',next_post_at=now()-interval '1 second' WHERE channel_id='channel'`, pgx.QueryExecModeSimpleProtocol, ru, delivery)
	if err != nil {
		t.Fatal(err)
	}
	if got = request(id2, v.Revision); got.Code != 409 || calls != 1 {
		t.Fatal("active digest lease ignored", got.Code)
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE viber_channel_leases SET delivery_id=NULL,lease_until=NULL,next_post_at=now()-interval '1 second' WHERE channel_id='channel'`); err != nil {
		t.Fatal(err)
	}
	responseCode = 500
	got = request(id2, v.Revision)
	if got.Code != 200 || !strings.Contains(got.Body.String(), `"state":"unknown"`) || calls != 2 {
		t.Fatal("unknown result", got.Code, got.Body.String())
	}
	got = request(id2, v.Revision)
	if got.Code != 200 || calls != 2 {
		t.Fatal("unknown result resent")
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE viber_channel_leases SET next_post_at=now()-interval '1 second' WHERE channel_id='channel'`); err != nil {
		t.Fatal(err)
	}
	responseCode = 429
	got = request(id3, v.Revision)
	if got.Code != 200 || !strings.Contains(got.Body.String(), `"state":"rejected"`) || !strings.Contains(got.Body.String(), `"status_code":429`) || calls != 3 {
		t.Fatal("rate limit result", got.Code, got.Body.String())
	}
	if _, err = credentials.Put(ctx, "viber", 1, []byte("new-fixture-token")); err != nil {
		t.Fatal(err)
	}
	if got = request("10000000-0000-4000-8000-000000000004", v.Revision); got.Code != 422 || calls != 3 {
		t.Fatal("rotated credential accepted", got.Code)
	}
}

func TestViberManualConcurrentDuplicate(t *testing.T) {
	s, _ := viberBackendFixture(t)
	now := time.Now().UTC()
	v, err := (appsettings.Store{Pool: s.Pool}).UpdateViber(context.Background(), appsettings.ViberUpdate{ChannelID: "channel", ChannelName: "Fixture", SenderID: "sender", CredentialRevision: 1, VerifiedCredentialRevision: 1, WebhookReady: true, VerifiedAt: &now})
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	var calls atomic.Int32
	s.ViberClient = &http.Client{Transport: viberTestRT(func(r *http.Request) (*http.Response, error) {
		calls.Add(1)
		close(entered)
		<-release
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":0,"message_token":"123"}`)), Request: r}, nil
	})}
	payload := map[string]any{"request_id": "30000000-0000-4000-8000-000000000001", "expected_revision": v.Revision}
	first := make(chan *httptest.ResponseRecorder, 1)
	go func() { first <- viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/test", payload) }()
	<-entered
	second := viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/test", payload)
	if second.Code != 200 || !strings.Contains(second.Body.String(), `"state":"sending"`) || calls.Load() != 1 {
		t.Fatal("duplicate request started another POST")
	}
	close(release)
	firstResult := <-first
	if firstResult.Code != 200 || !strings.Contains(firstResult.Body.String(), `"state":"sent"`) || calls.Load() != 1 {
		t.Fatal("first request failed")
	}
}

type viberGuardFailAfterPost struct{ calls atomic.Int32 }

func (g *viberGuardFailAfterPost) Check(context.Context) error {
	if g.calls.Add(1) == 3 {
		return errors.New("fixture lost ownership")
	}
	return nil
}
func TestViberManualTestGuardLossAndExpiredLease(t *testing.T) {
	s, _ := viberBackendFixture(t)
	ctx := context.Background()
	now := time.Now().UTC()
	v, err := (appsettings.Store{Pool: s.Pool}).UpdateViber(ctx, appsettings.ViberUpdate{ChannelID: "channel", ChannelName: "Fixture", SenderID: "sender", CredentialRevision: 1, VerifiedCredentialRevision: 1, WebhookReady: true, VerifiedAt: &now})
	if err != nil {
		t.Fatal(err)
	}
	guard := &viberGuardFailAfterPost{}
	s.Guard = guard
	calls := 0
	s.ViberClient = &http.Client{Transport: viberTestRT(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"status":0,"message_token":"123"}`)), Request: r}, nil
	})}
	const id = "40000000-0000-4000-8000-000000000001"
	got := viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/test", map[string]any{"request_id": id, "expected_revision": v.Revision})
	if got.Code != 200 || !strings.Contains(got.Body.String(), `"state":"unknown"`) || calls != 1 {
		t.Fatal("guard loss not marked unknown", got.Code, got.Body.String())
	}
	got = viberBackendRequest(t, s, "POST", "/internal/web/v1/viber/test", map[string]any{"request_id": id, "expected_revision": v.Revision})
	if got.Code != 200 || calls != 1 {
		t.Fatal("guard-loss receipt resent")
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE viber_channel_leases SET next_post_at=now()-interval '1 second'`); err != nil {
		t.Fatal(err)
	}
	store := viberdelivery.TestStore{Pool: s.Pool}
	const id2 = "40000000-0000-4000-8000-000000000002"
	_, send, err := store.Begin(ctx, id2, v.Revision, 1)
	if err != nil || !send {
		t.Fatal("reserve test fixture", err)
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE viber_channel_leases SET lease_until=now()-interval '1 second' WHERE test_request_id=$1`, id2); err != nil {
		t.Fatal(err)
	}
	if _, err = store.Check(ctx, id2); err == nil {
		t.Fatal("expired test lease authorized network send")
	}
	// A process restart cannot establish the POST outcome once its lease expired.
	recovered, err := store.Last(ctx, "channel")
	if err != nil || recovered.RequestID != id2 || recovered.State != "unknown" {
		t.Fatal("expired receipt not reconciled", err)
	}
	recovered, send, err = store.Begin(ctx, id2, v.Revision, 1)
	if err != nil || send || recovered.State != "unknown" {
		t.Fatal("expired receipt retried", err)
	}
	if _, err = s.Pool.Exec(ctx, `UPDATE viber_channel_leases SET test_request_id=NULL,lease_until=NULL,next_post_at=now()-interval '1 second' WHERE channel_id='channel'`); err != nil {
		t.Fatal(err)
	}
	const id3 = "40000000-0000-4000-8000-000000000003"
	_, send, err = store.Begin(ctx, id3, v.Revision, 1)
	if err != nil || !send {
		t.Fatal("second lease fixture", err)
	}
	const replacement = "40000000-0000-4000-8000-000000000004"
	if _, err = s.Pool.Exec(ctx, `INSERT INTO viber_test_receipts(request_id,channel_id,sender_id,settings_revision,credential_revision,state) VALUES($1,'channel','sender',$2,1,'sending'); UPDATE viber_channel_leases SET test_request_id=$1,lease_until=now()+interval '1 minute' WHERE channel_id='channel'`, pgx.QueryExecModeSimpleProtocol, replacement, v.Revision); err != nil {
		t.Fatal(err)
	}
	recovered, err = store.Get(ctx, id3)
	if err != nil || recovered.State != "unknown" {
		t.Fatal("reassigned receipt not reconciled", err)
	}
	recovered, send, err = store.Begin(ctx, id3, v.Revision, 1)
	if err != nil || send || recovered.State != "unknown" {
		t.Fatal("reassigned receipt retried", err)
	}
}
