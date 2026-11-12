package apnspush

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func pushTestDB(t *testing.T) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("isolated PostgreSQL required")
	}
	u, err := url.Parse(dsn)
	if err != nil || (u.Hostname() != "localhost" && u.Hostname() != "127.0.0.1") {
		t.Fatal("TEST_DATABASE_URL must be loopback")
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	schema := fmt.Sprintf("push_test_%d", time.Now().UnixNano())
	admin, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(context.Background(), "CREATE SCHEMA "+pgx.Identifier{schema}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		pool.Close()
		_, _ = admin.Exec(context.Background(), "DROP SCHEMA "+pgx.Identifier{schema}.Sanitize()+" CASCADE")
		admin.Close()
	})
	res, err := migration.Run(context.Background(), pool, true, false)
	if err != nil || len(res.Errors) != 0 {
		t.Fatalf("migration: %v %v", err, res.Errors)
	}
	return pool
}

func randomCapability(t *testing.T) string {
	t.Helper()
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func TestPostgresChallengeExpiryMatchesPersistedPrecision(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	clock := time.Now().UTC().Truncate(time.Second).Add(123456789 * time.Nanosecond)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key, Now: func() time.Time { return clock }}
	in := Registration{InstallationID: "c1234567-1234-4234-8234-123456789abc", DeviceToken: "0123456789abcdef", Locale: "ru", Environment: "sandbox"}
	capability := randomCapability(t)
	created, status, err := service.Register(ctx, in, capability, "192.0.2.42")
	if err != nil || status != 202 {
		t.Fatalf("registration: %d %v", status, err)
	}
	checkExpiry := func(got Status) {
		t.Helper()
		var installationExpiry, challengeExpiry time.Time
		if err := pool.QueryRow(ctx, `SELECT i.challenge_expires_at,c.expires_at FROM push_installations i JOIN push_challenges c USING(installation_id,subscription_version) WHERE i.installation_id=$1`, in.InstallationID).Scan(&installationExpiry, &challengeExpiry); err != nil {
			t.Fatal(err)
		}
		if got.ExpiresAt == nil || !got.ExpiresAt.Equal(installationExpiry) || !got.ExpiresAt.Equal(challengeExpiry) {
			t.Fatalf("response expiry differs from persisted proof expiry: response=%v installation=%v challenge=%v", got.ExpiresAt, installationExpiry, challengeExpiry)
		}
	}
	checkExpiry(created)
	in.DeviceToken = "1123456789abcdef"
	updated, err := service.Update(ctx, in.InstallationID, in, capability, "192.0.2.42", 1)
	if err != nil || updated.SubscriptionVersion != 2 {
		t.Fatalf("rotation: %+v %v", updated, err)
	}
	checkExpiry(updated)
}

func TestPostgresRegistrationRateLimitAndRotation(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: append([]byte(nil), key...)}
	id := "a1234567-1234-4234-8234-123456789abc"
	capability := randomCapability(t)
	deviceToken := hex.EncodeToString(make([]byte, 32))
	in := Registration{InstallationID: id, DeviceToken: deviceToken, Locale: "ru", Environment: "sandbox"}
	first, status, err := service.Register(ctx, in, capability, "192.0.2.10")
	if err != nil || status != 202 || first.State != "pending" || first.ExpiresAt == nil {
		t.Fatalf("register: %+v %d %v", first, status, err)
	}
	dup, status, err := service.Register(ctx, in, capability, "192.0.2.10")
	if err != nil || status != 200 || dup.ExpiresAt == nil || !dup.ExpiresAt.Equal(*first.ExpiresAt) {
		t.Fatalf("pending idempotency extended expiry: %+v %+v %d %v", first, dup, status, err)
	}
	var ciphertext, nonce []byte
	var envelopeVersion int
	var keyID string
	if err = pool.QueryRow(ctx, `SELECT token_envelope_version,token_key_id,token_nonce,token_ciphertext FROM push_installations WHERE installation_id=$1`, id).Scan(&envelopeVersion, &keyID, &nonce, &ciphertext); err != nil {
		t.Fatal(err)
	}
	plain, err := credentialstore.Open(key, keyID, "domain-fixture", aadProvider(id, 1), credentialstore.Envelope{Version: envelopeVersion, KeyID: keyID, Nonce: nonce, Ciphertext: ciphertext})
	if err != nil || hex.EncodeToString(plain) != deviceToken {
		t.Fatalf("token envelope did not round trip: %v", err)
	}
	for i := 0; i < 11; i++ {
		_, _, _ = service.Register(ctx, in, randomCapability(t), "192.0.2.10")
	}
	if _, _, err = service.Register(ctx, in, randomCapability(t), "192.0.2.10"); err != ErrRateLimit {
		t.Fatalf("wrong capability attempts not durably throttled: %v", err)
	}
	var nonceCipher, nonceIV []byte
	var nonceVer int
	var nonceKeyID string
	if err = pool.QueryRow(ctx, `SELECT nonce_envelope_version,nonce_key_id,nonce_nonce,nonce_ciphertext FROM push_challenges WHERE installation_id=$1 AND subscription_version=1`, id).Scan(&nonceVer, &nonceKeyID, &nonceIV, &nonceCipher); err != nil {
		t.Fatal(err)
	}
	noncePlain, err := credentialstore.Open(key, nonceKeyID, "domain-fixture", "apns-nonce:"+id+":1", credentialstore.Envelope{Version: nonceVer, KeyID: nonceKeyID, Nonce: nonceIV, Ciphertext: nonceCipher})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE push_challenges SET state='accepted' WHERE installation_id=$1 AND subscription_version=1`, id); err != nil {
		t.Fatal(err)
	}
	if _, err = service.Activate(ctx, id, capability, "192.0.2.11", string(noncePlain), 1); err != ErrRateLimit {
		t.Fatalf("activation attempts should share persisted installation quota by now: %v", err)
	}
	// Use a fresh installation to exercise successful confirmation and version rotation.
	id2 := "b1234567-1234-4234-8234-123456789abc"
	cap2 := randomCapability(t)
	in2 := Registration{InstallationID: id2, DeviceToken: hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef")), Locale: "ru", Environment: "sandbox"}
	registered, status, err := service.Register(ctx, in2, cap2, "192.0.2.20")
	if err != nil || status != 202 {
		t.Fatalf("second registration: %+v %d %v", registered, status, err)
	}
	var nver int
	var nkid string
	var niv, ncipher []byte
	if err = pool.QueryRow(ctx, `SELECT nonce_envelope_version,nonce_key_id,nonce_nonce,nonce_ciphertext FROM push_challenges WHERE installation_id=$1 AND subscription_version=1`, id2).Scan(&nver, &nkid, &niv, &ncipher); err != nil {
		t.Fatal(err)
	}
	nonce, err = credentialstore.Open(key, nkid, "domain-fixture", "apns-nonce:"+id2+":1", credentialstore.Envelope{Version: nver, KeyID: nkid, Nonce: niv, Ciphertext: ncipher})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE push_challenges SET state='accepted',attempts=1 WHERE installation_id=$1`, id2); err != nil {
		t.Fatal(err)
	}
	active, err := service.Activate(ctx, id2, cap2, "192.0.2.21", string(nonce), 1)
	if err != nil || active.State != "active" {
		t.Fatalf("activation: %+v %v", active, err)
	}
	in2.Locale = "sr-Latn"
	rotated, err := service.Update(ctx, id2, in2, cap2, "192.0.2.21", 1)
	if err != nil || rotated.SubscriptionVersion != 2 || rotated.State != "active" {
		t.Fatalf("locale rotation: %+v %v", rotated, err)
	}
	const editionID = "22222222-2222-4222-8222-222222222222"
	insertPushFixtureEdition(t, pool, editionID, "sr-Latn")
	if _, err = pool.Exec(ctx, `UPDATE push_cutover SET enabled=true,sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_readiness_events(edition_id,edition_date,locale,ready_at) SELECT id,edition_date,locale,now() FROM editorial_digest_editions WHERE id=$1`, editionID); err != nil {
		t.Fatal(err)
	}
	accepted := false
	sender := &Sender{Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("apns-topic") != Topic || !strings.Contains(r.URL.Path, hex.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))) {
			t.Fatal("locale-rotated delivery did not decrypt and route the active token")
		}
		accepted = true
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}, Credential: Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890"}, Key: func() *ecdsa.PrivateKey { k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader); return k }()}
	if err = service.Process(ctx, sender, 10); err != nil {
		t.Fatalf("process locale event: %v", err)
	}
	if !accepted {
		t.Fatal("locale-rotated digest was not sent")
	}
	var deliveryState string
	if err = pool.QueryRow(ctx, `SELECT state FROM push_deliveries WHERE installation_id=$1 AND edition_id=$2`, id2, editionID).Scan(&deliveryState); err != nil || deliveryState != "accepted" {
		t.Fatalf("locale-rotated receipt=%q err=%v", deliveryState, err)
	}
	in2.DeviceToken = hex.EncodeToString([]byte("abcdef0123456789abcdef0123456789"))
	rotated, err = service.Update(ctx, id2, in2, cap2, "192.0.2.22", 2)
	if err != nil || rotated.SubscriptionVersion != 3 || rotated.State != "pending" {
		t.Fatalf("token rotation: %+v %v", rotated, err)
	}
	var oldChallengeState string
	if err = pool.QueryRow(ctx, `SELECT state FROM push_challenges WHERE installation_id=$1 AND subscription_version=1`, id2).Scan(&oldChallengeState); err != nil || oldChallengeState != "confirmed" {
		t.Fatalf("old confirmed challenge changed: %q %v", oldChallengeState, err)
	}
}

func insertPushFixtureEdition(t *testing.T, pool *pgxpool.Pool, id, locale string) {
	t.Helper()
	hash := strings.Repeat("c", 64)
	edition, _ := json.Marshal(map[string]any{"version": 1, "locale": locale, "source_snapshot_hash": hash, "stories": []any{}})
	snapshot, _ := json.Marshal(map[string]any{"articles": []any{map[string]any{"id": 1}}})
	rendered, _ := json.Marshal(map[string]any{"rich_html": "<p>fixture</p>", "plain_parts": []any{}, "preview_text": "fixture"})
	if _, err := pool.Exec(context.Background(), `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,provider_usage) VALUES($1,CURRENT_DATE,$2,'fixture',$3,$4,$5,$6,'{}')`, id, locale, hash, snapshot, edition, rendered); err != nil {
		t.Fatal(err)
	}
}

func TestPostgresCanaryIsPrivateExactAndIdempotent(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: append([]byte(nil), key...)}
	id := "c1234567-1234-4234-8234-123456789abc"
	editionID := "d1234567-1234-4234-8234-123456789abc"
	capability := randomCapability(t)
	token := []byte("0123456789abcdef0123456789abcdef")
	th := sha256.Sum256(token)
	env, err := credentialstore.Seal(key, "fixture-key", "domain-fixture", aadProvider(id, 1), token)
	if err != nil {
		t.Fatal(err)
	}
	capHash, _ := capabilityHash(capability)
	_, err = pool.Exec(ctx, `INSERT INTO push_installations(installation_id,capability_hash,token_hash,token_envelope_version,token_key_id,token_nonce,token_ciphertext,locale,environment,state,subscription_version,activated_at,version_changed_at,last_seen_at) VALUES($1,$2,$3,$4,$5,$6,$7,'ru','sandbox','active',1,now()-interval '1 minute',now()-interval '1 minute',now())`, id, capHash, th[:], env.Version, env.KeyID, env.Nonce, env.Ciphertext)
	if err != nil {
		t.Fatal(err)
	}
	hash := strings.Repeat("a", 64)
	edition, _ := json.Marshal(map[string]any{"version": 1, "locale": "ru", "source_snapshot_hash": hash, "stories": []any{}})
	snapshot, _ := json.Marshal(map[string]any{"articles": []any{map[string]any{"id": 1}}})
	rendered, _ := json.Marshal(map[string]any{"rich_html": "<p>fixture</p>", "plain_parts": []any{}, "preview_text": "fixture"})
	_, err = pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,provider_usage) VALUES($1,CURRENT_DATE,'ru','fixture',$2,$3,$4,$5,'{}')`, editionID, hash, snapshot, edition, rendered)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE push_cutover SET enabled=false,sender_enabled=false WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	var historicalEvents int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM push_readiness_events`).Scan(&historicalEvents); err != nil || historicalEvents != 0 {
		t.Fatalf("enabling cutover backfilled a historical edition: %d %v", historicalEvents, err)
	}
	input := CanaryRequest{InstallationID: id, EditionID: editionID, IdempotencyKey: "e1234567-1234-4234-8234-123456789abc"}
	if _, status, canaryErr := service.Canary(ctx, input, "192.0.2.30"); canaryErr != ErrUnavailable || status != 0 {
		t.Fatalf("canary must require sender enabled: status=%d err=%v", status, canaryErr)
	}
	if _, err = pool.Exec(ctx, `UPDATE push_cutover SET sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	first, status, err := service.Canary(ctx, input, "192.0.2.30")
	if err != nil || status != 202 || first.DeliveryID == "" {
		t.Fatalf("canary: %+v %d %v", first, status, err)
	}
	duplicate, status, err := service.Canary(ctx, input, "192.0.2.30")
	if err != nil || status != 200 || duplicate.DeliveryID != first.DeliveryID {
		t.Fatalf("idempotent canary: %+v %d %v", duplicate, status, err)
	}
	if err = service.fanout(ctx, 1); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM push_readiness_events`).Scan(&historicalEvents); err != nil || historicalEvents != 0 {
		t.Fatalf("canary before readiness cutover created historical fanout: %d %v", historicalEvents, err)
	}
	var eventID *int64
	var kind, state string
	if err = pool.QueryRow(ctx, `SELECT event_id,kind,state FROM push_deliveries WHERE id=$1`, first.DeliveryID).Scan(&eventID, &kind, &state); err != nil || eventID != nil || kind != "canary" || state != "pending" {
		t.Fatalf("canary delivery unexpectedly entered digest fanout: %v %q %q %v", eventID, kind, state, err)
	}
	input.EditionID = "f1234567-1234-4234-8234-123456789abc"
	if _, status, err = service.Canary(ctx, input, "192.0.2.30"); err != ErrConflict || status != 409 {
		t.Fatalf("idempotency key reuse mismatch: %d %v", status, err)
	}
}

func TestPostgresFanoutPagesAndDeduplicatesSharedTokens(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	const editionID = "11111111-1111-4111-8111-111111111111"
	hash := strings.Repeat("b", 64)
	edition, _ := json.Marshal(map[string]any{"version": 1, "locale": "ru", "source_snapshot_hash": hash, "stories": []any{}})
	snapshot, _ := json.Marshal(map[string]any{"articles": []any{map[string]any{"id": 1}}})
	rendered, _ := json.Marshal(map[string]any{"rich_html": "<p>x</p>", "plain_parts": []any{}, "preview_text": "x"})
	if _, err := pool.Exec(ctx, `INSERT INTO editorial_digest_editions(id,edition_date,locale,policy_version,snapshot_hash,snapshot,edition,rendered,provider_usage) VALUES($1,CURRENT_DATE,'ru','fixture',$2,$3,$4,$5,'{}')`, editionID, hash, snapshot, edition, rendered); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE push_cutover SET enabled=true,sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	shared := sha256.Sum256([]byte("same-token"))
	for i := 0; i < 205; i++ {
		id := fmt.Sprintf("%08x-1234-4234-8234-%012x", i+1, i+1)
		capHash := sha256.Sum256([]byte("cap-" + id))
		tokenHash := sha256.Sum256([]byte("token-" + id))
		if i == 0 || i == 1 {
			tokenHash = shared
		}
		if _, err := pool.Exec(ctx, `INSERT INTO push_installations(installation_id,capability_hash,token_hash,locale,environment,state,subscription_version,activated_at,version_changed_at,last_seen_at) VALUES($1,$2,$3,'ru','sandbox','active',1,now()-interval '1 minute',now()-interval '1 minute',now())`, id, capHash[:], tokenHash[:]); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO push_readiness_events(edition_id,edition_date,locale,ready_at) VALUES($1,CURRENT_DATE,'ru',now())`, editionID); err != nil {
		t.Fatal(err)
	}
	for page := 1; page <= 3; page++ {
		if err := service.fanoutBatch(ctx); err != nil {
			t.Fatalf("page %d: %v", page, err)
		}
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_deliveries WHERE kind='digest'`).Scan(&n); err != nil {
			t.Fatal(err)
		}
		want := page*100 - 1
		if want > 204 {
			want = 204
		}
		if n != want {
			t.Fatalf("page %d inserted %d, want %d", page, n, want)
		}
	}
	var complete bool
	if err := pool.QueryRow(ctx, `SELECT fanout_completed_at IS NOT NULL FROM push_readiness_events`).Scan(&complete); err != nil || !complete {
		t.Fatalf("fanout incomplete: %v %v", complete, err)
	}
}

func TestPostgresAPNsOutcomesAndRetryIdentity(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	const editionID = "33333333-3333-4333-8333-333333333333"
	insertPushFixtureEdition(t, pool, editionID, "ru")
	if _, err := pool.Exec(ctx, `UPDATE push_cutover SET enabled=true,sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	statuses := map[string]string{"01": "200", "02": "400", "03": "410", "04": "429", "05": "timeout", "06": "500"}
	byPath := map[string]string{}
	for suffix, outcome := range statuses {
		id := fmt.Sprintf("%08x-1234-4234-8234-%012x", 700+len(byPath), 700+len(byPath))
		token := []byte("0123456789abcdef0123456789abcd" + suffix)
		th := sha256.Sum256(token)
		capHash := sha256.Sum256([]byte("cap" + id))
		env, err := credentialstore.Seal(key, "fixture-key", "domain-fixture", aadProvider(id, 1), token)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO push_installations(installation_id,capability_hash,token_hash,token_envelope_version,token_key_id,token_nonce,token_ciphertext,locale,environment,state,subscription_version,activated_at,version_changed_at,last_seen_at) VALUES($1,$2,$3,$4,$5,$6,$7,'ru','sandbox','active',1,now()-interval '1 minute',now()-interval '1 minute',now())`, id, capHash[:], th[:], env.Version, env.KeyID, env.Nonce, env.Ciphertext); err != nil {
			t.Fatal(err)
		}
		byPath[hex.EncodeToString(token)] = outcome
	}
	if _, err := pool.Exec(ctx, `INSERT INTO push_readiness_events(edition_id,edition_date,locale,ready_at) SELECT id,edition_date,locale,now() FROM editorial_digest_editions WHERE id=$1`, editionID); err != nil {
		t.Fatal(err)
	}
	requestIDs := map[string]string{}
	mode := "initial"
	sender := &Sender{Credential: Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890"}, Key: func() *ecdsa.PrivateKey { k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader); return k }(), Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		token := strings.TrimPrefix(r.URL.Path, "/3/device/")
		outcome := byPath[token]
		old := requestIDs[token]
		current := r.Header.Get("apns-id")
		if old != "" && old != current {
			t.Fatal("retry changed stable APNs id")
		}
		requestIDs[token] = current
		if outcome == "timeout" {
			return nil, fmt.Errorf("fixture timeout")
		}
		code, _ := strconv.Atoi(outcome)
		if mode == "retry" && outcome == "429" {
			code = 200
		}
		return &http.Response{StatusCode: code, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}}
	if err := service.Process(ctx, sender, 10); err != nil {
		t.Fatal(err)
	}
	var accepted, rejected, unknown, retryWait int
	if err := pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE state='accepted'),count(*) FILTER(WHERE state='rejected'),count(*) FILTER(WHERE state='unknown'),count(*) FILTER(WHERE state='retry_wait') FROM push_deliveries`).Scan(&accepted, &rejected, &unknown, &retryWait); err != nil {
		t.Fatal(err)
	}
	if accepted != 1 || rejected != 2 || unknown != 1 || retryWait != 2 {
		t.Fatalf("outcome counts accepted=%d rejected=%d unknown=%d retry=%d", accepted, rejected, unknown, retryWait)
	}
	for attempt := 0; attempt < 4; attempt++ {
		if _, err := pool.Exec(ctx, `UPDATE push_deliveries SET next_attempt_at=now()-interval '1 second' WHERE state='retry_wait' AND status_code=500`); err != nil {
			t.Fatal(err)
		}
		if err := service.Process(ctx, sender, 10); err != nil {
			t.Fatal(err)
		}
	}
	var exhausted string
	if err := pool.QueryRow(ctx, `SELECT state FROM push_deliveries WHERE status_code=500 ORDER BY attempts DESC LIMIT 1`).Scan(&exhausted); err != nil || exhausted != "rejected" {
		t.Fatalf("5xx attempt budget not exhausted: %q %v", exhausted, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE push_deliveries SET next_attempt_at=now()-interval '1 second' WHERE state='retry_wait'`); err != nil {
		t.Fatal(err)
	}
	mode = "retry"
	if err := service.Process(ctx, sender, 10); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_deliveries WHERE state='accepted'`).Scan(&accepted); err != nil || accepted != 2 {
		t.Fatalf("explicit retry acceptance count=%d err=%v", accepted, err)
	}
	var invalid int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_installations WHERE state='invalid'`).Scan(&invalid); err != nil || invalid != 1 {
		t.Fatalf("410 invalidation count=%d err=%v", invalid, err)
	}
	if _, err := pool.Exec(ctx, `WITH one AS (SELECT id FROM push_deliveries WHERE state='accepted' ORDER BY id LIMIT 1) UPDATE push_deliveries d SET state='sending',lease_id=gen_random_uuid(),lease_until=now()-interval '1 second' FROM one WHERE d.id=one.id`); err != nil {
		t.Fatal(err)
	}
	if err := service.expireSending(ctx, 10); err != nil {
		t.Fatal(err)
	}
	var staleState string
	if err := pool.QueryRow(ctx, `SELECT state FROM push_deliveries WHERE lease_until IS NULL AND state='unknown' ORDER BY updated_at DESC LIMIT 1`).Scan(&staleState); err != nil || staleState != "unknown" {
		t.Fatalf("expired lease not marked unknown: %q %v", staleState, err)
	}
	if _, ok, err := service.claim(ctx); err != nil || ok {
		t.Fatalf("expired ambiguous send was replayed: claimed=%v err=%v", ok, err)
	}
}

func TestPostgresLateAcceptedResponseFinishesOldLeaseOnly(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	id := "44444444-1234-4234-8234-123456789abc"
	editionID := "55555555-1234-4234-8234-123456789abc"
	cap := randomCapability(t)
	token := []byte("0123456789abcdef0123456789abcdef")
	th := sha256.Sum256(token)
	ch, _ := capabilityHash(cap)
	env, err := credentialstore.Seal(key, "fixture-key", "domain-fixture", aadProvider(id, 1), token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_installations(installation_id,capability_hash,token_hash,token_envelope_version,token_key_id,token_nonce,token_ciphertext,locale,environment,state,subscription_version,activated_at,version_changed_at,last_seen_at) VALUES($1,$2,$3,$4,$5,$6,$7,'ru','sandbox','active',1,now()-interval '1 minute',now()-interval '1 minute',now())`, id, ch, th[:], env.Version, env.KeyID, env.Nonce, env.Ciphertext); err != nil {
		t.Fatal(err)
	}
	insertPushFixtureEdition(t, pool, editionID, "ru")
	if _, err = pool.Exec(ctx, `UPDATE push_cutover SET enabled=true,sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_readiness_events(edition_id,edition_date,locale,ready_at) SELECT id,edition_date,locale,now() FROM editorial_digest_editions WHERE id=$1`, editionID); err != nil {
		t.Fatal(err)
	}
	keyPair, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	sender := &Sender{Credential: Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890"}, Key: keyPair, Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		rotated, updateErr := service.Update(ctx, id, Registration{InstallationID: id, DeviceToken: hex.EncodeToString([]byte("abcdef0123456789abcdef0123456789")), Locale: "ru", Environment: "sandbox"}, cap, "192.0.2.42", 1)
		if updateErr != nil || rotated.State != "pending" || rotated.SubscriptionVersion != 2 {
			t.Fatalf("rotation during APNs request: %+v %v", rotated, updateErr)
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}}
	if err = service.Process(ctx, sender, 10); err != nil {
		t.Fatal(err)
	}
	var state string
	var version int64
	if err = pool.QueryRow(ctx, `SELECT state,subscription_version FROM push_installations WHERE installation_id=$1`, id).Scan(&state, &version); err != nil || state != "pending" || version != 2 {
		t.Fatalf("late success changed current registration: %q v%d %v", state, version, err)
	}
	var deliveryState string
	if err = pool.QueryRow(ctx, `SELECT state FROM push_deliveries WHERE installation_id=$1`, id).Scan(&deliveryState); err != nil || deliveryState != "accepted" {
		t.Fatalf("late response did not finish its own receipt: %q %v", deliveryState, err)
	}
}

func TestPostgresStaleReadinessAndDeliveryNeverSend(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	const editionID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
	const installationID = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
	insertPushFixtureEdition(t, pool, editionID, "ru")
	capHash := sha256.Sum256([]byte("cap"))
	token := []byte("0123456789abcdef0123456789abcdef")
	tokenHash := sha256.Sum256(token)
	env, err := credentialstore.Seal(key, "fixture-key", "domain-fixture", aadProvider(installationID, 1), token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_installations(installation_id,capability_hash,token_hash,token_envelope_version,token_key_id,token_nonce,token_ciphertext,locale,environment,state,subscription_version,activated_at,version_changed_at,last_seen_at) VALUES($1,$2,$3,$4,$5,$6,$7,'ru','sandbox','active',1,now()-interval '30 hours',now()-interval '30 hours',now())`, installationID, capHash[:], tokenHash[:], env.Version, env.KeyID, env.Nonce, env.Ciphertext); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE push_cutover SET enabled=true,sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_readiness_events(edition_id,edition_date,locale,ready_at) SELECT id,edition_date,locale,now()-interval '25 hours' FROM editorial_digest_editions WHERE id=$1`, editionID); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_deliveries(event_id,installation_id,subscription_version,edition_id,edition_date,locale,token_hash,environment,kind,state,expires_at,apns_id) SELECT e.id,$1,1,e.edition_id,e.edition_date,'ru',$2,'sandbox','digest','pending',now()-interval '1 hour',gen_random_uuid() FROM push_readiness_events e WHERE e.edition_id=$3`, installationID, tokenHash[:], editionID); err != nil {
		t.Fatal(err)
	}
	keyPair, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	calls := 0
	sender := &Sender{Credential: Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890"}, Key: keyPair, Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		calls++
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}}
	if err = service.Process(ctx, sender, 10); err != nil {
		t.Fatal(err)
	}
	var eventComplete bool
	var state string
	if err = pool.QueryRow(ctx, `SELECT fanout_completed_at IS NOT NULL FROM push_readiness_events WHERE edition_id=$1`, editionID).Scan(&eventComplete); err != nil || !eventComplete {
		t.Fatalf("stale readiness not retired: %v %v", eventComplete, err)
	}
	if err = pool.QueryRow(ctx, `SELECT state FROM push_deliveries WHERE installation_id=$1`, installationID).Scan(&state); err != nil || state != "expired" {
		t.Fatalf("stale pending delivery state=%q err=%v", state, err)
	}
	if calls != 0 {
		t.Fatalf("stale work reached APNs: requests=%d", calls)
	}
}

func TestPostgresRetentionAndExpiredChallengeCleanup(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	unknownID := "acacacac-1234-4234-8234-123456789abc"
	_, _ = insertPendingChallenge(t, pool, service, unknownID, 1, 1, "unknown")
	if _, err := pool.Exec(ctx, `UPDATE push_installations SET state='active',activated_at=now(),challenge_hash=NULL,challenge_expires_at=NULL,last_seen_at=now() WHERE installation_id=$1; UPDATE push_challenges SET expires_at=now()-interval '11 minutes',updated_at=now() WHERE installation_id=$1`, pgx.QueryExecModeSimpleProtocol, unknownID); err != nil {
		t.Fatal(err)
	}
	const editionID = "66666666-1234-4234-8234-123456789abc"
	insertPushFixtureEdition(t, pool, editionID, "ru")
	oldTomb := "77777777-1234-4234-8234-123456789abc"
	expired := "88888888-1234-4234-8234-123456789abc"
	inactive := "99999999-1234-4234-8234-123456789abc"
	for _, id := range []string{oldTomb, expired, inactive} {
		capHash := sha256.Sum256([]byte("cap" + id))
		tokenHash := sha256.Sum256([]byte("token" + id))
		state := "disabled"
		seen := "now()-interval '31 days'"
		if id == expired {
			state = "pending"
			seen = "now()"
		}
		if id == inactive {
			state = "active"
			seen = "now()-interval '181 days'"
		}
		query := `INSERT INTO push_installations(installation_id,capability_hash,token_hash,locale,environment,state,subscription_version,challenge_hash,challenge_expires_at,last_seen_at,updated_at) VALUES($1,$2,$3,'ru','sandbox',$4,1,$5,$6,` + seen + `,` + seen + `)`
		var challengeHash, challengeExpiry any
		if id == expired {
			h := sha256.Sum256([]byte("nonce"))
			challengeHash = h[:]
			challengeExpiry = time.Now().Add(-time.Minute)
		}
		if _, err := pool.Exec(ctx, query, id, capHash[:], tokenHash[:], state, challengeHash, challengeExpiry); err != nil {
			t.Fatal(err)
		}
	}
	oldHash := sha256.Sum256([]byte("old-nonce"))
	if _, err := pool.Exec(ctx, `INSERT INTO push_challenges(installation_id,subscription_version,nonce_hash,environment,state,apns_id,expires_at,updated_at) VALUES($1,1,$2,'sandbox','confirmed',gen_random_uuid(),now()-interval '31 days',now()-interval '31 days')`, oldTomb, oldHash[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO push_deliveries(installation_id,subscription_version,edition_id,edition_date,locale,token_hash,environment,kind,state,apns_id,created_at,updated_at) SELECT $1,1,id,edition_date,'ru',decode(repeat('11',32),'hex'),'sandbox','digest','accepted',gen_random_uuid(),now()-interval '31 days',now()-interval '31 days' FROM editorial_digest_editions WHERE id=$2`, oldTomb, editionID); err != nil {
		t.Fatal(err)
	}
	bucket := sha256.Sum256([]byte("expired-bucket"))
	if _, err := pool.Exec(ctx, `INSERT INTO push_rate_buckets(bucket_key,window_started_at,hits,expires_at) VALUES($1,now()-interval '3 hours',1,now()-interval '1 second')`, bucket[:]); err != nil {
		t.Fatal(err)
	}
	if err := service.housekeep(ctx, 50); err != nil {
		t.Fatal(err)
	}
	var oldCount, rateCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_installations WHERE installation_id=$1`, oldTomb).Scan(&oldCount); err != nil || oldCount != 0 {
		t.Fatalf("30-day tombstone retained: %d %v", oldCount, err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_rate_buckets`).Scan(&rateCount); err != nil || rateCount != 0 {
		t.Fatalf("expired rate bucket retained: %d %v", rateCount, err)
	}
	var expiredState, inactiveState string
	var ciphertext []byte
	if err := pool.QueryRow(ctx, `SELECT state,token_ciphertext FROM push_installations WHERE installation_id=$1`, expired).Scan(&expiredState, &ciphertext); err != nil || expiredState != "disabled" || ciphertext != nil {
		t.Fatalf("expired pending installation not erased: %q %v %v", expiredState, ciphertext, err)
	}
	if err := pool.QueryRow(ctx, `SELECT state FROM push_installations WHERE installation_id=$1`, inactive).Scan(&inactiveState); err != nil || inactiveState != "disabled" {
		t.Fatalf("inactive installation not disabled: %q %v", inactiveState, err)
	}
	var unknownState string
	var proofErased bool
	if err := pool.QueryRow(ctx, `SELECT state,nonce_hash IS NULL AND nonce_envelope_version IS NULL AND nonce_key_id IS NULL AND nonce_nonce IS NULL AND nonce_ciphertext IS NULL FROM push_challenges WHERE installation_id=$1`, unknownID).Scan(&unknownState, &proofErased); err != nil || unknownState != "unknown" || !proofErased {
		t.Fatalf("expired unknown challenge must retain receipt and erase proof: state=%q erased=%v err=%v", unknownState, proofErased, err)
	}
}

func insertPendingChallenge(t *testing.T, pool *pgxpool.Pool, service *Service, id string, version int64, attempts int, challengeState string) (string, string) {
	t.Helper()
	ctx := context.Background()
	capability := randomCapability(t)
	capHash, _ := capabilityHash(capability)
	token := []byte("0123456789abcdef0123456789" + id[:2])
	tokenHash := sha256.Sum256(token)
	var nonceBytes [32]byte
	_, _ = rand.Read(nonceBytes[:])
	nonce := base64.RawURLEncoding.EncodeToString(nonceBytes[:])
	nonceHash := sha256.Sum256([]byte(nonce))
	tokenEnvelope, err := credentialstore.Seal(service.Key, service.KeyID, service.InstallationID, aadProvider(id, version), token)
	if err != nil {
		t.Fatal(err)
	}
	nonceEnvelope, err := credentialstore.Seal(service.Key, service.KeyID, service.InstallationID, "apns-nonce:"+id+":"+itoa64(version), []byte(nonce))
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO push_installations(installation_id,capability_hash,token_hash,token_envelope_version,token_key_id,token_nonce,token_ciphertext,locale,environment,state,subscription_version,challenge_hash,challenge_expires_at,version_changed_at) VALUES($1,$2,$3,$4,$5,$6,$7,'ru','sandbox','pending',$8,$9,now()+interval '10 minutes',now())`, id, capHash, tokenHash[:], tokenEnvelope.Version, tokenEnvelope.KeyID, tokenEnvelope.Nonce, tokenEnvelope.Ciphertext, version, nonceHash[:])
	if err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO push_challenges(installation_id,subscription_version,nonce_hash,nonce_envelope_version,nonce_key_id,nonce_nonce,nonce_ciphertext,environment,state,apns_id,expires_at,attempts) VALUES($1,$2,$3,$4,$5,$6,$7,'sandbox',$8,gen_random_uuid(),now()+interval '10 minutes',$9)`, id, version, nonceHash[:], nonceEnvelope.Version, nonceEnvelope.KeyID, nonceEnvelope.Nonce, nonceEnvelope.Ciphertext, challengeState, attempts)
	if err != nil {
		t.Fatal(err)
	}
	return capability, nonce
}

func TestPostgresChallengeAttemptsUsePersistedCounterNotVersion(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	idV5 := "aaaaaaaa-1234-4234-8234-123456789abc"
	idFifth := "bbbbbbbb-1234-4234-8234-123456789abc"
	_, _ = insertPendingChallenge(t, pool, service, idV5, 5, 0, "pending")
	_, _ = insertPendingChallenge(t, pool, service, idFifth, 1, 4, "pending")
	if _, err := pool.Exec(ctx, `UPDATE push_cutover SET enabled=true,sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	keyPair, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	sender := &Sender{Credential: Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890"}, Key: keyPair, Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 429, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}}
	if err := service.Process(ctx, sender, 10); err != nil {
		t.Fatal(err)
	}
	var stateV5, stateFifth string
	var attemptsV5, attemptsFifth int
	if err := pool.QueryRow(ctx, `SELECT state,attempts FROM push_challenges WHERE installation_id=$1`, idV5).Scan(&stateV5, &attemptsV5); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT state,attempts FROM push_challenges WHERE installation_id=$1`, idFifth).Scan(&stateFifth, &attemptsFifth); err != nil {
		t.Fatal(err)
	}
	if stateV5 != "retry_wait" || attemptsV5 != 1 || stateFifth != "expired" || attemptsFifth != 5 {
		t.Fatalf("challenge retry state v5=%s/%d fifth=%s/%d", stateV5, attemptsV5, stateFifth, attemptsFifth)
	}
}

func TestPostgresUpdateTargetTokenRateLimitIsGlobal(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	ids := make([]string, 25)
	caps := make([]string, 25)
	for i := range ids {
		ids[i] = fmt.Sprintf("%08x-1234-4234-8234-%012x", 900+i, 900+i)
		caps[i] = randomCapability(t)
		reg := Registration{InstallationID: ids[i], DeviceToken: hex.EncodeToString([]byte(fmt.Sprintf("source-token-%020d", i))), Locale: "ru", Environment: "sandbox"}
		if _, _, err := service.Register(ctx, reg, caps[i], fmt.Sprintf("192.0.2.%d", i+1)); err != nil {
			t.Fatalf("register %d: %v", i, err)
		}
	}
	target := hex.EncodeToString([]byte("one-shared-target-token"))
	for i := range ids {
		reg := Registration{InstallationID: ids[i], DeviceToken: target, Locale: "ru", Environment: "sandbox"}
		_, err := service.Update(ctx, ids[i], reg, caps[i], fmt.Sprintf("198.51.100.%d", i+1), 1)
		if i < 24 && err != nil {
			t.Fatalf("shared token update %d: %v", i, err)
		}
		if i == 24 && err != ErrRateLimit {
			t.Fatalf("shared target token quota did not reject 25th ID: %v", err)
		}
	}
	var version int64
	if err := pool.QueryRow(ctx, `SELECT subscription_version FROM push_installations WHERE installation_id=$1`, ids[24]).Scan(&version); err != nil || version != 1 {
		t.Fatalf("rate-limited update mutated installation: v%d %v", version, err)
	}
}

func TestPostgresDeviceProofConfirmsSendingOrUnknownChallenge(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	idSending := "cccccccc-1234-4234-8234-123456789abc"
	idUnknown := "dddddddd-1234-4234-8234-123456789abc"
	capSending, nonceSending := insertPendingChallenge(t, pool, service, idSending, 1, 0, "pending")
	capUnknown, nonceUnknown := insertPendingChallenge(t, pool, service, idUnknown, 1, 0, "pending")
	if _, err := pool.Exec(ctx, `UPDATE push_cutover SET enabled=true,sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	keyPair, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	activatedSending := false
	tokenSending := hex.EncodeToString([]byte("0123456789abcdef0123456789" + idSending[:2]))
	sender := &Sender{Credential: Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890"}, Key: keyPair, Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		token := strings.TrimPrefix(r.URL.Path, "/3/device/")
		if token == tokenSending {
			_, err := service.Activate(ctx, idSending, capSending, "192.0.2.70", nonceSending, 1)
			if err != nil {
				t.Fatalf("device proof while sending: %v", err)
			}
			activatedSending = true
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
		}
		return nil, fmt.Errorf("synthetic lost APNs response")
	})}}
	if err := service.Process(ctx, sender, 10); err != nil {
		t.Fatal(err)
	}
	if !activatedSending {
		t.Fatal("did not exercise activation during in-flight APNs request")
	}
	active, err := service.Activate(ctx, idUnknown, capUnknown, "192.0.2.71", nonceUnknown, 1)
	if err != nil || active.State != "active" {
		t.Fatalf("device proof after unknown APNs outcome: %+v %v", active, err)
	}
	var sendingChallenge string
	if err = pool.QueryRow(ctx, `SELECT state FROM push_challenges WHERE installation_id=$1`, idSending).Scan(&sendingChallenge); err != nil || sendingChallenge != "confirmed" {
		t.Fatalf("late sender overwrote device confirmation: %q %v", sendingChallenge, err)
	}
}

func TestPostgresUnknownChallengeRequeuesAfterLeaseAndWorkerProgresses(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	service := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	challengeID := "eeeeeeee-1234-4234-8234-123456789abc"
	capability, _ := insertPendingChallenge(t, pool, service, challengeID, 1, 0, "pending")
	if _, err := pool.Exec(ctx, `UPDATE push_challenges SET state='sending',attempts=1,lease_until=now()-interval '1 second' WHERE installation_id=$1`, challengeID); err != nil {
		t.Fatal(err)
	}
	const editionID = "ffffffff-1234-4234-8234-123456789abc"
	const activeID = "abababab-1234-4234-8234-123456789abc"
	insertPushFixtureEdition(t, pool, editionID, "ru")
	activeCap := sha256.Sum256([]byte("active-cap"))
	activeToken := []byte("abcdef0123456789abcdef0123456789")
	activeHash := sha256.Sum256(activeToken)
	activeEnv, err := credentialstore.Seal(key, "fixture-key", "domain-fixture", aadProvider(activeID, 1), activeToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_installations(installation_id,capability_hash,token_hash,token_envelope_version,token_key_id,token_nonce,token_ciphertext,locale,environment,state,subscription_version,activated_at,version_changed_at,last_seen_at) VALUES($1,$2,$3,$4,$5,$6,$7,'ru','sandbox','active',1,now()-interval '1 minute',now()-interval '1 minute',now())`, activeID, activeCap[:], activeHash[:], activeEnv.Version, activeEnv.KeyID, activeEnv.Nonce, activeEnv.Ciphertext); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE push_cutover SET enabled=true,sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_readiness_events(edition_id,edition_date,locale,ready_at) SELECT id,edition_date,locale,now() FROM editorial_digest_editions WHERE id=$1`, editionID); err != nil {
		t.Fatal(err)
	}
	keyPair, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	var alertCalls, challengeCalls int
	sender := &Sender{Credential: Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890"}, Key: keyPair, Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		if r.Header.Get("apns-push-type") == "background" {
			challengeCalls++
		} else {
			alertCalls++
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}}
	if err = service.Process(ctx, sender, 10); err != nil {
		t.Fatal(err)
	}
	var challengeState, deliveryState string
	if err = pool.QueryRow(ctx, `SELECT state FROM push_challenges WHERE installation_id=$1`, challengeID).Scan(&challengeState); err != nil || challengeState != "unknown" {
		t.Fatalf("expired sending lease state=%q err=%v", challengeState, err)
	}
	if err = pool.QueryRow(ctx, `SELECT state FROM push_deliveries WHERE installation_id=$1`, activeID).Scan(&deliveryState); err != nil || deliveryState != "accepted" || alertCalls != 1 {
		t.Fatalf("other delivery did not progress: %q alerts=%d err=%v", deliveryState, alertCalls, err)
	}
	registration := Registration{InstallationID: challengeID, DeviceToken: hex.EncodeToString([]byte("0123456789abcdef0123456789" + challengeID[:2])), Locale: "ru", Environment: "sandbox"}
	status, code, err := service.Register(ctx, registration, capability, "192.0.2.91")
	if err != nil || code != 200 || status.State != "pending" {
		t.Fatalf("authorized challenge resend: %+v code=%d err=%v", status, code, err)
	}
	if err = service.Process(ctx, sender, 10); err != nil {
		t.Fatal(err)
	}
	if err = pool.QueryRow(ctx, `SELECT state FROM push_challenges WHERE installation_id=$1`, challengeID).Scan(&challengeState); err != nil || challengeState != "accepted" || challengeCalls != 1 {
		t.Fatalf("requeued challenge did not make progress: state=%q calls=%d err=%v", challengeState, challengeCalls, err)
	}
}
