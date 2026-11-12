package apnspush

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
)

func TestAdmissionCommitsLimitedIPWithoutDownstreamBuckets(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	const ip = "198.51.100.200"
	for i := 0; i < 30; i++ {
		if err := s.admit(ctx, ip); err != nil {
			t.Fatalf("IP admission %d: %v", i+1, err)
		}
	}
	downstream := digestKey("review-downstream", []byte("must-not-be-touched"))
	if err := s.admit(ctx, ip, rateLimit{downstream, 10, time.Hour}); !errors.Is(err, ErrRateLimit) {
		t.Fatalf("31st IP admission = %v, want rate limit", err)
	}
	ipKey := digestKey("ip", s.addressHash(ip))
	var ipHits int
	if err := pool.QueryRow(ctx, `SELECT hits FROM push_rate_buckets WHERE bucket_key=$1`, ipKey).Scan(&ipHits); err != nil || ipHits != 31 {
		t.Fatalf("IP hit was not committed: hits=%d err=%v", ipHits, err)
	}
	var downstreamExists bool
	if err := pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM push_rate_buckets WHERE bucket_key=$1)`, downstream).Scan(&downstreamExists); err != nil || downstreamExists {
		t.Fatalf("IP limit touched downstream bucket: exists=%v err=%v", downstreamExists, err)
	}
}

func TestCapabilityFormatValidationAndWrongCapabilityAdmission(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	in := Registration{InstallationID: "12121212-1234-4234-8234-123456789abc", DeviceToken: hex.EncodeToString([]byte("fixture-device-token")), Locale: "ru", Environment: "sandbox"}
	const ip = "198.51.100.201"
	if _, _, err := s.Register(ctx, in, "malformed", ip); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("malformed capability = %v", err)
	}
	var bucketCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM push_rate_buckets`).Scan(&bucketCount); err != nil || bucketCount != 0 {
		t.Fatalf("malformed capability reached admission: buckets=%d err=%v", bucketCount, err)
	}
	capability := randomCapability(t)
	if _, _, err := s.Register(ctx, in, capability, ip); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Register(ctx, in, randomCapability(t), ip); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("valid-format wrong capability = %v", err)
	}
	for _, key := range [][]byte{digestKey("ip", s.addressHash(ip)), digestKey("installation", []byte(in.InstallationID)), digestKey("token", sha256Bytes(in.DeviceToken))} {
		var hits int
		if err := pool.QueryRow(ctx, `SELECT hits FROM push_rate_buckets WHERE bucket_key=$1`, key).Scan(&hits); err != nil || hits != 2 {
			t.Fatalf("wrong capability did not consume all established quotas: hits=%d err=%v", hits, err)
		}
	}
}

func TestIPv6AddressHashAggregatesOnlyWithin64(t *testing.T) {
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s := &Service{IPHashKey: key}
	a := s.addressHash("2001:db8:1:2:1111:2222:3333:4444")
	b := s.addressHash("2001:db8:1:2:aaaa:bbbb:cccc:dddd")
	c := s.addressHash("2001:db8:1:3:1111:2222:3333:4444")
	if !bytesEqual(a, b) || bytesEqual(a, c) {
		t.Fatal("IPv6 rate address hashing must aggregate a /64 and distinguish adjacent /64s")
	}
	if bytesEqual(s.addressHash("198.51.100.1"), s.addressHash("198.51.100.2")) {
		t.Fatal("IPv4 rate addresses must remain individually keyed")
	}
}

func TestCanaryQuotaIsIndependentFromRegistrationQuota(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	id := "13131313-1234-4234-8234-123456789abc"
	capability := randomCapability(t)
	in := Registration{InstallationID: id, DeviceToken: hex.EncodeToString([]byte("fixture-device-token")), Locale: "ru", Environment: "sandbox"}
	if _, _, err := s.Register(ctx, in, capability, "198.51.100.202"); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE push_installations SET state='active',activated_at=now(),challenge_hash=NULL,challenge_expires_at=NULL WHERE installation_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	const editionID = "14141414-1234-4234-8234-123456789abc"
	insertPushFixtureEdition(t, pool, editionID, "ru")
	if _, err := pool.Exec(ctx, `UPDATE push_cutover SET sender_enabled=true WHERE id=1`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 4; i++ {
		if _, _, err := s.Register(ctx, in, capability, "198.51.100.202"); err != nil {
			t.Fatalf("normal registration request %d: %v", i+2, err)
		}
	}
	for i, key := range []string{
		"a1000001-1234-4234-8234-123456789abc",
		"a1000002-1234-4234-8234-123456789abc",
		"a1000003-1234-4234-8234-123456789abc",
		"a1000004-1234-4234-8234-123456789abc",
		"a1000005-1234-4234-8234-123456789abc",
	} {
		out, status, err := s.Canary(ctx, CanaryRequest{InstallationID: id, EditionID: editionID, IdempotencyKey: key}, "198.51.100.202")
		if err != nil || status != 202 || out.State != "pending" {
			t.Fatalf("canary %d after ordinary registration calls: %+v status=%d err=%v", i+1, out, status, err)
		}
	}
	_, status, err := s.Canary(ctx, CanaryRequest{InstallationID: id, EditionID: editionID, IdempotencyKey: "a1000006-1234-4234-8234-123456789abc"}, "198.51.100.202")
	if !errors.Is(err, ErrRateLimit) || status != 429 {
		t.Fatalf("6th canary = status %d, err %v", status, err)
	}
	var registrationHits, canaryHits int
	if err := pool.QueryRow(ctx, `SELECT hits FROM push_rate_buckets WHERE bucket_key=$1`, digestKey("installation", []byte(id))).Scan(&registrationHits); err != nil || registrationHits != 5 {
		t.Fatalf("normal installation bucket = %d, err %v", registrationHits, err)
	}
	if err := pool.QueryRow(ctx, `SELECT hits FROM push_rate_buckets WHERE bucket_key=$1`, digestKey("canary-installation", []byte(id))).Scan(&canaryHits); err != nil || canaryHits != 6 {
		t.Fatalf("canary bucket = %d, err %v", canaryHits, err)
	}
}

func TestPendingLocaleUpdateCancelsOldChallengeAndPendingProofActivates(t *testing.T) {
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	id := "15151515-1234-4234-8234-123456789abc"
	capability := randomCapability(t)
	in := Registration{InstallationID: id, DeviceToken: hex.EncodeToString([]byte("fixture-device-token")), Locale: "ru", Environment: "sandbox"}
	if _, _, err := s.Register(ctx, in, capability, "198.51.100.203"); err != nil {
		t.Fatal(err)
	}
	in.Locale = "sr-Latn"
	updated, err := s.Update(ctx, id, in, capability, "198.51.100.203", 1)
	if err != nil || updated.SubscriptionVersion != 2 || updated.State != "pending" {
		t.Fatalf("pending locale update: %+v %v", updated, err)
	}
	var oldState string
	var oldNonceHash []byte
	if err = pool.QueryRow(ctx, `SELECT state,nonce_hash FROM push_challenges WHERE installation_id=$1 AND subscription_version=1`, id).Scan(&oldState, &oldNonceHash); err != nil || oldState != "cancelled" || oldNonceHash != nil {
		t.Fatalf("old unsent challenge not cancelled: state=%q nonce=%x err=%v", oldState, oldNonceHash, err)
	}
	var nonceVersion int
	var nonceKeyID string
	var nonce, ciphertext []byte
	if err = pool.QueryRow(ctx, `SELECT nonce_envelope_version,nonce_key_id,nonce_nonce,nonce_ciphertext FROM push_challenges WHERE installation_id=$1 AND subscription_version=2`, id).Scan(&nonceVersion, &nonceKeyID, &nonce, &ciphertext); err != nil {
		t.Fatal(err)
	}
	plain, err := credentialstore.Open(key, nonceKeyID, s.InstallationID, "apns-nonce:"+id+":2", credentialstore.Envelope{Version: nonceVersion, KeyID: nonceKeyID, Nonce: nonce, Ciphertext: ciphertext})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `UPDATE push_challenges SET attempts=1 WHERE installation_id=$1 AND subscription_version=2`, id); err != nil {
		t.Fatal(err)
	}
	active, err := s.Activate(ctx, id, capability, "198.51.100.203", string(plain), 2)
	if err != nil || active.State != "active" {
		t.Fatalf("activation while issued challenge is requeued pending: %+v %v", active, err)
	}
}

func TestLifecycleSelectDistinguishesMissingRowsFromDatabaseErrors(t *testing.T) {
	for _, operation := range []string{"update", "activate", "delete"} {
		t.Run(operation, func(t *testing.T) {
			ctx := context.Background()
			pool := pushTestDB(t)
			key := make([]byte, 32)
			_, _ = rand.Read(key)
			s := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
			id := "16161616-1234-4234-8234-123456789abc"
			capability := randomCapability(t)
			request := Registration{InstallationID: id, DeviceToken: hex.EncodeToString([]byte("fixture-device-token")), Locale: "ru", Environment: "sandbox"}
			call := func(ip string) error {
				switch operation {
				case "update":
					_, err := s.Update(ctx, id, request, capability, ip, 1)
					return err
				case "activate":
					_, err := s.Activate(ctx, id, capability, ip, randomCapability(t), 1)
					return err
				default:
					return s.Delete(ctx, id, capability, ip)
				}
			}
			if err := call("198.51.100.204"); !errors.Is(err, ErrUnauthorized) {
				t.Fatalf("missing installation = %v, want unauthorized", err)
			}
			if _, err := pool.Exec(ctx, `DROP TABLE push_installations CASCADE`); err != nil {
				t.Fatal(err)
			}
			if err := call("198.51.100.205"); !errors.Is(err, ErrUnavailable) {
				t.Fatalf("database query failure = %v, want unavailable", err)
			}
		})
	}
}

func sha256Bytes(tokenHex string) []byte {
	token, _ := hex.DecodeString(tokenHex)
	h := sha256.Sum256(token)
	return h[:]
}
