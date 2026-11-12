package apnspush

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
)

func claimedShutdownFixture(t *testing.T) (*Service, delivery) {
	t.Helper()
	ctx := context.Background()
	pool := pushTestDB(t)
	key := make([]byte, 32)
	_, _ = rand.Read(key)
	s := &Service{Pool: pool, Key: key, KeyID: "fixture-key", InstallationID: "domain-fixture", IPHashKey: key}
	const id = "61616161-1234-4234-8234-123456789abc"
	const editionID = "62626262-1234-4234-8234-123456789abc"
	token := []byte("0123456789abcdef0123456789abcdef")
	hash := sha256.Sum256(token)
	env, err := credentialstore.Seal(key, s.KeyID, s.InstallationID, aadProvider(id, 1), token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_installations(installation_id,capability_hash,token_hash,token_envelope_version,token_key_id,token_nonce,token_ciphertext,locale,environment,state,subscription_version,activated_at) VALUES($1,$2,$2,$3,$4,$5,$6,'ru','sandbox','active',1,now())`, id, hash[:], env.Version, env.KeyID, env.Nonce, env.Ciphertext); err != nil {
		t.Fatal(err)
	}
	insertPushFixtureEdition(t, pool, editionID, "ru")
	if _, err = pool.Exec(ctx, `UPDATE push_cutover SET sender_enabled=TRUE WHERE id=1;`); err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, `INSERT INTO push_deliveries(installation_id,subscription_version,edition_id,edition_date,locale,token_hash,environment,kind,state,apns_id) VALUES($1,1,$2,current_date,'ru',$3,'sandbox','canary','pending',gen_random_uuid())`, id, editionID, hash[:]); err != nil {
		t.Fatal(err)
	}
	d, ok, err := s.claim(ctx)
	if err != nil || !ok {
		t.Fatalf("claim: %v %v", ok, err)
	}
	return s, d
}

func TestPostgresCancelledEditionReadRetriesBeforeAPNs(t *testing.T) {
	s, d := claimedShutdownFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	// A cancelled database read happens before provider I/O. A nil sender also
	// ensures this path cannot accidentally make an external request.
	s.sendOne(ctx, nil, d)
	var state string
	var cleared bool
	if err := s.Pool.QueryRow(context.Background(), `SELECT state,lease_id IS NULL AND lease_until IS NULL FROM push_deliveries WHERE id=$1`, d.ID).Scan(&state, &cleared); err != nil || state != "retry_wait" || !cleared {
		t.Fatalf("safe pre-send failure was lost: state=%s cleared=%v err=%v", state, cleared, err)
	}
}

func TestPostgresAcceptedReceiptSurvivesCallerCancellation(t *testing.T) {
	s, d := claimedShutdownFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	sender := &Sender{Key: key, Credential: Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890"}, Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		cancel()
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}}
	s.sendOne(ctx, sender, d)
	var state string
	var status int
	if err := s.Pool.QueryRow(context.Background(), `SELECT state,status_code FROM push_deliveries WHERE id=$1`, d.ID).Scan(&state, &status); err != nil || state != "accepted" || status != 200 {
		t.Fatalf("received provider response was not persisted: state=%s status=%d err=%v", state, status, err)
	}
}

func TestPostgresChallengeReceiptSurvivesCallerCancellation(t *testing.T) {
	s, _ := claimedShutdownFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const id = "63636363-1234-4234-8234-123456789abc"
	insertPendingChallenge(t, s.Pool, s, id, 1, 0, "pending")
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	sender := &Sender{Key: key, Credential: Credential{KeyID: "ABCDEFGHIJ", TeamID: "1234567890"}, Client: &http.Client{Transport: rtFunc(func(r *http.Request) (*http.Response, error) {
		cancel()
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
	})}}
	if err := s.sendChallenge(ctx, sender); err != nil {
		t.Fatal(err)
	}
	var state string
	if err := s.Pool.QueryRow(context.Background(), `SELECT state FROM push_challenges WHERE installation_id=$1`, id).Scan(&state); err != nil || state != "accepted" {
		t.Fatalf("received challenge response was not persisted: state=%s err=%v", state, err)
	}
}

func TestPostgresProofAndCanaryReadsReportDatabaseUnavailability(t *testing.T) {
	t.Run("proof", func(t *testing.T) {
		s, _ := claimedShutdownFixture(t)
		ctx := context.Background()
		const id = "64646464-1234-4234-8234-123456789abc"
		capability, nonce := insertPendingChallenge(t, s.Pool, s, id, 1, 1, "pending")
		if _, err := s.Pool.Exec(ctx, `DROP TABLE push_challenges`); err != nil {
			t.Fatal(err)
		}
		if _, err := s.Activate(ctx, id, capability, "192.0.2.64", nonce, 1); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("proof read failure was reported as invalid proof: %v", err)
		}
	})
	for _, table := range []string{"push_installations", "editorial_digest_editions"} {
		t.Run(table, func(t *testing.T) {
			s, d := claimedShutdownFixture(t)
			ctx := context.Background()
			if _, err := s.Pool.Exec(ctx, "DROP TABLE "+table+" CASCADE"); err != nil {
				t.Fatal(err)
			}
			_, _, err := s.Canary(ctx, CanaryRequest{InstallationID: d.InstallationID, EditionID: d.EditionID, IdempotencyKey: "65656565-1234-4234-8234-123456789abc"}, "192.0.2.65")
			if !errors.Is(err, ErrUnavailable) {
				t.Fatalf("canary read failure was reported as a conflict: %v", err)
			}
		})
	}
}
