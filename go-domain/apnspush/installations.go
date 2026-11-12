package apnspush

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrRequest = errors.New("invalid push request")
var ErrUnauthorized = errors.New("push capability unauthorized")
var ErrRateLimit = errors.New("push rate limit reached")
var ErrConflict = errors.New("push state conflict")
var ErrUnavailable = errors.New("push service unavailable")
var installationID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

type Service struct {
	Pool                  *pgxpool.Pool
	Key                   []byte
	KeyID, InstallationID string
	IPHashKey             []byte
	Now                   func() time.Time
}
type Registration struct {
	InstallationID string `json:"installation_id"`
	DeviceToken    string `json:"device_token"`
	Locale         string `json:"locale"`
	Environment    string `json:"environment"`
}
type Status struct {
	InstallationID      string     `json:"installation_id"`
	State               string     `json:"state"`
	SubscriptionVersion int64      `json:"subscription_version"`
	ExpiresAt           *time.Time `json:"expires_at,omitempty"`
}
type CanaryRequest struct {
	InstallationID string `json:"installation_id"`
	EditionID      string `json:"edition_id"`
	IdempotencyKey string `json:"idempotency_key"`
}
type CanaryStatus struct {
	RequestID  string `json:"request_id"`
	DeliveryID string `json:"delivery_id"`
	State      string `json:"state"`
}

func (s *Service) now() time.Time {
	if s.Now != nil {
		return s.Now().UTC()
	}
	return time.Now().UTC()
}
func (s *Service) valid() bool {
	return s != nil && s.Pool != nil && len(s.Key) == 32 && s.KeyID != "" && s.InstallationID != "" && len(s.IPHashKey) >= 32
}
func validRegistration(in Registration) bool {
	if !installationID.MatchString(in.InstallationID) || (in.Locale != "ru" && in.Locale != "sr-Latn") || (in.Environment != "sandbox" && in.Environment != "production") || len(in.DeviceToken) < 2 || len(in.DeviceToken) > 1024 || len(in.DeviceToken)%2 != 0 {
		return false
	}
	_, err := hex.DecodeString(in.DeviceToken)
	return err == nil && strings.ToLower(in.DeviceToken) == in.DeviceToken
}
func capabilityHash(v string) ([]byte, bool) {
	if len(v) != 43 {
		return nil, false
	}
	b, e := base64.RawURLEncoding.DecodeString(v)
	if e != nil || len(b) != 32 {
		return nil, false
	}
	h := sha256.Sum256(b)
	return h[:], true
}
func (s *Service) safe(r *http.Request) bool { return s.valid() && r != nil }
func (s *Service) addressHash(ip string) []byte {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil
	}
	h := hmac.New(sha256.New, s.IPHashKey)
	if ipv4 := parsed.To4(); ipv4 != nil {
		_, _ = h.Write([]byte("push-ip-v4"))
		_, _ = h.Write(ipv4)
	} else {
		// Aggregate IPv6 privacy addresses within a /64 to prevent address rotation.
		prefix := parsed.Mask(net.CIDRMask(64, 128))
		_, _ = h.Write([]byte("push-ip-v6-64"))
		_, _ = h.Write(prefix)
	}
	return h.Sum(nil)
}
func (s *Service) rate(ctx context.Context, tx pgx.Tx, key []byte, cap int, window time.Duration) error {
	if len(key) != 32 {
		return ErrRequest
	}
	now := s.now()
	_, _ = tx.Exec(ctx, `DELETE FROM push_rate_buckets WHERE expires_at<=$1`, now)
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM push_rate_buckets WHERE bucket_key=$1)`, key).Scan(&exists); err != nil {
		return ErrUnavailable
	}
	if !exists {
		var count int
		if err := tx.QueryRow(ctx, `SELECT count(*) FROM push_rate_buckets`).Scan(&count); err != nil {
			return ErrUnavailable
		}
		if count >= 100000 {
			return ErrUnavailable
		}
	}
	var hits int
	err := tx.QueryRow(ctx, `INSERT INTO push_rate_buckets(bucket_key,window_started_at,hits,expires_at) VALUES($1,$2,1,$3) ON CONFLICT(bucket_key) DO UPDATE SET window_started_at=CASE WHEN push_rate_buckets.window_started_at <= $2-($4 * interval '1 second') THEN $2 ELSE push_rate_buckets.window_started_at END,hits=CASE WHEN push_rate_buckets.window_started_at <= $2-($4 * interval '1 second') THEN 1 ELSE push_rate_buckets.hits+1 END,expires_at=$3 RETURNING hits`, key, now, now.Add(2*time.Hour), window.Seconds()).Scan(&hits)
	if err != nil {
		return ErrUnavailable
	}
	if hits > cap {
		return ErrRateLimit
	}
	return nil
}

func (s *Service) admit(ctx context.Context, ip string, limits ...rateLimit) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer tx.Rollback(ctx)
	ipKey := digestKey("ip", s.addressHash(ip))
	if len(ipKey) != 32 {
		return ErrRequest
	}
	var limited bool
	err = s.rate(ctx, tx, ipKey, 30, time.Hour)
	if errors.Is(err, ErrRateLimit) {
		if err = tx.Commit(ctx); err != nil {
			return ErrUnavailable
		}
		return ErrRateLimit
	} else if err != nil {
		return err
	}
	for _, l := range limits {
		err = s.rate(ctx, tx, l.key, l.cap, l.window)
		if errors.Is(err, ErrRateLimit) {
			limited = true
			continue
		}
		if err != nil {
			return err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	if limited {
		return ErrRateLimit
	}
	return nil
}

type rateLimit struct {
	key    []byte
	cap    int
	window time.Duration
}

func digestKey(namespace string, data []byte) []byte {
	if namespace == "ip" && len(data) == 0 {
		return nil
	}
	sum := sha256.Sum256(append([]byte(namespace+"\x00"), data...))
	return sum[:]
}
func (s *Service) Register(ctx context.Context, in Registration, capability, canonicalIP string) (Status, int, error) {
	var zero Status
	if !s.valid() {
		return zero, 0, ErrUnavailable
	}
	if !validRegistration(in) {
		return zero, 0, ErrRequest
	}
	capHash, ok := capabilityHash(capability)
	if !ok {
		return zero, 0, ErrUnauthorized
	}
	token, _ := hex.DecodeString(in.DeviceToken)
	tokenHash := sha256.Sum256(token)
	if err := s.admit(ctx, canonicalIP, rateLimit{digestKey("installation", []byte(in.InstallationID)), 12, time.Hour}, rateLimit{digestKey("token", tokenHash[:]), 24, time.Hour}); err != nil {
		return zero, 429, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return zero, 0, ErrUnavailable
	}
	defer tx.Rollback(ctx)
	// Push-order lock serializes readiness, activation, and subscription changes.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728442)`); err != nil {
		return zero, 0, ErrUnavailable
	}
	var storedCap, storedToken []byte
	var state, locale, env string
	var version int64
	var challengeExpires *time.Time
	err = tx.QueryRow(ctx, `SELECT capability_hash,token_hash,state,locale,environment,subscription_version,challenge_expires_at FROM push_installations WHERE installation_id=$1 FOR UPDATE`, in.InstallationID).Scan(&storedCap, &storedToken, &state, &locale, &env, &version, &challengeExpires)
	if err == nil {
		if subtleConstant(storedCap, capHash) != 1 {
			return zero, 0, ErrUnauthorized
		}
		if state == "disabled" || state == "invalid" {
			return Status{in.InstallationID, state, version, nil}, 409, ErrConflict
		}
		if bytesEqual(storedToken, tokenHash[:]) && locale == in.Locale && env == in.Environment {
			if state == "pending" && challengeExpires != nil && challengeExpires.After(s.now()) {
				var attemptCount int
				if err = tx.QueryRow(ctx, `UPDATE push_installations SET challenge_attempts=challenge_attempts+1 WHERE installation_id=$1 AND challenge_attempts<8 RETURNING challenge_attempts`, in.InstallationID).Scan(&attemptCount); errors.Is(err, pgx.ErrNoRows) {
					if tx.Commit(ctx) != nil {
						return zero, 0, ErrUnavailable
					}
					return zero, 429, ErrRateLimit
				} else if err != nil {
					return zero, 0, ErrUnavailable
				}
				_, err = tx.Exec(ctx, `UPDATE push_challenges SET state='pending',next_attempt_at=$3,lease_id=NULL,lease_until=NULL,updated_at=$3 WHERE installation_id=$1 AND subscription_version=$2 AND expires_at>$3 AND state IN ('accepted','unknown','retry_wait') AND attempts<5`, in.InstallationID, version, s.now())
				if err != nil {
					return zero, 0, ErrUnavailable
				}
			}
			_, err = tx.Exec(ctx, `UPDATE push_installations SET last_seen_at=$2,updated_at=$2 WHERE installation_id=$1`, in.InstallationID, s.now())
			if err != nil {
				return zero, 0, ErrUnavailable
			}
			if err = tx.Commit(ctx); err != nil {
				return zero, 0, ErrUnavailable
			}
			return Status{in.InstallationID, state, version, challengeExpires}, 200, nil
		}
		return Status{in.InstallationID, state, version, challengeExpires}, 409, ErrConflict
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return zero, 0, ErrUnavailable
	}
	nonce := make([]byte, 32)
	if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
		return zero, 0, ErrUnavailable
	}
	nonceText := base64.RawURLEncoding.EncodeToString(nonce)
	nonceHash := sha256.Sum256([]byte(nonceText))
	// PostgreSQL timestamps retain microseconds; return the exact persisted expiry.
	expires := s.now().Add(10 * time.Minute).Truncate(time.Microsecond)
	provider := aadProvider(in.InstallationID, 1)
	envelope, err := credentialstore.Seal(s.Key, s.KeyID, s.InstallationID, provider, token)
	if err != nil {
		return zero, 0, ErrUnavailable
	}
	_, err = tx.Exec(ctx, `INSERT INTO push_installations(installation_id,capability_hash,token_hash,token_envelope_version,token_key_id,token_nonce,token_ciphertext,locale,environment,state,subscription_version,challenge_hash,challenge_expires_at,version_changed_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'pending',1,$10,$11,$12)`, in.InstallationID, capHash, tokenHash[:], envelope.Version, envelope.KeyID, envelope.Nonce, envelope.Ciphertext, in.Locale, in.Environment, nonceHash[:], expires, s.now())
	if err != nil {
		return zero, 0, ErrUnavailable
	}
	challengeEnvelope, sealErr := credentialstore.Seal(s.Key, s.KeyID, s.InstallationID, "apns-nonce:"+in.InstallationID+":1", []byte(nonceText))
	if sealErr != nil {
		return zero, 0, ErrUnavailable
	}
	challengeID, uuidErr := newUUID()
	if uuidErr != nil {
		return zero, 0, ErrUnavailable
	}
	_, err = tx.Exec(ctx, `INSERT INTO push_challenges(installation_id,subscription_version,nonce_hash,nonce_envelope_version,nonce_key_id,nonce_nonce,nonce_ciphertext,environment,state,apns_id,expires_at) VALUES($1,1,$2,$3,$4,$5,$6,$7,'pending',$8,$9)`, in.InstallationID, nonceHash[:], challengeEnvelope.Version, challengeEnvelope.KeyID, challengeEnvelope.Nonce, challengeEnvelope.Ciphertext, in.Environment, challengeID, expires)
	if err != nil {
		return zero, 0, ErrUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return zero, 0, ErrUnavailable
	}
	return Status{in.InstallationID, "pending", 1, &expires}, 202, nil
}

func (s *Service) Update(ctx context.Context, id string, in Registration, capability, canonicalIP string, expected int64) (Status, error) {
	var z Status
	if !s.valid() || !installationID.MatchString(id) || id != in.InstallationID || !validRegistration(in) || expected < 1 {
		return z, ErrRequest
	}
	capHash, ok := capabilityHash(capability)
	if !ok {
		return z, ErrUnauthorized
	}
	token, _ := hex.DecodeString(in.DeviceToken)
	tokenHash := sha256.Sum256(token)
	if err := s.admit(ctx, canonicalIP, rateLimit{digestKey("installation", []byte(id)), 20, time.Hour}, rateLimit{digestKey("token", tokenHash[:]), 24, time.Hour}); err != nil {
		return z, err
	}
	th := tokenHash
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return z, ErrUnavailable
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728442)`); err != nil {
		return z, ErrUnavailable
	}
	var ch, oldToken []byte
	var state, locale, env string
	var version int64
	var currentExpiry *time.Time
	err = tx.QueryRow(ctx, `SELECT capability_hash,token_hash,state,locale,environment,subscription_version,challenge_expires_at FROM push_installations WHERE installation_id=$1 FOR UPDATE`, id).Scan(&ch, &oldToken, &state, &locale, &env, &version, &currentExpiry)
	if errors.Is(err, pgx.ErrNoRows) {
		return z, ErrUnauthorized
	}
	if err != nil {
		return z, ErrUnavailable
	}
	if subtleConstant(ch, capHash) != 1 {
		return z, ErrUnauthorized
	}
	if version != expected {
		return Status{id, state, version, currentExpiry}, ErrConflict
	}
	changedToken := !bytesEqual(oldToken, th[:]) || env != in.Environment || state == "disabled" || state == "invalid"
	changedLocale := locale != in.Locale
	if !changedToken && !changedLocale {
		_, err = tx.Exec(ctx, `UPDATE push_installations SET last_seen_at=$2,updated_at=$2 WHERE installation_id=$1`, id, s.now())
		if err != nil {
			return z, ErrUnavailable
		}
		if tx.Commit(ctx) != nil {
			return z, ErrUnavailable
		}
		return Status{id, state, version, currentExpiry}, nil
	}
	newVersion := version + 1
	// Every subscription version has its own AAD, including locale-only changes.
	envelope, eerr := credentialstore.Seal(s.Key, s.KeyID, s.InstallationID, aadProvider(id, newVersion), token)
	if eerr != nil {
		return z, ErrUnavailable
	}
	var nonceHash any
	var challengeExpiresVal any
	var statusExpiry *time.Time
	var newState = "active"
	var activatedAt any
	if state == "active" && !changedToken {
		activatedAt = s.now()
	}
	if changedToken || state != "active" {
		newState = "pending"
		activatedAt = nil
		nonce := make([]byte, 32)
		if _, err = io.ReadFull(rand.Reader, nonce); err != nil {
			return z, ErrUnavailable
		}
		nonceText := base64.RawURLEncoding.EncodeToString(nonce)
		nh := sha256.Sum256([]byte(nonceText))
		nonceHash = nh[:]
		expiry := s.now().Add(10 * time.Minute).Truncate(time.Microsecond)
		challengeExpiresVal = expiry
		statusExpiry = &expiry
		ce, eerr := credentialstore.Seal(s.Key, s.KeyID, s.InstallationID, "apns-nonce:"+id+":"+strconv.FormatInt(newVersion, 10), []byte(nonceText))
		if eerr != nil {
			return z, ErrUnavailable
		}
		challengeID, uuidErr := newUUID()
		if uuidErr != nil {
			return z, ErrUnavailable
		}
		_, err = tx.Exec(ctx, `INSERT INTO push_challenges(installation_id,subscription_version,nonce_hash,nonce_envelope_version,nonce_key_id,nonce_nonce,nonce_ciphertext,environment,state,apns_id,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'pending',$9,$10)`, id, newVersion, nh[:], ce.Version, ce.KeyID, ce.Nonce, ce.Ciphertext, in.Environment, challengeID, expiry)
		if err != nil {
			return z, ErrUnavailable
		}
	}
	if changedToken {
		_, err = tx.Exec(ctx, `UPDATE push_installations SET token_hash=$2,token_envelope_version=$3,token_key_id=$4,token_nonce=$5,token_ciphertext=$6,locale=$7,environment=$8,state=$9,subscription_version=$10,challenge_hash=$11,challenge_expires_at=$12,challenge_attempts=0,activated_at=$13,last_seen_at=$14,updated_at=$14,version_changed_at=$14 WHERE installation_id=$1`, id, th[:], envelope.Version, envelope.KeyID, envelope.Nonce, envelope.Ciphertext, in.Locale, in.Environment, newState, newVersion, nonceHash, challengeExpiresVal, activatedAt, s.now())
	} else {
		_, err = tx.Exec(ctx, `UPDATE push_installations SET token_envelope_version=$2,token_key_id=$3,token_nonce=$4,token_ciphertext=$5,locale=$6,state=$7,subscription_version=$8,challenge_hash=$9,challenge_expires_at=$10,challenge_attempts=0,activated_at=COALESCE($11,activated_at),last_seen_at=$12,updated_at=$12,version_changed_at=$12 WHERE installation_id=$1`, id, envelope.Version, envelope.KeyID, envelope.Nonce, envelope.Ciphertext, in.Locale, newState, newVersion, nonceHash, challengeExpiresVal, activatedAt, s.now())
	}
	if err != nil {
		return z, ErrUnavailable
	}
	if changedToken || state != "active" {
		_, err = tx.Exec(ctx, `UPDATE push_challenges SET state='cancelled',nonce_hash=NULL,nonce_envelope_version=NULL,nonce_key_id=NULL,nonce_nonce=NULL,nonce_ciphertext=NULL,updated_at=$3 WHERE installation_id=$1 AND subscription_version<$2 AND state IN ('pending','retry_wait')`, id, newVersion, s.now())
		if err != nil {
			return z, ErrUnavailable
		}
	}
	_, err = tx.Exec(ctx, `UPDATE push_deliveries SET state='cancelled',updated_at=$2 WHERE installation_id=$1 AND state IN ('pending','retry_wait') AND subscription_version<$3`, id, s.now(), newVersion)
	if err != nil {
		return z, ErrUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return z, ErrUnavailable
	}
	return Status{id, newState, newVersion, statusExpiry}, nil
}

func (s *Service) Activate(ctx context.Context, id, capability, canonicalIP, nonce string, version int64) (Status, error) {
	var z Status
	if !s.valid() || !installationID.MatchString(id) || version < 1 || len(nonce) != 43 {
		return z, ErrRequest
	}
	capHash, ok := capabilityHash(capability)
	if !ok {
		return z, ErrUnauthorized
	}
	if err := s.admit(ctx, canonicalIP, rateLimit{digestKey("installation", []byte(id)), 12, time.Hour}); err != nil {
		return z, err
	}
	nonceBytes, e := base64.RawURLEncoding.DecodeString(nonce)
	if e != nil || len(nonceBytes) != 32 {
		return z, ErrUnauthorized
	}
	nh := sha256.Sum256([]byte(nonce))
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return z, ErrUnavailable
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728442)`); err != nil {
		return z, ErrUnavailable
	}
	var ch, stored []byte
	var state string
	var current int64
	var currentExpiry *time.Time
	err = tx.QueryRow(ctx, `SELECT capability_hash,state,subscription_version,challenge_hash,challenge_expires_at FROM push_installations WHERE installation_id=$1 FOR UPDATE`, id).Scan(&ch, &state, &current, &stored, &currentExpiry)
	if errors.Is(err, pgx.ErrNoRows) {
		return z, ErrUnauthorized
	}
	if err != nil {
		return z, ErrUnavailable
	}
	if subtleConstant(ch, capHash) != 1 {
		return z, ErrUnauthorized
	}
	if current != version {
		return Status{id, state, current, currentExpiry}, ErrConflict
	}
	if state == "active" {
		if err = tx.Commit(ctx); err != nil {
			return z, ErrUnavailable
		}
		return Status{id, state, current, nil}, nil
	}
	if state != "pending" || subtleConstant(stored, nh[:]) != 1 {
		return z, ErrRequest
	}
	var expires time.Time
	var chState string
	var attempts int
	var challengeHash []byte
	err = tx.QueryRow(ctx, `SELECT expires_at,state,attempts,nonce_hash FROM push_challenges WHERE installation_id=$1 AND subscription_version=$2 FOR UPDATE`, id, version).Scan(&expires, &chState, &attempts, &challengeHash)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return z, ErrUnavailable
	}
	if err != nil || !expires.After(s.now()) || attempts < 1 || (chState != "pending" && chState != "accepted" && chState != "sending" && chState != "unknown" && chState != "retry_wait") || subtleConstant(challengeHash, nh[:]) != 1 {
		return z, ErrRequest
	}
	_, err = tx.Exec(ctx, `UPDATE push_challenges SET state='confirmed',nonce_hash=NULL,nonce_envelope_version=NULL,nonce_key_id=NULL,nonce_nonce=NULL,nonce_ciphertext=NULL,updated_at=$3 WHERE installation_id=$1 AND subscription_version=$2`, id, version, s.now())
	if err != nil {
		return z, ErrUnavailable
	}
	_, err = tx.Exec(ctx, `UPDATE push_installations SET state='active',activated_at=$3,challenge_hash=NULL,challenge_expires_at=NULL,updated_at=$3 WHERE installation_id=$1 AND subscription_version=$2`, id, version, s.now())
	if err != nil {
		return z, ErrUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return z, ErrUnavailable
	}
	return Status{id, "active", version, nil}, nil
}

func (s *Service) Delete(ctx context.Context, id, capability, canonicalIP string) error {
	if !s.valid() || !installationID.MatchString(id) {
		return ErrRequest
	}
	capHash, ok := capabilityHash(capability)
	if !ok {
		return ErrUnauthorized
	}
	if err := s.admit(ctx, canonicalIP, rateLimit{digestKey("installation", []byte(id)), 12, time.Hour}); err != nil {
		return err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer tx.Rollback(ctx)
	// Match registration/activation ordering before locking the installation row.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728442)`); err != nil {
		return ErrUnavailable
	}
	var ch []byte
	var version int64
	err = tx.QueryRow(ctx, `SELECT capability_hash,subscription_version FROM push_installations WHERE installation_id=$1 FOR UPDATE`, id).Scan(&ch, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrUnauthorized
	}
	if err != nil {
		return ErrUnavailable
	}
	if subtleConstant(ch, capHash) != 1 {
		return ErrUnauthorized
	}
	_, err = tx.Exec(ctx, `UPDATE push_installations SET state='disabled',token_envelope_version=NULL,token_key_id=NULL,token_nonce=NULL,token_ciphertext=NULL,challenge_hash=NULL,challenge_expires_at=NULL,subscription_version=subscription_version+1,updated_at=$2 WHERE installation_id=$1`, id, s.now())
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `UPDATE push_challenges SET state='cancelled',updated_at=$3 WHERE installation_id=$1 AND subscription_version=$2 AND state IN ('pending','sending')`, id, version, s.now())
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `UPDATE push_challenges SET nonce_hash=NULL,nonce_envelope_version=NULL,nonce_key_id=NULL,nonce_nonce=NULL,nonce_ciphertext=NULL,updated_at=$2 WHERE installation_id=$1`, id, s.now())
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `UPDATE push_deliveries SET state='cancelled',updated_at=$2 WHERE installation_id=$1 AND state IN ('pending','retry_wait')`, id, s.now())
	if err != nil {
		return ErrUnavailable
	}
	if tx.Commit(ctx) != nil {
		return ErrUnavailable
	}
	return nil
}

func (s *Service) Canary(ctx context.Context, in CanaryRequest, canonicalIP string) (CanaryStatus, int, error) {
	var z CanaryStatus
	if !s.valid() || !installationID.MatchString(in.InstallationID) || !installationID.MatchString(in.EditionID) || !installationID.MatchString(in.IdempotencyKey) {
		return z, 0, ErrRequest
	}
	if err := s.admit(ctx, canonicalIP, rateLimit{digestKey("canary-installation", []byte(in.InstallationID)), 5, time.Hour}); err != nil {
		return z, 429, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return z, 0, ErrUnavailable
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728442)`); err != nil {
		return z, 0, ErrUnavailable
	}
	var current CanaryStatus
	err = tx.QueryRow(ctx, `SELECT id::text,installation_id::text,edition_id::text,state FROM push_deliveries WHERE idempotency_key=$1`, in.IdempotencyKey).Scan(&current.DeliveryID, &current.RequestID, new(string), &current.State)
	if err == nil {
		var existingInstallation, existingEdition string
		if qerr := tx.QueryRow(ctx, `SELECT installation_id::text,edition_id::text FROM push_deliveries WHERE idempotency_key=$1`, in.IdempotencyKey).Scan(&existingInstallation, &existingEdition); qerr != nil {
			return z, 0, ErrUnavailable
		}
		if existingInstallation != in.InstallationID || existingEdition != in.EditionID {
			return z, 409, ErrConflict
		}
		current.RequestID = in.IdempotencyKey
		if tx.Commit(ctx) != nil {
			return z, 0, ErrUnavailable
		}
		return current, 200, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return z, 0, ErrUnavailable
	}
	var enabled bool
	if err = tx.QueryRow(ctx, `SELECT sender_enabled FROM push_cutover WHERE id=1`).Scan(&enabled); err != nil || !enabled {
		return z, 0, ErrUnavailable
	}
	var locale, environment string
	var version int64
	var tokenHash []byte
	err = tx.QueryRow(ctx, `SELECT locale,environment,subscription_version,token_hash FROM push_installations WHERE installation_id=$1 AND state='active' FOR UPDATE`, in.InstallationID).Scan(&locale, &environment, &version, &tokenHash)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return z, 0, ErrUnavailable
		}
		return z, 0, ErrConflict
	}
	var day time.Time
	var editionLocale string
	if err = tx.QueryRow(ctx, `SELECT edition_date,locale FROM editorial_digest_editions WHERE id=$1`, in.EditionID).Scan(&day, &editionLocale); err != nil || editionLocale != locale {
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return z, 0, ErrUnavailable
		}
		return z, 0, ErrConflict
	}
	id, err := newUUID()
	if err != nil {
		return z, 0, ErrUnavailable
	}
	apnsID, err := newUUID()
	if err != nil {
		return z, 0, ErrUnavailable
	}
	_, err = tx.Exec(ctx, `INSERT INTO push_deliveries(id,event_id,installation_id,subscription_version,edition_id,edition_date,locale,token_hash,environment,kind,state,apns_id,idempotency_key) VALUES($1,NULL,$2,$3,$4,$5,$6,$7,$8,'canary','pending',$9,$10)`, id, in.InstallationID, version, in.EditionID, day, locale, tokenHash, environment, apnsID, in.IdempotencyKey)
	if err != nil {
		return z, 0, ErrUnavailable
	}
	if tx.Commit(ctx) != nil {
		return z, 0, ErrUnavailable
	}
	return CanaryStatus{RequestID: in.IdempotencyKey, DeliveryID: id, State: "pending"}, 202, nil
}
func aadProvider(id string, version int64) string {
	return "apns-route:" + id + ":" + strconv.FormatInt(version, 10)
}
func bytesEqual(a, b []byte) bool { return subtleConstant(a, b) == 1 }
func subtleConstant(a, b []byte) int {
	if len(a) != len(b) {
		return 0
	}
	var x byte
	for i := range a {
		x |= a[i] ^ b[i]
	}
	if x == 0 {
		return 1
	}
	return 0
}
