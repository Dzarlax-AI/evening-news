package apnspush

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/jackc/pgx/v5"
)

type delivery struct {
	ID, InstallationID, EditionID, Locale, Environment, APNsID string
	Kind                                                       string
	LeaseID                                                    string
	Version                                                    int64
	TokenHash, TokenCiphertext, TokenNonce                     []byte
	TokenEnvelopeVersion                                       int
	TokenKeyID                                                 string
	EditionDate                                                time.Time
	ExpiresAt                                                  time.Time
	Attempts                                                   int
}
type fanoutCandidate struct {
	id, env   string
	version   int64
	tokenHash []byte
}

// Process performs bounded fanout and delivery work. Call it only from the
// already-fenced domain loop; sender_enabled is an independent default-off gate.
func (s *Service) Process(ctx context.Context, sender *Sender, limit int) error {
	if !s.valid() {
		return ErrUnavailable
	}
	if limit < 1 || limit > 50 {
		limit = 10
	}
	if err := s.housekeep(ctx, limit); err != nil {
		return err
	}
	if err := s.expireSending(ctx, limit); err != nil {
		return err
	}
	if !s.SenderEnabled(ctx) {
		return nil
	}
	if err := s.fanout(ctx, limit); err != nil {
		return err
	}
	if sender == nil {
		return nil
	}
	for i := 0; i < limit; i++ {
		if err := s.sendChallenge(ctx, sender); err != nil {
			return err
		}
	}
	for i := 0; i < limit; i++ {
		d, ok, err := s.claim(ctx)
		if err != nil {
			return err
		}
		if !ok {
			break
		}
		s.sendOne(ctx, sender, d)
	}
	return nil
}
func (s *Service) SenderEnabled(ctx context.Context) bool {
	var enabled bool
	if !s.valid() || s.Pool.QueryRow(ctx, `SELECT sender_enabled FROM push_cutover WHERE id=1`).Scan(&enabled) != nil {
		return false
	}
	return enabled
}
func (s *Service) fanout(ctx context.Context, limit int) error {
	if limit > 8 {
		limit = 8
	}
	for n := 0; n < limit; n++ {
		if err := s.fanoutBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}
func (s *Service) fanoutBatch(ctx context.Context) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer tx.Rollback(ctx)
	var enabled bool
	if err = tx.QueryRow(ctx, `SELECT enabled AND sender_enabled FROM push_cutover WHERE id=1`).Scan(&enabled); err != nil {
		return ErrUnavailable
	}
	if !enabled {
		_ = tx.Rollback(ctx)
		return nil
	}
	// Push-order lock serializes fanout with activation and subscription changes.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728442)`); err != nil {
		return ErrUnavailable
	}
	var eventID int64
	var editionID, locale string
	var day time.Time
	var ready time.Time
	var cursor *string
	err = tx.QueryRow(ctx, `SELECT id,edition_id::text,locale,edition_date,ready_at,fanout_cursor::text FROM push_readiness_events WHERE fanout_completed_at IS NULL ORDER BY ready_at,id FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&eventID, &editionID, &locale, &day, &ready, &cursor)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return nil
	}
	if err != nil {
		return ErrUnavailable
	}
	if !ready.After(s.now().Add(-24 * time.Hour)) {
		if _, err = tx.Exec(ctx, `UPDATE push_readiness_events SET fanout_completed_at=clock_timestamp() WHERE id=$1`, eventID); err != nil || tx.Commit(ctx) != nil {
			return ErrUnavailable
		}
		return nil
	}
	rows, err := tx.Query(ctx, `SELECT installation_id::text,subscription_version,token_hash,environment FROM push_installations WHERE state='active' AND locale=$1 AND activated_at<=$2 AND version_changed_at<=$2 AND ($3::uuid IS NULL OR installation_id>$3::uuid) ORDER BY installation_id LIMIT 100`, locale, ready, cursor)
	if err != nil {
		return ErrUnavailable
	}
	var candidates []fanoutCandidate
	for rows.Next() {
		var c fanoutCandidate
		if rows.Scan(&c.id, &c.version, &c.tokenHash, &c.env) != nil {
			rows.Close()
			return ErrUnavailable
		}
		candidates = append(candidates, c)
	}
	rows.Close()
	if rows.Err() != nil {
		return ErrUnavailable
	}
	for _, c := range candidates {
		_, err = tx.Exec(ctx, `INSERT INTO push_deliveries(event_id,installation_id,subscription_version,edition_id,edition_date,locale,token_hash,environment,kind,state,expires_at,apns_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'digest','pending',$9,gen_random_uuid()) ON CONFLICT DO NOTHING`, eventID, c.id, c.version, editionID, day, locale, c.tokenHash, c.env, ready.Add(24*time.Hour))
		if err != nil {
			return ErrUnavailable
		}
	}
	if len(candidates) < 100 {
		_, err = tx.Exec(ctx, `UPDATE push_readiness_events SET fanout_completed_at=clock_timestamp(),fanout_cursor=COALESCE($2::uuid,fanout_cursor) WHERE id=$1`, eventID, lastCandidate(candidates, cursor))
	} else {
		_, err = tx.Exec(ctx, `UPDATE push_readiness_events SET fanout_cursor=$2 WHERE id=$1`, eventID, candidates[len(candidates)-1].id)
	}
	if err != nil {
		return ErrUnavailable
	}
	if tx.Commit(ctx) != nil {
		return ErrUnavailable
	}
	return nil
}
func (s *Service) housekeep(ctx context.Context, limit int) error {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer tx.Rollback(ctx)
	// Serialize subscription state changes with lifecycle cleanup to keep row locks ordered.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728442)`); err != nil {
		return ErrUnavailable
	}
	now := s.now()
	_, err = tx.Exec(ctx, `WITH due AS (SELECT installation_id,subscription_version FROM push_challenges WHERE state IN ('pending','retry_wait','accepted') AND expires_at<=$1 ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE push_challenges c SET state='expired',nonce_hash=NULL,nonce_envelope_version=NULL,nonce_key_id=NULL,nonce_nonce=NULL,nonce_ciphertext=NULL,lease_id=NULL,lease_until=NULL,updated_at=$1 FROM due d WHERE c.installation_id=d.installation_id AND c.subscription_version=d.subscription_version`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `WITH due AS (SELECT installation_id,subscription_version FROM push_challenges WHERE state='sending' AND lease_until<=$1 ORDER BY lease_until LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE push_challenges c SET state='unknown',lease_id=NULL,lease_until=NULL,updated_at=$1 FROM due d WHERE c.installation_id=d.installation_id AND c.subscription_version=d.subscription_version`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	// Expiry erases challenge proof material for every outcome, while preserving
	// state and receipt rows long enough to reconcile ambiguous delivery.
	_, err = tx.Exec(ctx, `WITH due AS (SELECT installation_id,subscription_version FROM push_challenges WHERE expires_at<=$1 AND (nonce_hash IS NOT NULL OR nonce_envelope_version IS NOT NULL OR nonce_key_id IS NOT NULL OR nonce_nonce IS NOT NULL OR nonce_ciphertext IS NOT NULL) ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE push_challenges c SET nonce_hash=NULL,nonce_envelope_version=NULL,nonce_key_id=NULL,nonce_nonce=NULL,nonce_ciphertext=NULL FROM due d WHERE c.installation_id=d.installation_id AND c.subscription_version=d.subscription_version`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `WITH due AS (SELECT installation_id FROM push_installations WHERE state='pending' AND challenge_expires_at<=$1 ORDER BY challenge_expires_at LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE push_installations i SET state='disabled',token_envelope_version=NULL,token_key_id=NULL,token_nonce=NULL,token_ciphertext=NULL,challenge_hash=NULL,challenge_expires_at=NULL,updated_at=$1 FROM due d WHERE i.installation_id=d.installation_id`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `WITH due AS (SELECT installation_id FROM push_installations WHERE state='active' AND last_seen_at<=$1::timestamptz-interval '180 days' ORDER BY last_seen_at LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE push_installations i SET state='disabled',token_envelope_version=NULL,token_key_id=NULL,token_nonce=NULL,token_ciphertext=NULL,updated_at=$1 FROM due d WHERE i.installation_id=d.installation_id`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `WITH due AS (SELECT id FROM push_deliveries WHERE state IN ('pending','retry_wait') AND expires_at<=$1 ORDER BY expires_at LIMIT $2 FOR UPDATE SKIP LOCKED) UPDATE push_deliveries d SET state='expired',lease_id=NULL,lease_until=NULL,updated_at=$1 FROM due x WHERE d.id=x.id`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `WITH doomed AS (SELECT id FROM push_deliveries WHERE state IN ('accepted','rejected','unknown','cancelled','expired') AND updated_at<=$1::timestamptz-interval '30 days' ORDER BY updated_at LIMIT $2) DELETE FROM push_deliveries d USING doomed x WHERE d.id=x.id`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `WITH doomed AS (SELECT installation_id,subscription_version FROM push_challenges WHERE state IN ('confirmed','accepted','unknown','expired','cancelled') AND updated_at<=$1::timestamptz-interval '30 days' ORDER BY updated_at LIMIT $2) DELETE FROM push_challenges c USING doomed x WHERE c.installation_id=x.installation_id AND c.subscription_version=x.subscription_version`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `WITH doomed AS (SELECT installation_id FROM push_installations i WHERE state IN ('disabled','invalid') AND updated_at<=$1::timestamptz-interval '30 days' AND NOT EXISTS(SELECT 1 FROM push_deliveries d WHERE d.installation_id=i.installation_id) AND NOT EXISTS(SELECT 1 FROM push_challenges c WHERE c.installation_id=i.installation_id) ORDER BY updated_at LIMIT $2) DELETE FROM push_installations i USING doomed x WHERE i.installation_id=x.installation_id`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	_, err = tx.Exec(ctx, `WITH expired AS (SELECT bucket_key FROM push_rate_buckets WHERE expires_at<=$1 ORDER BY expires_at LIMIT $2) DELETE FROM push_rate_buckets b USING expired x WHERE b.bucket_key=x.bucket_key`, now, limit*20)
	if err != nil {
		return ErrUnavailable
	}
	if tx.Commit(ctx) != nil {
		return ErrUnavailable
	}
	return nil
}
func lastCandidate(cs []fanoutCandidate, cursor *string) any {
	if len(cs) > 0 {
		return cs[len(cs)-1].id
	}
	if cursor != nil {
		return *cursor
	}
	return nil
}
func (s *Service) claim(ctx context.Context) (delivery, bool, error) {
	var d delivery
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return d, false, ErrUnavailable
	}
	defer tx.Rollback(ctx)
	// Claim and lifecycle mutations take the same lock before delivery/install rows.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728442)`); err != nil {
		return d, false, ErrUnavailable
	}
	var enabled bool
	if err = tx.QueryRow(ctx, `SELECT sender_enabled FROM push_cutover WHERE id=1`).Scan(&enabled); err != nil {
		return d, false, ErrUnavailable
	}
	if !enabled {
		_ = tx.Rollback(ctx)
		return d, false, nil
	}
	err = tx.QueryRow(ctx, `SELECT id::text,installation_id::text,edition_id::text,locale,environment,apns_id::text,subscription_version,token_hash,edition_date,expires_at,attempts,kind FROM push_deliveries WHERE kind IN ('digest','canary') AND state IN ('pending','retry_wait') AND expires_at>clock_timestamp() AND next_attempt_at<=clock_timestamp() ORDER BY next_attempt_at,created_at FOR UPDATE SKIP LOCKED LIMIT 1`).Scan(&d.ID, &d.InstallationID, &d.EditionID, &d.Locale, &d.Environment, &d.APNsID, &d.Version, &d.TokenHash, &d.EditionDate, &d.ExpiresAt, &d.Attempts, &d.Kind)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return d, false, nil
	}
	if err != nil {
		return d, false, ErrUnavailable
	}
	lease, err := newUUID()
	if err != nil {
		return d, false, ErrUnavailable
	}
	_, err = tx.Exec(ctx, `UPDATE push_deliveries SET state='sending',attempts=attempts+1,lease_id=$2,lease_until=clock_timestamp()+interval '45 seconds',updated_at=clock_timestamp() WHERE id=$1`, d.ID, lease)
	if err != nil {
		return d, false, ErrUnavailable
	}
	err = tx.QueryRow(ctx, `SELECT token_envelope_version,token_key_id,token_nonce,token_ciphertext FROM push_installations WHERE installation_id=$1 AND state='active' AND subscription_version=$2 AND token_hash=$3 AND environment=$4 AND locale=$5 FOR UPDATE`, d.InstallationID, d.Version, d.TokenHash, d.Environment, d.Locale).Scan(&d.TokenEnvelopeVersion, &d.TokenKeyID, &d.TokenNonce, &d.TokenCiphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		_, _ = tx.Exec(ctx, `UPDATE push_deliveries SET state='cancelled',lease_id=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1`, d.ID)
		if tx.Commit(ctx) != nil {
			return d, false, ErrUnavailable
		}
		return d, false, nil
	}
	if err != nil {
		return d, false, ErrUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return d, false, ErrUnavailable
	}
	d.LeaseID = lease
	return d, true, nil
}
func newUUID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b[:])
	return h[:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:], nil
}
func (s *Service) sendOne(ctx context.Context, sender *Sender, d delivery) {
	provider := "apns-route:" + d.InstallationID + ":" + itoa64(d.Version)
	token, err := credentialstore.Open(s.Key, s.KeyID, s.InstallationID, provider, credentialstore.Envelope{Version: d.TokenEnvelopeVersion, KeyID: d.TokenKeyID, Nonce: d.TokenNonce, Ciphertext: d.TokenCiphertext})
	if err != nil {
		s.finish(ctx, d, "rejected", 0, 0)
		return
	}
	var edition []byte
	if err = s.Pool.QueryRow(ctx, `SELECT edition FROM editorial_digest_editions WHERE id=$1`, d.EditionID).Scan(&edition); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			s.finish(ctx, d, "rejected", 0, 0)
		} else {
			// APNs has not been called; a failed database read can safely retry.
			s.retry(ctx, d, 0, 0)
		}
		return
	}
	var editionMap map[string]any
	if json.Unmarshal(edition, &editionMap) != nil {
		s.finish(ctx, d, "rejected", 0, 0)
		return
	}
	title, body := "Дайджест готов", "Новый выпуск уже доступен."
	if d.Locale == "sr-Latn" {
		title, body = "Pregled je spreman", "Novo izdanje je dostupno."
	}
	if d.Kind == "canary" {
		if d.Locale == "sr-Latn" {
			title, body = "Testno obaveštenje", "Ovo je testno obaveštenje za izdanje."
		} else {
			title, body = "Тестовое уведомление", "Это тестовое уведомление о выпуске."
		}
	}
	payloadMap := map[string]any{"aps": map[string]any{"alert": map[string]string{"title": title, "body": body}, "sound": "default"}, "type": "digest_ready", "version": 1, "edition_id": d.EditionID, "date": d.EditionDate.Format("2006-01-02"), "locale": d.Locale}
	if d.Kind == "canary" {
		payloadMap["test"] = true
	}
	payload, _ := json.Marshal(payloadMap)
	collapse := d.EditionDate.Format("20060102") + "-" + d.Locale
	result, err := sender.Send(ctx, d.Environment, fmt.Sprintf("%x", token), d.APNsID, "alert", collapse, 5, d.ExpiresAt, payload)
	if err != nil {
		s.finish(ctx, d, "unknown", 0, 0)
		return
	}
	switch {
	case result.Status == 200:
		s.finish(ctx, d, "accepted", 200, 0)
	case result.Status == 410:
		s.invalidate(ctx, d)
		s.finish(ctx, d, "rejected", 410, 0)
	case result.Status == 429 || result.Status >= 500:
		s.retry(ctx, d, result.Status, result.RetryAfter)
	default:
		s.finish(ctx, d, "rejected", result.Status, 0)
	}
}
func (s *Service) finish(ctx context.Context, d delivery, state string, status int, retry time.Duration) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return
	}
	defer tx.Rollback(ctx)
	if state == "retry_wait" {
		_, _ = tx.Exec(ctx, `UPDATE push_deliveries SET state='retry_wait',status_code=$4,next_attempt_at=clock_timestamp()+($5 * interval '1 second'),lease_id=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1 AND lease_id=$2 AND state='sending' AND subscription_version=$3 AND attempts<5`, d.ID, d.LeaseID, d.Version, status, retry.Seconds())
		_ = tx.Commit(ctx)
		return
	}
	// lease ids are selected and attached in claim; CAS also checks the exact
	// sent subscription version and token hash in the installation row.
	_, _ = tx.Exec(ctx, `UPDATE push_deliveries SET state=$4,status_code=$5,lease_id=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE id=$1 AND lease_id=$2 AND state='sending' AND subscription_version=$3 AND token_hash=$6`, d.ID, d.LeaseID, d.Version, state, status, d.TokenHash)
	_ = tx.Commit(ctx)
}
func (s *Service) retry(ctx context.Context, d delivery, status int, after time.Duration) {
	if after <= 0 {
		after = time.Duration(1<<minInt(d.Attempts, 8)) * time.Second
	}
	if after > 30*time.Minute {
		after = 30 * time.Minute
	}
	d.Attempts++
	if d.Attempts >= 5 {
		s.finish(ctx, d, "rejected", status, 0)
		return
	}
	s.finish(ctx, d, "retry_wait", status, after)
}
func (s *Service) invalidate(ctx context.Context, d delivery) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	_, _ = s.Pool.Exec(ctx, `UPDATE push_installations SET state='invalid',token_envelope_version=NULL,token_key_id=NULL,token_nonce=NULL,token_ciphertext=NULL,updated_at=clock_timestamp() WHERE installation_id=$1 AND subscription_version=$2 AND token_hash=$3`, d.InstallationID, d.Version, d.TokenHash)
}
func (s *Service) expireSending(ctx context.Context, limit int) error {
	_, err := s.Pool.Exec(ctx, `WITH expired AS (SELECT id FROM push_deliveries WHERE state='sending' AND lease_until<clock_timestamp() ORDER BY lease_until LIMIT $1 FOR UPDATE SKIP LOCKED) UPDATE push_deliveries d SET state='unknown',lease_id=NULL,lease_until=NULL,updated_at=clock_timestamp() FROM expired WHERE d.id=expired.id`, limit)
	if err != nil {
		return ErrUnavailable
	}
	return nil
}

type challenge struct {
	ID, InstallationID, Environment, APNsID, LeaseID                    string
	Version                                                             int64
	Attempts                                                            int
	TokenHash, TokenNonce, TokenCiphertext, NonceNonce, NonceCiphertext []byte
	TokenKeyID, NonceKeyID                                              string
	TokenVersion, NonceVersion                                          int
	Expires                                                             time.Time
}

func (s *Service) sendChallenge(ctx context.Context, sender *Sender) error {
	var c challenge
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer tx.Rollback(ctx)
	// Match lifecycle lock order; never hold this lock during the APNs request.
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728442)`); err != nil {
		return ErrUnavailable
	}
	var enabled bool
	if err = tx.QueryRow(ctx, `SELECT sender_enabled FROM push_cutover WHERE id=1`).Scan(&enabled); err != nil {
		return ErrUnavailable
	}
	if !enabled {
		_ = tx.Rollback(ctx)
		return nil
	}
	err = tx.QueryRow(ctx, `SELECT c.installation_id::text,i.installation_id::text,c.environment,c.apns_id::text,c.subscription_version,c.attempts,i.token_hash,i.token_envelope_version,i.token_key_id,i.token_nonce,i.token_ciphertext,c.nonce_envelope_version,c.nonce_key_id,c.nonce_nonce,c.nonce_ciphertext,c.expires_at FROM push_challenges c JOIN push_installations i ON i.installation_id=c.installation_id AND i.subscription_version=c.subscription_version AND i.state='pending' WHERE c.state IN ('pending','retry_wait') AND c.attempts<5 AND c.next_attempt_at<=clock_timestamp() AND c.expires_at>clock_timestamp() ORDER BY c.next_attempt_at FOR UPDATE OF c SKIP LOCKED LIMIT 1`).Scan(&c.ID, &c.InstallationID, &c.Environment, &c.APNsID, &c.Version, &c.Attempts, &c.TokenHash, &c.TokenVersion, &c.TokenKeyID, &c.TokenNonce, &c.TokenCiphertext, &c.NonceVersion, &c.NonceKeyID, &c.NonceNonce, &c.NonceCiphertext, &c.Expires)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = tx.Rollback(ctx)
		return nil
	}
	if err != nil {
		return ErrUnavailable
	}
	c.LeaseID, _ = newUUID()
	if c.LeaseID == "" {
		return ErrUnavailable
	}
	err = tx.QueryRow(ctx, `UPDATE push_challenges SET state='sending',attempts=attempts+1,lease_id=$3,lease_until=clock_timestamp()+interval '45 seconds',updated_at=clock_timestamp() WHERE installation_id=$1 AND subscription_version=$2 RETURNING attempts`, c.ID, c.Version, c.LeaseID).Scan(&c.Attempts)
	if err != nil {
		return ErrUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return ErrUnavailable
	}
	token, err := credentialstore.Open(s.Key, s.KeyID, s.InstallationID, "apns-route:"+c.ID+":"+itoa64(c.Version), credentialstore.Envelope{Version: c.TokenVersion, KeyID: c.TokenKeyID, Nonce: c.TokenNonce, Ciphertext: c.TokenCiphertext})
	if err != nil {
		s.finishChallenge(ctx, c, "expired", 0, 0)
		return nil
	}
	nonceText, err := credentialstore.Open(s.Key, s.KeyID, s.InstallationID, "apns-nonce:"+c.ID+":"+itoa64(c.Version), credentialstore.Envelope{Version: c.NonceVersion, KeyID: c.NonceKeyID, Nonce: c.NonceNonce, Ciphertext: c.NonceCiphertext})
	if err != nil {
		s.finishChallenge(ctx, c, "expired", 0, 0)
		return nil
	}
	payload, _ := json.Marshal(map[string]any{"aps": map[string]any{"content-available": 1}, "type": "installation_challenge", "installation_id": c.ID, "subscription_version": c.Version, "nonce": string(nonceText)})
	result, sendErr := sender.Send(ctx, c.Environment, fmt.Sprintf("%x", token), c.APNsID, "background", "", 5, c.Expires, payload)
	if sendErr != nil {
		s.finishChallenge(ctx, c, "unknown", 0, 0)
		return nil
	}
	switch {
	case result.Status == 200:
		s.finishChallenge(ctx, c, "accepted", 200, 0)
	case result.Status == 429 || result.Status >= 500:
		if c.Expires.Sub(s.now()) < time.Duration(0) {
			s.finishChallenge(ctx, c, "expired", result.Status, 0)
		} else {
			s.retryChallenge(ctx, c, result.Status, result.RetryAfter)
		}
	case result.Status == 410:
		s.invalidate(ctx, delivery{InstallationID: c.ID, Version: c.Version, TokenHash: c.TokenHash})
		s.finishChallenge(ctx, c, "expired", 410, 0)
	default:
		s.finishChallenge(ctx, c, "expired", result.Status, 0)
	}
	return nil
}
func (s *Service) finishChallenge(ctx context.Context, c challenge, state string, status int, delay time.Duration) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if state == "retry_wait" {
		_, _ = s.Pool.Exec(ctx, `UPDATE push_challenges SET state='retry_wait',next_attempt_at=clock_timestamp()+$4::interval,lease_id=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE installation_id=$1 AND subscription_version=$2 AND lease_id=$3 AND state='sending' AND attempts<5`, c.ID, c.Version, c.LeaseID, delay.String())
		return
	}
	_, _ = s.Pool.Exec(ctx, `UPDATE push_challenges SET state=$4,lease_id=NULL,lease_until=NULL,updated_at=clock_timestamp() WHERE installation_id=$1 AND subscription_version=$2 AND lease_id=$3 AND state='sending' AND expires_at>clock_timestamp()`, c.ID, c.Version, c.LeaseID, state)
}
func (s *Service) retryChallenge(ctx context.Context, c challenge, status int, after time.Duration) {
	if after <= 0 {
		after = time.Duration(1<<minInt(c.Attempts, 8)) * time.Second
	}
	if after > 5*time.Minute {
		after = 5 * time.Minute
	}
	if c.Expires.Sub(s.now()) <= after || c.Attempts >= 5 {
		s.finishChallenge(ctx, c, "expired", status, 0)
		return
	}
	s.finishChallenge(ctx, c, "retry_wait", status, after)
}
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
func itoa64(n int64) string { return strconv.FormatInt(n, 10) }
