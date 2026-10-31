package credentialstore

import (
	"context"
	"errors"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrConflict = errors.New("credential revision conflict")
var ErrInvalid = errors.New("invalid credential request")
var ErrUnavailable = errors.New("credential store unavailable")

// Metadata is safe to expose through authenticated APIs; it contains no secret.
type Metadata struct {
	Provider   string `json:"provider"`
	Revision   int64  `json:"revision"`
	Configured bool   `json:"configured"`
}

// Store is domain-only: key material must not be handed to web or background.
type Store struct {
	Pool                  *pgxpool.Pool
	Key                   []byte
	KeyID, InstallationID string
}

func validProvider(p string) bool {
	return p == "gemini" || p == "openai" || p == "telegram" || p == "viber"
}
func (s *Store) valid(provider string) bool {
	return s != nil && s.Pool != nil && len(s.Key) == 32 && s.KeyID != "" && s.InstallationID != "" && validProvider(provider)
}
func (s *Store) checkIdentity(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) error {
	var id string
	if err := q.QueryRow(ctx, `SELECT installation_id FROM installation_state WHERE id=1`).Scan(&id); err != nil || id != s.InstallationID {
		return ErrUnavailable
	}
	return nil
}
func (s *Store) Metadata(ctx context.Context, provider string) (Metadata, error) {
	if !s.valid(provider) {
		return Metadata{}, ErrUnavailable
	}
	if err := s.checkIdentity(ctx, s.Pool); err != nil {
		return Metadata{}, err
	}
	m := Metadata{Provider: provider}
	err := s.Pool.QueryRow(ctx, `SELECT revision,ciphertext IS NOT NULL FROM integration_credentials WHERE provider=$1`, provider).Scan(&m.Revision, &m.Configured)
	if errors.Is(err, pgx.ErrNoRows) {
		return m, nil
	}
	if err != nil {
		return Metadata{}, ErrUnavailable
	}
	return m, nil
}

// Resolve decrypts a snapshot for domain operations. Deleted and absent keys do
// not fall back to the environment. Decryption errors fail closed.
func (s *Store) Resolve(ctx context.Context, provider string) ([]byte, Metadata, error) {
	if !s.valid(provider) {
		return nil, Metadata{}, ErrUnavailable
	}
	if err := s.checkIdentity(ctx, s.Pool); err != nil {
		return nil, Metadata{}, err
	}
	m := Metadata{Provider: provider}
	var version *int
	var keyID *string
	var nonce, ciphertext []byte
	err := s.Pool.QueryRow(ctx, `SELECT revision,envelope_version,key_id,nonce,ciphertext FROM integration_credentials WHERE provider=$1`, provider).Scan(&m.Revision, &version, &keyID, &nonce, &ciphertext)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, m, nil
	}
	if err != nil {
		return nil, Metadata{}, ErrUnavailable
	}
	if version == nil && keyID == nil && nonce == nil && ciphertext == nil {
		return nil, m, nil
	}
	if version == nil || keyID == nil || nonce == nil || ciphertext == nil {
		return nil, Metadata{}, ErrUnavailable
	}
	plain, err := Open(s.Key, s.KeyID, s.InstallationID, provider, Envelope{Version: *version, KeyID: *keyID, Nonce: nonce, Ciphertext: ciphertext})
	if err != nil {
		return nil, Metadata{}, ErrUnavailable
	}
	m.Configured = true
	return plain, m, nil
}
func (s *Store) Put(ctx context.Context, provider string, expected int64, secret []byte) (Metadata, error) {
	if !s.valid(provider) {
		return Metadata{}, ErrUnavailable
	}
	if expected < 0 || len(secret) == 0 || len(secret) > 8192 || strings.TrimSpace(string(secret)) == "" {
		return Metadata{}, ErrInvalid
	}
	envelope, err := Seal(s.Key, s.KeyID, s.InstallationID, provider, secret)
	if err != nil {
		return Metadata{}, ErrUnavailable
	}
	return s.update(ctx, provider, expected, &envelope)
}

// Delete retains a revisioned tombstone to prevent stale recreation.
func (s *Store) Delete(ctx context.Context, provider string, expected int64) (Metadata, error) {
	if !s.valid(provider) {
		return Metadata{}, ErrUnavailable
	}
	if expected < 0 {
		return Metadata{}, ErrInvalid
	}
	return s.update(ctx, provider, expected, nil)
}
func (s *Store) update(ctx context.Context, provider string, expected int64, e *Envelope) (Metadata, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Metadata{}, ErrUnavailable
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728438)`); err != nil {
		return Metadata{}, ErrUnavailable
	}
	if err = s.checkIdentity(ctx, tx); err != nil {
		return Metadata{}, err
	}
	var revision int64
	err = tx.QueryRow(ctx, `SELECT revision FROM integration_credentials WHERE provider=$1 FOR UPDATE`, provider).Scan(&revision)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Metadata{}, ErrUnavailable
	}
	if revision != expected {
		return Metadata{}, ErrConflict
	}
	var version, keyID, nonce, ciphertext any
	if e != nil {
		version = e.Version
		keyID = e.KeyID
		nonce = e.Nonce
		ciphertext = e.Ciphertext
	}
	revision++
	_, err = tx.Exec(ctx, `INSERT INTO integration_credentials(provider,revision,envelope_version,key_id,nonce,ciphertext) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(provider) DO UPDATE SET revision=excluded.revision,envelope_version=excluded.envelope_version,key_id=excluded.key_id,nonce=excluded.nonce,ciphertext=excluded.ciphertext,updated_at=now()`, provider, revision, version, keyID, nonce, ciphertext)
	if err != nil {
		return Metadata{}, ErrUnavailable
	}
	if err = tx.Commit(ctx); err != nil {
		return Metadata{}, ErrUnavailable
	}
	return Metadata{Provider: provider, Revision: revision, Configured: e != nil}, nil
}
