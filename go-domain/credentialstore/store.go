package credentialstore

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
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
	return p == "gemini" || p == "openai" || p == "telegram" || p == "viber" || p == "apns"
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
	if provider == "apns" {
		if !validAPNsCredential(secret) {
			return Metadata{}, ErrInvalid
		}
	}
	envelope, err := Seal(s.Key, s.KeyID, s.InstallationID, provider, secret)
	if err != nil {
		return Metadata{}, ErrUnavailable
	}
	return s.update(ctx, provider, expected, &envelope)
}

func validAPNsCredential(raw []byte) bool {
	var in struct {
		KeyID      string `json:"key_id"`
		TeamID     string `json:"team_id"`
		PrivateKey string `json:"private_key"`
	}
	if len(raw) == 0 || len(raw) > 8192 {
		return false
	}
	var values map[string]json.RawMessage
	if json.Unmarshal(raw, &values) != nil || values == nil || len(values) != 3 {
		return false
	}
	for _, k := range []string{"key_id", "team_id", "private_key"} {
		v, ok := values[k]
		if !ok || strings.TrimSpace(string(v)) == "null" {
			return false
		}
	}
	if hasDuplicateCredentialKey(raw) {
		return false
	}
	d := json.NewDecoder(strings.NewReader(string(raw)))
	d.DisallowUnknownFields()
	if d.Decode(&in) != nil || d.Decode(new(any)) != io.EOF || !validAPNsID(in.KeyID) || !validAPNsID(in.TeamID) {
		return false
	}
	b, rest := pem.Decode([]byte(in.PrivateKey))
	if b == nil || b.Type != "PRIVATE KEY" || len(b.Headers) != 0 || len(strings.TrimSpace(string(rest))) != 0 {
		return false
	}
	k, err := x509.ParsePKCS8PrivateKey(b.Bytes)
	if err != nil {
		return false
	}
	e, ok := k.(*ecdsa.PrivateKey)
	return ok && e.Curve == elliptic.P256() && e.D != nil && e.D.Sign() > 0 && e.D.Cmp(e.Params().N) < 0
}

func validAPNsID(s string) bool {
	if len(s) != 10 {
		return false
	}
	for _, c := range s {
		if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}
func hasDuplicateCredentialKey(raw []byte) bool {
	d := json.NewDecoder(strings.NewReader(string(raw)))
	var walk func() bool
	walk = func() bool {
		t, e := d.Token()
		if e != nil {
			return false
		}
		del, ok := t.(json.Delim)
		if !ok {
			return false
		}
		switch del {
		case '{':
			seen := map[string]bool{}
			for d.More() {
				k, e := d.Token()
				if e != nil {
					return true
				}
				key, ok := k.(string)
				if !ok || seen[key] {
					return true
				}
				seen[key] = true
				if walk() {
					return true
				}
			}
			_, e = d.Token()
			return e != nil
		case '[':
			for d.More() {
				if walk() {
					return true
				}
			}
			_, e = d.Token()
			return e != nil
		}
		return false
	}
	return walk()
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
