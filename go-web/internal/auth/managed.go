package auth

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/transport"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/argon2"
)

// ManagedStore owns only local administrator and first-claim state.
type ManagedStore struct {
	Pool           *pgxpool.Pool
	InstallationID string
	jobs           chan struct{}
}

var errInvalidClaim = errors.New("invalid claim")
var errClaimUnavailable = errors.New("claim unavailable")
var errPasswordBusy = errors.New("password verifier busy")

func NewManagedStore(pool *pgxpool.Pool, id string) *ManagedStore {
	return &ManagedStore{Pool: pool, InstallationID: id, jobs: make(chan struct{}, 2)}
}
func (m *ManagedStore) hash(password string, salt []byte) ([]byte, error) {
	select {
	case m.jobs <- struct{}{}:
	default:
		return nil, errPasswordBusy
	}
	defer func() { <-m.jobs }()
	return argon2.IDKey([]byte(password), salt, 2, 19456, 1, 32), nil
}
func (m *ManagedStore) passwordHash(password string) (string, error) {
	salt := make([]byte, 16)
	if _, err := rand.Read(salt); err != nil {
		return "", err
	}
	h, err := m.hash(password, salt)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("$argon2id$v=19$m=19456,t=2,p=1$%s$%s", base64.RawStdEncoding.EncodeToString(salt), base64.RawStdEncoding.EncodeToString(h)), nil
}
func (m *ManagedStore) check(password, encoded string) bool {
	p := strings.Split(encoded, "$")
	if len(p) != 6 || p[1] != "argon2id" || p[2] != "v=19" || p[3] != "m=19456,t=2,p=1" {
		return false
	}
	salt, e := base64.RawStdEncoding.DecodeString(p[4])
	if e != nil || len(salt) != 16 {
		return false
	}
	want, e := base64.RawStdEncoding.DecodeString(p[5])
	if e != nil || len(want) != 32 {
		return false
	}
	got, err := m.hash(password, salt)
	return err == nil && subtle.ConstantTimeCompare(got, want) == 1
}
func (m *ManagedStore) account(ctx context.Context) (username, hash string, revision int64, created time.Time, err error) {
	err = m.Pool.QueryRow(ctx, `SELECT username,password_hash,revision,created_at FROM managed_admin WHERE id=1`).Scan(&username, &hash, &revision, &created)
	return
}
func (m *ManagedStore) credentials(user, password string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	name, hash, _, _, err := m.account(ctx)
	return err == nil && subtle.ConstantTimeCompare([]byte(user), []byte(name)) == 1 && m.check(password, hash)
}
func (m *ManagedStore) claim(ctx context.Context, token, username, password string) error {
	if len(token) < 32 || len(token) > 512 || len(username) < 1 || len(username) > 128 || strings.TrimSpace(username) != username || len(password) < 12 || len(password) > 1024 {
		return errInvalidClaim
	}
	hash, err := m.passwordHash(password)
	if err != nil {
		return err
	}
	tokenHash := sha256.Sum256([]byte(token))
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var id string
	if err = tx.QueryRow(ctx, `SELECT installation_id FROM installation_state WHERE id=1 FOR UPDATE`).Scan(&id); err != nil {
		return err
	}
	if id != m.InstallationID {
		return errors.New("installation unavailable")
	}
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM managed_admin WHERE id=1)`).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return errClaimUnavailable
	}
	var stored []byte
	var expires time.Time
	var consumed *time.Time
	if err = tx.QueryRow(ctx, `SELECT token_hash,expires_at,consumed_at FROM setup_tokens WHERE id=1 FOR UPDATE`).Scan(&stored, &expires, &consumed); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return errClaimUnavailable
		}
		return err
	}
	if consumed != nil || !time.Now().Before(expires) || subtle.ConstantTimeCompare(tokenHash[:], stored) != 1 {
		return errClaimUnavailable
	}
	if _, err = tx.Exec(ctx, `INSERT INTO managed_admin(id,username,password_hash) VALUES(1,$1,$2)`, username, hash); err != nil {
		return err
	}
	if _, err = tx.Exec(ctx, `UPDATE setup_tokens SET consumed_at=now() WHERE id=1`); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
func (m *ManagedStore) validRevision(p Principal, claims map[string]any) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	name, _, rev, created, err := m.account(ctx)
	if err != nil || name != p.Username {
		return false
	}
	value, ok := claims["revision"].(float64)
	if !ok {
		return rev == 1 && time.Now().Before(created.Add(TokenTTL))
	}
	return value == float64(rev)
}
func (m *ManagedStore) revision(username string) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	name, _, rev, _, err := m.account(ctx)
	if err != nil || username != name {
		return 0, errors.New("account unavailable")
	}
	return rev, nil
}
func (m *ManagedStore) configured(ctx context.Context) (bool, error) {
	_, _, _, _, err := m.account(ctx)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}
func (s *Service) managedClaim(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if s.managed == nil {
		http.NotFound(w, r)
		return
	}
	if !transport.SecureRequest(r) {
		writeJSON(w, 403, map[string]string{"detail": "HTTPS required"})
		return
	}
	if crossSite(r) || r.Header.Get("Origin") == "" || r.Header.Get("X-CSRF-Token") != "1" {
		writeJSON(w, 403, map[string]string{"detail": "Cross-site request rejected"})
		return
	}
	ip := remoteIP(r)
	s.mu.Lock()
	now := s.now()
	s.prune(now)
	allowed := s.allow(ip, now, 5)
	s.mu.Unlock()
	if !allowed {
		writeJSON(w, 429, map[string]string{"detail": "Rate limit exceeded"})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var input struct {
		Token    string `json:"token"`
		Username string `json:"username"`
		Password string `json:"password"`
	}
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if dec.Decode(&input) != nil || dec.Decode(new(any)) != io.EOF {
		writeJSON(w, 422, map[string]string{"detail": "Invalid request"})
		return
	}
	if err := s.managed.claim(r.Context(), input.Token, input.Username, input.Password); err != nil {
		status := http.StatusServiceUnavailable
		if errors.Is(err, errPasswordBusy) {
			status = http.StatusTooManyRequests
		}
		if errors.Is(err, errInvalidClaim) {
			status = http.StatusUnprocessableEntity
		}
		if errors.Is(err, errClaimUnavailable) {
			status = http.StatusConflict
		}
		writeJSON(w, status, map[string]string{"detail": "Claim unavailable or invalid"})
		return
	}
	cookie, err := s.AdminCookie(input.Username)
	if err != nil {
		writeJSON(w, 503, map[string]string{"detail": "Session unavailable"})
		return
	}
	http.SetCookie(w, cookie)
	writeJSON(w, 201, map[string]string{"status": "created"})
}
func (s *Service) managedStatus(w http.ResponseWriter, r *http.Request) {
	if s.managed == nil {
		http.NotFound(w, r)
		return
	}
	configured, err := s.managed.configured(r.Context())
	if err != nil {
		writeJSON(w, 503, map[string]string{"detail": "Setup unavailable"})
		return
	}
	writeJSON(w, 200, map[string]any{"admin_configured": configured})
}
