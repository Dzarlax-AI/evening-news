package appsettings

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

// Viber contains non-secret configuration and verification state.
type Viber struct {
	Revision                   int64      `json:"revision"`
	DeliveryEnabled            bool       `json:"delivery_enabled"`
	ChannelID                  string     `json:"channel_id"`
	ChannelName                string     `json:"channel_name"`
	SenderID                   string     `json:"sender_id"`
	CredentialRevision         int64      `json:"credential_revision"`
	VerifiedCredentialRevision int64      `json:"verified_credential_revision"`
	WebhookReady               bool       `json:"webhook_ready"`
	VerifiedAt                 *time.Time `json:"verified_at"`
	EnabledFrom                *time.Time `json:"enabled_from"`
}
type ViberUpdate struct {
	ExpectedRevision           int64
	DeliveryEnabled            bool
	ChannelID                  string
	ChannelName                string
	SenderID                   string
	CredentialRevision         int64
	VerifiedCredentialRevision int64
	WebhookReady               bool
	VerifiedAt                 *time.Time
}

func (s Store) Viber(ctx context.Context) (Viber, error) {
	var v Viber
	if s.Pool == nil {
		return v, errors.New("settings unavailable")
	}
	var raw []byte
	err := s.Pool.QueryRow(ctx, `SELECT value FROM settings WHERE key='viber_settings'`).Scan(&raw)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, nil
	}
	if err != nil {
		return v, err
	}
	err = json.Unmarshal(raw, &v)
	return v, err
}

// UpdateViber accepts trusted server-side verification data; untrusted clients must not populate it.
func (s Store) UpdateViber(ctx context.Context, in ViberUpdate) (Viber, error) {
	var v Viber
	if s.Pool == nil {
		return v, errors.New("settings unavailable")
	}
	if in.ExpectedRevision < 0 || in.CredentialRevision < 0 || in.VerifiedCredentialRevision < 0 || !validID(in.ChannelID) || !validID(in.SenderID) || len(in.ChannelName) > 256 || strings.TrimSpace(in.ChannelName) != in.ChannelName {
		return v, ErrInvalid
	}
	if in.DeliveryEnabled && (in.ChannelID == "" || in.SenderID == "" || in.CredentialRevision == 0 || in.VerifiedCredentialRevision != in.CredentialRevision || !in.WebhookReady || in.VerifiedAt == nil) {
		return v, ErrInvalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return v, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728441)`); err != nil {
		return v, err
	}
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT value FROM settings WHERE key='viber_settings'`).Scan(&raw)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return v, err
	}
	if err == nil {
		if err = json.Unmarshal(raw, &v); err != nil {
			return v, err
		}
	}
	if v.Revision != in.ExpectedRevision {
		return Viber{}, ErrConflict
	}
	if in.DeliveryEnabled || in.WebhookReady || in.VerifiedCredentialRevision > 0 || in.VerifiedAt != nil {
		if in.CredentialRevision == 0 || in.VerifiedCredentialRevision != in.CredentialRevision || in.VerifiedAt == nil || in.ChannelID == "" {
			return Viber{}, ErrInvalid
		}
		var actual int64
		var configured bool
		err = tx.QueryRow(ctx, `SELECT revision,ciphertext IS NOT NULL FROM integration_credentials WHERE provider='viber' FOR SHARE`).Scan(&actual, &configured)
		if errors.Is(err, pgx.ErrNoRows) {
			return Viber{}, ErrInvalid
		}
		if err != nil {
			return Viber{}, err
		}
		if !configured || actual != in.CredentialRevision {
			return Viber{}, ErrInvalid
		}
	}
	if v.DeliveryEnabled && !in.DeliveryEnabled {
		if _, err = tx.Exec(ctx, `UPDATE viber_digest_deliveries SET state='cancelled',updated_at=now() WHERE state IN ('waiting_edition','ready','retry_wait')`); err != nil {
			return Viber{}, err
		}
	}
	enabledFrom := v.EnabledFrom
	if in.DeliveryEnabled && !v.DeliveryEnabled {
		now := time.Now().UTC().Truncate(time.Microsecond)
		enabledFrom = &now
	}
	if !in.DeliveryEnabled {
		enabledFrom = nil
	}
	v = Viber{Revision: v.Revision + 1, DeliveryEnabled: in.DeliveryEnabled, ChannelID: in.ChannelID, ChannelName: in.ChannelName, SenderID: in.SenderID, CredentialRevision: in.CredentialRevision, VerifiedCredentialRevision: in.VerifiedCredentialRevision, WebhookReady: in.WebhookReady, VerifiedAt: in.VerifiedAt, EnabledFrom: enabledFrom}
	raw, err = json.Marshal(v)
	if err != nil {
		return Viber{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES('viber_settings',$1::jsonb,now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, raw); err != nil {
		return Viber{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Viber{}, err
	}
	return v, nil
}
