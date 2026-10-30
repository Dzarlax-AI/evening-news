// Package appsettings owns typed non-secret operational settings.
package appsettings

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"strings"
	"unicode"
)

var ErrInvalid = errors.New("invalid settings")
var ErrConflict = errors.New("settings revision conflict")

type Telegram struct {
	Revision               int64  `json:"revision"`
	ChatID                 string `json:"chat_id"`
	ServiceChatID          string `json:"service_chat_id"`
	SerbianChatID          string `json:"serbian_chat_id"`
	SerbianDeliveryEnabled bool   `json:"serbian_delivery_enabled"`
}
type TelegramUpdate struct {
	ExpectedRevision       int64  `json:"expected_revision"`
	ChatID                 string `json:"chat_id"`
	ServiceChatID          string `json:"service_chat_id"`
	SerbianChatID          string `json:"serbian_chat_id"`
	SerbianDeliveryEnabled bool   `json:"serbian_delivery_enabled"`
}
type Store struct{ Pool *pgxpool.Pool }

func (s Store) Telegram(ctx context.Context) (Telegram, error) {
	var v Telegram
	if s.Pool == nil {
		return v, errors.New("settings unavailable")
	}
	return telegramInTx(ctx, s.Pool)
}

type telegramReader interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func telegramInTx(ctx context.Context, q telegramReader) (Telegram, error) {
	var v Telegram
	err := q.QueryRow(ctx, `SELECT COALESCE((SELECT (value #>> '{}')::bigint FROM settings WHERE key='telegram_settings_revision'),0),COALESCE((SELECT value #>> '{}' FROM settings WHERE key='telegram_chat_id'),''),COALESCE((SELECT value #>> '{}' FROM settings WHERE key='telegram_service_chat_id'),''),COALESCE((SELECT value #>> '{}' FROM settings WHERE key='telegram_serbian_chat_id'),''),COALESCE((SELECT (value #>> '{}')::boolean FROM settings WHERE key='telegram_serbian_delivery_enabled'),false)`).Scan(&v.Revision, &v.ChatID, &v.ServiceChatID, &v.SerbianChatID, &v.SerbianDeliveryEnabled)
	return v, err
}

func validID(s string) bool {
	if len(s) > 128 || strings.TrimSpace(s) != s {
		return false
	}
	for _, r := range s {
		if unicode.IsControl(r) || unicode.IsSpace(r) {
			return false
		}
	}
	return true
}
func (s Store) UpdateTelegram(ctx context.Context, in TelegramUpdate) (Telegram, error) {
	if ValidateTelegram(in) != nil {
		return Telegram{}, ErrInvalid
	}
	if s.Pool == nil {
		return Telegram{}, errors.New("settings unavailable")
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Telegram{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(1649258121,728439)`); err != nil {
		return Telegram{}, err
	}
	var rev int64
	err = tx.QueryRow(ctx, `SELECT (value #>> '{}')::bigint FROM settings WHERE key='telegram_settings_revision'`).Scan(&rev)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return Telegram{}, err
	}
	if rev != in.ExpectedRevision {
		return Telegram{}, ErrConflict
	}
	for _, pair := range [][2]any{{"telegram_chat_id", in.ChatID}, {"telegram_service_chat_id", in.ServiceChatID}, {"telegram_serbian_chat_id", in.SerbianChatID}, {"telegram_serbian_delivery_enabled", in.SerbianDeliveryEnabled}} {
		raw, marshalErr := json.Marshal(pair[1])
		if marshalErr != nil {
			return Telegram{}, marshalErr
		}
		if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES($1,$2::jsonb,now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, pair[0], raw); err != nil {
			return Telegram{}, err
		}
	}
	rev++
	if _, err = tx.Exec(ctx, `INSERT INTO settings(key,value,updated_at) VALUES('telegram_settings_revision',to_jsonb($1::bigint),now()) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at`, rev); err != nil {
		return Telegram{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return Telegram{}, err
	}
	return Telegram{Revision: rev, ChatID: in.ChatID, ServiceChatID: in.ServiceChatID, SerbianChatID: in.SerbianChatID, SerbianDeliveryEnabled: in.SerbianDeliveryEnabled}, nil
}

// ValidateTelegram prevents a Serbian publication from sharing a destination
// with Russian news or service notifications.
func ValidateTelegram(in TelegramUpdate) error {
	if in.ExpectedRevision < 0 || !validID(in.ChatID) || !validID(in.ServiceChatID) || !validID(in.SerbianChatID) {
		return ErrInvalid
	}
	if in.SerbianDeliveryEnabled && (in.SerbianChatID == "" || in.ChatID == "" || strings.EqualFold(in.SerbianChatID, in.ChatID) || strings.EqualFold(in.SerbianChatID, in.ServiceChatID)) {
		return ErrInvalid
	}
	return nil
}
