package webbackend

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
	"io"
	"net/http"
	"time"
)

// RuntimeConfiguration exposes only non-secret control-plane state to the
// independently authenticated background purpose boundary.
func (s *Server) RuntimeConfiguration(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	var paused bool
	if s.Credentials == nil || s.Pool == nil || s.Pool.QueryRow(ctx, `SELECT maintenance_mode FROM installation_state WHERE id=1`).Scan(&paused) != nil {
		reject(w, 503, "Runtime configuration unavailable")
		return
	}
	write(w, 200, map[string]bool{"maintenance_mode": paused})
}

func (s *Server) telegramConfiguration(w http.ResponseWriter, r *http.Request) {
	store := appsettings.Store{Pool: s.Pool}
	if r.Method == http.MethodGet {
		v, err := store.Telegram(r.Context())
		if err != nil {
			reject(w, 503, "Settings unavailable")
			return
		}
		write(w, 200, v)
		return
	}
	var in struct {
		ExpectedRevision *int64  `json:"expected_revision"`
		ChatID           *string `json:"chat_id"`
		ServiceChatID    *string `json:"service_chat_id"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	d.DisallowUnknownFields()
	if d.Decode(&in) != nil || d.Decode(new(any)) != io.EOF || in.ExpectedRevision == nil || in.ChatID == nil || in.ServiceChatID == nil {
		reject(w, 422, "Invalid settings request")
		return
	}
	v, err := store.UpdateTelegram(r.Context(), appsettings.TelegramUpdate{ExpectedRevision: *in.ExpectedRevision, ChatID: *in.ChatID, ServiceChatID: *in.ServiceChatID})
	switch {
	case errors.Is(err, appsettings.ErrInvalid):
		reject(w, 422, "Invalid settings request")
	case errors.Is(err, appsettings.ErrConflict):
		reject(w, 409, "Settings changed; reload before saving")
	case err != nil:
		reject(w, 503, "Settings unavailable")
	default:
		write(w, 200, v)
	}
}

func (s *Server) operationalConfiguration(w http.ResponseWriter, r *http.Request) {
	store := appsettings.Store{Pool: s.Pool}
	if r.Method == http.MethodGet {
		v, err := store.Operational(r.Context())
		if err != nil {
			reject(w, 503, "Settings unavailable")
			return
		}
		write(w, 200, v)
		return
	}
	var in struct {
		ExpectedRevision  *int64  `json:"expected_revision"`
		LimitsEnabled     *bool   `json:"limits_enabled"`
		MaxArticles       *int    `json:"max_articles"`
		PerSource         *int    `json:"per_source"`
		Days              *int    `json:"days"`
		NewestDate        *string `json:"newest_date"`
		ProcessingEnabled *bool   `json:"processing_enabled"`
		DeliveryEnabled   *bool   `json:"delivery_enabled"`
	}
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096))
	d.DisallowUnknownFields()
	if d.Decode(&in) != nil || d.Decode(new(any)) != io.EOF || in.ExpectedRevision == nil || in.LimitsEnabled == nil || in.MaxArticles == nil || in.PerSource == nil || in.Days == nil || in.NewestDate == nil || in.ProcessingEnabled == nil || in.DeliveryEnabled == nil {
		reject(w, 422, "Invalid settings request")
		return
	}
	v, err := store.UpdateOperational(r.Context(), appsettings.OperationalUpdate{ExpectedRevision: *in.ExpectedRevision, LimitsEnabled: *in.LimitsEnabled, MaxArticles: *in.MaxArticles, PerSource: *in.PerSource, Days: *in.Days, NewestDate: *in.NewestDate, ProcessingEnabled: *in.ProcessingEnabled, DeliveryEnabled: *in.DeliveryEnabled})
	switch {
	case errors.Is(err, appsettings.ErrInvalid):
		reject(w, 422, "Invalid settings request")
	case errors.Is(err, appsettings.ErrConflict):
		reject(w, 409, "Settings changed; reload before saving")
	case err != nil:
		reject(w, 503, "Settings unavailable")
	default:
		write(w, 200, v)
	}
}
func (s *Server) configurationReadiness(w http.ResponseWriter, r *http.Request) {
	if s.Runtime == nil || s.Credentials == nil {
		reject(w, 503, "Settings unavailable")
		return
	}
	err := s.Runtime.ValidateAISettings(r.Context())
	if err != nil && !errors.Is(err, airuntime.ErrAISettingsInvalid) {
		reject(w, 503, "Settings unavailable")
		return
	}
	ai := err == nil
	tg, err := s.Credentials.Metadata(r.Context(), "telegram")
	if err != nil {
		reject(w, 503, "Settings unavailable")
		return
	}
	v, err := (appsettings.Store{Pool: s.Pool}).Telegram(r.Context())
	if err != nil {
		reject(w, 503, "Settings unavailable")
		return
	}
	operational, err := (appsettings.Store{Pool: s.Pool}).Operational(r.Context())
	if err != nil {
		reject(w, 503, "Settings unavailable")
		return
	}
	write(w, 200, map[string]bool{"ai_configured": ai, "telegram_configured": tg.Configured && v.ChatID != "", "processing_enabled": operational.ProcessingEnabled, "delivery_enabled": operational.DeliveryEnabled})
}
