package airuntime

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/appsettings"
	"github.com/Dzarlax-AI/evening-news/go-domain/digestworkflow"
	"github.com/Dzarlax-AI/evening-news/go-domain/telegramdelivery"
)

type telegramRequest struct {
	commonRequest
	Channel  string          `json:"channel"`
	Method   string          `json:"method"`
	Text     string          `json:"text"`
	Keyboard json.RawMessage `json:"keyboard,omitempty"`
}

// telegramSend is for non-digest producers whose caller already owns alert
// lifecycle. Digest sends require the separate durable receipt path.
func (s *Server) telegramSend(w http.ResponseWriter, r *http.Request) {
	var v telegramRequest
	if !decodeTyped(r, &v) {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	ctx, cancel, ok := s.contextFor(r, w, v.commonRequest)
	if !ok {
		return
	}
	defer cancel()
	result, err := s.sendTelegram(ctx, v)
	if err != nil {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": map[string]any{"state": result.State, "status_code": result.StatusCode, "error_code": result.ErrorCode, "message_id": result.MessageID, "retry_after_seconds": result.RetryAfterSeconds}})
}

// SendServiceTest shares the existing Telegram rate limiter and cancellation
// policy. A caller must track this operation during drain.
func (s *Server) SendServiceTest(ctx context.Context, text string) (telegramdelivery.Result, error) {
	if s.draining.Load() {
		return telegramdelivery.Result{}, errors.New("runtime_draining")
	}
	if deadline, ok := ctx.Deadline(); !ok || !deadline.After(time.Now()) {
		return telegramdelivery.Result{}, errors.New("invalid_deadline")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	stop := make(chan struct{})
	go func() {
		select {
		case <-s.closing:
			cancel()
		case <-ctx.Done():
		case <-stop:
		}
	}()
	defer func() { close(stop); cancel() }()
	return s.sendTelegram(ctx, telegramRequest{Channel: "service", Method: "sendMessage", Text: text})
}
func (s *Server) sendTelegram(ctx context.Context, v telegramRequest) (telegramdelivery.Result, error) {
	if (v.Channel != "main" && v.Channel != "service") || (v.Method != "sendMessage" && v.Method != "sendRichMessage") || v.Text == "" || len(v.Text) > 1<<20 {
		return telegramdelivery.Result{}, errors.New("invalid_input")
	}
	token := s.config.TelegramToken
	if s.credentials != nil {
		secret, _, err := s.credentials.Resolve(ctx, "telegram")
		if err != nil {
			return telegramdelivery.Result{}, err
		}
		token = string(secret)
	}
	if token == "" {
		return telegramdelivery.Result{}, errors.New("invalid_input")
	}
	var mainOverride, serviceOverride string
	// A failed settings lookup occurs before any provider request. Match the
	// existing alert fallback by using the configured defaults in that case.
	if s.db != nil {
		rows, queryErr := s.db.Query(ctx, `SELECT key,CASE WHEN jsonb_typeof(value::jsonb)='string' THEN value::jsonb #>> '{}' ELSE NULL END FROM settings WHERE key IN ('telegram_chat_id','telegram_service_chat_id')`)
		if queryErr == nil {
			for rows.Next() {
				var key string
				var value *string
				if queryErr = rows.Scan(&key, &value); queryErr != nil {
					break
				}
				if value != nil {
					if key == "telegram_chat_id" {
						mainOverride = *value
					} else {
						serviceOverride = *value
					}
				}
			}
			if queryErr == nil {
				queryErr = rows.Err()
			}
			rows.Close()
		}
		if queryErr != nil && s.credentials != nil {
			return telegramdelivery.Result{}, errors.New("settings_unavailable")
		}
		if queryErr != nil {
			mainOverride = ""
			serviceOverride = ""
		}
	}

	mainDefault, serviceDefault := s.config.TelegramMain, s.config.TelegramService
	if s.credentials != nil {
		// Empty managed destinations are authoritative, including after deletion.
		mainDefault, serviceDefault = "", ""
	}
	main, service, err := digestworkflow.Destinations(mainOverride, serviceOverride, mainDefault, serviceDefault)
	if err != nil {
		return telegramdelivery.Result{}, errors.New("invalid_input")
	}
	chat := main
	if v.Channel == "service" {
		chat = service
	}
	var payload any
	if v.Method == "sendRichMessage" {
		if len(v.Keyboard) > 0 {
			return telegramdelivery.Result{}, errors.New("invalid_input")
		}
		payload = map[string]any{"chat_id": chat, "rich_message": map[string]string{"html": v.Text}}
	} else {
		data := map[string]any{"chat_id": chat, "text": v.Text, "parse_mode": "HTML", "disable_web_page_preview": true}
		if len(v.Keyboard) > 0 {
			var keyboard any
			if json.Unmarshal(v.Keyboard, &keyboard) != nil {
				return telegramdelivery.Result{}, errors.New("invalid_input")
			}
			data["reply_markup"] = map[string]any{"inline_keyboard": keyboard}
		}
		payload = data
	}
	return (telegramdelivery.Transport{Client: s.client, Admission: s.limiter, Endpoint: s.config.TelegramEndpoint, Token: token}).Send(ctx, v.Method, payload), nil
}

// managedDestination is read before provider admission and does not revive a
// legacy environment destination after an operator clears managed settings.
func (s *Server) managedDestination(ctx context.Context) (string, error) {
	settings, err := (appsettings.Store{Pool: s.db}).Telegram(ctx)
	if err != nil {
		return "", err
	}
	main, _, err := digestworkflow.Destinations(settings.ChatID, settings.ServiceChatID, "", "")
	return main, err
}
