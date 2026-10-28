package airuntime

import (
	"encoding/json"
	"net/http"

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
	if (v.Channel != "main" && v.Channel != "service") || (v.Method != "sendMessage" && v.Method != "sendRichMessage") || v.Text == "" || len(v.Text) > 1<<20 || s.config.TelegramToken == "" {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
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
		if queryErr != nil {
			mainOverride = ""
			serviceOverride = ""
		}
	}

	main, service, err := digestworkflow.Destinations(mainOverride, serviceOverride, s.config.TelegramMain, s.config.TelegramService)
	if err != nil {
		writeJSON(w, 400, errorEnvelope("invalid_input", 400))
		return
	}
	chat := main
	if v.Channel == "service" {
		chat = service
	}
	var payload any
	if v.Method == "sendRichMessage" {
		if len(v.Keyboard) > 0 {
			writeJSON(w, 400, errorEnvelope("invalid_input", 400))
			return
		}
		payload = map[string]any{"chat_id": chat, "rich_message": map[string]string{"html": v.Text}}
	} else {
		data := map[string]any{"chat_id": chat, "text": v.Text, "parse_mode": "HTML", "disable_web_page_preview": true}
		if len(v.Keyboard) > 0 {
			var keyboard any
			if json.Unmarshal(v.Keyboard, &keyboard) != nil {
				writeJSON(w, 400, errorEnvelope("invalid_input", 400))
				return
			}
			data["reply_markup"] = map[string]any{"inline_keyboard": keyboard}
		}
		payload = data
	}
	result := (telegramdelivery.Transport{Client: s.client, Admission: s.limiter, Endpoint: s.config.TelegramEndpoint, Token: s.config.TelegramToken}).Send(ctx, v.Method, payload)
	writeJSON(w, 200, map[string]any{"version": 1, "ok": true, "result": map[string]any{"state": result.State, "status_code": result.StatusCode, "error_code": result.ErrorCode, "message_id": result.MessageID, "retry_after_seconds": result.RetryAfterSeconds}})
}
