package management

import (
	"context"
	"encoding/json"
)

type configurationReader interface {
	Configuration(context.Context, string, string, json.RawMessage) (json.RawMessage, error)
}

// NewConfigurationPreflight prevents configuration errors from becoming queued
// article attempts. It runs before the queue opens any write transaction.
func NewConfigurationPreflight(reader configurationReader) func(context.Context, string, map[string]any) error {
	return func(ctx context.Context, kind string, payload map[string]any) error {
		needsAI, needsTelegram := false, false
		switch kind {
		case "news_processing", "article_reprocess", "reprocess_failed", "summaries_generate":
			needsAI = true
		case "news_digest":
			needsAI = true
			needsTelegram = true
			if send, ok := payload["send_telegram"].(bool); ok {
				needsTelegram = send
			}
		case "telegram_digest":
			needsTelegram = true
		}
		if !needsAI && !needsTelegram {
			return nil
		}
		raw, err := reader.Configuration(ctx, "GET", "readiness", nil)
		if err != nil {
			return &APIError{Status: 503, Detail: "Integration configuration unavailable"}
		}
		var state struct {
			AI         bool  `json:"ai_configured"`
			Telegram   bool  `json:"telegram_configured"`
			Processing *bool `json:"processing_enabled"`
			Delivery   *bool `json:"delivery_enabled"`
		}
		if json.Unmarshal(raw, &state) != nil {
			return &APIError{Status: 503, Detail: "Integration configuration unavailable"}
		}
		if state.Processing != nil && !*state.Processing {
			switch kind {
			case "news_processing":
				needsAI = false // Collection remains available.
			case "article_reprocess", "reprocess_failed":
				return &APIError{Status: 409, Detail: "Article processing is disabled"}
			}
		}
		if needsTelegram && state.Delivery != nil && !*state.Delivery {
			return &APIError{Status: 409, Detail: "Telegram delivery is disabled"}
		}
		if needsAI && !state.AI {
			return &APIError{Status: 409, Detail: "Configure AI before starting processing"}
		}
		if needsTelegram && !state.Telegram {
			return &APIError{Status: 409, Detail: "Configure Telegram before starting delivery"}
		}
		return nil
	}
}
