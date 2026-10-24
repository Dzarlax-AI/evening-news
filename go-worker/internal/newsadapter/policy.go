package newsadapter

import (
	"encoding/json"
)

type QueuePolicy interface {
	KnownKind(string) bool
	MayDeliver(kind string, payload json.RawMessage) bool
}

type Adapter struct{}

func (Adapter) KnownKind(kind string) bool {
	switch kind {
	case "news_processing", "news_digest", "telegram_digest", "backup", "reprocess_failed", "article_reprocess", "summaries_generate", "source_test":
		return true
	default:
		return false
	}
}

func (Adapter) MayDeliver(kind string, raw json.RawMessage) bool {
	if kind == "telegram_digest" {
		return true
	}
	if kind != "news_digest" {
		return false
	}
	var payload map[string]any
	if json.Unmarshal(raw, &payload) != nil {
		return true
	}
	return payload["send_telegram"] != false
}
