package domainops

import (
	"encoding/json"
	"errors"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
)

func mediaArticleID(raw json.RawMessage, c authority.ExecutionContext) (int64, error) {
	invalid := errors.New("media refresh not authorized")
	if c.Kind != "article_reprocess" {
		return 0, invalid
	}
	var input map[string]json.RawMessage
	if json.Unmarshal(raw, &input) != nil || len(input) != 1 {
		return 0, invalid
	}
	var id int64
	if json.Unmarshal(input["article_id"], &id) != nil || id <= 0 {
		return 0, invalid
	}
	var payload struct {
		ArticleIDs     []int64 `json:"article_ids"`
		ReextractMedia bool    `json:"reextract_media"`
	}
	if json.Unmarshal(c.Payload, &payload) != nil || !payload.ReextractMedia {
		return 0, invalid
	}
	for _, allowed := range payload.ArticleIDs {
		if allowed == id {
			return id, nil
		}
	}
	return 0, invalid
}
func sourceID(raw json.RawMessage, c authority.ExecutionContext) (int64, error) {
	invalid := errors.New("source test not authorized")
	if c.Kind != "source_test" {
		return 0, invalid
	}
	var input map[string]json.RawMessage
	if json.Unmarshal(raw, &input) != nil || len(input) != 1 {
		return 0, invalid
	}
	var id int64
	if json.Unmarshal(input["source_id"], &id) != nil || id <= 0 {
		return 0, invalid
	}
	var payload struct {
		SourceID int64 `json:"source_id"`
	}
	if json.Unmarshal(c.Payload, &payload) != nil || payload.SourceID != id {
		return 0, invalid
	}
	return id, nil
}
