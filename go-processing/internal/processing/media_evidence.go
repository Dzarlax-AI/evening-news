package processing

import (
	"encoding/json"
	"net/url"
)

func validatedMediaEvidence(raw json.RawMessage, articleURL string) json.RawMessage {
	if len(raw) == 0 || len(raw) > 8192 {
		return nil
	}
	var evidence struct {
		Version int `json:"version"`
		Cover   struct {
			URL        string `json:"url"`
			Source     string `json:"source"`
			ArticleURL string `json:"article_url"`
		} `json:"editorial_cover"`
	}
	if json.Unmarshal(raw, &evidence) != nil || evidence.Version != 1 ||
		evidence.Cover.Source != "og_image" || evidence.Cover.ArticleURL != articleURL {
		return nil
	}
	u, err := url.Parse(evidence.Cover.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return nil
	}
	canonical, err := json.Marshal(evidence)
	// Reserve space for PostgreSQL's JSONB text formatting.
	if err != nil || len(canonical) > 8000 {
		return nil
	}
	return canonical
}
