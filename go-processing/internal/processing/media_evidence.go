package processing

import (
	"encoding/json"
	"net/url"
	"strings"
)

func validatedMediaEvidence(raw json.RawMessage, articleURL string) json.RawMessage {
	if len(raw) == 0 || len(raw) > 8192 {
		return nil
	}
	var evidence struct {
		Version      int    `json:"version"`
		ArticleScope string `json:"article_scope,omitempty"`
		ArticleURL   string `json:"article_url,omitempty"`
		Cover        *struct {
			URL        string `json:"url"`
			Source     string `json:"source"`
			ArticleURL string `json:"article_url"`
		} `json:"editorial_cover,omitempty"`
	}
	if json.Unmarshal(raw, &evidence) != nil || evidence.Version != 1 {
		return nil
	}
	validScope := (evidence.ArticleScope == "verified" || evidence.ArticleScope == "unknown") && evidence.ArticleURL == articleURL
	if !validScope {
		evidence.ArticleScope, evidence.ArticleURL = "", ""
	}
	if evidence.Cover != nil {
		u, err := url.Parse(evidence.Cover.URL)
		if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" ||
			(evidence.Cover.Source != "og_image" && evidence.Cover.Source != "selector_study") || evidence.Cover.ArticleURL != articleURL ||
			evidence.ArticleScope == "unknown" {
			evidence.Cover = nil
		}
	}
	if !validScope && evidence.Cover == nil {
		return nil
	}
	canonical, err := json.Marshal(evidence)
	// Reserve space for PostgreSQL's JSONB text formatting.
	if err != nil || len(canonical) > 8000 {
		return nil
	}
	return canonical
}

// Accept the complete rendered result only when it belongs to the same page
// and improves the gallery without discarding already confirmed media or text.
func betterEditorialSnapshot(original, rendered Extracted) bool {
	if rendered.Outcome != "success" || rendered.Error != "" || rendered.pendingEditorialMedia ||
		original.documentURL == "" || original.documentURL != rendered.documentURL ||
		!strings.EqualFold(strings.Join(strings.Fields(original.Title), " "), strings.Join(strings.Fields(rendered.Title), " ")) ||
		len([]rune(rendered.Content))*10 < len([]rune(original.Content))*9 ||
		!sharesEditorialText(original.Content, rendered.Content) {
		return false
	}
	media := confirmedEditorialURLs(rendered.MediaFiles)
	for u, kind := range confirmedEditorialURLs(original.MediaFiles) {
		if media[u] != kind {
			return false
		}
		delete(media, u)
	}
	for _, kind := range media {
		if kind == "image" {
			return true
		}
	}
	return false
}

func sharesEditorialText(original, rendered string) bool {
	words := strings.Fields(original)
	if len(words) < 24 {
		return false
	}
	text := strings.Join(strings.Fields(rendered), " ")
	first := strings.Join(words[:12], " ")
	mid := len(words) / 2
	last := strings.Join(words[mid:mid+12], " ")
	return strings.Contains(text, first) && strings.Contains(text, last)
}

func confirmedEditorialURLs(files []map[string]any) map[string]string {
	out := make(map[string]string)
	for _, item := range files {
		data, _ := item["data"].(map[string]any)
		order, _ := data["editorial_order"].(float64)
		width, _ := data["width"].(float64)
		height, _ := data["height"].(float64)
		if order < 1 || data["decorative"] == true || (width > 0 && width <= 64 && height > 0 && height <= 64) {
			continue
		}
		u, _ := item["url"].(string)
		kind, _ := item["type"].(string)
		if u != "" {
			out[u] = kind
		}
	}
	return out
}
