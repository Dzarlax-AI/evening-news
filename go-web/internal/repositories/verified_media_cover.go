package repositories

import (
	"encoding/json"
	"net/url"
)

// projectArticleMediaWithCover keeps a verified editorial cover separate from
// gallery items and never treats the legacy image_url as evidence.
func projectArticleMediaWithCover(raw []any, sourceType, articleURL string, evidence []byte) ([]any, *string) {
	media, cover := projectArticleMedia(raw, sourceType)
	if sourceType == "rss" || len(evidence) == 0 || len(evidence) > 8192 {
		return media, cover
	}
	var item struct {
		Version int `json:"version"`
		Cover   struct {
			URL        string `json:"url"`
			Source     string `json:"source"`
			ArticleURL string `json:"article_url"`
		} `json:"editorial_cover"`
	}
	if json.Unmarshal(evidence, &item) != nil || item.Version != 1 ||
		(item.Cover.Source != "og_image" && item.Cover.Source != "selector_study") || item.Cover.ArticleURL != articleURL {
		return media, cover
	}
	u, err := url.Parse(item.Cover.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return media, cover
	}
	return media, &item.Cover.URL
}
