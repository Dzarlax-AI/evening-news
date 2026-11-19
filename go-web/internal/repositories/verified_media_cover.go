package repositories

import (
	"encoding/json"
	"net/url"
	"sort"
)

// projectArticleMediaWithCover keeps a verified editorial cover separate from
// gallery items and never treats the legacy image_url as evidence.
func projectArticleMediaWithCover(raw []any, sourceType, articleURL string, evidence []byte) ([]any, *string) {
	if sourceType == "rss" || len(evidence) == 0 || len(evidence) > 8192 {
		return projectArticleMedia(raw, sourceType)
	}
	var item struct {
		Version      int    `json:"version"`
		ArticleScope string `json:"article_scope"`
		ArticleURL   string `json:"article_url"`
		Cover        struct {
			URL        string `json:"url"`
			Source     string `json:"source"`
			ArticleURL string `json:"article_url"`
		} `json:"editorial_cover"`
	}
	if json.Unmarshal(evidence, &item) != nil || item.Version != 1 {
		return projectArticleMedia(raw, sourceType)
	}
	if item.ArticleURL == articleURL && (item.ArticleScope == "verified" || item.ArticleScope == "unknown") {
		selected := make([]any, 0, len(raw))
		for _, entry := range raw {
			if editorialOrder(entry) > 0 && item.ArticleScope == "verified" {
				selected = append(selected, entry)
			}
		}
		sort.SliceStable(selected, func(i, j int) bool { return editorialOrder(selected[i]) < editorialOrder(selected[j]) })
		raw = selected
	}
	media, cover := projectArticleMedia(raw, sourceType)
	if (item.Cover.Source != "og_image" && item.Cover.Source != "selector_study") || item.Cover.ArticleURL != articleURL ||
		(item.ArticleScope == "unknown" && item.ArticleURL == articleURL) {
		return media, cover
	}
	u, err := url.Parse(item.Cover.URL)
	if err != nil || u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
		return media, cover
	}
	return media, &item.Cover.URL
}

func editorialOrder(entry any) float64 {
	item, _ := entry.(map[string]any)
	data, _ := item["data"].(map[string]any)
	switch order := data["editorial_order"].(type) {
	case int:
		return float64(order)
	case float64:
		return order
	}
	return 0
}
