package repositories

import (
	"net/url"
)

// projectArticleMedia returns a new view; stored media is never modified. RSS
// provenance remains mandatory even for old records without extra metadata.
func projectArticleMedia(raw []any, sourceType string) ([]any, *string) {
	out := make([]any, 0, len(raw))
	seen := make(map[string]bool)
	for _, entry := range raw {
		m, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if sourceType == "rss" && m["source"] != "rss_feed" {
			continue
		}
		data, _ := m["data"].(map[string]any)
		if sourceType != "rss" && data["article_body_scope"] == "outside" {
			continue
		}
		if data["html_wrapper"] == true {
			continue
		}
		typ, _ := m["type"].(string)
		u, _ := m["url"].(string)
		if typ != "image" {
			out = append(out, entry)
			continue
		}
		parsed, err := url.Parse(u)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			continue
		}
		if data["decorative"] == true || (smallDimension(data["width"]) && smallDimension(data["height"])) {
			continue
		}
		key := u
		// Explicit canonical metadata can group unsigned variants. Keep all query
		// parameters on either URL intact: they may select content or sign access.
		if canonical, ok := data["canonical_url"].(string); ok && parsed.RawQuery == "" {
			if c, err := url.Parse(canonical); err == nil && (c.Scheme == "http" || c.Scheme == "https") && c.Host != "" && c.RawQuery == "" {
				key = canonical
			}
		}
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, entry)
	}
	var primary *string
	for _, entry := range out {
		if m, ok := entry.(map[string]any); ok && m["type"] == "image" {
			if value, ok := m["url"].(string); ok {
				primary = &value
				break
			}
		}
	}
	return out, primary
}

func smallDimension(value any) bool {
	switch n := value.(type) {
	case float64:
		return n > 0 && n <= 64
	case int:
		return n > 0 && n <= 64
	}
	return false
}
