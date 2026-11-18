package repositories

import (
	"net/url"
	"strconv"
	"strings"
)

// projectArticleMedia returns a new view; stored media is never modified. RSS
// provenance remains mandatory even for old records without extra metadata.
func projectArticleMedia(raw []any, sourceType string) ([]any, *string) {
	out := make([]any, 0, len(raw))
	seen := make(map[string]int)
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
		if k := vergeFullImageKey(parsed); k != "" {
			key = k
		}
		// Explicit canonical metadata can group unsigned variants. Keep all query
		// parameters on either URL intact: they may select content or sign access.
		if canonical, ok := data["canonical_url"].(string); ok && parsed.RawQuery == "" {
			if c, err := url.Parse(canonical); err == nil && (c.Scheme == "http" || c.Scheme == "https") && c.Host != "" && c.RawQuery == "" {
				key = canonical
			}
		}
		if index, exists := seen[key]; exists {
			if strings.HasPrefix(key, "verge-full:") {
				previous, ok := out[index].(map[string]any)
				if ok && vergeImageWidth(u) > vergeImageWidth(stringValue(previous["url"])) {
					out[index] = entry
				}
			}
			continue
		}
		seen[key] = len(out)
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

func vergeImageWidth(raw string) int {
	u, err := url.Parse(raw)
	if err != nil {
		return 0
	}
	v := u.Query().Get("w")
	if v == "" {
		return 10001
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

// The Verge's observed full-frame crop and width/quality variants identify
// the same image. Unknown, signed or cropped requests remain independent.
func vergeFullImageKey(u *url.URL) string {
	if u == nil || u.Scheme != "https" || u.Host != "platform.theverge.com" || u.User != nil || u.Fragment != "" || !strings.HasPrefix(u.EscapedPath(), "/wp-content/uploads/") {
		return ""
	}
	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return ""
	}
	for k, values := range q {
		if len(values) != 1 {
			return ""
		}
		switch k {
		case "w", "quality":
			n, err := strconv.Atoi(values[0])
			if err != nil || n < 1 || (k == "w" && n > 10000) || (k == "quality" && n > 100) {
				return ""
			}
		case "strip":
			if values[0] != "all" {
				return ""
			}
		case "crop":
			if values[0] != "0,0,100,100" {
				return ""
			}
		default:
			return ""
		}
	}
	return "verge-full:" + u.EscapedPath()
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
