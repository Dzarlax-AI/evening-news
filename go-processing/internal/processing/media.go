package processing

import (
	"net/url"
	"regexp"
	"strings"
)

var (
	mediaImageTagRE  = regexp.MustCompile(`(?i)<img[^>]+src\s*=\s*["']([^"']+)["'][^>]*>`)
	mediaVideoTagRE  = regexp.MustCompile(`(?i)<video[^>]+src\s*=\s*["']([^"']+)["'][^>]*>`)
	mediaSourceTagRE = regexp.MustCompile(`(?i)<source[^>]+src\s*=\s*["']([^"']+)["'][^>]*>`)
	mediaLinkTagRE   = regexp.MustCompile(`(?i)<a[^>]+href\s*=\s*["']([^"']+)["'][^>]*>`)
	mediaAltRE       = regexp.MustCompile(`(?i)alt\s*=\s*["']([^"']*)["']`)
	mediaEmbedREs    = []*regexp.Regexp{
		regexp.MustCompile(`(?i)https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]+)`),
		regexp.MustCompile(`(?i)https?://(?:www\.)?youtu\.be/([a-zA-Z0-9_-]+)`),
		regexp.MustCompile(`(?i)https?://(?:www\.)?vimeo\.com/(\d+)`),
		regexp.MustCompile(`(?i)https?://(?:www\.)?dailymotion\.com/video/([a-zA-Z0-9]+)`),
	}
)

var (
	mediaImageExtensions    = []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".tiff", ".ico"}
	mediaVideoExtensions    = []string{".mp4", ".avi", ".mov", ".webm", ".mkv", ".flv", ".wmv", ".m4v"}
	mediaDocumentExtensions = []string{".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".rtf"}
)

// extractMediaFiles mirrors the retained Python MediaExtractor. It parses only
// the provided article text and resolves links locally; it performs no requests.
func extractMediaFiles(content, baseURL string) []map[string]any {
	if content == "" {
		return []map[string]any{}
	}
	media := make([]map[string]any, 0)
	for _, match := range mediaImageTagRE.FindAllStringSubmatch(content, -1) {
		src := strings.TrimSpace(match[1])
		if src == "" || strings.HasPrefix(src, "data:") {
			continue
		}
		src = resolveMediaURL(src, baseURL)
		alt := ""
		if attr := mediaAltRE.FindStringSubmatch(match[0]); len(attr) > 1 {
			alt = attr[1]
		}
		mediaType := "image"
		if ext := matchingMediaExtension(src, mediaImageExtensions); ext != "" {
			mediaType = "image/" + strings.TrimPrefix(ext, ".")
		}
		media = append(media, map[string]any{"url": src, "type": "image", "media_type": mediaType, "alt_text": alt, "source": "img_tag"})
	}
	for _, match := range mediaVideoTagRE.FindAllStringSubmatch(content, -1) {
		if src := mediaElementURL(match[1], baseURL); src != "" {
			media = append(media, map[string]any{"url": src, "type": "video", "media_type": "video", "source": "video_tag"})
		}
	}
	for _, match := range mediaSourceTagRE.FindAllStringSubmatch(content, -1) {
		if src := mediaElementURL(match[1], baseURL); src != "" {
			media = append(media, map[string]any{"url": src, "type": "video", "media_type": "video", "source": "source_tag"})
		}
	}
	for _, pattern := range mediaEmbedREs {
		for _, match := range pattern.FindAllStringSubmatch(content, -1) {
			media = append(media, map[string]any{"url": match[0], "type": "video", "media_type": "video/embed", "source": "embedded_video"})
		}
	}
	for _, match := range mediaLinkTagRE.FindAllStringSubmatch(content, -1) {
		href := strings.TrimSpace(match[1])
		if href == "" || hasMediaLinkSkipPrefix(href) {
			continue
		}
		href = resolveMediaURL(href, baseURL)
		switch {
		case matchingMediaExtension(href, mediaImageExtensions) != "":
			ext := matchingMediaExtension(href, mediaImageExtensions)
			media = append(media, map[string]any{"url": href, "type": "image", "media_type": "image/" + strings.TrimPrefix(ext, "."), "source": "href_link"})
		case matchingMediaExtension(href, mediaVideoExtensions) != "":
			ext := matchingMediaExtension(href, mediaVideoExtensions)
			media = append(media, map[string]any{"url": href, "type": "video", "media_type": "video/" + strings.TrimPrefix(ext, "."), "source": "href_link"})
		case matchingMediaExtension(href, mediaDocumentExtensions) != "":
			ext := matchingMediaExtension(href, mediaDocumentExtensions)
			media = append(media, map[string]any{"url": href, "type": "document", "media_type": "document/" + strings.TrimPrefix(ext, "."), "source": "href_link"})
		}
	}
	return deduplicateMediaURLs(media)
}

func mediaElementURL(raw, baseURL string) string {
	src := strings.TrimSpace(raw)
	if src == "" || strings.HasPrefix(src, "data:") {
		return ""
	}
	return resolveMediaURL(src, baseURL)
}

func resolveMediaURL(raw, baseURL string) string {
	if baseURL != "" && !strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://") {
		base, baseErr := url.Parse(baseURL)
		relative, relErr := url.Parse(raw)
		if baseErr == nil && relErr == nil {
			raw = base.ResolveReference(relative).String()
		}
	}
	return raw
}

func matchingMediaExtension(raw string, extensions []string) string {
	parsed, err := url.Parse(raw)
	if err != nil {
		return ""
	}
	path := strings.ToLower(parsed.EscapedPath())
	for _, ext := range extensions {
		if strings.HasSuffix(path, ext) {
			return ext
		}
	}
	return ""
}

func hasMediaLinkSkipPrefix(raw string) bool {
	for _, prefix := range []string{"mailto:", "tel:", "#", "javascript:"} {
		if strings.HasPrefix(raw, prefix) {
			return true
		}
	}
	return false
}

func deduplicateMediaURLs(media []map[string]any) []map[string]any {
	seen := make(map[string]struct{}, len(media))
	filtered := make([]map[string]any, 0, len(media))
	for _, item := range media {
		raw, _ := item["url"].(string)
		if raw == "" || (!strings.HasPrefix(raw, "http://") && !strings.HasPrefix(raw, "https://")) {
			continue
		}
		if _, exists := seen[raw]; exists {
			continue
		}
		seen[raw] = struct{}{}
		filtered = append(filtered, item)
	}
	return filtered
}
