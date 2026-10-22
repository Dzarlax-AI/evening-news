package extractor

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/PuerkitoBio/goquery"
)

const (
	maxArticleMediaCount        = 256
	maxArticleMediaURLBytes     = 8 * 1024
	maxArticleMediaAltTextBytes = 1024
	maxArticleMediaJSONBytes    = 256 * 1024
)

// articleMedia reads DOM attributes, resolves relative URLs, and deduplicates by URL.
func articleMedia(h, base string) []Media {
	media, _ := boundedArticleMedia(h, base)
	return media
}

func boundedArticleMedia(h, base string) ([]Media, bool) {
	d := parse(h)
	var out []Media
	seen := map[string]bool{}
	truncated := false
	encodedBytes := 2 // The serialized array's opening and closing brackets.
	add := func(m Media) bool {
		m.URL = strings.TrimSpace(m.URL)
		if strings.TrimSpace(m.URL) == "" || strings.HasPrefix(m.URL, "data:") {
			return true
		}
		if len(m.URL) > maxArticleMediaURLBytes {
			truncated = true
			return true
		}
		reference, err := url.Parse(m.URL)
		if err != nil {
			return true
		}
		if !reference.IsAbs() && reference.Host == "" && len(base) > maxArticleMediaURLBytes {
			truncated = true
			return true
		}
		u := resolve(base, m.URL)
		p, err := url.Parse(u)
		if err != nil || (p.Scheme != "http" && p.Scheme != "https") || seen[u] {
			return true
		}
		if len(u) > maxArticleMediaURLBytes {
			truncated = true
			return true
		}
		if len(out) >= maxArticleMediaCount {
			truncated = true
			return false
		}
		m.URL = u
		if len(m.AltText) > maxArticleMediaAltTextBytes {
			m.AltText = truncateUTF8(m.AltText, maxArticleMediaAltTextBytes)
			truncated = true
		}
		encoded, err := json.Marshal(m)
		if err != nil {
			truncated = true
			return true
		}
		nextBytes := encodedBytes + len(encoded)
		if len(out) > 0 {
			nextBytes++ // JSON array comma.
		}
		if nextBytes > maxArticleMediaJSONBytes {
			truncated = true
			return false
		}
		seen[u] = true
		out = append(out, m)
		encodedBytes = nextBytes
		return true
	}
	extensions := map[string][]string{"image": {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".tiff", ".ico"}, "video": {".mp4", ".avi", ".mov", ".webm", ".mkv", ".flv", ".wmv", ".m4v"}, "document": {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".rtf"}}
	hasMediaExtension := func(raw string) bool {
		pathEnd := len(raw)
		if delimiter := strings.IndexAny(raw, "?#"); delimiter >= 0 {
			pathEnd = delimiter
		}
		for _, exts := range extensions {
			for _, ext := range exts {
				if pathEnd >= len(ext) && strings.EqualFold(raw[pathEnd-len(ext):pathEnd], ext) {
					return true
				}
			}
		}
		return false
	}
	mediaType := func(kind, u string) string {
		p, err := url.Parse(u)
		if err == nil {
			for _, ext := range extensions[kind] {
				if strings.HasSuffix(strings.ToLower(p.Path), ext) {
					return kind + "/" + ext[1:]
				}
			}
		}
		return kind
	}
	stopped := false
	d.Find("img[src],img[data-src],img[data-lazy-src],img[data-original]").EachWithBreak(func(_ int, s *goquery.Selection) bool {
		u := strings.TrimSpace(s.AttrOr("src", ""))
		if u == "" || strings.HasPrefix(u, "data:") {
			for _, attribute := range []string{"data-src", "data-lazy-src", "data-original"} {
				if candidate := strings.TrimSpace(s.AttrOr(attribute, "")); candidate != "" && !strings.HasPrefix(candidate, "data:") {
					u = candidate
					break
				}
			}
		}
		kind := "image"
		if len(u) <= maxArticleMediaURLBytes {
			kind = mediaType("image", u)
		}
		if kind == "image" {
			canonical := s.AttrOr("data-canonical-src", "")
			if len(canonical) <= maxArticleMediaURLBytes {
				if parsed, err := url.Parse(canonical); err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") {
					kind = mediaType("image", canonical)
				}
			}
		}
		stopped = !add(Media{Type: "image", URL: u, AltText: s.AttrOr("alt", ""), MediaType: kind, Source: "img_tag"})
		return !stopped
	})
	for _, kind := range []string{"video", "source"} {
		if stopped {
			break
		}
		d.Find(kind + "[src]").EachWithBreak(func(_ int, s *goquery.Selection) bool {
			stopped = !add(Media{Type: "video", URL: s.AttrOr("src", ""), MediaType: "video", Source: kind + "_tag"})
			return !stopped
		})
	}
	for _, pattern := range []string{`https?://(?:www\.)?youtube\.com/watch\?v=[a-zA-Z0-9_-]+`, `https?://(?:www\.)?youtu\.be/[a-zA-Z0-9_-]+`, `https?://(?:www\.)?vimeo\.com/\d+`, `https?://(?:www\.)?dailymotion\.com/video/[a-zA-Z0-9]+`} {
		if stopped {
			break
		}
		matcher := regexp.MustCompile(pattern)
		for offset := 0; offset < len(h); {
			match := matcher.FindStringIndex(h[offset:])
			if match == nil {
				break
			}
			start, end := offset+match[0], offset+match[1]
			u := h[start:end]
			if !add(Media{Type: "video", URL: u, MediaType: "video/embed", Source: "embedded_video"}) {
				stopped = true
				break
			}
			offset = end
		}
	}
	if !stopped {
		d.Find("a[href]").EachWithBreak(func(_ int, s *goquery.Selection) bool {
			u := strings.TrimSpace(s.AttrOr("href", ""))
			if len(u) > maxArticleMediaURLBytes {
				if hasMediaExtension(u) {
					truncated = true
				}
				return true
			}
			p, e := url.Parse(u)
			if e != nil {
				return true
			}
			path := strings.ToLower(p.Path)
			kind := ""
			for k, exts := range extensions {
				for _, ext := range exts {
					if strings.HasSuffix(path, ext) {
						kind = k
						break
					}
				}
			}
			if kind != "" {
				stopped = !add(Media{Type: kind, URL: u, MediaType: mediaType(kind, u), Source: "href_link"})
			}
			return !stopped
		})
	}
	return out, truncated
}

func truncateUTF8(s string, maxBytes int) string {
	if len(s) <= maxBytes {
		return s
	}
	end := maxBytes
	for end > 0 && !utf8.ValidString(s[:end]) {
		end--
	}
	return s[:end]
}
