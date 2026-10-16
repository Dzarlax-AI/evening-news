package extractor

import (
	"net/url"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// articleMedia reads DOM attributes, resolves relative URLs, and deduplicates by URL.
func articleMedia(h, base string) []Media {
	d := parse(h)
	var out []Media
	seen := map[string]bool{}
	add := func(m Media) {
		if strings.TrimSpace(m.URL) == "" || strings.HasPrefix(m.URL, "data:") {
			return
		}
		u := resolve(base, m.URL)
		p, e := url.Parse(u)
		if e != nil || (p.Scheme != "http" && p.Scheme != "https") || seen[u] {
			return
		}
		seen[u] = true
		m.URL = u
		out = append(out, m)
	}
	extensions := map[string][]string{"image": {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".bmp", ".tiff", ".ico"}, "video": {".mp4", ".avi", ".mov", ".webm", ".mkv", ".flv", ".wmv", ".m4v"}, "document": {".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx", ".txt", ".rtf"}}
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
	d.Find("img[src],img[data-src],img[data-lazy-src],img[data-original]").Each(func(_ int, s *goquery.Selection) {
		u := strings.TrimSpace(s.AttrOr("src", ""))
		if u == "" || strings.HasPrefix(u, "data:") {
			for _, attribute := range []string{"data-src", "data-lazy-src", "data-original"} {
				if candidate := strings.TrimSpace(s.AttrOr(attribute, "")); candidate != "" && !strings.HasPrefix(candidate, "data:") {
					u = candidate
					break
				}
			}
		}
		kind := mediaType("image", u)
		if kind == "image" {
			canonical := s.AttrOr("data-canonical-src", "")
			if parsed, err := url.Parse(canonical); err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") {
				kind = mediaType("image", canonical)
			}
		}
		add(Media{Type: "image", URL: u, AltText: s.AttrOr("alt", ""), MediaType: kind, Source: "img_tag"})
	})
	for _, kind := range []string{"video", "source"} {
		d.Find(kind + "[src]").Each(func(_ int, s *goquery.Selection) {
			add(Media{Type: "video", URL: s.AttrOr("src", ""), MediaType: "video", Source: kind + "_tag"})
		})
	}
	for _, pattern := range []string{`https?://(?:www\.)?youtube\.com/watch\?v=[a-zA-Z0-9_-]+`, `https?://(?:www\.)?youtu\.be/[a-zA-Z0-9_-]+`, `https?://(?:www\.)?vimeo\.com/\d+`, `https?://(?:www\.)?dailymotion\.com/video/[a-zA-Z0-9]+`} {
		for _, u := range regexp.MustCompile(pattern).FindAllString(h, -1) {
			add(Media{Type: "video", URL: u, MediaType: "video/embed", Source: "embedded_video"})
		}
	}
	d.Find("a[href]").Each(func(_ int, s *goquery.Selection) {
		u := s.AttrOr("href", "")
		p, e := url.Parse(u)
		if e != nil {
			return
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
			add(Media{Type: kind, URL: u, MediaType: mediaType(kind, u), Source: "href_link"})
		}
	})
	return out
}
