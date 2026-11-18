package extractor

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strconv"
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

// articleMediaFromBody keeps only the DOM selected by the text extractor.
// A missing body is intentionally not replaced with the whole document.
func articleMediaFromBody(body *goquery.Selection, base string) ([]Media, bool, bool) {
	if body == nil || body.Length() == 0 {
		return nil, false, false
	}
	clone := body.Clone()
	pruneRecommendationControls(clone)
	clone.Find("script,style,template,nav,aside,footer,form,dialog,[role=dialog],[aria-modal=true],[role=navigation],[role=contentinfo],[data-native-ad-id],.newsletter-signup,.subscription-widget,.subscribe-widget,.ad-container,.advertisement").Remove()
	if u, err := url.Parse(base); err == nil && (u.Hostname() == "blic.rs" || u.Hostname() == "www.blic.rs") {
		clone.Find(".article__author").Remove()
	}
	if u, err := url.Parse(base); err == nil {
		switch u.Hostname() {
		case "n1info.rs", "www.n1info.rs":
			clone.Find(`.social-share-bar, section.live-blog > .header, img[src^="/media/images/Follow_Us"], img[src="/media/images/x.original.original.svg"], img[src="/static/images/footer.avif"]`).Remove()
		case "huggingface.co", "www.huggingface.co":
			clone.Find(`img[src^="https://cdn-avatars.huggingface.co/"], img[src^="https://img.shields.io/badge/"]`).Remove()
		}
	}
	var parts []string
	clone.Each(func(_ int, s *goquery.Selection) {
		if h, err := goquery.OuterHtml(s); err == nil {
			parts = append(parts, h)
		}
	})
	media, truncated, incomplete := boundedArticleMediaDetailed(strings.Join(parts, "\n"), base)
	if addPictureEvidence(clone, media, base) {
		truncated = true
	}
	if markHuggingFaceHTMLWrappers(clone, media, base) {
		truncated = true
	}
	return media, truncated, incomplete
}

// The confirmed Hugging Face blob link wraps the corresponding resolve image;
// blob serves HTML, not an image. Require exact, query-free same-page paths.
func markHuggingFaceHTMLWrappers(body *goquery.Selection, media []Media, base string) bool {
	page, err := url.Parse(base)
	if err != nil || page.Hostname() != "huggingface.co" {
		return false
	}
	wrappers := map[string]bool{}
	body.Find("a[href]").Each(func(_ int, a *goquery.Selection) {
		href := safeRSSHTTPURL(a.AttrOr("href", ""), base)
		u, err := url.Parse(href)
		if err != nil || u.Hostname() != "huggingface.co" || u.RawQuery != "" || u.Fragment != "" || strings.Count(u.Path, "/blob/") != 1 {
			return
		}
		img := safeRSSHTTPURL(a.Find("img[src]").First().AttrOr("src", ""), base)
		v, err := url.Parse(img)
		if err != nil || v.Hostname() != "huggingface.co" || v.RawQuery != "" || v.Fragment != "" || v.Path != strings.Replace(u.Path, "/blob/", "/resolve/", 1) {
			return
		}
		wrappers[href] = true
	})
	for i := range media {
		if media[i].Source != "href_link" || !wrappers[media[i].URL] {
			continue
		}
		old := media[i].Data
		data := make(map[string]any, len(old)+1)
		for k, v := range old {
			data[k] = v
		}
		data["html_wrapper"] = true
		media[i].Data = data
		encoded, err := json.Marshal(media)
		if err != nil || len(encoded) > maxArticleMediaJSONBytes {
			media[i].Data = old
			return true
		}
	}
	return false
}

// addPictureEvidence links only candidates explicitly present in the same
// picture's unconditional src/srcset. Query-bearing URLs remain independent.
func addPictureEvidence(body *goquery.Selection, media []Media, base string) bool {
	publisher, err := url.Parse(base)
	if err != nil || (publisher.Hostname() != "blic.rs" && publisher.Hostname() != "www.blic.rs") {
		return false
	}
	indexes := make(map[string]int, len(media))
	for i, m := range media {
		if m.Type == "image" {
			indexes[m.URL] = i
		}
	}
	truncated := false
	body.Find("picture").AddBackFiltered("picture").Each(func(_ int, picture *goquery.Selection) {
		img := picture.Find("img[src]").First()
		if img.Length() == 0 {
			return
		}
		primary := safeRSSHTTPURL(img.AttrOr("src", ""), base)
		p, err := url.Parse(primary)
		if err != nil || p.Hostname() != "ocdn.eu" || p.RawQuery != "" || p.Fragment != "" {
			return
		}
		first, exists := indexes[primary]
		if !exists {
			return
		}
		members := []int{first}
		seen := map[int]bool{first: true}
		picture.Find("source[srcset]").Each(func(_ int, s *goquery.Selection) {
			if s.AttrOr("media", "") != "" || !strings.EqualFold(s.AttrOr("type", ""), "image/webp") {
				return
			}
			urls, ok := strictPictureSrcset(s.AttrOr("srcset", ""), base)
			if !ok {
				return
			}
			for _, u := range urls {
				parsed, e := url.Parse(u)
				if e != nil || parsed.Hostname() != "ocdn.eu" || parsed.RawQuery != "" || parsed.Fragment != "" {
					continue
				}
				if i, ok := indexes[u]; ok && !seen[i] {
					members = append(members, i)
					seen[i] = true
				}
			}
		})
		if len(members) < 2 {
			return
		}
		for _, i := range members {
			if _, exists := media[i].Data["canonical_url"]; exists {
				return
			}
		}
		prior := make([]map[string]any, len(members))
		for j, i := range members {
			prior[j] = media[i].Data
			copyData := make(map[string]any, len(media[i].Data)+1)
			for k, v := range media[i].Data {
				copyData[k] = v
			}
			copyData["canonical_url"] = primary
			media[i].Data = copyData
		}
		encoded, err := json.Marshal(media)
		if err != nil || len(encoded) > maxArticleMediaJSONBytes {
			for j, i := range members {
				media[i].Data = prior[j]
			}
			truncated = true
		}
	})
	return truncated
}

var pictureWidthDescriptor = regexp.MustCompile(`^[1-9][0-9]*w$`)
var pictureDensityDescriptor = regexp.MustCompile(`^(?:[0-9]+(?:\.[0-9]+)?|\.[0-9]+)x$`)

// Only accept a complete, unambiguous subset: a URL plus one positive w/x
// descriptor per candidate. A comma inside a URL makes the whole set invalid.
func strictPictureSrcset(raw, base string) ([]string, bool) {
	var urls []string
	for _, part := range strings.Split(raw, ",") {
		fields := strings.Fields(part)
		if len(fields) != 2 {
			return nil, false
		}
		descriptor := fields[1]
		valid := pictureWidthDescriptor.MatchString(descriptor)
		if !valid && pictureDensityDescriptor.MatchString(descriptor) {
			n, err := strconv.ParseFloat(strings.TrimSuffix(descriptor, "x"), 64)
			valid = err == nil && n > 0
		}
		if !valid {
			return nil, false
		}
		u := safeRSSHTTPURL(fields[0], base)
		if u == "" {
			return nil, false
		}
		urls = append(urls, u)
	}
	return urls, len(urls) > 0
}

// scopedArticleMedia retains the legacy bounded candidates for rollback. Scope
// is asserted only when the selected body's complete media list was obtained.
func scopedArticleMedia(page, base string, body *goquery.Selection) ([]Media, bool) {
	raw, rawTruncated := boundedArticleMedia(page, base)
	if body == nil || body.Length() == 0 {
		return raw, rawTruncated
	}
	selected, selectedTruncated, selectedIncomplete := articleMediaFromBody(body, base)
	if selectedIncomplete {
		return raw, true
	}
	inBody := make(map[string]Media, len(selected))
	for _, m := range selected {
		inBody[m.URL] = m
	}
	annotated := make([]Media, len(raw))
	copy(annotated, raw)
	for i := range annotated {
		annotated[i].Data = nil
	}
	encoded, err := json.Marshal(annotated)
	if err != nil {
		return raw, true
	}
	bytes := len(encoded)
	setData := func(i int, data map[string]any) bool {
		before, _ := json.Marshal(annotated[i])
		candidate := annotated[i]
		candidate.Data = data
		after, e := json.Marshal(candidate)
		if e != nil || bytes-len(before)+len(after) > maxArticleMediaJSONBytes {
			return false
		}
		bytes += len(after) - len(before)
		annotated[i] = candidate
		return true
	}
	// Scope takes priority over optional evidence; never sacrifice raw URLs.
	for i, m := range annotated {
		if _, ok := inBody[m.URL]; !ok && !setData(i, map[string]any{"article_body_scope": "outside"}) {
			rawTruncated = true
		}
	}
	for i := range annotated {
		m := annotated[i]
		data := make(map[string]any, len(raw[i].Data)+1)
		for k, v := range m.Data {
			data[k] = v
		}
		if selectedMedia, ok := inBody[m.URL]; ok {
			for k, v := range selectedMedia.Data {
				data[k] = v
			}
		} else {
			for k, v := range raw[i].Data {
				data[k] = v
			}
		}
		if len(data) > 0 && !setData(i, data) {
			rawTruncated = true
		}
	}
	return annotated, rawTruncated || selectedTruncated
}

func boundedArticleMedia(h, base string) ([]Media, bool) {
	out, truncated, _ := boundedArticleMediaDetailed(h, base)
	return out, truncated
}

func boundedArticleMediaDetailed(h, base string) ([]Media, bool, bool) {
	d := parse(h)
	var out []Media
	seen := map[string]bool{}
	truncated := false
	incomplete := false
	var evidence []map[string]any
	encodedBytes := 2 // The serialized array's opening and closing brackets.
	add := func(m Media) bool {
		m.URL = strings.TrimSpace(m.URL)
		if strings.TrimSpace(m.URL) == "" || strings.HasPrefix(m.URL, "data:") {
			return true
		}
		if len(m.URL) > maxArticleMediaURLBytes {
			truncated = true
			incomplete = true
			return true
		}
		reference, err := url.Parse(m.URL)
		if err != nil {
			return true
		}
		if !reference.IsAbs() && reference.Host == "" && len(base) > maxArticleMediaURLBytes {
			truncated = true
			incomplete = true
			return true
		}
		u := resolve(base, m.URL)
		p, err := url.Parse(u)
		if err != nil || (p.Scheme != "http" && p.Scheme != "https") || seen[u] {
			return true
		}
		if len(u) > maxArticleMediaURLBytes {
			truncated = true
			incomplete = true
			return true
		}
		if len(out) >= maxArticleMediaCount {
			truncated = true
			incomplete = true
			return false
		}
		m.URL = u
		data := m.Data
		m.Data = nil
		if len(m.AltText) > maxArticleMediaAltTextBytes {
			m.AltText = truncateUTF8(m.AltText, maxArticleMediaAltTextBytes)
			truncated = true
		}
		encoded, err := json.Marshal(m)
		if err != nil {
			truncated = true
			incomplete = true
			return true
		}
		nextBytes := encodedBytes + len(encoded)
		if len(out) > 0 {
			nextBytes++ // JSON array comma.
		}
		if nextBytes > maxArticleMediaJSONBytes {
			truncated = true
			incomplete = true
			return false
		}
		seen[u] = true
		out = append(out, m)
		evidence = append(evidence, data)
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
		attrs := map[string]string{}
		for _, name := range []string{"width", "height", "role", "aria-hidden", "data-canonical-src"} {
			attrs[name] = s.AttrOr(name, "")
		}
		stopped = !add(Media{Type: "image", URL: u, AltText: s.AttrOr("alt", ""), MediaType: kind, Source: "img_tag", Data: imageEvidence(attrs, base)})
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
					incomplete = true
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
	// Allocate optional evidence only after reserving the legacy bounded list.
	for i, data := range evidence {
		if len(data) == 0 {
			continue
		}
		before, _ := json.Marshal(out[i])
		m := out[i]
		m.Data = data
		after, err := json.Marshal(m)
		if err != nil || encodedBytes-len(before)+len(after) > maxArticleMediaJSONBytes {
			truncated = true
			continue
		}
		encodedBytes += len(after) - len(before)
		out[i] = m
	}
	return out, truncated, incomplete
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
