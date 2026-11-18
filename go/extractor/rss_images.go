package extractor

import (
	"encoding/json"
	"encoding/xml"
	"strconv"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type rssImageCandidate struct {
	url, base, alt string
	html           bool
	parts          []string
	attributes     map[string]string
}

func isRSSMediaNamespace(namespace string) bool {
	return strings.TrimRight(namespace, "/") == "http://search.yahoo.com/mrss" || namespace == "media"
}

// Atom text constructs default to plain text. XHTML images are XML elements;
// reparsing their character data would turn escaped code examples into images.
func rssHasHTMLImageContent(element xml.StartElement) bool {
	name := strings.ToLower(element.Name.Local)
	if isRSSMediaNamespace(element.Name.Space) || (name != "content" && name != "encoded" && name != "summary" && name != "description") {
		return false
	}
	if element.Name.Space != "http://www.w3.org/2005/Atom" {
		return true
	}
	for _, attr := range element.Attr {
		if attr.Name.Local == "type" {
			kind := strings.ToLower(strings.TrimSpace(attr.Value))
			return kind == "html" || kind == "text/html"
		}
	}
	return false
}

func (meta *rssEntryMetadata) collectRSSImage(element xml.StartElement, base string) {
	attrs := map[string]string{}
	for _, a := range element.Attr {
		attrs[strings.ToLower(a.Name.Local)] = strings.TrimSpace(a.Value)
	}
	tag := strings.ToLower(element.Name.Local)
	imageType := strings.HasPrefix(strings.ToLower(attrs["type"]), "image/")
	raw := ""
	switch {
	case tag == "img":
		raw = attrs["src"]
	case tag == "enclosure" && imageType:
		raw = attrs["url"]
	case tag == "link" && strings.EqualFold(attrs["rel"], "enclosure") && imageType:
		raw = attrs["href"]
	case isRSSMediaNamespace(element.Name.Space) && tag == "thumbnail":
		raw = attrs["url"]
	case isRSSMediaNamespace(element.Name.Space) && tag == "content" && (imageType || attrs["medium"] == "image" || looksImage(attrs["url"])):
		raw = attrs["url"]
	}
	if raw != "" {
		meta.images = append(meta.images, rssImageCandidate{url: raw, base: base, alt: attrs["alt"], attributes: attrs})
	}
}

// rssImages consumes only the captured feed entry, never an article page.
// Candidates retain their XML order; only exact resolved URLs are deduplicated.
func rssImages(meta rssEntryMetadata) ([]Media, error) {
	result := []Media{}
	seen := map[string]int{}
	bytes := 2
	add := func(raw, base, alt string, attrs map[string]string) error {
		u := safeRSSHTTPURL(raw, base)
		if u == "" {
			return nil
		}
		m := Media{Type: "image", URL: u, Source: "rss_feed", AltText: truncateUTF8(alt, maxArticleMediaAltTextBytes)}
		// Preserve bounded evidence for a reversible read-time projection. Never
		// discard the original candidate during ingestion.
		data := imageEvidence(attrs, base)
		if len(data) > 0 {
			m.Data = data
		}
		if index, exists := seen[u]; exists {
			previous := result[index]
			merged := previous
			merged.Data = make(map[string]any, len(previous.Data)+len(data))
			for key, value := range previous.Data {
				merged.Data[key] = value
			}
			for key, value := range data {
				if _, ok := merged.Data[key]; !ok {
					merged.Data[key] = value
				}
			}
			if len(merged.Data) == 0 {
				merged.Data = nil
			}
			if merged.AltText == "" {
				merged.AltText = m.AltText
			}
			before, err1 := json.Marshal(previous)
			after, err2 := json.Marshal(merged)
			if err1 != nil || err2 != nil || bytes-len(before)+len(after) > maxArticleMediaJSONBytes {
				return ErrRSSResourceLimit
			}
			bytes += len(after) - len(before)
			result[index] = merged
			return nil
		}
		b, err := json.Marshal(m)
		if err != nil || len(u) > maxArticleMediaURLBytes || len(result) >= maxArticleMediaCount || bytes+len(b)+1 > maxArticleMediaJSONBytes {
			return ErrRSSResourceLimit
		}
		seen[u] = len(result)
		result = append(result, m)
		bytes += len(b) + 1
		return nil
	}
	for _, candidate := range meta.images {
		if !candidate.html {
			if err := add(candidate.url, candidate.base, candidate.alt, candidate.attributes); err != nil {
				return nil, err
			}
			continue
		}
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(strings.Join(candidate.parts, "")))
		if err != nil {
			return nil, ErrInvalidRSSFeed
		}
		var imageErr error
		doc.Find("img[src]").EachWithBreak(func(_ int, image *goquery.Selection) bool {
			attrs := map[string]string{}
			for _, name := range []string{"width", "height", "role", "aria-hidden", "data-canonical-src"} {
				attrs[name] = image.AttrOr(name, "")
			}
			imageErr = add(image.AttrOr("src", ""), candidate.base, image.AttrOr("alt", ""), attrs)
			return imageErr == nil
		})
		if imageErr != nil {
			return nil, imageErr
		}
	}
	return result, nil
}

func imageEvidence(attrs map[string]string, base string) map[string]any {
	data := map[string]any{}
	for _, name := range []string{"width", "height"} {
		if n, err := strconv.Atoi(strings.TrimSpace(attrs[name])); err == nil && n > 0 && n <= 100000 {
			data[name] = n
		}
	}
	if strings.EqualFold(attrs["role"], "presentation") || strings.EqualFold(attrs["aria-hidden"], "true") {
		data["decorative"] = true
	}
	if canonical := safeRSSHTTPURL(attrs["data-canonical-src"], base); canonical != "" && len(canonical) <= maxArticleMediaURLBytes {
		data["canonical_url"] = canonical
	}
	return data
}
