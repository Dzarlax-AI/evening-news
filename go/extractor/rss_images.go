package extractor

import (
	"encoding/json"
	"encoding/xml"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

type rssImageCandidate struct {
	url, base, alt string
	html           bool
	parts          []string
}

func isRSSMediaNamespace(namespace string) bool {
	return strings.TrimRight(namespace, "/") == "http://search.yahoo.com/mrss" || namespace == "media"
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
		meta.images = append(meta.images, rssImageCandidate{url: raw, base: base, alt: attrs["alt"]})
	}
}

// rssImages consumes only the captured feed entry, never an article page.
// Candidates retain their XML order; only exact resolved URLs are deduplicated.
func rssImages(meta rssEntryMetadata) ([]Media, error) {
	result := []Media{}
	seen := map[string]bool{}
	bytes := 2
	add := func(raw, base, alt string) error {
		u := safeRSSHTTPURL(raw, base)
		if u == "" || seen[u] {
			return nil
		}
		m := Media{Type: "image", URL: u, Source: "rss_feed", AltText: truncateUTF8(alt, maxArticleMediaAltTextBytes)}
		b, err := json.Marshal(m)
		if err != nil || len(u) > maxArticleMediaURLBytes || len(result) >= maxArticleMediaCount || bytes+len(b)+1 > maxArticleMediaJSONBytes {
			return ErrRSSResourceLimit
		}
		seen[u] = true
		result = append(result, m)
		bytes += len(b) + 1
		return nil
	}
	for _, candidate := range meta.images {
		if !candidate.html {
			if err := add(candidate.url, candidate.base, candidate.alt); err != nil {
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
			imageErr = add(image.AttrOr("src", ""), candidate.base, image.AttrOr("alt", ""))
			return imageErr == nil
		})
		if imageErr != nil {
			return nil, imageErr
		}
	}
	return result, nil
}
