package extractor

import (
	"net/url"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"
)

// verifiedSourceLayout is deliberately limited to two observed publishers.
// Selectors are rechecked on every captured DOM; a mismatch uses normal fallback.
func verifiedSourceLayout(d *goquery.Document, rawURL string, max int) (string, string, string, *goquery.Selection, bool) {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "https" || u.User != nil || (u.Port() != "" && u.Port() != "443") {
		return "", "", "", nil, false
	}
	var bodyCSS, coverCSS, rootCSS, excludes string
	switch u.Hostname() {
	case "www.blic.rs", "blic.rs":
		if !strings.HasPrefix(u.EscapedPath(), "/vesti/") || len(strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")) < 4 {
			return "", "", "", nil, false
		}
		rootCSS = "article.article"
		bodyCSS = "article.article .article__text"
		coverCSS = "article.article > header .news__image picture img"
		excludes = "#piano-story-related,.article__author,.article__related,.article__community-groups-blic,.banner--article"
	case "studenti.org.rs", "www.studenti.org.rs":
		parts := strings.Split(strings.Trim(u.EscapedPath(), "/"), "/")
		if len(parts) != 2 || parts[0] != "vesti" || parts[1] == "" {
			return "", "", "", nil, false
		}
		rootCSS = "main > article"
		bodyCSS = "main > article > div.prose > article"
		coverCSS = "main > article > div.hero-image img"
	default:
		return "", "", "", nil, false
	}
	root, body, cover := d.Find(rootCSS), d.Find(bodyCSS), d.Find(coverCSS)
	if root.Length() != 1 || body.Length() != 1 || cover.Length() != 1 || insideRecommendationContainer(body) || insideRecommendationContainer(cover) || layoutInsideChrome(body) || layoutInsideChrome(cover) {
		return "", "", "", nil, false
	}
	if !nodeInside(root.Nodes[0], body.Nodes[0]) || !nodeInside(root.Nodes[0], cover.Nodes[0]) {
		return "", "", "", nil, false
	}
	coverURL := safeRSSHTTPURL(cover.AttrOr("src", cover.AttrOr("data-src", "")), rawURL)
	if coverURL == "" || len(coverURL) > maxArticleMediaURLBytes {
		return "", "", "", nil, false
	}
	cleanBody := body.Clone()
	if excludes != "" {
		cleanBody.Find(excludes).Remove()
	}
	content := text(cleanBody)
	if !good(content, true, max) {
		return "", "", "", nil, false
	}
	// Retain the complete picture for explicit srcset associations.
	photo := cover
	if p := cover.Closest("picture"); p.Length() == 1 {
		photo = p
	}
	mediaBody := cleanBody.AddSelection(photo.Clone())
	return content, bodyCSS, coverURL, mediaBody, true
}

func layoutInsideChrome(s *goquery.Selection) bool {
	const chrome = "nav,aside,footer,button,[role=navigation],[role=contentinfo],[role=button],.article__author,.article__related"
	return s.Is(chrome) || s.ParentsFiltered(chrome).Length() > 0
}

func nodeInside(parent, node *html.Node) bool {
	for n := node; n != nil; n = n.Parent {
		if n == parent {
			return true
		}
	}
	return false
}

// Only the verified Studenti archive path is classified; a sparse or changed
// DOM remains unknown and uses the existing fallback.
func confirmedStudentiListing(d *goquery.Document, rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || u.Scheme != "https" || u.User != nil || (u.Port() != "" && u.Port() != "443") || (u.Hostname() != "studenti.org.rs" && u.Hostname() != "www.studenti.org.rs") || strings.TrimRight(u.EscapedPath(), "/") != "/vesti" || u.RawQuery != "" {
		return false
	}
	if d.Find("main > article").Length() != 0 {
		return false
	}
	unique := map[string]bool{}
	d.Find("main section li > a.clear[href]").EachWithBreak(func(_ int, a *goquery.Selection) bool {
		raw := safeRSSHTTPURL(a.AttrOr("href", ""), rawURL)
		v, e := url.Parse(raw)
		if e == nil && (v.Hostname() == "studenti.org.rs" || v.Hostname() == "www.studenti.org.rs") && strings.HasPrefix(v.EscapedPath(), "/vesti/") && strings.TrimRight(v.EscapedPath(), "/") != "/vesti" {
			unique[v.EscapedPath()] = true
		}
		return len(unique) < 10
	})
	return len(unique) >= 10
}
