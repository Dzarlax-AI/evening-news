package pagelayout

import (
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/PuerkitoBio/goquery"
	"github.com/andybalholm/cascadia"
	"golang.org/x/net/html"
)

// Layout is a bounded, optional description of an article or listing page.
type Layout struct {
	PageKind         string   `json:"page_kind,omitempty"`
	ArticleBody      string   `json:"article_body_selector,omitempty"`
	EditorialCover   string   `json:"editorial_cover_selector,omitempty"`
	ExcludeBlocks    []string `json:"exclude_selectors,omitempty"`
	ContentSelectors []string `json:"content_selectors,omitempty"`
	TitleSelectors   []string `json:"title_selectors,omitempty"`
	DateSelectors    []string `json:"date_selectors,omitempty"`
}

// Validate evaluates optional AI hints against the supplied captured DOM.
func Validate(a *Layout, raw string) {
	if a == nil {
		return
	}
	kind := strings.ToLower(strings.TrimSpace(a.PageKind))
	a.PageKind = "unknown"
	bodySelector, coverSelector, excludes := a.ArticleBody, a.EditorialCover, a.ExcludeBlocks
	a.ArticleBody = ""
	a.EditorialCover = ""
	a.ExcludeBlocks = nil
	if len(a.ContentSelectors) > 16 || len(a.TitleSelectors) > 16 || len(a.DateSelectors) > 16 || len(excludes) > 8 {
		return
	}
	if len(raw) == 0 || len(raw) > 5*1024*1024 {
		return
	}
	d, err := goquery.NewDocumentFromReader(strings.NewReader(raw))
	if err != nil {
		return
	}
	find := func(selector string) *goquery.Selection {
		if len(selector) == 0 || len(selector) > 256 || !utf8.ValidString(selector) {
			return nil
		}
		m, err := cascadia.Compile(selector)
		if err != nil {
			return nil
		}
		return d.FindMatcher(m)
	}
	switch kind {
	case "listing":
		var verified []string
		for _, selector := range a.ContentSelectors {
			s := find(selector)
			if s == nil || s.Length() < 2 || s.Length() > 1000 {
				continue
			}
			distinct := map[string]bool{}
			s.Each(func(_ int, e *goquery.Selection) {
				if inChrome(e) {
					return
				}
				e.Find("a[href]").AddBackFiltered("a[href]").Each(func(_ int, l *goquery.Selection) {
					if inChrome(l) {
						return
					}
					v := strings.TrimSpace(l.AttrOr("href", ""))
					if v != "" && !strings.HasPrefix(v, "#") {
						distinct[v] = true
					}
				})
			})
			if len(distinct) >= 2 {
				verified = append(verified, selector)
			}
		}
		if len(verified) == 0 {
			return
		}
		a.ContentSelectors = verified
		filter := func(values []string) []string {
			out := []string{}
			for _, v := range values {
				if s := find(v); s != nil && s.Length() > 0 && s.Length() <= 1000 {
					out = append(out, v)
				}
			}
			return out
		}
		a.TitleSelectors = filter(a.TitleSelectors)
		a.DateSelectors = filter(a.DateSelectors)
		a.PageKind = "listing"
	case "article":
		body := find(bodySelector)
		if body == nil || body.Length() != 1 || inChrome(body) || inCoverChrome(body) || body.Is("html,body") ||
			utf8.RuneCountInString(strings.TrimSpace(body.Text())) < 100 || !plausibleArticleBody(body) {
			return
		}
		var cover *goquery.Selection
		if coverSelector != "" {
			cover = find(coverSelector)
			if cover == nil || cover.Length() != 1 || !cover.Is("img[src],img[data-src]") || inChrome(cover) || inCoverChrome(cover) || !sameArticle(body, cover) || !usableImage(cover) {
				cover = nil
				coverSelector = ""
			}
		}
		if len(excludes) > 8 {
			excludes = excludes[:8]
		}
		validExcludes := []string{}
		for _, selector := range excludes {
			excluded := find(selector)
			if excluded == nil || excluded.Length() == 0 || excluded.Length() > 32 {
				continue
			}
			unsafe := false
			excluded.Each(func(_ int, n *goquery.Selection) {
				if n.Is("html,body") || containsNode(n.Nodes[0], body.Nodes[0]) || (containsNode(body.Nodes[0], n.Nodes[0]) && utf8.RuneCountInString(strings.TrimSpace(n.Text())) >= 100 && !semanticRelatedAside(n)) || (cover != nil && containsNode(n.Nodes[0], cover.Nodes[0])) {
					unsafe = true
				}
			})
			if unsafe {
				continue
			}
			validExcludes = append(validExcludes, selector)
		}
		a.PageKind = "article"
		a.ArticleBody = bodySelector
		a.EditorialCover = coverSelector
		a.ExcludeBlocks = validExcludes
	}
}

// An unmarked body needs several coherent paragraphs near a unique headline.
// This permits editorial markup without a semantic article element.
func plausibleArticleBody(body *goquery.Selection) bool {
	if body.Closest("article,[itemprop=articleBody],[role=article]").Length() > 0 {
		return true
	}
	count, prose := 0, 0
	body.Find("p").Each(func(_ int, p *goquery.Selection) {
		if inChrome(p) || inCoverChrome(p) {
			return
		}
		length := utf8.RuneCountInString(strings.TrimSpace(p.Text()))
		linked := 0
		p.Find("a[href]").Each(func(_ int, link *goquery.Selection) {
			linked += utf8.RuneCountInString(strings.TrimSpace(link.Text()))
		})
		if length >= 45 && linked*2 < length {
			count++
			prose += length
		}
	})
	if count < 2 || prose < 200 {
		return false
	}
	for current, depth := body, 0; current.Length() == 1 && depth < 6 && !current.Is("html,body"); current, depth = current.Parent(), depth+1 {
		headings := current.Find("h1").FilterFunction(func(_ int, heading *goquery.Selection) bool {
			return !inChrome(heading) && !inCoverChrome(heading) && heading.Closest("a[href]").Length() == 0 && heading.Find("a[href]").Length() == 0
		})
		if headings.Length() > 0 {
			return headings.Length() == 1
		}
	}
	return false
}

func semanticRelatedAside(s *goquery.Selection) bool {
	if !s.Is("aside") {
		return false
	}
	for _, attr := range []string{"class", "id", "aria-label", "data-testid"} {
		for _, part := range strings.FieldsFunc(strings.ToLower(s.AttrOr(attr, "")), func(r rune) bool { return r == '-' || r == '_' || r == ' ' }) {
			if part == "related" || part == "recommendations" || part == "recommended" {
				return true
			}
		}
	}
	return false
}
func inChrome(s *goquery.Selection) bool {
	return s.Is("nav,footer,[role=navigation],[role=contentinfo]") || s.ParentsFiltered("nav,footer,[role=navigation],[role=contentinfo]").Length() > 0
}
func inCoverChrome(s *goquery.Selection) bool {
	const controls = "aside,button,[role=button],.article__author,.author,.byline,.social-share-bar"
	if s.Is(controls) || s.ParentsFiltered(controls).Length() > 0 {
		return true
	}
	return s.Parents().FilterFunction(func(_ int, p *goquery.Selection) bool { return hasRelatedSignal(p) }).Length() > 0
}
func sameArticle(body, cover *goquery.Selection) bool {
	bodyNode, coverNode := body.Nodes[0], cover.Nodes[0]
	if containsNode(bodyNode, coverNode) && body.Closest("article,[role=article]").Length() == 0 {
		return withinArticleBoundary(bodyNode, coverNode)
	}
	// An inner prose article can belong to the same outer editorial article as
	// its hero. Do not cross a different nested article on the cover's path.
	for n := bodyNode; n != nil; n = n.Parent {
		if n.Type == html.ElementNode && (n.Data == "article" || nodeHasArticleRole(n)) && containsNode(n, coverNode) {
			return withinArticleBoundary(n, coverNode)
		}
	}
	return false
}

func withinArticleBoundary(boundary, cover *html.Node) bool {
	for p := cover.Parent; p != nil && p != boundary; p = p.Parent {
		if p.Type == html.ElementNode && (p.Data == "article" || nodeHasArticleRole(p)) {
			return false
		}
	}
	return true
}

func nodeHasArticleRole(n *html.Node) bool {
	for _, a := range n.Attr {
		if a.Key == "role" && strings.EqualFold(a.Val, "article") {
			return true
		}
	}
	return false
}
func hasRelatedSignal(s *goquery.Selection) bool {
	for _, attr := range []string{"class", "id", "aria-label", "data-testid"} {
		for _, part := range strings.FieldsFunc(strings.ToLower(s.AttrOr(attr, "")), func(r rune) bool { return r == '-' || r == '_' || r == ' ' }) {
			if part == "related" || part == "recommendations" || part == "recommended" {
				return true
			}
		}
	}
	return false
}
func usableImage(s *goquery.Selection) bool {
	raw := strings.TrimSpace(s.AttrOr("src", s.AttrOr("data-src", "")))
	if raw == "" || len(raw) > 2048 || strings.HasPrefix(raw, "#") {
		return false
	}
	u, err := url.Parse(raw)
	return err == nil && u.User == nil && (u.Scheme == "" || u.Scheme == "http" || u.Scheme == "https")
}
func containsNode(parent, node *html.Node) bool {
	for n := node; n != nil; n = n.Parent {
		if n == parent {
			return true
		}
	}
	return false
}
