package extractor

import (
	"net/url"
	"path"
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"
)

const mediaChrome = "script,style,template,noscript,nav,aside,footer,form,dialog,button,[role=button],[role=dialog],[aria-modal=true],[role=navigation],[role=contentinfo],[data-native-ad-id],.newsletter-signup,.subscription-widget,.subscribe-widget,.ad-container,.advertisement,.social-share-bar,[itemprop=author]"
const articleContainer = "article,[role=article],[itemprop=articleBody]"

// A markup-shaped media insertion point is stronger evidence than an empty
// paragraph or a comment mentioning images. The browser executes the page's
// own loader. The extractor never interprets the marker as a URL or command.
var mediaComment = regexp.MustCompile(`(?i)^<[a-z][a-z0-9:-]*\s+[^<>]*\b(?:media|image|picture)\b[^<>]*>\s*$`)

type editorialRegion struct {
	root, body *goquery.Selection
}

func findEditorialRegion(doc *goquery.Document, selected *goquery.Selection, title string) editorialRegion {
	if selected == nil || selected.Length() == 0 {
		return editorialRegion{}
	}
	body := selected
	if selected.Length() != 1 {
		return editorialRegion{body: body}
	}
	if doc == nil || selected.Nodes[0].Type != html.ElementNode || selected.Is("html,body") {
		return editorialRegion{}
	}
	heading := doc.Find("h1").FilterFunction(func(_ int, h *goquery.Selection) bool {
		return !insideRecommendationContainer(h) && !inMediaChrome(h) && (title == "" || clean(h.Text()) == clean(title))
	})
	if heading.Length() != 1 && title != "" {
		heading = selected.Find("h1").FilterFunction(func(_ int, h *goquery.Selection) bool {
			return !inMediaChrome(h) && !insideRecommendationContainer(h)
		})
	}
	root := selected
	if heading.Length() == 1 {
		prose := paragraphWeight(selected)
		for n, depth := heading.Parent(), 0; depth < 8 && n.Length() == 1 && !n.Is("html,body"); n, depth = n.Parent(), depth+1 {
			if !containsSelection(selected, n) && !containsSelection(n, selected) {
				continue
			}
			if prose >= 100 && paragraphWeight(n)*4 >= prose*3 {
				root = n
				if containsSelection(selected, n) {
					body = n
				}
				break
			}
		}
	}
	for body.Length() == 1 {
		weight := paragraphWeight(body)
		if weight < 200 {
			break
		}
		var next *goquery.Selection
		body.ChildrenFiltered("article,main,section,div,[role=article]").EachWithBreak(func(_ int, child *goquery.Selection) bool {
			if paragraphWeight(child)*5 >= weight*4 {
				next = child
				return false
			}
			return true
		})
		if next == nil {
			break
		}
		body = next
	}
	// A semantic article can place a hero before the inner title/prose
	// wrapper. Keep that boundary, while still requiring editorial placement.
	if semantic := root.Closest("article,[role=article]"); semantic.Length() == 1 {
		root = semantic
	}
	if heading.Length() != 1 && !root.Is(articleContainer) && paragraphWeight(body) < 100 &&
		(!body.Is("p") && (body.Is("main,[role=main]") || body.Find("article,main,nav,aside").Length() > 0)) {
		return editorialRegion{}
	}
	return editorialRegion{root: root, body: body}
}

func containsSelection(container, child *goquery.Selection) bool {
	if container == nil || child == nil || container.Length() != 1 || child.Length() != 1 {
		return false
	}
	for n := child.Nodes[0]; n != nil; n = n.Parent {
		if n == container.Nodes[0] {
			return true
		}
	}
	return false
}

func paragraphWeight(s *goquery.Selection) int {
	total := 0
	s.Find("p").AddBackFiltered("p").Each(func(_ int, p *goquery.Selection) {
		if inMediaChrome(p) {
			return
		}
		length := runeLen(clean(p.Text()))
		linked := 0
		p.Find("a[href]").Each(func(_ int, a *goquery.Selection) { linked += runeLen(clean(a.Text())) })
		if length >= 45 && linked*2 < length {
			total += length
		}
	})
	return total
}

func inMediaChrome(s *goquery.Selection) bool {
	return s.Closest(mediaChrome).Length() > 0 || s.Parents().AddBack().FilterFunction(func(_ int, n *goquery.Selection) bool {
		for _, attr := range []string{"class", "id"} {
			for _, word := range strings.FieldsFunc(strings.ToLower(n.AttrOr(attr, "")), func(r rune) bool {
				return r == ' ' || r == '-' || r == '_'
			}) {
				switch word {
				case "author", "avatar", "byline", "sidebar":
					return true
				}
			}
		}
		return false
	}).Length() > 0
}

func articleDestination(s *goquery.Selection, base string) string {
	raw := strings.TrimSpace(s.AttrOr("href", ""))
	if raw == "" || strings.HasPrefix(raw, "#") {
		return ""
	}
	target, err := url.Parse(safeRSSHTTPURL(raw, base))
	if err != nil || target.Host == "" {
		return ""
	}
	switch strings.ToLower(path.Ext(target.Path)) {
	case ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".avif", ".bmp", ".tiff", ".mp4", ".webm", ".mp3", ".pdf":
		return ""
	}
	return articleLinkIdentity(target)
}

func articleLinkIdentity(u *url.URL) string {
	identity := strings.ToLower(u.Host) + strings.TrimRight(u.EscapedPath(), "/")
	query, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return identity + "?" + u.RawQuery
	}
	for key := range query {
		lower := strings.ToLower(key)
		if strings.HasPrefix(lower, "utm_") || lower == "fbclid" || lower == "gclid" {
			delete(query, key)
		}
	}
	if encoded := query.Encode(); encoded != "" {
		identity += "?" + encoded
	}
	return identity
}

// Cards may have arbitrary names, or different modifier classes. Their linked
// headline and repeated destinations distinguish them from editorial figures.
// A bare image link inside a figure can open a full-size image using
// an extensionless URL. A linked headline or caption still describes a card.
func articleMediaLink(link *goquery.Selection, base string) bool {
	destination := articleDestination(link, base)
	if destination == "" {
		return false
	}
	article, err := url.Parse(base)
	if err == nil {
		current := articleLinkIdentity(article)
		destinationPath, _, _ := strings.Cut(destination, "?")
		currentPath, _, _ := strings.Cut(current, "?")
		if destinationPath == currentPath {
			return destination != current
		}
	}
	return link.Closest("figure").Length() == 0 || clean(link.Text()) != "" ||
		link.Find("h1,h2,h3,h4,h5,h6").Length() > 0 || link.Find("img").Length() != 1
}

func cardRoots(root *goquery.Selection, base string) map[*html.Node]bool {
	cards := make(map[*html.Node]bool)
	const groupSelector = "div,section,ul,ol,li,article"
	root.Find(groupSelector).AddBackFiltered(groupSelector).Each(func(_ int, group *goquery.Selection) {
		if group.Children().Length() < 2 {
			return
		}
		destinations := make(map[string]bool)
		var members []*html.Node
		group.Children().Each(func(_ int, card *goquery.Selection) {
			if card.Find("img,picture").Length() == 0 {
				return
			}
			destination := ""
			card.Find("a[href]").AddBackFiltered("a[href]").EachWithBreak(func(_ int, a *goquery.Selection) bool {
				if runeLen(clean(a.Text())) >= 12 || a.Find("h2,h3,h4").Length() > 0 {
					destination = articleDestination(a, base)
				}
				return destination == ""
			})
			if destination != "" && paragraphWeight(card) < 100 {
				destinations[destination] = true
				members = append(members, card.Nodes[0])
				if hasLinkedHeadline(card) {
					cards[card.Nodes[0]] = true
				}
			}
		})
		if len(destinations) >= 2 {
			for _, member := range members {
				cards[member] = true
			}
		}
	})
	return cards
}

func (region editorialRegion) mediaSelection(base string, cover *goquery.Selection) (*goquery.Selection, bool, bool) {
	if region.body == nil || region.body.Length() == 0 {
		return nil, false, false
	}
	if region.root == nil || region.root.Length() != 1 {
		return region.body, false, false
	}
	root := region.root
	cards := cardRoots(root, base)
	allowed := func(n *goquery.Selection) bool {
		if inMediaChrome(n) || insideRecommendationContainer(n) {
			return false
		}
		for p := n.Nodes[0]; p != nil; p = p.Parent {
			if cards[p] {
				return false
			}
			if p == root.Nodes[0] {
				break
			}
			if p != n.Nodes[0] && p.Type == html.ElementNode && goquery.NewDocumentFromNode(p).Selection.Is("article,[role=article]") &&
				!containsSelection(goquery.NewDocumentFromNode(p).Selection, region.body) &&
				!containsSelection(region.body, goquery.NewDocumentFromNode(p).Selection) {
				return false
			}
		}
		return !articleMediaLink(n.Closest("a[href]"), base)
	}
	clone := root.Clone()
	original, copied := root.Find("*"), clone.Find("*")
	coverAdmitted := false
	removed := make(map[*html.Node]bool)
	original.Each(func(i int, n *goquery.Selection) {
		node := n.Nodes[0]
		if removed[node.Parent] {
			removed[node] = true
			return
		}
		if !allowed(n) || (!containsSelection(region.body, n) && !leadMediaAllowed(root, n)) {
			removed[node] = true
			copied.Eq(i).Remove()
			return
		}
		if cover != nil && cover.Length() == 1 && cover.Nodes[0] == n.Nodes[0] {
			coverAdmitted = true
		}
	})

	// Walk only the pruned prose subtree for pending media. A header, related
	// card, or empty article cannot independently request browser admission.
	incomplete := false
	weight := paragraphWeight(region.body)
	if !removed[region.body.Nodes[0]] && weight >= 200 && (root.Find("h1").Length() == 1 || root.Is(articleContainer) || region.body.Is(articleContainer)) {
		prunedBody := clone
		if region.body.Nodes[0] != root.Nodes[0] {
			for i, node := range original.Nodes {
				if node == region.body.Nodes[0] {
					prunedBody = copied.Eq(i)
					break
				}
			}
		}
		incomplete = pendingEditorialMedia(prunedBody)
	}
	return clone, incomplete, coverAdmitted
}

func leadMediaAllowed(root, n *goquery.Selection) bool {
	switch {
	case n.Is("audio"), n.Is("source") && n.Closest("audio").Length() > 0:
		// A narration player can precede the title inside the article.
		return true
	case n.Is("video"):
		return containsSelection(root, n.Closest("figure"))
	case n.Is("img,picture,source"):
		parent := n.Parent()
		return n.Closest("figure,picture,header").Length() > 0 ||
			(parent.Children().Length() == 1 && containsSelection(root, parent) &&
				parent.Parent().Length() == 1 && parent.Parent().Nodes[0] == root.Nodes[0])
	case n.Is("a[href]"):
		return n.Find("img").Length() > 0 && n.Closest("figure,picture,header").Length() > 0
	default:
		return !n.Is("video,iframe")
	}
}

func pendingEditorialMedia(body *goquery.Selection) bool {
	for _, node := range body.Nodes {
		var walk func(*html.Node) bool
		walk = func(n *html.Node) bool {
			if n.Type == html.CommentNode && n.Parent != nil &&
				(n.Parent.Data == "p" || n.Parent.Data == "figure" || n.Parent.Data == "div") &&
				mediaComment.MatchString(strings.TrimSpace(n.Data)) {
				return true
			}
			if n.Type == html.ElementNode && n.Data == "img" {
				image := goquery.NewDocumentFromNode(n).Selection
				raw := strings.TrimSpace(image.AttrOr("src", ""))
				if (raw == "" || strings.HasPrefix(raw, "data:")) &&
					image.Closest("figure,picture").Length() > 0 &&
					(image.AttrOr("srcset", "") != "" || image.AttrOr("data-srcset", "") != "" ||
						image.Closest("picture").Find("source[srcset],source[data-srcset]").Length() > 0) {
					return true
				}
			}
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if walk(c) {
					return true
				}
			}
			return false
		}
		if walk(node) {
			return true
		}
	}
	return false
}

// applyEditorialMedia stores bounded rollback candidates separately from the
// snapshot's positive evidence. Unknown scope is explicit even with zero media.
func applyEditorialMedia(r *Result, doc *goquery.Document, page, base string, body *goquery.Selection, studiedCover *goquery.Selection) {
	region := findEditorialRegion(doc, body, r.Title)
	selected, pending, studiedCoverAdmitted := region.mediaSelection(base, studiedCover)
	r.PendingEditorialMedia = pending
	media, truncated, verified := scopedArticleMedia(page, base, selected)
	r.Media = media
	r.ImageURL = ""
	evidence := &MediaEvidence{Version: 1, ArticleScope: "unknown", ArticleURL: base}
	r.MediaEvidence = evidence
	if verified {
		evidence.ArticleScope = "verified"
		cover, source := "", ""
		if studiedCoverAdmitted {
			cover, source = safeRSSHTTPURL(studiedCover.AttrOr("src", studiedCover.AttrOr("data-src", "")), base), "selector_study"
		}
		if cover == "" && strings.EqualFold(strings.TrimSpace(meta(doc, `meta[property="og:type"]`)), "article") {
			cover, source = safeRSSHTTPURL(meta(doc, `meta[property="og:image"]`), base), "og_image"
		}
		firstOrder := maxArticleMediaCount + 1
		for _, item := range r.Media {
			order, _ := item.Data["editorial_order"].(int)
			if item.Type != "image" || order < 1 {
				continue
			}
			if order < firstOrder {
				firstOrder = order
				r.ImageURL = item.URL
			}
			if item.URL == cover && len(cover) <= 2048 && len(base) <= 2048 {
				evidence.EditorialCover = EditorialCoverEvidence{URL: cover, Source: source, ArticleURL: base}
			}
		}
		if evidence.EditorialCover.URL != "" {
			r.ImageURL = evidence.EditorialCover.URL
		}
	}
	if truncated {
		r.Trace = append(r.Trace, "media_truncated")
	}
}

func decorativeArticleImage(item Media) bool {
	return item.Data["decorative"] == true ||
		(smallImageDimension(item.Data["width"]) && smallImageDimension(item.Data["height"]))
}

func smallImageDimension(value any) bool {
	n, _ := value.(int)
	return n > 0 && n <= 64
}

// A cleaned Readability node is not a source DOM boundary. Recover a unique
// original body containing a substantial passage of the extracted prose.
func matchingProseBody(doc *goquery.Document, content string) *goquery.Selection {
	var result *goquery.Selection
	doc.Find(articleContainer).EachWithBreak(func(_ int, n *goquery.Selection) bool {
		if inMediaChrome(n) || insideRecommendationContainer(n) || paragraphWeight(n) < 100 {
			return true
		}
		found := n.Find("p").FilterFunction(func(_ int, p *goquery.Selection) bool {
			value := clean(p.Text())
			return runeLen(value) >= 100 && strings.Contains(content, value)
		}).Length() > 0
		if !found {
			return true
		}
		if result == nil {
			result = n
			return true
		}
		if containsSelection(result, n) {
			return true
		}
		result = nil
		return false
	})
	return result
}
