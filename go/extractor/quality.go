package extractor

import (
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
)

// Prefer explicitly marked prose over surrounding player, byline and sharing
// controls. Repeated body components are joined in document order.
func editorialBody(s *goquery.Selection) *goquery.Selection {
	for _, q := range []string{`[itemprop="articleBody"]`, `.body.markup`, `.real-body`, `[class*="article-body"]`} {
		n := s.Find(q).FilterFunction(func(_ int, e *goquery.Selection) bool {
			return e.Is("article,main,[role=article]") || e.ParentsFiltered("article,main,[role=article]").Length() > 0
		})
		if n.Length() == 0 || n.Find("p").Length() < 2 {
			continue
		}
		// Nested matches otherwise duplicate prose when serialized.
		n = n.FilterFunction(func(_ int, e *goquery.Selection) bool { return e.ParentsFiltered(q).Length() == 0 })
		if good(text(n), true, 100000) {
			return n
		}
	}
	return &goquery.Selection{}
}

// Decorative fragment permalinks must not turn a heading-only wrapper into
// a low-scoring text container that Readability discards.
func unwrapPermalinkHeadings(s *goquery.Selection) {
	s.Find("h1,h2,h3,h4,h5,h6").Each(func(_ int, h *goquery.Selection) {
		p := h.Parent()
		if !p.Is("div") || p.Children().Length() != 2 {
			return
		}
		a := p.ChildrenFiltered("a")
		if a.Length() != 1 || !strings.HasPrefix(a.AttrOr("href", ""), "#") || clean(a.Text()) != "" {
			return
		}
		p.ReplaceWithSelection(h)
	})
}

// Restore only a bounded lead tied to a paragraph already selected by the
// extractor. Never use a page-wide description as a substitute for body text.
func restoreIntroduction(d *goquery.Document, content, title string) string {
	lead := ""
	admit := func(n *goquery.Selection) bool {
		if n.Length() == 0 || n.ParentsFiltered("nav,aside,footer,figure,figcaption,dialog,[role=navigation]").Length() > 0 || n.Find("a,button,input,img,figure").Length() > 0 {
			return false
		}
		v := text(n)
		if runeLen(v) < 60 || runeLen(v) > 600 || strings.Contains(content, v) {
			return false
		}
		lead = v
		return true
	}
	d.Find("p").EachWithBreak(func(_ int, p *goquery.Selection) bool {
		anchor := text(p)
		if runeLen(anchor) < 60 || !strings.Contains(content, anchor) || p.ParentsFiltered("article,main,[role=article]").Length() == 0 {
			return true
		}
		// A standfirst immediately precedes the selected body container.
		for n, depth := p, 0; n.Length() > 0 && depth < 3; n, depth = n.Parent(), depth+1 {
			if n.Is("article,main,body") {
				break
			}
			prev := n.Prev()
			if prev.Is("h2,h3") && admit(prev) {
				return false
			}
		}
		// Title/subtitle siblings must share a nearby article container with
		// the confirmed body paragraph; this excludes site mastheads.
		for scope, depth := p.Parent(), 0; scope.Length() > 0 && depth < 6; scope, depth = scope.Parent(), depth+1 {
			if scope.Is("body,html") {
				break
			}
			found := false
			scope.Find("h1,h2").EachWithBreak(func(_ int, h *goquery.Selection) bool {
				if clean(text(h)) != clean(title) {
					return true
				}
				n := h.Next()
				cls := strings.ToLower(n.AttrOr("class", ""))
				if (n.Is("p,h2,h3") || strings.Contains(cls, "subtitle") || strings.Contains(cls, "standfirst")) && admit(n) {
					found = true
					return false
				}
				return true
			})
			if found {
				return false
			}
			if scope.Is("article,main,[role=article]") {
				break
			}
		}
		return false
	})
	if lead == "" {
		return content
	}
	if title != "" && strings.HasPrefix(content, title+" ") {
		return title + " " + lead + " " + strings.TrimPrefix(content, title+" ")
	}
	return lead + " " + content
}

// Prune widgets on the clone while keeping surrounding article paragraphs.
func pruneRecommendationControls(s *goquery.Selection) {
	// Ghost bookmark cards contain article excerpts and separate linked-site
	// attribution. Keep the excerpt/title while excluding card metadata.
	s.Find(".kg-bookmark-metadata").Remove()
	s.Find(`[role="region"][aria-label]`).AddBackFiltered(`[role="region"][aria-label]`).Each(func(_ int, n *goquery.Selection) {
		label := strings.ToLower(strings.TrimSpace(n.AttrOr("aria-label", "")))
		if label == "recent episodes" && n.Find("a[href]").Length() > 1 {
			n.Empty()
		}
	})
	s.Find("strong").Each(func(_ int, n *goquery.Selection) {
		if !strings.EqualFold(strings.TrimSpace(n.Text()), "Follow topics and authors") {
			return
		}
		for p := n.Parent(); p.Length() > 0; p = p.Parent() {
			if p.Is("article,main,body,html") {
				break
			}
			if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(p.Text())), "follow topics and authors") {
				break
			}
			if p.Find(`[role="button"],button`).Length() > 0 {
				p.Empty()
				break
			}
		}
	})
}

var subscriptionPrice = regexp.MustCompile(`(?i)\$\s*\d+(?:\.\d+)?\s*/\s*(?:year|month|week)`)

// Plain-text fallback candidates need a complete plan-chooser signature. Words
// describing subscriptions or privacy rules are also ordinary editorial prose.
func candidateRejection(s string) string {
	lower := strings.ToLower(s)
	prefix := lower[:min(len(lower), 100)]
	ending := strings.TrimRight(lower, " .→\n\t")
	if strings.Contains(prefix, "membership") && len(subscriptionPrice.FindAllStringIndex(lower, -1)) >= 2 && strings.Count(lower, "billed annually") >= 2 && strings.Contains(lower, "cancel anytime") && strings.HasSuffix(ending, "continue with free") {
		return "membership_ui"
	}
	return ""
}

// Vocabulary naturally repeats across long interviews. Measure bounded windows
// instead of dividing a document's finite vocabulary by its entire word count.
func locallyDiverse(words []string) bool {
	chunks := map[string]int{}
	duplicates, complete := 0, 0
	for start := 0; start < len(words); start += 100 {
		end := min(start+100, len(words))
		if end-start < 30 && start > 0 {
			break
		}
		if end-start == 100 {
			complete++
			chunk := strings.ToLower(strings.Join(words[start:end], " "))
			if chunks[chunk] > 0 {
				duplicates++
			}
			chunks[chunk]++
		}
		unique := map[string]bool{}
		for _, w := range words[start:end] {
			w = strings.TrimFunc(strings.ToLower(w), func(r rune) bool { return strings.ContainsRune(".,:;!?\"'()[]", r) })
			if runeLen(w) > 3 {
				unique[w] = true
			}
		}
		if float64(len(unique))/float64(end-start) < 0.3 {
			return false
		}
	}
	if complete >= 3 && duplicates*2 >= complete {
		return false
	}
	return true
}
