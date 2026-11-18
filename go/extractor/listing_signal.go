package extractor

import (
	"strings"
	"unicode/utf8"

	"github.com/PuerkitoBio/goquery"
)

// StrongListingSignal is deliberately conservative: it is only a reason to
// ask for a page study, never enough on its own to reject an article.
func StrongListingSignal(raw string) bool {
	if raw == "" || len(raw) > 2*1024*1024 {
		return false
	}
	doc := parse(raw)
	if doc == nil {
		return false
	}
	main := doc.Find("main,[role=main]").First()
	if main.Length() != 1 {
		return false
	}
	clone := main.Clone()
	clone.Find("nav,footer,aside,[role=navigation],[role=contentinfo]").Remove()
	groups := 0
	cardText := 0
	clone.Find("ul,ol").Each(func(_ int, group *goquery.Selection) {
		if group.ParentsFiltered("article,[role=article]").Length() > 0 {
			return
		}
		distinct := map[string]bool{}
		group.ChildrenFiltered("li").Each(func(_ int, item *goquery.Selection) {
			links := item.Find("a[href]")
			if links.Length() != 1 {
				return
			}
			href := strings.TrimSpace(links.First().AttrOr("href", ""))
			label := strings.TrimSpace(links.First().Text())
			if href != "" && !strings.HasPrefix(href, "#") && utf8.RuneCountInString(label) >= 15 && utf8.RuneCountInString(label) <= 800 {
				distinct[href] = true
			}
		})
		if len(distinct) >= 5 {
			groups++
			cardText += utf8.RuneCountInString(strings.TrimSpace(group.Text()))
			group.Remove()
		}
	})
	if groups == 0 {
		return false
	}
	// Prose that survives card removal indicates a real article with a list or
	// recommendations. Unknown cases must keep the baseline extraction.
	if clone.Find("article,[role=article],[itemprop=articleBody]").FilterFunction(func(_ int, s *goquery.Selection) bool {
		return utf8.RuneCountInString(strings.TrimSpace(s.Text())) >= 30
	}).Length() > 0 {
		return false
	}
	prose := 0
	clone.Find("p").Each(func(_ int, p *goquery.Selection) {
		if p.ParentsFiltered("article,nav,footer,aside").Length() == 0 {
			prose += utf8.RuneCountInString(strings.TrimSpace(p.Text()))
		}
	})
	return prose < 1000 && cardText > prose*4
}
