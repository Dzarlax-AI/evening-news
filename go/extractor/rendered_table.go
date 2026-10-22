package extractor

import (
	"github.com/PuerkitoBio/goquery"
	"sort"
	"strings"
)

// A populated counterpart is evidence that an empty static widget was not the
// complete source. Match table identity and order, not arbitrary page text.
func hasPopulatedTableCounterpart(static, rendered *goquery.Document) bool {
	before, after := static.Find("table"), rendered.Find("table")
	if before.Length() == 0 || before.Length() != after.Length() {
		return false
	}
	classes := func(s *goquery.Selection) string {
		parts := strings.Fields(s.AttrOr("class", ""))
		sort.Strings(parts)
		return strings.Join(parts, " ")
	}
	for i := range before.Nodes {
		a, b := before.Eq(i), after.Eq(i)
		if a.Find("td,th").Length() != 0 || b.Find("td,th").Length() == 0 {
			continue
		}
		aid, bid := a.AttrOr("id", ""), b.AttrOr("id", "")
		if aid != "" || bid != "" {
			if aid == bid {
				return true
			}
			continue
		}
		if classes(a) == classes(b) {
			return true
		}
	}
	return false
}
