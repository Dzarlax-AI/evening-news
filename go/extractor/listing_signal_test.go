package extractor

import (
	"fmt"
	"strings"
	"testing"
)

func TestStrongListingSignalRequiresDominantCardsAndNoArticle(t *testing.T) {
	var cards strings.Builder
	for i := 0; i < 12; i++ {
		fmt.Fprintf(&cards, `<li><a href="/news/%d">An individual headline about the city and its residents number %d</a></li>`, i, i)
	}
	list := `<main><ul>` + cards.String() + `</ul></main>`
	if !StrongListingSignal(list) {
		t.Fatal("dominant listing missed")
	}
	for _, html := range []string{
		`<main><article><p>A short article contains real original reporting.</p></article><ul>` + cards.String() + `</ul></main>`,
		`<main><article><p>` + strings.Repeat("Article words and substantive editorial content. ", 8) + `</p></article><ul>` + cards.String() + `</ul></main>`,
		`<main><ul><li><a href="/a">One recommendation headline</a></li><li><a href="/b">Another recommendation headline</a></li></ul></main>`,
	} {
		if StrongListingSignal(html) {
			t.Fatal("article or small recommendation group treated as listing")
		}
	}
}
