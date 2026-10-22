package extractor

import (
	"context"
	"strings"
	"testing"
)

func TestAutoRetainsRenderedTableFacts(t *testing.T) {
	static := `<main><article><p>` + prose + `</p><table id="measurements"></table></article></main>`
	rendered := strings.Replace(static, `<table id="measurements"></table>`, `<table id="measurements"><tr><th>Station</th><th>Reading</th></tr><tr><td>North station</td><td>42 units</td></tr></table>`, 1)
	got := Extract(context.Background(), Input{URL: "https://example.test/report", SourceType: "article", Strategy: "auto", HTML: static, RenderedHTML: rendered})
	if got.State != "extracted" || got.Method != "browser_rendering" || !strings.Contains(got.Content, "North station 42 units") {
		t.Fatalf("rendered table facts lost: %+v", got)
	}
}

func TestRenderedTableRequiresCorrespondingIdentity(t *testing.T) {
	if hasPopulatedTableCounterpart(parse(`<table id="measurements"></table>`), parse(`<table id="recommendations"><tr><td>Unrelated</td></tr></table>`)) {
		t.Fatal("unrelated table accepted as counterpart")
	}
	if hasPopulatedTableCounterpart(parse(`<table class="prices"></table>`), parse(`<table class="other"><tr><td>Unrelated</td></tr></table>`)) {
		t.Fatal("different table class accepted")
	}
}
