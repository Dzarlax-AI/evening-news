package extractor

import (
	"context"
	"testing"
)

func TestCustomLinkContainersPreserveTitleAndIdentity(t *testing.T) {
	for _, tc := range []struct{ name, markup, selector string }{
		{"self", `<a class="project" href="/projects/river">Independent river observation project</a>`, "a.project"},
		{"ancestor", `<a href="/projects/river"><div class="project"><h2>Independent river observation project</h2></div></a>`, ".project"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := Extract(context.Background(), Input{URL: "https://fixture.test/", SourceType: "custom", Strategy: "custom_selectors", HTML: tc.markup, Now: "2026-10-15T12:00:00Z", Selectors: map[string][]string{"article": {tc.selector}}})
			if got.State != "extracted" || got.URL != "https://fixture.test/projects/river" || got.Title != "Independent river observation project" {
				t.Fatalf("unexpected custom link extraction: %+v", got)
			}
		})
	}
}
