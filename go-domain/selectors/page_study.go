package selectors

import "github.com/Dzarlax-AI/evening-news/go/pagelayout"

// validatePageStudy evaluates optional AI hints against the same captured DOM.
func validatePageStudy(a *Analysis, raw string) {
	if a == nil || !a.pageKindProvided {
		return
	}
	layout := pagelayout.Layout{PageKind: a.PageKind, ArticleBody: a.ArticleBody, EditorialCover: a.EditorialCover, ExcludeBlocks: a.ExcludeBlocks, ContentSelectors: a.ContentSelectors, TitleSelectors: a.TitleSelectors, DateSelectors: a.DateSelectors}
	pagelayout.Validate(&layout, raw)
	a.PageKind, a.ArticleBody, a.EditorialCover, a.ExcludeBlocks = layout.PageKind, layout.ArticleBody, layout.EditorialCover, layout.ExcludeBlocks
	a.ContentSelectors, a.TitleSelectors, a.DateSelectors = layout.ContentSelectors, layout.TitleSelectors, layout.DateSelectors
}
