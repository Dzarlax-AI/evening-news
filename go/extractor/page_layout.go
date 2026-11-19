package extractor

import (
	"crypto/sha256"
	"encoding/hex"

	"github.com/Dzarlax-AI/evening-news/go/pagelayout"
	"github.com/PuerkitoBio/goquery"
)

// ReparseArticleLayout applies a layout only to the exact snapshot it was
// studied against. A false return leaves the caller's original result intact.
// This function does no network I/O and never mutates the supplied HTML.
func ReparseArticleLayout(in Input, original Result, snapshotDigest string, layout pagelayout.Layout) (Result, bool) {
	if (in.SourceType != "" && in.SourceType != "article" && in.SourceType != "web") || in.HTML == "" || len(in.HTML) > 5*1024*1024 || len(snapshotDigest) != 64 {
		return original, false
	}
	sum := sha256.Sum256([]byte(in.HTML))
	if snapshotDigest != hex.EncodeToString(sum[:]) {
		return original, false
	}
	pagelayout.Validate(&layout, in.HTML)
	if layout.PageKind != "article" {
		return original, false
	}
	doc := parse(in.HTML)
	if doc == nil {
		return original, false
	}
	for _, selector := range layout.ExcludeBlocks {
		selectSafe(doc.Selection, selector).Remove()
	}
	body := selectSafe(doc.Selection, layout.ArticleBody)
	if body.Length() != 1 {
		return original, false
	}
	maxChars := in.MaxChars
	if maxChars == 0 {
		maxChars = 100000
	}
	content := text(body)
	if maxChars < 1 || !good(content, true, maxChars) {
		return original, false
	}
	result := original
	result.Trace = append([]string(nil), original.Trace...)
	result.Content = content
	result.State = "extracted"
	result.Error = ""
	result.Method = "ai_selectors"
	result.MethodUsed = "ai_selectors"
	result.SelectorUsed = layout.ArticleBody
	var cover *goquery.Selection
	if layout.EditorialCover != "" {
		cover = selectSafe(doc.Selection, layout.EditorialCover)
	}
	applyEditorialMedia(&result, doc, in.HTML, in.URL, body, cover)
	result.Quality = qualityScore(content)
	result.OriginalChars = runeLen(content)
	result.Truncated = false
	result.TruncationPolicy = ""
	if (in.SourceType == "article" || in.SourceType == "web" || in.SourceType == "") && result.OriginalChars > 8000 {
		result.Content = smartTruncate(content, 8000)
		result.Truncated = true
		result.TruncationPolicy = "sentence_or_word_8000"
		result.Trace = append(append([]string(nil), result.Trace...), "finalize_truncated")
	}
	return result, true
}
