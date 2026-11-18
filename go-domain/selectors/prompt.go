package selectors

import "fmt"

func StudyPrompt(url, prepared, context string) string {
	return StructurePrompt(url, prepared, context) + `

Also return page_kind as "article", "listing", or "unknown". The existing
content_selectors continue to select list entries, never a single article body.
For a listing, choose selectors for the repeated sibling cards across all list
branches, not just a single featured card. In the structural outline, count=N
shows how often a path occurs; verify that list selectors match multiple items.
For an article, optionally return article_body_selector, editorial_cover_selector
(an actual img element), and exclude_selectors (author, share, and related blocks).
Check the article header and hero near its headline for the editorial cover,
even when the text body is nested more deeply. Ignore images in other cards.
Use only CSS present in the HTML; when uncertain use "unknown" and empty values.`
}

type promptContext struct{ description, patterns, examples string }

func StructurePrompt(url, prepared, context string) string {
	contexts := map[string]promptContext{
		"changelog": {"changelog entries, version updates, release notes", `version numbers, dates, "Added", "Fixed", "Changed"`, "changelog items, release entries, update notes"},
		"news":      {"news articles, announcements, press releases", "publication dates, headlines, article summaries", "news items, articles, announcements"},
		"blog":      {"blog posts, articles, tutorials", "post titles, author names, publication dates", "blog posts, articles, tutorials"},
	}
	c, ok := contexts[context]
	if !ok {
		c = contexts["news"]
	}
	return fmt.Sprintf(`Analyze this webpage to find the best CSS selectors for extracting %s.

URL: %s
Context: Looking for %s
Typical patterns: %s

HTML STRUCTURE:
`+"```html\n"+`%s
`+"```"+`

TASK: Identify CSS selectors that would reliably extract %s from this page.

Look for:
1. CONTENT SELECTORS - Elements that contain the main %s
2. TITLE SELECTORS - Elements with titles/headlines within those content elements
3. DATE SELECTORS - Elements with dates/timestamps
4. PAGE TYPE - What kind of page this is (changelog, blog, news, docs)

RESPOND IN JSON FORMAT:
{
  "content_selectors": ["selector1", "selector2", "selector3"],
  "title_selectors": ["h1", "h2", ".title"],
  "date_selectors": ["time", ".date", ".published"],
  "confidence_scores": {
    "selector1": 0.9,
    "selector2": 0.8
  },
  "page_type": "changelog",
  "reasoning": "This page contains version update entries organized in a timeline format...",
  "suggested_config": {
    "min_title_length": 10,
    "max_articles_per_check": 20,
    "wait_for_js": true
  }
}

Focus on selectors that:
- Target actual content, not navigation/ads/boilerplate
- Are specific enough to avoid false positives
- Will work consistently across page updates
- Follow common patterns for this type of content`, c.description, url, c.examples, c.patterns, prepared, c.examples, c.examples)
}
