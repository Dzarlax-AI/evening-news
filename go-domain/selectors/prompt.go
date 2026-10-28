package selectors

import "fmt"

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
