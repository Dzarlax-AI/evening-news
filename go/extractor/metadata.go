package extractor

import (
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/itlightning/dateparse"
)

var metadataDate = regexp.MustCompile(`(?i)\b(?:\d{4}-\d{2}-\d{2}(?:T\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?(?:Z|[+-]\d{2}:?\d{2})?)?|\d{1,2}/\d{1,2}/\d{4}|[A-Za-z]+ \d{1,2},? \d{4}|\d{1,2} [A-Za-z]+,? \d{4})\b`)
var dayMonthComma = regexp.MustCompile(`(?i)(\d{1,2} [A-Za-z]+), (\d{4})`)

func normalizeDate(s string) string {
	if s == "" {
		return ""
	}
	parseValue := dayMonthComma.ReplaceAllString(s, "$1 $2")
	t, e := dateparse.ParseAny(parseValue)
	if e != nil {
		return s
	}
	// Python preserves the absence of a timezone for naive metadata dates.
	if !strings.ContainsAny(s, "Zz+") && !strings.Contains(s, "GMT") && !strings.Contains(s, "UTC") && !strings.Contains(s, " -") && !strings.Contains(s, "T") {
		return t.Format("2006-01-02T15:04:05")
	}
	if strings.Contains(s, "T") && !strings.ContainsAny(strings.SplitN(s, "T", 2)[1], "Zz+-") {
		return t.Format("2006-01-02T15:04:05")
	}
	return t.UTC().Format(time.RFC3339)
}

func normalizeMetadataDate(s string) string {
	parseValue := dayMonthComma.ReplaceAllString(s, "$1 $2")
	if _, err := dateparse.ParseAny(parseValue); err != nil {
		return ""
	}
	return normalizeDate(s)
}

func metadata(d *goquery.Document, r *Result) {
	if d == nil {
		return
	}
	r.Title = meta(d, `meta[property="og:title"]`, `meta[name="twitter:title"]`, "title", "h1")
	r.Description = meta(d, `meta[property="og:description"]`, `meta[name="twitter:description"]`, `meta[name="description"]`)
	r.Author = ""
	for _, q := range []string{`[itemprop="author"]`, `[rel="author"]`, ".author", ".byline", ".author-name", ".writer", ".journalist"} {
		s := text(selectSafe(d.Selection, q).First())
		if s != "" && runeLen(s) < 100 {
			r.Author = s
			break
		}
	}
	if r.Author == "" {
		r.Author = meta(d, `meta[name="author"]`)
	}
	items := jsonItems(d)
	r.PublicationDate = ""
	for _, it := range items {
		typ := fmt.Sprint(it["@type"])
		if a, ok := it["@type"].([]any); ok && len(a) > 0 {
			typ = fmt.Sprint(a[0])
		}
		if typ != "Article" && typ != "NewsArticle" && typ != "BlogPosting" && typ != "WebPage" {
			continue
		}
		for _, k := range []string{"datePublished", "publishedTime", "dateCreated", "dateModified"} {
			if s, ok := it[k].(string); ok && s != "" {
				r.PublicationDate = s
				break
			}
		}
		if r.PublicationDate != "" {
			break
		}
	}
	if r.PublicationDate == "" {
		for _, q := range []string{`[itemprop="datePublished"]`, `[itemprop="publishedTime"]`, `[itemprop="dateCreated"]`, `meta[property="article:published_time"]`, `meta[property="article:published"]`, `meta[name="pubdate"]`, `meta[name="publishdate"]`, `meta[name="date"]`, ".publish-date", ".published-date", ".publication-date", ".date-published", ".pub-date", ".article-date", ".date-time", ".datetime", ".timestamp", ".entry-date", ".post-date", ".news-date", "time[datetime]", "time[pubdate]", "time", ".byline .date", ".meta .date", ".info .date", "header .date", ".article-header .date", ".article-meta time", ".entry-meta time"} {
			s := selectSafe(d.Selection, q)
			for i := range s.Nodes {
				e := s.Eq(i)
				v := e.AttrOr("datetime", e.AttrOr("content", e.AttrOr("value", text(e))))
				if metadataDate.MatchString(v) {
					if date := normalizeMetadataDate(v); date != "" {
						r.PublicationDate = date
						break
					}
				}
			}
			if r.PublicationDate != "" {
				break
			}
		}
	}
	if r.PublicationDate == "" {
		visible := d.Selection.Clone()
		visible.Find("script,style,noscript,template,nav,footer").Remove()
		for _, v := range metadataDate.FindAllString(visible.Text(), -1) {
			if date := normalizeMetadataDate(v); date != "" {
				r.PublicationDate = date
				break
			}
		}
	}
	if r.ImageURL == "" {
		r.ImageURL = meta(d, `meta[property="og:image"]`, `meta[property="og:image:url"]`, `meta[name="twitter:image"]`, `meta[name="twitter:image:src"]`, `link[rel="image_src"]`)
		if strings.HasPrefix(r.ImageURL, "//") {
			r.ImageURL = "https:" + r.ImageURL
		}
		if !strings.HasPrefix(r.ImageURL, "http://") && !strings.HasPrefix(r.ImageURL, "https://") {
			r.ImageURL = ""
		}
	}
	if r.ImageURL == "" {
		var image func(any) string
		image = func(v any) string {
			switch x := v.(type) {
			case string:
				if strings.HasPrefix(x, "http") {
					return x
				}
			case map[string]any:
				return image(x["url"])
			case []any:
				if len(x) > 0 {
					return image(x[0])
				}
			}
			return ""
		}
		for _, it := range items {
			if s := image(it["image"]); s != "" {
				r.ImageURL = s
				break
			}
		}
	}
	if r.ImageURL == "" {
		for _, q := range []string{"article", "main", `[role="main"]`} {
			selectSafe(d.Selection, q).First().Find("img[src]").EachWithBreak(func(_ int, e *goquery.Selection) bool {
				src := e.AttrOr("src", "")
				if !strings.HasPrefix(src, "http") {
					return true
				}
				low := strings.ToLower(src)
				for _, p := range []string{"icon", "avatar", "logo", "pixel", "tracker", "1x1"} {
					if strings.Contains(low, p) {
						return true
					}
				}
				var w, h int
				if _, err := fmt.Sscan(e.AttrOr("width", ""), &w); err == nil {
					if _, err = fmt.Sscan(e.AttrOr("height", ""), &h); err == nil && (w < 100 || h < 100) {
						return true
					}
				}
				r.ImageURL = src
				return false
			})
			if r.ImageURL != "" {
				break
			}
		}
	}
}
