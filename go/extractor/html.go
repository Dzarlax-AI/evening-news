package extractor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	readability "codeberg.org/readeck/go-readability/v2"
	"github.com/PuerkitoBio/goquery"
	"github.com/andybalholm/cascadia"
	"golang.org/x/net/html"
	"golang.org/x/text/unicode/norm"
)

var selectors = []string{".mb-14", `[itemprop="articleBody"]`, `[itemprop="text"]`, "article", "main", ".article-content", ".post-content", ".entry-content", ".content", ".main-content", ".page-content", ".story-content", ".news-content", ".blog-content", "#content", "#main-content", "#article-content", "#post-content", "#story", "#article", ".container .content", ".wrapper .content", `div[role="main"]`, `[role="article"]`}

func runeLen(s string) int { return utf8.RuneCountInString(s) }

var punctuationSpace = regexp.MustCompile(`\s+([.,:;!?])`)

func clean(s string) string {
	return strings.TrimSpace(punctuationSpace.ReplaceAllString(strings.Join(strings.Fields(s), " "), "$1"))
}
func cleanURL(s string) string {
	for _, r := range []rune{'\u200b', '\u200c', '\u200d', '\u2060', '\ufeff', '\u00a0'} {
		s = strings.ReplaceAll(s, string(r), "")
	}
	s = norm.NFKC.String(s)
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) || unicode.In(r, unicode.Cf) || r == '\u00a0' {
			return -1
		}
		return r
	}, s))
}
func smartTruncate(s string, limit int) string {
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	cut := string(r[:limit])
	last := -1
	for _, end := range []string{". ", "! ", "? "} {
		if p := strings.LastIndex(cut, end); p >= 0 {
			n := runeLen(cut[:p]) + 1
			if n > last {
				last = n
			}
		}
	}
	if last > limit/2 {
		return strings.TrimSpace(string(r[:last+1]))
	}
	words := strings.Fields(cut)
	if len(words) > 1 {
		return strings.Join(words[:len(words)-1], " ") + "..."
	}
	return cut + "..."
}
func validURL(s string) bool {
	u, e := url.Parse(s)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || !strings.Contains(u.Hostname(), ".") {
		return false
	}
	p := strings.ToLower(u.Path)
	for _, ext := range []string{".jpg", ".jpeg", ".png", ".gif", ".bmp", ".svg", ".ico", ".mp4", ".avi", ".mov", ".wmv", ".flv", ".mp3", ".wav", ".ogg", ".flac", ".zip", ".rar", ".tar", ".gz", ".pdf", ".doc", ".docx", ".xls", ".xlsx"} {
		if strings.HasSuffix(p, ext) {
			return false
		}
	}
	return true
}
func parse(s string) *goquery.Document {
	d, _ := goquery.NewDocumentFromReader(strings.NewReader(s))
	return d
}
func selectSafe(s *goquery.Selection, q string) *goquery.Selection {
	m, e := cascadia.Compile(q)
	if e != nil {
		return &goquery.Selection{}
	}
	return s.FindMatcher(m)
}
func text(s *goquery.Selection) string {
	if s == nil || s.Length() == 0 {
		return ""
	}
	if s.Is("dialog,[role=dialog],[aria-modal=true]") || s.ParentsFiltered("dialog,[role=dialog],[aria-modal=true]").Length() > 0 {
		return ""
	}
	for _, n := range s.Nodes {
		if n == nil {
			return ""
		}
	}
	c := s.Clone()
	pruneRecommendationControls(c)
	c.Find("script,style,template,nav,aside,footer,form,dialog,[role=dialog],[aria-modal=true],[role=navigation],[role=contentinfo],[data-native-ad-id],.newsletter-signup,.subscription-widget,.subscribe-widget,.ad-container,.advertisement").Remove()
	var parts []string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			if v := strings.TrimSpace(n.Data); v != "" {
				parts = append(parts, v)
			}
		}
		for ch := n.FirstChild; ch != nil; ch = ch.NextSibling {
			walk(ch)
		}
	}
	for _, n := range c.Nodes {
		walk(n)
	}
	return clean(strings.Join(parts, " "))
}
func good(s string, full bool, max int) bool {
	if candidateRejection(s) != "" {
		return false
	}
	n := runeLen(strings.TrimSpace(s))
	min, ns := 200, 2
	if full {
		min = 100
		ns = 1
	}
	if n < min || n > max {
		return false
	}
	words := strings.Fields(s)
	ratio := float64(len(words)) / float64(n)
	if ratio < 0.05 || ratio > 0.5 || len(words) < 10 {
		return false
	}
	cnt, total := 0, 0
	for _, p := range strings.Split(s, ".") {
		p = strings.TrimSpace(p)
		if runeLen(p) > 10 {
			cnt++
			total += runeLen(p)
		}
	}
	if cnt < ns || float64(total)/float64(cnt) < 15 {
		return false
	}
	counts := map[string]int{}
	nav := 0
	for _, w := range words {
		w = strings.ToLower(w)
		if runeLen(w) > 4 {
			counts[w]++
			if float64(counts[w]) > float64(len(words))*0.15 {
				return false
			}
		}
		for _, p := range []string{"menu", "navigation", "footer", "sidebar", "advertisement", "cookie", "subscribe", "newsletter", "copyright", "all rights reserved", "terms of service", "privacy policy", "follow us", "social media"} {
			if strings.Contains(w, p) {
				nav++
				break
			}
		}
	}
	if !locallyDiverse(words) {
		return false
	}
	limit := 0.1
	if full {
		limit = 0.15
	}
	if float64(nav) > float64(len(words))*limit {
		return false
	}
	for _, p := range []string{"404 not found", "page not found", "access denied", "error 404", "error 500", "internal server error", "page not available", "content not found", "please enable javascript", "javascript is required"} {
		if strings.Contains(strings.ToLower(s), p) {
			return false
		}
	}
	return true
}
func qualityScore(s string) float64 {
	if runeLen(s) < 50 {
		return 0
	}
	q := 0.1
	n := runeLen(s)
	if n >= 200 && n <= 3000 {
		q += 0.3
	} else {
		q += 0.2
	}
	if strings.Count(s, ".") >= 3 {
		q += 0.2
	} else if strings.ContainsAny(s, ".!?") {
		q += 0.1
	}
	words := strings.Fields(strings.ToLower(s))
	u := map[string]bool{}
	for _, w := range words {
		u[w] = true
	}
	if float64(len(u))/float64(len(words)) >= 0.4 {
		q += 0.2
	}
	p := 0
	for _, ch := range s {
		if strings.ContainsRune(".!?,:;", ch) {
			p++
		}
	}
	ratio := float64(p) / float64(n)
	if ratio >= 0.02 && ratio <= 0.15 {
		q += 0.2
	}
	return q
}
func enhanced(d *goquery.Document, max int) (string, string) {
	if body := editorialBody(d.Selection); body.Length() > 0 {
		v := restoreIntroduction(d, text(body), meta(d, `meta[property="og:title"]`, "title"))
		if good(v, false, max) {
			return v, "editorial_body"
		}
	}
	for _, q := range selectors {
		ss := selectSafe(d.Selection, q)
		for i := range ss.Nodes {
			v := text(ss.Eq(i))
			if good(v, false, max) {
				return v, q
			}
		}
	}
	return "", ""
}
func heuristic(d *goquery.Document, max int) string {
	type candidate struct {
		s     string
		score float64
	}
	var top []candidate
	d.Find("p,div,article,section").Each(func(_ int, e *goquery.Selection) {
		s := text(e)
		n := runeLen(s)
		if n <= 50 || n > max {
			return
		}
		score := 0.0
		if n > 500 {
			score += 3
		} else if n > 200 {
			score += 2
		} else if n > 100 {
			score++
		}
		tag := goquery.NodeName(e)
		switch tag {
		case "article":
			score += 4
		case "main":
			score += 3
		case "section", "div":
			score++
		case "p":
			score += 0.5
		}
		a := strings.ToLower(e.AttrOr("class", "") + " " + e.AttrOr("id", ""))
		for _, p := range []string{"content", "article", "post", "entry", "story", "news", "text", "body"} {
			if strings.Contains(a, p) {
				score += 2
			}
		}
		for _, p := range []string{"nav", "sidebar", "footer", "header", "ad", "comment", "social"} {
			if strings.Contains(a, p) {
				score -= 3
			}
		}
		score += min(float64(e.Find("p").Length())*0.5, 3)
		top = append(top, candidate{s, score})
		sort.SliceStable(top, func(i, j int) bool { return top[i].score > top[j].score })
		if len(top) > 5 {
			top = top[:5]
		}
	})
	for _, c := range top {
		if good(c.s, false, max) {
			return c.s
		}
	}
	var parts []string
	for _, c := range top[:min(len(top), 3)] {
		if c.score > 1 {
			parts = append(parts, c.s)
		}
	}
	s := strings.Join(parts, " ")
	if good(s, false, max) {
		return s
	}
	return ""
}
func jsonItems(d *goquery.Document) []map[string]any {
	var out []map[string]any
	var visit func(any)
	visit = func(v any) {
		switch x := v.(type) {
		case []any:
			for _, v := range x {
				visit(v)
			}
		case map[string]any:
			if g, ok := x["@graph"]; ok {
				visit(g)
			} else {
				out = append(out, x)
			}
		}
	}
	d.Find(`script[type="application/ld+json"]`).Each(func(_ int, s *goquery.Selection) {
		var v any
		if json.Unmarshal([]byte(s.Text()), &v) == nil {
			visit(v)
		}
	})
	return out
}
func jsonBody(d *goquery.Document) string {
	for _, it := range jsonItems(d) {
		t := fmt.Sprint(it["@type"])
		if arr, ok := it["@type"].([]any); ok && len(arr) > 0 {
			t = fmt.Sprint(arr[0])
		}
		if t != "Article" && t != "NewsArticle" && t != "BlogPosting" {
			continue
		}
		for _, k := range []string{"articleBody", "text", "description"} {
			if s, ok := it[k].(string); ok && runeLen(s) > 200 {
				return s
			}
		}
	}
	return ""
}

func openGraphBody(d *goquery.Document) string {
	for _, q := range []string{`meta[property="og:description"]`, `meta[name="description"]`} {
		if s := meta(d, q); runeLen(s) > 200 {
			return s
		}
	}
	return ""
}
func meta(d *goquery.Document, queries ...string) string {
	for _, q := range queries {
		s := selectSafe(d.Selection, q).First()
		if s.Length() > 0 {
			if c := s.AttrOr("content", ""); c != "" {
				return strings.TrimSpace(c)
			}
			if c := s.AttrOr("href", ""); c != "" {
				return strings.TrimSpace(c)
			}
			if c := text(s); c != "" {
				return c
			}
		}
	}
	return ""
}

func extractWeb(ctx context.Context, in Input, r Result) Result {
	auto := in.Strategy == "" || in.Strategy == "auto" || in.Strategy == "pipeline"
	strategy := in.Strategy
	if (auto && strings.Contains(in.URL, "reddit.com/")) || strategy == "reddit_json" || strategy == "reddit_linked_article" {
		r.Trace = append(r.Trace, "reddit_json")
		rr := extractReddit(in, r)
		if rr.Content != "" {
			return rr
		}
		if !auto {
			in.Strategy = "auto"
			auto = true
		}
	}
	d := parse(in.HTML)
	if d == nil {
		r.Error = "invalid_html"
		return r
	}
	metadata(d, &r)
	try := func(method string) bool {
		if ctx.Err() != nil {
			r.Error = ctx.Err().Error()
			return false
		}
		r.Trace = append(r.Trace, method)
		var s, sel string
		md := d
		switch method {
		case "learned_pattern":
			sel = in.LearnedSelector
			if sel != "" {
				s = text(selectSafe(d.Selection, sel).First())
			}
			if !good(s, true, in.MaxChars) {
				r.Trace = append(r.Trace, "learned_pattern_degraded")
				return false
			}
			m := in.LearnedMethod
			for strings.HasPrefix(m, "learned_pattern_") {
				m = strings.TrimPrefix(m, "learned_pattern_")
			}
			if m == "" {
				m = "unknown"
			}
			r.Method = "learned_pattern_" + m
		case "learned_pattern_browser_rendering", "browser_rendering", "browser_heuristics":
			if in.RenderedHTML == "" {
				r.Trace = append(r.Trace, "browser_unavailable")
				return false
			}
			md = parse(in.RenderedHTML)
			if method != "browser_heuristics" {
				s, sel = enhanced(md, in.MaxChars)
			}
			if s == "" {
				s = heuristic(md, in.MaxChars)
				sel = "heuristics"
			}
			if method == "learned_pattern_browser_rendering" && in.LearnedSelector != "" {
				v := text(selectSafe(md.Selection, in.LearnedSelector).First())
				if runeLen(v) > runeLen(s) {
					s = v
					sel = in.LearnedSelector
				}
			}
			r.Method = method
		case "enhanced_selectors":
			s, sel = enhanced(d, in.MaxChars)
			r.Method = method
		case "readability":
			u, _ := url.Parse(in.URL)
			// Remove controls before Readability can flatten away their structural
			// identity (for example an author popover or follow widget).
			candidate := d.Clone()
			if body := editorialBody(candidate); body.Length() > 0 {
				var parts []string
				body.Each(func(_ int, n *goquery.Selection) { h, _ := goquery.OuterHtml(n); parts = append(parts, h) })
				candidate = parse("<html><body><article>" + strings.Join(parts, " ") + "</article></body></html>").Selection
			}
			pruneRecommendationControls(candidate)
			unwrapPermalinkHeadings(candidate)
			candidate.Find("template,aside,nav,footer,dialog,[role=dialog],[aria-modal=true],[role=navigation],form").Remove()
			candidate.Find("h1 a,h2 a,h3 a,h4 a,h5 a,h6 a").Each(func(_ int, a *goquery.Selection) {
				if clean(a.Text()) == "" {
					a.Remove()
				}
			})
			h, _ := candidate.Html()
			a, e := readability.FromReader(strings.NewReader(h), u)
			if e != nil {
				r.Trace = append(r.Trace, "readability_error")
				return false
			}
			if a.Node == nil {
				return false
			}
			s = restoreIntroduction(d, text(goquery.NewDocumentFromNode(a.Node).Selection), r.Title)
			sel = "readability"
			r.Method = method
		case "json_ld":
			s = jsonBody(d)
			sel = "json_ld"
			r.Method = "fallback_extraction"
		case "open_graph", "meta_description":
			if method == "open_graph" {
				s = openGraphBody(d)
			} else {
				s = meta(d, `meta[name="description"]`)
			}
			sel = method
			r.Method = "fallback_extraction"
		case "enhanced_heuristics":
			s = heuristic(d, in.MaxChars)
			sel = "heuristics"
			r.Method = "fallback_extraction"
		case "fallback_extraction":
			s = jsonBody(d)
			sel = "json_ld"
			if s == "" {
				s = openGraphBody(d)
				sel = "open_graph"
			}
			if runeLen(s) <= 200 {
				s = heuristic(d, in.MaxChars)
				sel = "heuristics"
			}
			r.Method = method
		default:
			r.State = "unsupported"
			r.Error = "unsupported_strategy: " + method
			return false
		}
		full := method == "learned_pattern" || method == "learned_pattern_browser_rendering" || method == "browser_rendering" || method == "browser_heuristics" || method == "enhanced_selectors"
		if !good(s, full, in.MaxChars) {
			if reason := candidateRejection(s); reason != "" {
				r.Trace = append(r.Trace, method+":"+reason)
			}
			r.Trace = append(r.Trace, method+":quality_rejected")
			return false
		}
		r.Content = s
		r.SelectorUsed = sel
		metadata(md, &r)
		return true
	}
	if auto && in.RenderedHTML != "" && hasPopulatedTableCounterpart(d, parse(in.RenderedHTML)) {
		r.Trace = append(r.Trace, "rendered_table_ready")
		if try("browser_rendering") {
			return r
		}
	}
	if !auto {
		if !try(strategy) && strategy == "learned_pattern" {
			for _, m := range []string{"enhanced_selectors", "readability", "browser_rendering", "fallback_extraction"} {
				if try(m) {
					break
				}
			}
		}
		return r
	}
	if in.LearnedMethod == "browser_rendering" {
		if try("learned_pattern_browser_rendering") {
			return r
		}
	} else if in.LearnedSelector != "" {
		if try("learned_pattern") {
			return r
		}
	}
	for _, m := range []string{"enhanced_selectors", "readability", "browser_rendering", "fallback_extraction"} {
		if try(m) {
			return r
		}
	}
	r.Trace = append(r.Trace, "alternative_links")
	var alternatives []string
	addAlternative := func(s *goquery.Selection) {
		href := s.AttrOr("href", "")
		if href == "" {
			return
		}
		u, _ := url.Parse(in.URL)
		ref, e := url.Parse(href)
		if e == nil {
			alternatives = append(alternatives, u.ResolveReference(ref).String())
		}
	}
	for _, rel := range []string{"canonical", "amphtml"} {
		d.Find("link[rel]").EachWithBreak(func(_ int, s *goquery.Selection) bool {
			for _, token := range strings.Fields(s.AttrOr("rel", "")) {
				if token == rel {
					addAlternative(s)
					return false
				}
			}
			return true
		})
	}
	d.Find(`a[href]`).Each(func(_ int, s *goquery.Selection) {
		tag := goquery.NodeName(s)
		if tag == "a" {
			t := strings.ToLower(s.Text())
			ok := false
			for _, p := range []string{"read more", "full article", "continue reading", "read full", "view full", "more details"} {
				ok = ok || strings.Contains(t, p)
			}
			if !ok {
				return
			}
		}
		u := resolve(in.URL, s.AttrOr("href", ""))
		for _, existing := range alternatives {
			if existing == u {
				return
			}
		}
		addAlternative(s)
	})
	for _, u := range alternatives[:min(len(alternatives), 2)] {
		if h := in.AlternativeHTML[u]; h != "" {
			ni := in
			ni.URL = u
			ni.HTML = h
			ni.AlternativeHTML = nil
			ni.Strategy = "auto"
			ni.RenderedHTML = ""
			ni.RedditJSON = ""
			ni.LinkedHTML = ""
			rr := extractWeb(ctx, ni, Result{URL: u, State: "failed", Trace: r.Trace})
			if rr.Content != "" {
				rr.ExtractedFrom = u
				rr.URL = r.URL
				rr.Trace = append(rr.Trace, "alternative_url_success")
				return rr
			}
		}
	}
	r.Method = ""
	r.Error = "no_quality_content"
	return r
}

var textDate = regexp.MustCompile(`(?i)\b(?:\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4}|[A-Za-z]+ \d{1,2},? \d{4}|\d{1,2} [A-Za-z]+ \d{4})\b`)
