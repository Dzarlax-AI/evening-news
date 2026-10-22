package extractor

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/PuerkitoBio/goquery"
	"github.com/andybalholm/cascadia"
	"golang.org/x/net/html"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var ErrPageMonitorSelector = errors.New("unsupported_selector")

type PageMonitorSelectors struct {
	Article []string `json:"article,omitempty"`
	Title   []string `json:"title,omitempty"`
	Date    []string `json:"date,omitempty"`
	Link    []string `json:"link,omitempty"`
}

type PageMonitorItem struct {
	Title       string    `json:"title"`
	Link        string    `json:"link"`
	Description string    `json:"description"`
	Published   time.Time `json:"published"`
	ContentType string    `json:"content_type"`
	ID          string    `json:"id"`
	Source      string    `json:"source"`
	SourceURL   string    `json:"source_url"`
}

type PageMonitorOptions struct {
	Context                   context.Context
	URL                       string
	HTML                      string
	Now                       time.Time
	Limit                     int
	MinTitleLength            int
	Selectors                 PageMonitorSelectors
	SourceName                string
	LearnedContainerSelectors []string
	LearnedTitleSelectors     []string
	LearnedLinkSelectors      []string
	LearnedDateSelectors      []string
	LearnedGeneral            []PageMonitorGeneralSelector
}

type PageMonitorGeneralSelector struct {
	Selector   string  `json:"selector"`
	Confidence float64 `json:"confidence"`
}

// ParsePageMonitorList extracts a bounded list from one parsed document. Selector
// groups preserve Python's priority order, including learned confidence order.
func ParsePageMonitorList(in PageMonitorOptions) ([]PageMonitorItem, []string, error) {
	if in.Context == nil {
		in.Context = context.Background()
	}
	if len(in.HTML) > 5*1024*1024 {
		return nil, nil, ErrOutputLimit
	}
	if in.Limit == 0 {
		in.Limit = 20
	}
	if in.Limit < 1 || in.Limit > 100 {
		return nil, nil, errors.New("invalid_limit")
	}
	if in.MinTitleLength == 0 {
		in.MinTitleLength = 10
	}
	if in.Now.IsZero() {
		in.Now = time.Now().UTC()
	}
	if in.Now.Location() != time.UTC {
		in.Now = in.Now.UTC()
	}
	for _, group := range [][]string{in.Selectors.Article, in.Selectors.Title, in.Selectors.Date, in.Selectors.Link, in.LearnedContainerSelectors, in.LearnedTitleSelectors, in.LearnedLinkSelectors, in.LearnedDateSelectors} {
		for _, selector := range group {
			if _, e := cascadia.Compile(selector); e != nil {
				return nil, nil, fmt.Errorf("%w: %s", ErrPageMonitorSelector, selector)
			}
		}
	}
	for _, learned := range in.LearnedGeneral {
		if !finitePageMonitorConfidence(learned.Confidence) {
			return nil, nil, errors.New("invalid_learned_confidence")
		}
		if _, e := cascadia.Compile(learned.Selector); e != nil {
			return nil, nil, fmt.Errorf("%w: %s", ErrPageMonitorSelector, learned.Selector)
		}
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(in.HTML))
	if err != nil {
		return nil, nil, err
	}
	rawItems := []PageMonitorItem{}
	used := []string{}
	collect := func(selectors []string, stopAtFirst bool, rawLimit int) []PageMonitorItem {
		collected := []PageMonitorItem{}
		for _, selector := range selectors {
			if in.Context.Err() != nil {
				return collected
			}
			matcher, _ := cascadia.Compile(selector)
			before := len(collected)
			doc.FindMatcher(matcher).EachWithBreak(func(_ int, element *goquery.Selection) bool {
				if in.Context.Err() != nil {
					return false
				}
				item, ok := pageMonitorItem(in, element, in.Now)
				if ok {
					collected = append(collected, item)
				}
				return rawLimit == 0 || len(collected) < rawLimit
			})
			if len(collected) > before {
				used = append(used, selector)
				if stopAtFirst {
					break
				}
			}
			if rawLimit > 0 && len(collected) >= rawLimit {
				break
			}
		}
		return collected
	}
	if len(in.LearnedContainerSelectors) > 0 {
		rawItems = collect(in.LearnedContainerSelectors, true, 0)
	}
	general := append([]PageMonitorGeneralSelector(nil), in.LearnedGeneral...)
	sort.SliceStable(general, func(i, j int) bool { return general[i].Confidence > general[j].Confidence })
	generalCount := 0
	if len(rawItems) == 0 {
		for _, g := range general {
			if g.Confidence > .7 {
				rawItems = collect([]string{g.Selector}, true, 0)
				generalCount++
				if len(rawItems) > 0 || generalCount >= 3 {
					break
				}
			}
		}
	}
	if len(rawItems) == 0 {
		rawItems = collect(in.Selectors.Article, false, in.Limit)
	}
	items := make([]PageMonitorItem, 0, min(len(rawItems), in.Limit))
	seenTitles := map[string]bool{}
	for _, item := range rawItems {
		if in.Context.Err() != nil {
			break
		}
		key := cases.Lower(language.Und).String(strings.TrimSpace(item.Title))
		if seenTitles[key] {
			continue
		}
		seenTitles[key] = true
		items = append(items, item)
		if len(items) >= in.Limit {
			break
		}
	}
	if err := in.Context.Err(); err != nil {
		return items, used, err
	}
	return items, used, nil
}

func finitePageMonitorConfidence(v float64) bool { return !math.IsNaN(v) && !math.IsInf(v, 0) }

func pageMonitorItem(in PageMonitorOptions, element *goquery.Selection, now time.Time) (PageMonitorItem, bool) {
	title := pageMonitorTextBySelectors(in.Context, element, append(append([]string{}, in.LearnedTitleSelectors...), in.Selectors.Title...))
	if in.Context.Err() != nil {
		return PageMonitorItem{}, false
	}
	if title == "" {
		title = pageMonitorText(element, false)
		if len([]rune(title)) > 200 {
			title = string([]rune(title)[:200])
		}
	}
	if title == "" || utf8.RuneCountInString(title) < in.MinTitleLength {
		return PageMonitorItem{}, false
	}
	link, linkFound := pageMonitorLink(in.Context, element, in.LearnedLinkSelectors)
	if in.Context.Err() != nil {
		return PageMonitorItem{}, false
	}
	if linkFound && !strings.HasPrefix(link, "#") {
		if base, e := url.Parse(in.URL); e == nil {
			if ref, e := url.Parse(link); e == nil {
				link = base.ResolveReference(ref).String()
			}
		}
	}
	if !linkFound || strings.HasPrefix(link, "#") {
		link = in.URL
	}
	published, ok := pageMonitorSelectedDate(in.Context, element, in.LearnedDateSelectors, true, now)
	if in.Context.Err() != nil {
		return PageMonitorItem{}, false
	}
	if !ok {
		published, ok = pageMonitorSelectedDate(in.Context, element, in.Selectors.Date, false, now)
	}
	if !ok {
		text := pageMonitorText(element, false)
		if len([]rune(text)) > 300 {
			text = string([]rune(text)[:300])
		}
		published, ok = pageMonitorDateFromText(text, now)
	}
	if !ok {
		published = now
	}
	description := pageMonitorText(element, true)
	if len([]rune(description)) > 500 {
		description = string([]rune(description)[:500]) + "..."
	}
	contentType := pageMonitorContentType(title + " " + description)
	if in.Context.Err() != nil {
		return PageMonitorItem{}, false
	}
	return PageMonitorItem{Title: title, Link: link, Description: description, Published: published, ContentType: contentType, ID: pageMonitorMD5(title + link), Source: in.SourceName, SourceURL: in.URL}, true
}

func pageMonitorTextBySelectors(ctx context.Context, s *goquery.Selection, selectors []string) string {
	for _, q := range selectors {
		if ctx.Err() != nil {
			return ""
		}
		if s == nil || s.Length() == 0 {
			return ""
		}
		m, e := cascadia.Compile(q)
		if e != nil {
			continue
		}
		t := pageMonitorText(s.FindMatcher(m).First(), false)
		if t != "" {
			return t
		}
	}
	return ""
}
func pageMonitorLink(ctx context.Context, s *goquery.Selection, selectors []string) (string, bool) {
	for _, q := range selectors {
		if ctx.Err() != nil {
			return "", false
		}
		if s == nil || s.Length() == 0 {
			return "", false
		}
		m, e := cascadia.Compile(q)
		if e != nil {
			continue
		}
		selected := s.FindMatcher(m).First()
		v, ok := selected.Attr("href")
		if selected.Length() > 0 && ok && v != "" && !strings.HasPrefix(v, "#") {
			return v, true
		}
	}
	descendant := s.Find("a[href]").First()
	if descendant.Length() > 0 {
		v, _ := descendant.Attr("href")
		if v == "" || strings.HasPrefix(v, "#") {
			return "", false
		}
		return v, true
	}
	if s.Is("a[href]") {
		v, _ := s.Attr("href")
		if v != "" && !strings.HasPrefix(v, "#") {
			return v, true
		}
	}
	parent := s.ParentsFiltered("a[href]").First()
	if parent.Length() > 0 {
		v, _ := parent.Attr("href")
		return v, true
	}
	return "", false
}
func pageMonitorSelectedDate(ctx context.Context, s *goquery.Selection, selectors []string, learned bool, now time.Time) (time.Time, bool) {
	for _, q := range selectors {
		if ctx.Err() != nil {
			return time.Time{}, false
		}
		if s == nil || s.Length() == 0 {
			return time.Time{}, false
		}
		m, e := cascadia.Compile(q)
		if e != nil {
			continue
		}
		dateElement := s.FindMatcher(m).First()
		if dateElement.Length() == 0 {
			continue
		}
		v, _ := dateElement.Attr("datetime")
		if v != "" {
			if d, ok := pageMonitorDate(v, now); ok {
				return d, true
			}
		}
		if !learned || v == "" {
			v = pageMonitorText(dateElement, false)
			if d, ok := pageMonitorDate(v, now); ok {
				return d, true
			}
		}
	}
	return time.Time{}, false
}
func pageMonitorText(s *goquery.Selection, separator bool) string {
	if s == nil || s.Length() == 0 {
		return ""
	}
	var parts []string
	var walk func(*html.Node, bool)
	walk = func(n *html.Node, excluded bool) {
		if n.Type == html.ElementNode && (n.Data == "script" || n.Data == "style" || n.Data == "template") {
			excluded = true
		}
		if n.Type == html.TextNode && !excluded {
			v := strings.TrimSpace(n.Data)
			if v != "" {
				parts = append(parts, v)
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c, excluded)
		}
	}
	for _, node := range s.Nodes {
		walk(node, false)
	}
	join := ""
	if separator {
		join = " "
		return strings.Join(strings.Fields(strings.Join(parts, join)), " ")
	}
	return strings.Join(parts, join)
}
func pageMonitorMD5(s string) string { h := md5.Sum([]byte(s)); return hex.EncodeToString(h[:]) }

var pmRelativeDate = regexp.MustCompile(`(?i)(\d+)\s+(second|minute|hour|day|week|month|year)s?\s+ago`)
var pmLongFraction = regexp.MustCompile(`T\d{2}:\d{2}:\d{2}\.\d{7,}Z$`)
var pmEmbeddedDatePatterns = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b(\d{4})-(\d{2})-(\d{2})\b`),
	regexp.MustCompile(`(?i)\b(\w+)\s+(\d{1,2}),?\s+(\d{4})\b`),
	regexp.MustCompile(`(?i)\b(\d{1,2})\s+(\w+)\s+(\d{4})\b`),
	regexp.MustCompile(`(?i)(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{1,2}),?\s+(\d{4})`),
	regexp.MustCompile(`(?i)(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s+(\d{4})`),
	regexp.MustCompile(`(?i)(\d{1,2})\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(\d{4})`),
	regexp.MustCompile(`(?i)(\d{1,2})\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})`),
	regexp.MustCompile(`(\d{1,2})/(\d{1,2})/(\d{4})`),
	regexp.MustCompile(`(\d{1,2})-(\d{1,2})-(\d{4})`),
	regexp.MustCompile(`(\d{4})/(\d{1,2})/(\d{1,2})`),
}

func pageMonitorDate(s string, now time.Time) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" || pmLongFraction.MatchString(s) {
		return time.Time{}, false
	}
	formats := []string{"2006-01-02", "2006-01-02T15:04:05", "2006-01-02T15:04:05Z", "2006-01-02T15:04:05.999999Z", "2006-01-02 15:04:05", "2006/01/02", "2006/01/02 15:04:05", "January 2, 2006", "Jan 2, 2006", "1/2/2006", "1-2-2006", "2 January 2006", "2 Jan 2006", "2/1/2006", "2.1.2006", "2-1-2006", "2006-01-02 15:04", "2.1.2006 15:04", "January 2006", "2006-01"}
	for _, f := range formats {
		var d time.Time
		var e error
		d, e = time.ParseInLocation(f, s, time.UTC)
		if e == nil {
			if f == "January 2006" || f == "2006-01" {
				d = d.AddDate(0, 0, 0)
			}
			if d.After(now.Add(24*time.Hour)) || d.Before(now.AddDate(0, 0, -730)) {
				continue
			}
			return d.UTC(), true
		}
	}
	lower := strings.ToLower(s)
	if m := pmRelativeDate.FindStringSubmatch(lower); len(m) == 3 {
		count := 0
		fmt.Sscanf(m[1], "%d", &count)
		switch m[2] {
		case "second":
			return now.Add(-time.Duration(count) * time.Second), true
		case "minute":
			return now.Add(-time.Duration(count) * time.Minute), true
		case "hour":
			return now.Add(-time.Duration(count) * time.Hour), true
		case "day":
			return now.AddDate(0, 0, -count), true
		case "week":
			return now.AddDate(0, 0, -7*count), true
		case "month":
			return now.AddDate(0, 0, -30*count), true
		case "year":
			return now.AddDate(0, 0, -365*count), true
		}
	}
	for _, x := range []struct {
		needle string
		days   int
	}{{"yesterday", 1}, {"last week", 7}, {"last month", 30}} {
		if strings.Contains(lower, x.needle) {
			return now.AddDate(0, 0, -x.days), true
		}
	}
	if strings.Contains(lower, "today") || strings.Contains(lower, "just now") {
		return now, true
	}
	return time.Time{}, false
}

func pageMonitorDateFromText(text string, now time.Time) (time.Time, bool) {
	for i, pattern := range pmEmbeddedDatePatterns {
		candidate := pattern.FindString(text)
		if candidate == "" {
			continue
		}
		parts := pattern.FindStringSubmatch(text)
		if len(parts) == 4 {
			switch i {
			case 1, 3, 4:
				candidate = parts[1] + " " + parts[2] + ", " + parts[3]
			case 2, 5, 6:
				candidate = parts[1] + " " + parts[2] + " " + parts[3]
			}
		}
		if date, ok := pageMonitorDate(candidate, now); ok {
			return date, true
		}
	}
	return time.Time{}, false
}
func pageMonitorContentType(text string) string {
	t := strings.ToLower(text)
	for i, g := range pmContentPatterns {
		n := 0
		for _, p := range g {
			if p.MatchString(t) {
				n++
			}
		}
		if n >= 2 {
			return pmContentTypes[i]
		}
	}
	return "general"
}

var pmContentPatterns = [][]*regexp.Regexp{
	{regexp.MustCompile(`\b(version|v\d+|\d+\.\d+)`), regexp.MustCompile(`\b(released?|updated?|fixed?|added?|improved?)`), regexp.MustCompile(`\b(feature|bug|improvement|enhancement)`)},
	{regexp.MustCompile(`\b(breaking|urgent|announced?|launched?)`), regexp.MustCompile(`\b(today|yesterday|this week|latest)`), regexp.MustCompile(`\b(update|news|press|release)`)},
	{regexp.MustCompile(`\b(posted|published|written|authored)`), regexp.MustCompile(`\b(tutorial|guide|how.?to|tips)`), regexp.MustCompile(`\b(learn|understand|master)`)},
}

var pmContentTypes = []string{"changelog", "news", "blog"}
