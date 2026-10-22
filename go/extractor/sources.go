package extractor

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"encoding/xml"
	"fmt"
	stdhtml "html"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
	"github.com/mmcdole/gofeed"
	nethtml "golang.org/x/net/html"
)

// customContainerDescription mirrors page_monitor_source._extract_content:
// BeautifulSoup joins each stripped descendant text node with spaces, folds
// whitespace, then truncates by Unicode code point at 500 characters.
func customContainerDescription(s *goquery.Selection) string {
	if s == nil || s.Length() == 0 {
		return ""
	}
	var parts []string
	var walk func(*nethtml.Node)
	walk = func(n *nethtml.Node) {
		if n.Type == nethtml.TextNode {
			if value := strings.TrimSpace(n.Data); value != "" {
				parts = append(parts, value)
			}
		}
		for child := n.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	for _, node := range s.Nodes {
		if node != nil {
			walk(node)
		}
	}
	value := strings.Join(strings.Fields(strings.Join(parts, " ")), " ")
	runes := []rune(value)
	if len(runes) > 500 {
		return string(runes[:500]) + "..."
	}
	return value
}

func extractReddit(in Input, r Result) Result {
	var data []struct {
		Data struct {
			Children []struct {
				Data map[string]any `json:"data"`
			} `json:"children"`
		} `json:"data"`
	}
	if err := json.Unmarshal([]byte(in.RedditJSON), &data); err != nil || len(data) == 0 || len(data[0].Data.Children) == 0 {
		r.Error = "invalid_reddit_json"
		return r
	}
	p := data[0].Data.Children[0].Data
	str := func(k string) string { s, _ := p[k].(string); return s }
	r.Title = str("title")
	r.Author = str("author")
	selftext := strings.TrimSpace(str("selftext"))
	r.Content = r.Title
	isSelf, _ := p["is_self"].(bool)
	hint := str("post_hint")
	var comments []string
	if len(data) > 1 {
		for _, c := range data[1].Data.Children[:min(len(data[1].Data.Children), 10)] {
			a, _ := c.Data["author"].(string)
			b, _ := c.Data["body"].(string)
			dist := c.Data["distinguished"]
			if runeLen(strings.TrimSpace(b)) > 20 && b != "[deleted]" && b != "[removed]" && !strings.EqualFold(a, "automoderator") && !strings.EqualFold(a, "automod") && !strings.EqualFold(a, "bot") && (dist == nil || dist == false || dist == "") {
				comments = append(comments, strings.TrimSpace(b))
				if len(comments) == 5 {
					break
				}
			}
		}
	}
	if isSelf && selftext != "" {
		r.Content += "\n\n" + selftext
	} else if hint == "image" || selftext == "" {
		if len(comments) > 0 {
			r.Content += "\n\nКомментарии:\n" + strings.Join(comments, "\n\n")
		}
	} else {
		r.Content += "\n\n" + selftext
		if len(comments) > 0 {
			r.Content += "\n\nКомментарии:\n" + strings.Join(comments, "\n\n")
		}
	}
	r.Description = selftext
	if runeLen(selftext) > 200 {
		r.Description = string([]rune(selftext)[:200])
	}
	if f, ok := p["created_utc"].(float64); ok {
		r.PublicationDate = time.Unix(int64(f), 0).UTC().Format(time.RFC3339)
	}
	if hint == "image" || looksImage(str("url")) {
		r.ImageURL = str("url")
	}
	if pr, ok := p["preview"].(map[string]any); ok {
		if imgs, ok := pr["images"].([]any); ok && len(imgs) > 0 {
			if im, ok := imgs[0].(map[string]any); ok {
				if src, ok := im["source"].(map[string]any); ok {
					if s, ok := src["url"].(string); ok && s != "" {
						r.ImageURL = stdhtml.UnescapeString(s)
					}
				}
			}
		}
	}
	r.Method = "reddit_json"
	r.SelectorUsed = "reddit_json"
	if !isSelf && hint != "image" && str("url") != "" && in.LinkedHTML != "" {
		r.Trace = append(r.Trace, "reddit_linked_article")
		d := parse(in.LinkedHTML)
		if s, sel := enhanced(d, in.MaxChars); s != "" && good(s, true, in.MaxChars) {
			r.Content = s
			r.SelectorUsed = sel
			if r.ImageURL == "" {
				tmp := Result{}
				metadata(d, &tmp)
				r.ImageURL = tmp.ImageURL
			}
		}
	}
	return r
}
func looksImage(s string) bool {
	s = strings.ToLower(strings.Split(s, "?")[0])
	for _, e := range []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp"} {
		if strings.HasSuffix(s, e) {
			return true
		}
	}
	return false
}
func extractFeed(in Input, r Result) Result {
	r.Trace = append(r.Trace, "rss_parse")
	f, err := gofeed.NewParser().ParseString(in.RSSXML)
	if err != nil {
		r.Error = "invalid_feed: " + err.Error()
		return r
	}
	if in.EntryIndex < 0 || in.EntryIndex >= len(f.Items) {
		r.Error = "feed_entry_missing"
		return r
	}
	e := f.Items[in.EntryIndex]
	if e.Link == "" {
		r.Error = "feed_entry_url_missing"
		return r
	}
	r.URL = e.Link
	r.Title = strings.TrimSpace(e.Title)
	r.Content = e.Content
	if r.Content == "" {
		r.Content = e.Description
		r.Description = e.Description
	}
	d := parse(r.Content)
	if len(e.Authors) > 0 {
		r.Author = e.Authors[0].Name
		if r.Author == "" {
			r.Author = e.Authors[0].Email
		}
	}
	if r.Author == "" && f.FeedType == "rss" {
		var raw struct {
			Items []struct {
				Author string `xml:"author"`
			} `xml:"channel>item"`
		}
		if xml.Unmarshal([]byte(in.RSSXML), &raw) == nil && in.EntryIndex < len(raw.Items) {
			r.Author = strings.TrimSpace(raw.Items[in.EntryIndex].Author)
		}
	}
	if e.PublishedParsed != nil {
		r.PublicationDate = e.PublishedParsed.UTC().Format(time.RFC3339)
	} else if e.UpdatedParsed != nil {
		r.PublicationDate = e.UpdatedParsed.UTC().Format(time.RFC3339)
	}
	if r.PublicationDate == "" {
		r.PublicationDate = sourceNow(in)
	}
	for _, en := range e.Enclosures {
		if r.ImageURL == "" && strings.HasPrefix(en.Type, "image/") {
			r.ImageURL = en.URL
		}
	}
	if r.ImageURL == "" {
		for _, en := range e.Extensions["media"]["content"] {
			u := en.Attrs["url"]
			if u != "" && (en.Attrs["medium"] == "image" || strings.HasPrefix(en.Attrs["type"], "image/") || looksImage(u)) {
				r.ImageURL = u
				break
			}
		}
	}
	if r.ImageURL == "" {
		for _, en := range e.Extensions["media"]["thumbnail"] {
			if en.Attrs["url"] != "" {
				r.ImageURL = en.Attrs["url"]
				break
			}
		}
	}
	if r.ImageURL == "" {
		s := d.Find("img[src]").First().AttrOr("src", "")
		if strings.HasPrefix(s, "http") && looksImage(s) {
			r.ImageURL = s
		}
	}
	r.Method = "rss_summary"
	if e.Content != "" {
		r.Method = "rss_content"
	}
	if f.FeedType == "atom" {
		r.Method = "atom"
	}
	return r
}
func extractCustom(in Input, r Result) Result {
	browser := in.Strategy == "custom_browser" || (in.SourceType == "custom" && (in.Strategy == "" || in.Strategy == "auto" || in.Strategy == "custom" || in.Strategy == "browser_rendering")) || (in.Strategy == "custom_selectors" && in.RenderedHTML != "")
	if in.UseBrowser != nil {
		browser = *in.UseBrowser
	}
	h := in.HTML
	if browser {
		h = in.RenderedHTML
		r.Trace = append(r.Trace, "custom_browser")
		if h == "" {
			r.Error = "browser_unavailable"
			return r
		}
	} else {
		r.Trace = append(r.Trace, "custom_http")
	}
	d := parse(h)
	qs := in.Selectors["article"]
	if len(qs) == 0 {
		qs = []string{in.LearnedSelector}
	}
	if len(qs) == 1 && qs[0] == "" {
		qs = append(selectors, ".news-item", ".post", ".entry", ".changelog-item", ".update-item", ".release-note")
	}
	var e *goquery.Selection
	for _, q := range qs {
		s := selectSafe(d.Selection, q)
		if s.Length() > in.EntryIndex && in.EntryIndex >= 0 {
			e = s.Eq(in.EntryIndex)
			r.SelectorUsed = q
			break
		}
	}
	if e == nil {
		r.Error = "custom_container_missing"
		return r
	}
	find := func(kind string, defaults []string) string {
		queries := in.Selectors[kind]
		if len(queries) == 0 {
			queries = defaults
		}
		for _, q := range queries {
			if v := text(selectSafe(e, q).First()); v != "" {
				return v
			}
		}
		return ""
	}
	r.Title = find("title", []string{"h1", "h2", "h3", "h4", ".title", ".headline", ".heading", "a[href]"})
	if r.Title == "" {
		r.Title = strings.Join(strings.Fields(e.Text()), " ")
		if title := []rune(r.Title); len(title) > 200 {
			r.Title = string(title[:200])
		}
	}
	if runeLen(r.Title) < 10 {
		r.Error = "custom_title_short"
		return r
	}
	r.Content = customContainerDescription(e)
	links := in.Selectors["link"]
	if len(links) == 0 {
		links = []string{"a[href]", ".link", ".read-more", ".permalink"}
	}
	linkFound := false
	setLink := func(href string) bool {
		if href != "" && !strings.HasPrefix(href, "#") {
			u, _ := url.Parse(in.URL)
			ref, err := url.Parse(href)
			if err == nil {
				r.URL = u.ResolveReference(ref).String()
				return true
			}
		}
		return false
	}
	for _, q := range links {
		if setLink(selectSafe(e, q).First().AttrOr("href", "")) {
			linkFound = true
			break
		}
	}
	// Page-monitor containers may be the link itself or live inside one.
	if !linkFound {
		if e.Is("a[href]") {
			linkFound = setLink(e.AttrOr("href", ""))
		}
		if !linkFound {
			setLink(e.ParentsFiltered("a[href]").First().AttrOr("href", ""))
		}
	}
	dates := in.Selectors["date"]
	if len(dates) == 0 {
		dates = []string{"time", "[datetime]", ".date", ".timestamp", ".published", ".created", ".updated", ".publish-date", ".post-date"}
	}
	for _, q := range dates {
		s := selectSafe(e, q).First()
		v := s.AttrOr("datetime", s.AttrOr("content", text(s)))
		if v != "" {
			if date := sourceDateCandidate(v); date != "" {
				r.PublicationDate = date
				break
			}
		}
	}
	if r.PublicationDate == "" {
		value := []rune(customContainerDescription(e))
		r.PublicationDate = sourceDateCandidate(string(value[:min(len(value), 300)]))
	}
	if r.PublicationDate == "" {
		r.PublicationDate = sourceNow(in)
	}
	r.Method = "custom_selectors"
	if in.Strategy == "ai_selectors" {
		r.Method = "ai_selectors"
		r.Trace = append(r.Trace, "ai_selectors_replay")
	}
	return r
}

func extractTelegram(ctx context.Context, in Input, r Result) Result {
	h := in.TelegramHTML
	if h == "" {
		h = in.HTML
	}
	if in.Strategy == "telegram_browser" {
		h = in.RenderedHTML
		if h == "" {
			r.Error = "browser_unavailable"
			return r
		}
	}
	d := parse(h)
	var msg *goquery.Selection
	for _, q := range []string{".tgme_widget_message", ".tgme_widget_message_wrap", ".message"} {
		s := selectSafe(d.Selection, q)
		if s.Length() > in.EntryIndex && in.EntryIndex >= 0 {
			msg = s.Eq(in.EntryIndex)
			break
		}
	}
	if msg == nil {
		r.Error = "telegram_message_missing"
		return r
	}
	r.Trace = append(r.Trace, "telegram_metadata_before_cleanup")
	if u := msg.Find("a.tgme_widget_message_date").First().AttrOr("href", ""); u != "" {
		r.URL = resolve("https://t.me", u)
	} else if post := msg.AttrOr("data-post", ""); post != "" {
		r.URL = "https://t.me/" + post
	}
	r.PublicationDate = msg.Find("a.tgme_widget_message_date time[datetime],time[datetime]").First().AttrOr("datetime", "")
	if r.PublicationDate != "" {
		r.PublicationDate = normalizeDate(r.PublicationDate)
	}
	if r.PublicationDate == "" {
		r.PublicationDate = sourceNow(in)
	}
	var external string
	msg.Find("a[href]").EachWithBreak(func(_ int, a *goquery.Selection) bool {
		u := a.AttrOr("href", "")
		if strings.HasPrefix(u, "http") && !strings.Contains(u, "t.me/") && !strings.Contains(u, "telegram.me/") {
			external = u
			return false
		}
		return true
	})
	r.Media = telegramMedia(msg)
	for _, m := range r.Media {
		if m.Type == "image" {
			r.ImageURL = m.URL
			break
		}
	}
	msg = msg.Clone()
	msg.Find(".tgme_widget_message_forwarded_from,.tgme_widget_message_reply,.tgme_widget_message_footer,.tgme_widget_message_info,.tgme_widget_message_date,.tgme_widget_message_views,.tgme_widget_message_service,.tgme_widget_message_keyboard").Remove()
	for _, q := range []string{".tgme_widget_message_text", ".message-text", ".text", `div[dir="ltr"]`, ".tgme_widget_message_text_wrapper", ".message_text", ".post-content"} {
		if v := text(selectSafe(msg, q).First()); v != "" {
			r.Content = v
			break
		}
	}
	if r.Content == "" {
		r.Content = text(msg)
	}
	if runeLen(r.Content) < 20 {
		if s := meta(goquery.NewDocumentFromNode(msg.Nodes[0]), `meta[property="og:description"]`, `meta[name="description"]`); runeLen(s) > runeLen(r.Content) {
			r.Content = s
		}
	}
	if runeLen(r.Content) < 10 && len(r.Media) == 0 {
		r.Content = ""
		r.Error = "telegram_content_short"
		return r
	}
	if external != "" {
		r.URL = external
		if runeLen(r.Content) < 200 && in.LinkedHTML != "" {
			ni := in
			ni.URL = external
			ni.HTML = in.LinkedHTML
			ni.Strategy = "readability"
			rr := extractWeb(ctx, ni, Result{})
			if runeLen(rr.Content) > 2*runeLen(r.Content) {
				r.Content = rr.Content
				r.Trace = append(r.Trace, "telegram_external_expansion")
			}
		}
	}
	r.Title = telegramTitle(r.Content)
	r.Method = "telegram_http"
	if in.Strategy == "telegram_browser" {
		r.Method = "telegram_browser"
	}
	return r
}

var telegramTitlePrefix = regexp.MustCompile(`(?i)^(Forwarded from|Reply to|@[\p{L}\p{N}_]+:?\s*)`)
var telegramTitleEmoji = regexp.MustCompile(`^\s*[🔗📎📷🎥📄]\s*`)

func telegramTitle(content string) string {
	var title string
	for _, line := range strings.Split(content, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			title = line
			break
		}
	}
	if title == "" {
		return "Telegram Post"
	}
	title = telegramTitlePrefix.ReplaceAllString(title, "")
	title = telegramTitleEmoji.ReplaceAllString(title, "")
	r := []rune(title)
	if len(r) <= 120 {
		return title
	}
	cut := string(r[:120])
	for _, ending := range []string{". ", "! ", "? ", ": ", " - ", " – ", " — "} {
		if pos := strings.LastIndex(cut, ending); pos >= 0 {
			runePos := runeLen(cut[:pos])
			if runePos > 60 {
				return strings.TrimSpace(string(r[:runePos+1]))
			}
		}
	}
	words := strings.Fields(cut)
	if len(words) > 1 {
		return strings.Join(words[:len(words)-1], " ") + "..."
	}
	return string(r[:117]) + "..."
}
func resolve(base, href string) string {
	u, e := url.Parse(base)
	v, e2 := url.Parse(href)
	if e != nil || e2 != nil {
		return ""
	}
	return u.ResolveReference(v).String()
}
func stableRef(kind, s string) string {
	sum := sha256.Sum256([]byte(s))
	return fmt.Sprintf("#%s_%x", kind, sum[:8])
}

func sourceNow(in Input) string {
	t := time.Now().UTC()
	if in.Now != "" {
		if parsed, e := time.Parse(time.RFC3339Nano, in.Now); e == nil {
			t = parsed.UTC()
		}
	}
	return t.Format("2006-01-02T15:04:05.999999")
}

func sourceDateCandidate(value string) string {
	if date := normalizeMetadataDate(value); date != "" {
		return date
	}
	for _, value := range metadataDate.FindAllString(value, -1) {
		if date := normalizeMetadataDate(value); date != "" {
			return date
		}
	}
	return ""
}
