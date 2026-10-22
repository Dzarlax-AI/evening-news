package extractor

import (
	"errors"
	"math"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"
)

var (
	ErrTelegramInvalidInput = errors.New("telegram_invalid_input")
	ErrTelegramMalformed    = errors.New("telegram_malformed_html")
	ErrTelegramByteLimit    = errors.New("telegram_byte_limit")
	ErrTelegramTextLimit    = errors.New("telegram_text_limit")
	ErrTelegramEmpty        = errors.New("telegram_empty_channel")
)

const telegramSourceMaxBytes = 5 * 1024 * 1024
const telegramSourceMaxChars = 100000

// TelegramArticle mirrors the Python Article shape for offline Telegram page parsing.
// ParseTelegramChannel performs no network or persistence operations.
type TelegramArticle struct {
	Title       string           `json:"title"`
	URL         string           `json:"url"`
	Content     string           `json:"content"`
	Summary     *string          `json:"summary"`
	PublishedAt *time.Time       `json:"published_at"`
	SourceType  string           `json:"source_type"`
	SourceName  string           `json:"source_name"`
	ImageURL    *string          `json:"image_url"`
	MediaFiles  []map[string]any `json:"media_files"`
	RawData     map[string]any   `json:"raw_data"`
}

// ParseTelegramChannel extracts ordered posts from captured Telegram channel HTML.
// fallbackTime is used only for posts whose page markup has no parseable date.
func ParseTelegramChannel(pageHTML, pageURL, sourceName string, limit int, fallbackTime time.Time) ([]TelegramArticle, error) {
	if limit <= 0 || fallbackTime.IsZero() || strings.TrimSpace(pageURL) == "" || strings.TrimSpace(sourceName) == "" {
		return nil, ErrTelegramInvalidInput
	}
	if len(pageHTML) > telegramSourceMaxBytes {
		return nil, ErrTelegramByteLimit
	}
	if strings.TrimSpace(pageHTML) == "" {
		return nil, ErrTelegramMalformed
	}
	u, err := url.Parse(pageURL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, ErrTelegramInvalidInput
	}
	doc, parseErr := goquery.NewDocumentFromReader(strings.NewReader(pageHTML))
	if parseErr != nil {
		return nil, ErrTelegramMalformed
	}
	var messages *goquery.Selection
	for _, selector := range []string{".tgme_widget_message", ".message", "[data-post]"} {
		messages = doc.Find(selector)
		if messages.Length() > 0 {
			break
		}
	}
	if messages == nil || messages.Length() == 0 {
		return []TelegramArticle{}, ErrTelegramEmpty
	}
	channel := telegramChannelName(u)
	articles := make([]TelegramArticle, 0, min(limit, messages.Length()))
	var postErr error
	messages.EachWithBreak(func(_ int, msg *goquery.Selection) bool {
		if len(articles) >= limit {
			return false
		}
		article, ok, err := parseTelegramPost(msg, pageURL, channel, sourceName, fallbackTime)
		if err != nil {
			postErr = err
			return false
		}
		if ok {
			articles = append(articles, article)
		}
		return len(articles) < limit
	})
	if postErr != nil {
		return articles, postErr
	}
	if len(articles) == 0 {
		return articles, ErrTelegramEmpty
	}
	return articles, nil
}

func telegramChannelName(page *url.URL) string {
	parts := strings.Split(strings.Trim(page.Path, "/"), "/")
	for i, part := range parts {
		if part == "s" && i+1 < len(parts) {
			return parts[i+1]
		}
	}
	if len(parts) > 0 && parts[0] != "" {
		return parts[0]
	}
	return ""
}

func parseTelegramPost(original *goquery.Selection, pageURL, channel, sourceName string, fallback time.Time) (TelegramArticle, bool, error) {
	msg := original.Clone()
	forwarded := optionalTelegramText(original.Find(".tgme_widget_message_forwarded_from").First())
	messageURL := telegramPostURL(original, channel)
	messageID := telegramPostID(original, messageURL)
	published := telegramPostDate(original, fallback)
	externalLinks := telegramExternalLinks(original, pageURL)
	originalLink := telegramOriginalLink(externalLinks)
	media := telegramSourceMedia(original, pageURL)
	imageURL := ""
	for _, item := range media {
		if item["type"] == "image" {
			imageURL, _ = item["url"].(string)
			break
		}
	}
	mediaInfo := telegramMediaInfo(original, pageURL)
	hasMedia := imageURL != "" || mediaInfo["video_url"] != nil || mediaInfo["document_url"] != nil || len(media) > 0
	for _, selector := range []string{
		".tgme_widget_message_forwarded_from", ".tgme_widget_message_reply", ".tgme_widget_message_footer",
		".tgme_widget_message_info", ".tgme_widget_message_date", ".tgme_widget_message_views",
		".tgme_widget_message_service", ".tgme_widget_message_keyboard",
	} {
		msg.Find(selector).Remove()
	}
	content := ""
	for _, selector := range []string{
		".tgme_widget_message_text", ".message-text", ".text", `div[dir="ltr"]`,
		".tgme_widget_message_text_wrapper", ".message_text", ".post-content",
	} {
		if sel := msg.Find(selector).First(); sel.Length() > 0 {
			content = telegramText(sel)
			if content != "" {
				break
			}
		}
	}
	if content == "" {
		content = telegramText(msg)
	}
	if runeLen(content) < 20 {
		for _, selector := range []string{`meta[property="og:description"]`, `meta[name="description"]`, `meta[name="twitter:description"]`} {
			if meta := msg.Find(selector).First().AttrOr("content", ""); runeLen(strings.TrimSpace(meta)) > runeLen(content) {
				content = strings.TrimSpace(meta)
				break
			}
		}
	}
	content = cleanTelegramContent(content)
	if runeLen(content) < 10 && !hasMedia {
		return TelegramArticle{}, false, nil
	}
	if runeLen(content) > telegramSourceMaxChars {
		return TelegramArticle{}, false, ErrTelegramTextLimit
	}
	finalURL := messageURL
	if originalLink != nil {
		finalURL = *originalLink
	}
	var imagePtr *string
	if imageURL != "" {
		imagePtr = &imageURL
	}
	return TelegramArticle{
		Title:       telegramPythonTitle(content),
		URL:         finalURL,
		Content:     content,
		Summary:     nil,
		PublishedAt: &published,
		SourceType:  "telegram",
		SourceName:  sourceName,
		ImageURL:    imagePtr,
		MediaFiles:  media,
		RawData: map[string]any{
			"channel": channel, "access_method": pageURL, "content_length": runeLen(content),
			"media_info": mediaInfo, "has_media": hasMedia,
			"media_count": len(media), "external_links": externalLinks, "hashtags": telegramHashtags(content),
			"original_link": originalLink, "forwarded_from": forwarded, "message_id": messageID, "telegram_url": messageURL,
		},
	}, true, nil
}

func optionalTelegramText(s *goquery.Selection) any {
	if s == nil || s.Length() == 0 {
		return nil
	}
	var value strings.Builder
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.TextNode {
			value.WriteString(strings.TrimSpace(node.Data))
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	for _, node := range s.Nodes {
		walk(node)
	}
	return strings.TrimSpace(value.String())
}

func telegramPostURL(msg *goquery.Selection, channel string) string {
	if href := msg.Find("a.tgme_widget_message_date").First().AttrOr("href", ""); href != "" {
		if strings.HasPrefix(href, "http") {
			return href
		}
		if strings.HasPrefix(href, "/") {
			return resolve("https://t.me", href)
		}
	}
	if post := msg.AttrOr("data-post", ""); post != "" {
		return "https://t.me/" + post
	}
	var result string
	msg.Find("a[href]").EachWithBreak(func(_ int, a *goquery.Selection) bool {
		href := a.AttrOr("href", "")
		if strings.Contains(href, "t.me/") || strings.Contains(href, "telegram.me/") {
			if strings.HasPrefix(href, "http") {
				result = href
			} else if strings.HasPrefix(href, "/") {
				result = "https://t.me" + href
			}
			return result == ""
		}
		return true
	})
	if result != "" {
		return result
	}
	return "https://t.me/s/" + channel
}

func telegramPostID(msg *goquery.Selection, permalink string) any {
	if post := msg.AttrOr("data-post", ""); strings.Contains(post, "/") {
		parts := strings.Split(post, "/")
		return parts[len(parts)-1]
	}
	parts := strings.Split(strings.TrimRight(permalink, "/"), "/")
	if len(parts) > 0 {
		last := parts[len(parts)-1]
		if last != "" && strings.IndexFunc(last, func(r rune) bool { return !unicode.IsDigit(r) }) == -1 {
			return last
		}
	}
	return nil
}

func telegramPostDate(msg *goquery.Selection, fallback time.Time) time.Time {
	for _, selector := range []string{"time[datetime]", ".datetime", ".date", "[data-time]"} {
		el := msg.Find(selector).First()
		if el.Length() == 0 {
			continue
		}
		if raw := el.AttrOr("datetime", ""); raw != "" {
			for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02"} {
				if parsed, err := time.Parse(layout, raw); err == nil {
					return parsed.UTC().Truncate(time.Microsecond)
				}
			}
		}
		if raw := el.AttrOr("data-time", ""); raw != "" {
			if seconds, err := strconv.ParseFloat(raw, 64); err == nil && !math.IsNaN(seconds) && !math.IsInf(seconds, 0) {
				return time.Unix(0, int64(seconds*1e9)).UTC().Round(time.Microsecond)
			}
		}
	}
	return fallback.UTC().Truncate(time.Microsecond)
}

func telegramExternalLinks(msg *goquery.Selection, pageURL string) []string {
	var candidates []*goquery.Selection
	for _, selector := range []string{`.tgme_widget_message_link_preview a[href]`, `.link_preview a[href]`, `.tgme_widget_message_forwarded_from a[href]`} {
		msg.Find(selector).Each(func(_ int, a *goquery.Selection) { candidates = append(candidates, a) })
	}
	if len(candidates) == 0 {
		msg.Find("a[href]").Each(func(_ int, a *goquery.Selection) { candidates = append(candidates, a) })
	}
	seen := map[string]bool{}
	links := make([]string, 0, 5)
	for _, a := range candidates {
		href := a.AttrOr("href", "")
		if !strings.HasPrefix(href, "http://") && !strings.HasPrefix(href, "https://") || strings.Contains(href, "t.me") || strings.Contains(href, "telegram.me") {
			continue
		}
		u, err := url.Parse(href)
		if err != nil || u.Host == "" {
			continue
		}
		u.RawQuery = telegramCleanQuery(u.RawQuery)
		u.Fragment = ""
		cleaned := u.String()
		if !seen[cleaned] {
			seen[cleaned] = true
			links = append(links, cleaned)
			if len(links) == 5 {
				break
			}
		}
	}
	return links
}

func telegramCleanQuery(raw string) string {
	tracking := map[string]bool{"utm_source": true, "utm_medium": true, "utm_campaign": true, "utm_content": true, "utm_term": true, "fbclid": true, "gclid": true, "_ga": true, "mc_cid": true, "mc_eid": true}
	values, err := url.ParseQuery(raw)
	if err != nil {
		return ""
	}
	var keys []string
	seen := map[string]bool{}
	for _, pair := range strings.Split(raw, "&") {
		key, _, _ := strings.Cut(pair, "=")
		decoded, decodeErr := url.QueryUnescape(key)
		if decodeErr == nil && !tracking[decoded] && !seen[decoded] && len(values[decoded]) > 0 {
			keys = append(keys, decoded)
			seen[decoded] = true
		}
	}
	var out []string
	for _, key := range keys {
		for _, value := range values[key] {
			out = append(out, url.QueryEscape(key)+"="+url.QueryEscape(value))
		}
	}
	return strings.Join(out, "&")
}

func telegramOriginalLink(links []string) *string {
	blocked := []string{"facebook.com", "twitter.com", "x.com", "instagram.com", "vk.com", "ok.ru", "youtube.com", "youtu.be", "t.me", "telegram.me"}
	for _, link := range links {
		u, _ := url.Parse(link)
		host := strings.ToLower(u.Host)
		good := true
		for _, fragment := range blocked {
			if strings.Contains(host, fragment) {
				good = false
				break
			}
		}
		if good {
			value := link
			return &value
		}
	}
	return nil
}

func telegramText(s *goquery.Selection) string {
	if s == nil || s.Length() == 0 {
		return ""
	}
	var parts []string
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.TextNode {
			if value := strings.TrimSpace(node.Data); value != "" {
				parts = append(parts, value)
			}
		}
		if node.Type == html.ElementNode && (node.Data == "script" || node.Data == "style") {
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	for _, node := range s.Nodes {
		walk(node)
	}
	return strings.Join(parts, " ")
}

func cleanTelegramContent(content string) string {
	content = strings.Join(strings.FieldsFunc(content, func(r rune) bool {
		return unicode.IsSpace(r) || r >= 0x1c && r <= 0x1f
	}), " ")
	content = strings.Map(func(r rune) rune {
		if r <= 0x08 || r >= 0x0b && r <= 0x0c || r >= 0x0e && r <= 0x1f || r >= 0x7f && r <= 0x9f {
			return -1
		}
		return r
	}, content)
	content = regexp.MustCompile(`(?i)\s*\n\s*View in Telegram\s*$`).ReplaceAllString(content, "")
	content = regexp.MustCompile(`(?i)\s*\n\s*Open in Telegram\s*$`).ReplaceAllString(content, "")
	content = regexp.MustCompile(`(?m)^[↩↪➡⬅→←]\s*.+$\n?`).ReplaceAllString(content, "")
	content = regexp.MustCompile(`(?im)^Forwarded from\s*.+$\n?`).ReplaceAllString(content, "")
	return strings.TrimSpace(content)
}

func telegramPythonTitle(content string) string {
	lines := strings.Split(content, "\n")
	title := ""
	for _, line := range lines {
		if strings.TrimSpace(line) != "" {
			title = strings.TrimSpace(line)
			break
		}
	}
	if title == "" {
		return "Telegram Post"
	}
	for _, prefix := range []*regexp.Regexp{regexp.MustCompile(`(?i)^(Forwarded from|Reply to|@[\p{L}\p{N}_]+:?\s*)`), regexp.MustCompile(`^\s*[🔗📎📷🎥📄]\s*`)} {
		title = prefix.ReplaceAllString(title, "")
	}
	if len([]rune(title)) <= 120 {
		return title
	}
	r := []rune(title)
	cut := string(r[:120])
	for _, ending := range []string{". ", "! ", "? ", ": ", " - ", " – ", " — "} {
		if p := strings.LastIndex(cut, ending); p >= 0 && runeLen(cut[:p]) > 60 {
			return strings.TrimSpace(cut[:p+1])
		}
	}
	words := strings.Fields(cut)
	if len(words) > 1 {
		return strings.Join(words[:len(words)-1], " ") + "..."
	}
	return string(r[:117]) + "..."
}

func telegramHashtags(content string) []string {
	tags := []string{}
	seen := map[string]bool{}
	for _, loc := range regexp.MustCompile(`(?:^|\s)#([\p{L}\p{N}_]+)`).FindAllStringSubmatch(content, -1) {
		if len(loc) < 2 {
			continue
		}
		tag := strings.ToLower(strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
				return r
			}
			return -1
		}, loc[1]))
		if tag != "" && !seen[tag] {
			seen[tag] = true
			tags = append(tags, tag)
			if len(tags) == 20 {
				break
			}
		}
	}
	return tags
}

func telegramMediaInfo(msg *goquery.Selection, pageURL string) map[string]any {
	info := map[string]any{"video_url": nil, "document_url": nil, "audio_url": nil, "sticker_url": nil, "poll_data": nil, "location_data": nil, "media_type": nil, "duration": nil, "file_size": nil, "file_name": nil}
	for _, sel := range []string{"video[src]", ".video-message video", ".media-video video", "source[src]"} {
		if el := msg.Find(sel).First(); el.Length() > 0 && el.AttrOr("src", "") != "" {
			info["video_url"] = telegramNormalizeURL(el.AttrOr("src", ""), pageURL)
			info["media_type"] = "video"
			if d := el.AttrOr("duration", ""); d != "" {
				info["duration"] = d
			}
			break
		}
	}
	for _, sel := range []string{`a[href*="/file/"]`, ".document a[href]", ".attachment a[href]", ".file-download a[href]"} {
		if el := msg.Find(sel).First(); el.Length() > 0 {
			href := el.AttrOr("href", "")
			if href == "" {
				continue
			}
			info["document_url"] = telegramNormalizeURL(href, pageURL)
			info["media_type"] = "document"
			if name := strings.TrimSpace(el.Find(".file-name,.document-name").First().Text()); name != "" {
				info["file_name"] = name
			}
			if size := strings.TrimSpace(msg.Find(".file-size,.document-size").First().Text()); size != "" {
				info["file_size"] = size
			}
			break
		}
	}
	for _, sel := range []string{"audio[src]", ".audio-message audio", ".voice-message audio"} {
		if el := msg.Find(sel).First(); el.Length() > 0 && el.AttrOr("src", "") != "" {
			info["audio_url"] = telegramNormalizeURL(el.AttrOr("src", ""), pageURL)
			info["media_type"] = "audio"
			if d := el.AttrOr("duration", ""); d != "" {
				info["duration"] = d
			}
			break
		}
	}
	if el := msg.Find(".sticker img,.animated-sticker img").First(); el.Length() > 0 {
		info["sticker_url"] = telegramNormalizeURL(el.AttrOr("src", ""), pageURL)
		info["media_type"] = "sticker"
	}
	if poll := msg.Find(".poll,.quiz").First(); poll.Length() > 0 {
		if question := poll.Find(".poll-question,.quiz-question").First(); question.Length() > 0 {
			options := []string{}
			poll.Find(".poll-option,.quiz-option").Each(func(_ int, option *goquery.Selection) { options = append(options, strings.TrimSpace(option.Text())) })
			kind := "poll"
			if poll.Is(".quiz") {
				kind = "quiz"
			}
			info["poll_data"] = map[string]any{"question": strings.TrimSpace(question.Text()), "options": options, "type": kind}
			info["media_type"] = "poll"
		}
	}
	if location := msg.Find(".location,.venue").First(); location.Length() > 0 {
		var locationURL any
		if href := location.Find(`a[href*="maps.google.com"],a[href*="openstreetmap.org"]`).First().AttrOr("href", ""); href != "" {
			locationURL = href
		}
		info["location_data"] = map[string]any{"text": strings.TrimSpace(location.Text()), "url": locationURL}
		info["media_type"] = "location"
	}
	return info
}

func telegramNormalizeURL(raw, pageURL string) string {
	if raw == "" {
		return ""
	}
	if strings.HasPrefix(raw, "//") {
		return "https:" + raw
	}
	if strings.HasPrefix(raw, "/") {
		return "https://t.me" + raw
	}
	if strings.Contains(raw, "cdn") && strings.Contains(raw, "t.me") {
		if match := regexp.MustCompile(`cdn\d*\.t\.me/file/(.*)`).FindStringSubmatch(raw); len(match) > 1 {
			return "https://t.me/file/" + match[1]
		}
	}
	return raw
}

func telegramSourceMedia(msg *goquery.Selection, pageURL string) []map[string]any {
	mediaMsg := msg.Clone()
	mediaMsg.Find("img").Each(func(_ int, image *goquery.Selection) {
		if image.AttrOr("src", "") == "" && image.AttrOr("data-lazy-src", "") != "" {
			image.SetAttr("src", image.AttrOr("data-lazy-src", ""))
		}
	})
	media := telegramMedia(mediaMsg)
	backgroundImages := telegramBackgroundImageURLs(msg, pageURL)
	seen := map[string]bool{}
	out := make([]map[string]any, 0, len(media))
	for _, item := range media {
		u := telegramNormalizeURL(item.URL, pageURL)
		if u == "" || seen[u] {
			continue
		}
		seen[u] = true
		m := map[string]any{"url": u, "type": item.Type}
		switch item.Type {
		case "image":
			m["thumbnail"] = u
		case "video":
			m["thumbnail"] = nil
			if item.Thumbnail != "" {
				m["thumbnail"] = telegramNormalizeURL(item.Thumbnail, pageURL)
			}
			m["duration"] = nil
			if item.Duration != "" {
				m["duration"] = item.Duration
			}
			m["source"] = item.Source
			if duration := telegramMediaDuration(msg, item.URL, pageURL, "video[src],.video-message video,.media-video video,source[src]"); duration != nil {
				m["duration"] = duration
			}
		case "audio":
			m["duration"] = nil
			if item.Duration != "" {
				m["duration"] = item.Duration
			}
			m["source"] = item.Source
			if duration := telegramMediaDuration(msg, item.URL, pageURL, "audio[src],.audio-message audio,.voice-message audio"); duration != nil {
				m["source"] = "direct_audio"
				m["duration"] = duration
			}
		case "document":
			m["thumbnail"] = nil
			if item.Thumbnail != "" {
				m["thumbnail"] = telegramNormalizeURL(item.Thumbnail, pageURL)
			}
			if item.Type == "document" && m["thumbnail"] == nil {
				continue
			}
		case "gif", "sticker", "animated_sticker":
			m["thumbnail"] = nil
			if item.Thumbnail != "" {
				m["thumbnail"] = telegramNormalizeURL(item.Thumbnail, pageURL)
			}
			m["source"] = item.Source
		case "poll":
			m["source"] = item.Source
			m["poll_data"] = item.PollData
		case "location":
			m["source"] = item.Source
			locationData := map[string]any{"text": item.LocationData["text"], "url": nil}
			if value, ok := item.LocationData["url"].(string); ok && value != "" {
				locationData["url"] = value
			}
			m["location_data"] = locationData
		case "contact":
			m["source"] = item.Source
			m["contact_data"] = item.ContactData
		}
		out = append(out, m)
	}
	rank := map[string]int{"image": 0, "video": 1, "audio": 2, "sticker": 3, "animated_sticker": 3, "gif": 4, "document": 5, "poll": 6, "location": 7, "contact": 8}
	sort.SliceStable(out, func(i, j int) bool {
		a, _ := out[i]["type"].(string)
		b, _ := out[j]["type"].(string)
		aRank, bRank := rank[a], rank[b]
		if a == "image" && backgroundImages[out[i]["url"].(string)] {
			aRank = 9
		}
		if b == "image" && backgroundImages[out[j]["url"].(string)] {
			bRank = 9
		}
		return aRank < bRank
	})
	return out
}

func telegramBackgroundImageURLs(msg *goquery.Selection, pageURL string) map[string]bool {
	excluded := map[string]bool{
		"tg-emoji": true, "emoji": true, "tg-emoji-wrap": true,
		"tgme_widget_message_user_photo": true, "tgme_widget_message_owner_photo": true,
		"message_author_photo": true, "avatar": true, "channel_photo": true,
		"bgcolor0": true, "bgcolor1": true, "bgcolor2": true, "bgcolor3": true,
		"bgcolor4": true, "bgcolor5": true, "bgcolor6": true, "bgcolor7": true,
	}
	backgrounds := map[string]bool{}
	msg.Find("[style*='background-image']").Each(func(_ int, el *goquery.Selection) {
		classNames, _ := el.Attr("class")
		for _, className := range strings.Fields(classNames) {
			if excluded[className] {
				return
			}
		}
		style := el.AttrOr("style", "")
		for _, match := range backgroundURL.FindAllStringSubmatch(style, -1) {
			if len(match) > 1 {
				backgrounds[telegramNormalizeURL(strings.TrimSpace(match[1]), pageURL)] = true
			}
		}
	})
	return backgrounds
}

func telegramMediaDuration(msg *goquery.Selection, mediaURL, pageURL, selector string) any {
	var duration any
	msg.Find(selector).EachWithBreak(func(_ int, el *goquery.Selection) bool {
		raw := el.AttrOr("src", "")
		if raw == "" {
			raw = el.AttrOr("data-src", "")
		}
		if raw == "" || telegramNormalizeURL(raw, pageURL) != telegramNormalizeURL(mediaURL, pageURL) {
			return true
		}
		duration = telegramDuration(el)
		return false
	})
	return duration
}

func telegramDuration(el *goquery.Selection) any {
	if value := el.AttrOr("duration", ""); value != "" {
		return value
	}
	if value := el.AttrOr("data-duration", ""); value != "" {
		return value
	}
	if value := el.AttrOr("data-time", ""); value != "" {
		return value
	}
	if match := regexp.MustCompile(`(\d{1,2}:\d{2})`).FindStringSubmatch(el.Text()); len(match) > 1 {
		return match[1]
	}
	return nil
}
