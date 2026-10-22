package extractor

import (
	"regexp"
	"strings"

	"github.com/PuerkitoBio/goquery"
	"golang.org/x/net/html"
)

// Styles are parsed only after DOM selection; HTML is never parsed with regular expressions.
var backgroundURL = regexp.MustCompile(`(?i)url\(\s*['"]?([^'"\)]+)['"]?\s*\)`)

func compactText(s *goquery.Selection) string {
	var parts []string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.TextNode {
			parts = append(parts, strings.TrimSpace(n.Data))
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	for _, n := range s.Nodes {
		walk(n)
	}
	return strings.Join(parts, "")
}

func telegramVideoURL(s *goquery.Selection) string {
	if candidate := s.Find("source[src]").First().AttrOr("src", ""); isTelegramVideoURL(candidate) {
		return candidate
	}
	if goquery.NodeName(s) == "video" {
		if candidate := s.AttrOr("src", ""); isTelegramVideoURL(candidate) {
			return candidate
		}
	}
	if candidate := s.Find("video[src]").First().AttrOr("src", ""); isTelegramVideoURL(candidate) {
		return candidate
	}
	for _, attr := range []string{"data-video", "data-src", "data-video-src", "data-url", "data-video-url", "data-mp4"} {
		if candidate := s.AttrOr(attr, ""); isTelegramVideoURL(candidate) {
			return candidate
		}
	}
	if candidate := s.AttrOr("href", ""); isTelegramVideoURL(candidate) {
		return candidate
	}
	return ""
}

func isTelegramVideoURL(candidate string) bool {
	lower := strings.ToLower(strings.SplitN(candidate, "?", 2)[0])
	for _, ext := range []string{".mp4", ".avi", ".mov", ".wmv", ".flv", ".webm", ".mkv", ".m4v"} {
		if strings.Contains(lower, ext) {
			return true
		}
	}
	return strings.Contains(lower, "video")
}

func isTelegramContentImageURL(candidate string) bool {
	lower := strings.ToLower(candidate)
	for _, blocked := range []string{"/img/emoji/", "/userpic/", "/channel_photo/", "/profile_photo/", "16x16", "32x32", "50x50", "64x64", "thumb", "icon", "avatar"} {
		if strings.Contains(lower, blocked) {
			return false
		}
	}
	for _, ext := range []string{".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".svg"} {
		if strings.Contains(lower, ext) {
			return true
		}
	}
	return strings.Contains(lower, "t.me") || strings.Contains(lower, "cdn.telegram")
}

func telegramMedia(msg *goquery.Selection) []Media {
	var out []Media
	seen := map[string]bool{}
	excluded := map[string]bool{}
	blocked := ".tgme_widget_message_user_photo,.tgme_widget_message_owner_photo,.tgme_widget_message_forwarded_from,.tgme_widget_message_reply,.message_author_photo,.avatar,.profile,.channel_photo,.emoji,.tg-emoji,tg-emoji"
	msg.Find(blocked).Each(func(_ int, s *goquery.Selection) {
		s.Find("img[src]").Each(func(_ int, i *goquery.Selection) { excluded[i.AttrOr("src", "")] = true })
		if m := backgroundURL.FindStringSubmatch(s.AttrOr("style", "")); len(m) > 1 {
			excluded[m[1]] = true
		}
	})
	add := func(m Media) {
		if m.URL == "" || excluded[m.URL] {
			return
		}
		if m.Type == "image" && !isTelegramContentImageURL(m.URL) {
			return
		}
		if m.Type == "image" && m.Thumbnail == "" {
			m.Thumbnail = m.URL
		}
		k := m.Type + ":" + m.URL
		if !seen[k] {
			seen[k] = true
			out = append(out, m)
		}
	}
	cleanMsg := msg.Clone()
	cleanMsg.Find(blocked).Remove()
	for _, kind := range []string{"image", "video", "audio", "document", "sticker", "gif", "poll", "location", "contact"} {
		queries := map[string]string{
			"image":    ".media img,.photo img,.attachment img,.message_media img,.tgme_widget_message_photo img,.tgme_widget_message_photo,.message_media_photo img,.js-message_photo img",
			"video":    ".tgme_widget_message_video,.tgme_widget_message_video_player,.message_media_video,.js-message_video,video,.video-thumb img,.message_video,.video_note,.round_video",
			"audio":    ".tgme_widget_message_voice,.tgme_widget_message_audio,.message_media_audio,.js-message_audio,.audio_file,.voice_message,.voice_note,audio",
			"document": ".tgme_widget_message_document,.document-thumb img,.message_media_document,.js-message_document,.document_wrap,.document_file",
			"sticker":  ".tgme_widget_message_sticker,.sticker_wrap,.animated_sticker,.message_sticker,.sticker img",
			"gif":      ".tgme_widget_message_gif,.message_media_gif,.js-message_gif,.gif_wrap,.animated_gif",
			"poll":     ".tgme_widget_message_poll,.message_media_poll,.poll_wrap",
			"location": ".tgme_widget_message_location,.message_media_location,.location_wrap,.location_point",
			"contact":  ".tgme_widget_message_contact,.message_media_contact,.contact_wrap",
		}
		cleanMsg.Find(queries[kind]).Each(func(_ int, s *goquery.Selection) {
			m := Media{Type: kind}
			if kind == "video" {
				m.URL = telegramVideoURL(s)
			} else {
				m.URL = s.AttrOr("src", s.AttrOr("href", s.AttrOr("data-src", s.AttrOr("data-video", ""))))
			}
			if m.URL == "" && kind != "video" {
				m.URL = s.Find("source[src],video[src],audio[src],a[href],img[src]").First().AttrOr("src", "")
				if m.URL == "" {
					m.URL = s.Find("a[href]").First().AttrOr("href", "")
				}
			}
			if m.URL == "" {
				if b := backgroundURL.FindStringSubmatch(s.AttrOr("style", "")); len(b) > 1 {
					m.URL = b[1]
				}
			}
			if kind == "video" {
				m.Thumbnail = s.AttrOr("poster", s.Find("video[poster]").First().AttrOr("poster", ""))
				if m.URL == "" && m.Thumbnail == "" {
					m.Thumbnail = s.Find("img[src]").First().AttrOr("src", "")
				}
			} else {
				m.Thumbnail = s.AttrOr("poster", s.Find("img[src]").First().AttrOr("src", ""))
			}
			m.Duration = s.AttrOr("duration", text(s.Find(".duration,.audio_duration,.video_duration").First()))
			switch kind {
			case "video":
				m.Source = "container_video"
				if goquery.NodeName(s) == "video" || s.Find("video[src],source[src]").Length() > 0 {
					m.Source = "direct_video"
				}
			case "audio":
				m.Source = "container_audio"
				if goquery.NodeName(s) == "audio" {
					m.Source = "direct_audio"
				}
			case "sticker":
				m.Source = "sticker"
				if strings.Contains(m.URL, ".webp") || strings.Contains(m.URL, ".tgs") {
					m.Type = "animated_sticker"
				}
			case "gif":
				m.Source = "gif"
				for _, a := range []string{"data-gif", "data-src", "data-gif-src", "data-url", "href", "data-href", "data-gif-url"} {
					if v := s.AttrOr(a, ""); strings.Contains(strings.ToLower(v), "gif") {
						m.URL = v
						break
					}
				}
			}
			m.Title = text(s.Find(".tgme_widget_message_document_title,.document_name,.audio_title,.title").First())
			switch kind {
			case "poll":
				q := text(s.Find(".poll_question,.poll_title").First())
				if q == "" {
					q = "Poll"
				}
				var opts []string
				s.Find(".poll_option,.poll_answer").Each(func(_ int, v *goquery.Selection) {
					if t := text(v); t != "" {
						opts = append(opts, t)
					}
				})
				m.URL = stableRef("poll", q+strings.Join(opts, ""))
				m.PollData = map[string]any{"question": q, "options": opts}
				m.Source = "poll"
			case "contact":
				t := compactText(s)
				if t == "" {
					return
				}
				m.URL = stableRef("contact", t)
				m.ContactData = map[string]any{"text": t, "phone": text(s.Find(".contact_phone,.phone").First())}
				m.Source = "contact"
			case "location":
				t := text(s)
				if t == "" && m.URL == "" {
					return
				}
				if m.URL == "" {
					m.URL = stableRef("location", t)
				}
				m.LocationData = map[string]any{"text": t, "url": s.AttrOr("href", s.Find("a[href]").First().AttrOr("href", ""))}
				m.Source = "location"
			}
			if strings.HasPrefix(m.URL, "//") {
				m.URL = "https:" + m.URL
			}
			add(m)
		})
	}
	cleanMsg.Find("[style]").Each(func(_ int, s *goquery.Selection) {
		if strings.Contains(s.AttrOr("class", ""), "bgcolor") {
			return
		}
		if m := backgroundURL.FindStringSubmatch(s.AttrOr("style", "")); len(m) > 1 {
			u := m[1]
			if strings.HasPrefix(u, "//") {
				u = "https:" + u
			}
			add(Media{Type: "image", URL: u})
		}
	})
	return out
}
