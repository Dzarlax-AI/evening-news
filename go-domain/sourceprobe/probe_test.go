package sourceprobe

import (
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"testing"
)

func wrap(t *testing.T, s Source, payloadKey string, payload any) json.RawMessage {
	t.Helper()
	raw, e := json.Marshal(map[string]any{"contract_version": "1", "build_version": "fixture", "request_id": "req", "results": []any{map[string]any{"source_id": s.ID, "type": s.Type, "outcome": "success", payloadKey: payload}}})
	if e != nil {
		t.Fatal(e)
	}
	return raw
}
func TestProbeResultValidationAcrossSources(t *testing.T) {
	rss := Source{ID: 1, Name: "RSS", Type: "rss", URL: "https://example.test/feed"}
	rssArticle := map[string]any{"title": "Title", "url": "https://example.test/story", "content": nil, "summary": nil, "image_url": nil, "published_at": "2026-10-28T12:00:00Z", "source_type": "rss", "source_name": "RSS", "raw_data": map[string]any{"guid": "1", "author": nil, "tags": []any{}}}
	if out, _, ok := validateResponse(wrap(t, rss, "rss_articles", []any{rssArticle}), "req", rss, nil); !ok || out != "success" {
		t.Fatal("valid RSS rejected")
	}
	rssArticle["url"] = "https://bad.test@attacker.test"
	if _, _, ok := validateResponse(wrap(t, rss, "rss_articles", []any{rssArticle}), "req", rss, nil); ok {
		t.Fatal("unsafe RSS URL accepted")
	}
	tg := Source{ID: 2, Name: "Telegram", Type: "telegram", URL: "https://t.me/example"}
	article := map[string]any{"title": "Title", "url": "https://t.me/example/1", "content": "text", "summary": nil, "published_at": nil, "source_type": "telegram", "source_name": "Telegram", "image_url": nil, "media_files": []any{}, "raw_data": map[string]any{"channel": "example", "access_method": "http", "content_length": 4, "media_info": map[string]any{}, "has_media": false, "media_count": 0, "external_links": []any{}, "hashtags": []any{}, "original_link": nil, "forwarded_from": nil, "message_id": nil, "telegram_url": "https://t.me/example/1"}}
	telegram := map[string]any{"contract_version": "1", "build_version": "fixture", "request_id": "source-2", "original_url": tg.URL, "final_url": "https://t.me/s/example", "outcome": "success", "fetch_method": "http", "articles": []any{article}}
	if _, _, ok := validateResponse(wrap(t, tg, "telegram", telegram), "req", tg, nil); !ok {
		t.Fatal("valid Telegram rejected")
	}
	article["media_files"] = nil
	if _, _, ok := validateResponse(wrap(t, tg, "telegram", telegram), "req", tg, nil); ok {
		t.Fatal("malformed Telegram accepted")
	}
	page := Source{ID: 3, Name: "Page", Type: "custom", URL: "https://example.test/news"}
	link := "https://example.test/story"
	title := "Fixture"
	id := md5.Sum([]byte(title + link))
	item := map[string]any{"title": title, "link": link, "description": "text", "published": "2026-10-28T12:00:00Z", "content_type": "news", "id": hex.EncodeToString(id[:]), "source": "Page", "source_url": page.URL}
	result := map[string]any{"contract_version": "1", "build_version": "fixture", "request_id": "source-3", "original_url": page.URL, "final_url": page.URL, "outcome": "success", "fetch_method": "http", "snapshot_at": "2026-10-28T12:00:00Z", "content_hash": "0123456789abcdef0123456789abcdef", "selectors_used": []any{}, "items": []any{item}, "needs_selector_study": false}
	if _, _, ok := validateResponse(wrap(t, page, "page_monitor", result), "req", page, map[string]any{"limit": 20}); !ok {
		t.Fatal("valid page rejected")
	}
	item["id"] = "bad"
	if _, _, ok := validateResponse(wrap(t, page, "page_monitor", result), "req", page, map[string]any{"limit": 20}); ok {
		t.Fatal("bad item identity accepted")
	}
}
func TestDescriptorDefaultsAndInvalidTelegram(t *testing.T) {
	s := Source{ID: 3, Name: "Page", Type: "custom", URL: "https://example.test", Config: map[string]any{}}
	d, ok := descriptor(s)
	if !ok {
		t.Fatal("default descriptor")
	}
	p := d["page_monitor"].(map[string]any)
	if len(p["article_selectors"].([]string)) != 22 || d["limit"] != 20 {
		t.Fatalf("descriptor=%v", d)
	}
	s.Type = "telegram"
	s.Config["limit"] = json.Number("0")
	if _, ok = descriptor(s); ok {
		t.Fatal("zero Telegram limit accepted")
	}
}
