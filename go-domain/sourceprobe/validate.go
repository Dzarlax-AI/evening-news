package sourceprobe

import (
	"bytes"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

func str(v any) (string, bool)         { s, ok := v.(string); return s, ok }
func obj(v any) (map[string]any, bool) { m, ok := v.(map[string]any); return m, ok && m != nil }
func arr(v any) ([]any, bool)          { a, ok := v.([]any); return a, ok && a != nil }
func exact(m map[string]any, keys ...string) bool {
	if len(m) != len(keys) {
		return false
	}
	for _, k := range keys {
		if _, ok := m[k]; !ok {
			return false
		}
	}
	return true
}
func nullableString(v any) bool {
	if v == nil {
		return true
	}
	_, ok := v.(string)
	return ok
}
func safeURL(value any) bool {
	s, ok := str(value)
	if !ok || s == "" || len(s) > 8192 {
		return false
	}
	for _, r := range s {
		if r == '"' || r == '\'' || r == '<' || r == '>' || r == '`' || unicode.IsSpace(r) || unicode.IsControl(r) {
			return false
		}
	}
	u, e := url.Parse(s)
	return e == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil
}
func utcDate(v any) bool {
	s, ok := str(v)
	if !ok {
		return false
	}
	x, e := time.Parse(time.RFC3339Nano, s)
	if e != nil {
		return false
	}
	_, off := x.Zone()
	return off == 0
}
func dateWithZone(v any) bool {
	s, ok := str(v)
	if !ok {
		return false
	}
	_, e := time.Parse(time.RFC3339Nano, s)
	return e == nil
}
func bounded(v any, n int) bool {
	s, ok := str(v)
	return ok && utf8.RuneCountInString(s) <= n && len([]byte(s)) <= n
}
func nonnegativeInt(v any) bool { i, ok := intValue(v); return ok && i >= 0 }
func validateResponse(raw json.RawMessage, requestID string, s Source, d map[string]any) (string, int, bool) {
	var top map[string]any
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if dec.Decode(&top) != nil || !exact(top, "contract_version", "build_version", "request_id", "results") || top["contract_version"] != "1" || top["request_id"] != requestID {
		return "", 0, false
	}
	build, ok := str(top["build_version"])
	if !ok || build == "" {
		return "", 0, false
	}
	results, ok := arr(top["results"])
	if !ok || len(results) != 1 {
		return "", 0, false
	}
	r, ok := obj(results[0])
	if !ok {
		return "", 0, false
	}
	id, ok := intValue(r["source_id"])
	if !ok || int64(id) != s.ID || r["type"] != s.Type {
		return "", 0, false
	}
	outcome, ok := str(r["outcome"])
	if !ok {
		return "", 0, false
	}
	switch outcome {
	case "success", "partial", "failed", "rejected", "deferred":
	default:
		return "", 0, false
	}
	delay := 0
	if er := r["error"]; er != nil {
		errorObj, ok := obj(er)
		if !ok {
			return "", 0, false
		}
		code, ok := str(errorObj["code"])
		if !ok || !errorCodes[code] {
			return "", 0, false
		}
		if _, ok = errorObj["retryable"].(bool); !ok {
			return "", 0, false
		}
		if v, ok := errorObj["retry_after_seconds"]; ok {
			delay, ok = intValue(v)
			if !ok || delay < 0 {
				return "", 0, false
			}
		}
	} else if outcome == "failed" || outcome == "rejected" {
		return "", 0, false
	}
	if v, ok := r["final_url"]; ok {
		if !bounded(v, 8192) {
			return "", 0, false
		}
	}
	if v, ok := r["fetched_at"]; ok && v != nil && !dateWithZone(v) {
		return "", 0, false
	}
	if outcome != "success" && outcome != "partial" {
		return outcome, delay, true
	}
	switch s.Type {
	case "rss":
		if !validRSS(r["rss_articles"], s.Name) {
			return "", 0, false
		}
	case "telegram":
		if !validTelegram(r["telegram"], s, d) {
			return "", 0, false
		}
	case "custom":
		if !validPage(r["page_monitor"], s, d) {
			return "", 0, false
		}
	default:
		return "", 0, false
	}
	return outcome, delay, true
}

var errorCodes = func() map[string]bool {
	v := map[string]bool{}
	for _, s := range strings.Fields(`batch_deadline_exceeded busy cancelled canceled deadline_exceeded duplicate_source_id invalid_feed invalid_input invalid_request invalid_runner_response invalid_source page_monitor_unavailable public_url_denied request_too_large response_budget_exhausted resource_limit response_too_large rss_unavailable source_disabled source_failed source_panic telegram_unavailable timeout transport_error unauthorized unavailable unsupported_source_type upstream_auth_required upstream_http_error upstream_rate_limited upstream_timeout write_deadline_unavailable invalid_page no_posts parse_failed event_overflow admission_unavailable upstream_challenge upstream_forbidden upstream_http_rejected upstream_unavailable transport_failed native_browser_policy_denied native_browser_busy native_browser_unavailable native_browser_timeout native_browser_http_rejected unsupported_selector`) {
		v[s] = true
	}
	return v
}()

func validRSS(v any, name string) bool {
	items, ok := arr(v)
	if !ok || len(items) > 1000 {
		return false
	}
	for _, item := range items {
		a, ok := obj(item)
		if !ok || !exact(a, "title", "url", "content", "summary", "image_url", "published_at", "source_type", "source_name", "raw_data") {
			return false
		}
		if _, ok = str(a["title"]); !ok || !safeURL(a["url"]) || a["source_type"] != "rss" || a["source_name"] != name || !utcDate(a["published_at"]) {
			return false
		}
		for _, key := range []string{"content", "summary", "image_url"} {
			if !nullableString(a[key]) {
				return false
			}
		}
		if a["image_url"] != nil && !safeURL(a["image_url"]) {
			return false
		}
		raw, ok := obj(a["raw_data"])
		if !ok || !exact(raw, "guid", "author", "tags") {
			return false
		}
		if _, ok = str(raw["guid"]); !ok || !nullableString(raw["author"]) {
			return false
		}
		tags, ok := arr(raw["tags"])
		if !ok {
			return false
		}
		for _, tag := range tags {
			if _, ok = str(tag); !ok {
				return false
			}
		}
	}
	return true
}
func channelName(raw string) string {
	u, e := url.Parse(raw)
	if e != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Port() != "" && u.Port() != "443") || (!strings.EqualFold(u.Hostname(), "t.me") && !strings.EqualFold(u.Hostname(), "telegram.me")) {
		return ""
	}
	parts := strings.FieldsFunc(u.Path, func(r rune) bool { return r == '/' })
	if len(parts) == 2 && parts[0] == "s" {
		parts = parts[1:]
	}
	if len(parts) != 1 {
		return ""
	}
	return strings.ToLower(parts[0])
}
func validTelegram(v any, s Source, d map[string]any) bool {
	m, ok := obj(v)
	if !ok {
		return false
	}
	required := []string{"contract_version", "build_version", "request_id", "original_url", "final_url", "outcome", "articles"}
	allowed := map[string]bool{"fetch_method": true, "error": true}
	for _, k := range required {
		allowed[k] = true
		if _, ok = m[k]; !ok {
			return false
		}
	}
	for k := range m {
		if !allowed[k] {
			return false
		}
	}
	build, ok := str(m["build_version"])
	if !ok || build == "" || m["contract_version"] != "1" || m["request_id"] != "source-"+strconv.FormatInt(s.ID, 10) || m["original_url"] != s.URL {
		return false
	}
	outcome, _ := str(m["outcome"])
	method, _ := str(m["fetch_method"])
	if outcome != "success" && outcome != "partial" {
		return false
	}
	if method != "http" && method != "native_browser" {
		return false
	}
	final, ok := str(m["final_url"])
	if !ok {
		return false
	}
	u, e := url.Parse(final)
	if e != nil || u.Scheme != "https" || u.Hostname() != "t.me" || u.Path != "/s/"+channelName(s.URL) || u.User != nil || u.RawQuery != "" || u.Fragment != "" || (u.Port() != "" && u.Port() != "443") {
		return false
	}
	articles, ok := arr(m["articles"])
	if !ok || len(articles) == 0 || len(articles) > 100 {
		return false
	}
	if limit, ok := d["limit"].(int); ok && len(articles) > limit {
		return false
	}
	for _, item := range articles {
		if !validTelegramArticle(item, s.Name) {
			return false
		}
	}
	if outcome == "success" && m["error"] != nil {
		return false
	}
	if outcome == "partial" && m["error"] == nil {
		return false
	}
	return validNestedError(m["error"], true)
}
func validNestedError(v any, telegram bool) bool {
	if v == nil {
		return true
	}
	m, ok := obj(v)
	if !ok {
		return false
	}
	if telegram && !exact(m, "code", "retryable") {
		return false
	}
	code, ok := str(m["code"])
	allowed := pageErrors
	if telegram {
		allowed = telegramErrors
	}
	if !ok || !allowed[code] {
		return false
	}
	if _, ok = m["retryable"].(bool); !ok {
		return false
	}
	if !telegram {
		for k, v := range m {
			switch k {
			case "code", "retryable":
			case "upstream_status":
				n, ok := intValue(v)
				if !ok || n < 0 || n > 599 {
					return false
				}
			case "retry_after_seconds":
				if !nonnegativeInt(v) {
					return false
				}
			case "reason":
				if !bounded(v, 256) {
					return false
				}
			default:
				return false
			}
		}
	}
	return true
}

func errorSet(values string) map[string]bool {
	result := map[string]bool{}
	for _, s := range strings.Fields(values) {
		result[s] = true
	}
	return result
}

var telegramErrors = errorSet(`invalid_request request_too_large invalid_runner_response telegram_unavailable busy unavailable invalid_page no_posts parse_failed resource_limit response_too_large event_overflow public_url_denied admission_unavailable upstream_challenge upstream_rate_limited upstream_auth_required upstream_forbidden upstream_http_rejected upstream_unavailable transport_failed deadline_exceeded cancelled native_browser_policy_denied native_browser_busy native_browser_unavailable native_browser_timeout native_browser_http_rejected`)
var pageErrors = errorSet(`invalid_request request_too_large invalid_runner_response busy unavailable unsupported_selector public_url_denied admission_unavailable deadline_exceeded cancelled event_overflow resource_limit parse_failed transport_failed native_browser_policy_denied native_browser_busy native_browser_unavailable native_browser_timeout native_browser_http_rejected response_too_large`)

func validTelegramArticle(v any, name string) bool {
	m, ok := obj(v)
	if !ok || !exact(m, "title", "url", "content", "summary", "published_at", "source_type", "source_name", "image_url", "media_files", "raw_data") {
		return false
	}
	for _, k := range []string{"title", "url", "content", "source_type", "source_name"} {
		if _, ok = str(m[k]); !ok {
			return false
		}
	}
	content, _ := str(m["content"])
	if utf8.RuneCountInString(content) > 100000 || m["source_type"] != "telegram" || m["source_name"] != name {
		return false
	}
	for _, k := range []string{"summary", "image_url", "published_at"} {
		if !nullableString(m[k]) {
			return false
		}
	}
	if m["published_at"] != nil && !dateWithZone(m["published_at"]) {
		return false
	}
	media, ok := arr(m["media_files"])
	if !ok || len(media) > 4096 {
		return false
	}
	for _, item := range media {
		x, ok := obj(item)
		if !ok {
			return false
		}
		if _, ok = str(x["type"]); !ok {
			return false
		}
		if u, ok := x["url"]; ok && !nullableString(u) {
			return false
		}
	}
	raw, ok := obj(m["raw_data"])
	if !ok || !exact(raw, "channel", "access_method", "content_length", "media_info", "has_media", "media_count", "external_links", "hashtags", "original_link", "forwarded_from", "message_id", "telegram_url") {
		return false
	}
	for _, k := range []string{"channel", "access_method", "telegram_url"} {
		if _, ok = str(raw[k]); !ok {
			return false
		}
	}
	for _, k := range []string{"original_link", "forwarded_from", "message_id"} {
		if !nullableString(raw[k]) {
			return false
		}
	}
	if !nonnegativeInt(raw["content_length"]) || !nonnegativeInt(raw["media_count"]) {
		return false
	}
	if _, ok = raw["has_media"].(bool); !ok {
		return false
	}
	if _, ok = obj(raw["media_info"]); !ok {
		return false
	}
	for _, k := range []string{"external_links", "hashtags"} {
		list, ok := arr(raw[k])
		if !ok || len(list) > 4096 {
			return false
		}
		for _, v := range list {
			if _, ok = str(v); !ok {
				return false
			}
		}
	}
	return true
}
func validPage(v any, s Source, d map[string]any) bool {
	m, ok := obj(v)
	if !ok {
		return false
	}
	required := []string{"contract_version", "build_version", "request_id", "outcome", "original_url", "final_url", "selectors_used", "items", "needs_selector_study"}
	allowed := map[string]bool{"fetch_method": true, "snapshot_at": true, "content_hash": true, "study_html": true, "study_html_omitted": true, "error": true, "next_offset": true}
	for _, k := range required {
		allowed[k] = true
		if _, ok = m[k]; !ok {
			return false
		}
	}
	for k := range m {
		if !allowed[k] {
			return false
		}
	}
	build, ok := str(m["build_version"])
	if !ok || build == "" || m["contract_version"] != "1" || m["request_id"] != "source-"+strconv.FormatInt(s.ID, 10) || m["original_url"] != s.URL {
		return false
	}
	if _, ok = m["needs_selector_study"].(bool); !ok {
		return false
	}
	outcome, _ := str(m["outcome"])
	method, _ := str(m["fetch_method"])
	if (outcome != "success" && outcome != "partial") || (method != "http" && method != "native_browser") || !safeURL(m["final_url"]) || !utcDate(m["snapshot_at"]) {
		return false
	}
	hash, ok := str(m["content_hash"])
	if !ok || len(hash) != 32 {
		return false
	}
	if _, e := hex.DecodeString(hash); e != nil || strings.ToLower(hash) != hash {
		return false
	}
	selectors, ok := arr(m["selectors_used"])
	if !ok || len(selectors) > 256 {
		return false
	}
	for _, x := range selectors {
		if !bounded(x, 2048) {
			return false
		}
	}
	items, ok := arr(m["items"])
	if !ok || len(items) > 100 {
		return false
	}
	if limit, ok := d["limit"].(int); ok && len(items) > limit {
		return false
	}
	for _, x := range items {
		item, ok := obj(x)
		if !ok || !exact(item, "title", "link", "description", "published", "content_type", "id", "source", "source_url") {
			return false
		}
		for k, n := range map[string]int{"title": 5 * 1024 * 1024, "link": 8192, "description": 2048, "content_type": 64, "id": 64, "source": 256, "source_url": 8192} {
			if !bounded(item[k], n) {
				return false
			}
		}
		if item["source"] != s.Name || item["source_url"] != s.URL || !safeURL(item["link"]) || !utcDate(item["published"]) {
			return false
		}
		title, _ := str(item["title"])
		link, _ := str(item["link"])
		id := md5.Sum([]byte(title + link))
		if item["id"] != hex.EncodeToString(id[:]) {
			return false
		}
		kind, _ := str(item["content_type"])
		if kind != "changelog" && kind != "news" && kind != "blog" && kind != "general" {
			return false
		}
	}
	if x, ok := m["study_html"]; ok && !bounded(x, 5*1024*1024) {
		return false
	}
	if x, ok := m["study_html_omitted"]; ok {
		if _, ok = x.(bool); !ok {
			return false
		}
	}
	if outcome == "success" && m["error"] != nil {
		return false
	}
	if outcome == "partial" {
		er, ok := obj(m["error"])
		if !ok || !validNestedError(er, false) {
			return false
		}
		code, _ := str(er["code"])
		if code != "resource_limit" && code != "deadline_exceeded" && code != "cancelled" && code != "parse_failed" {
			return false
		}
	}
	if next, ok := m["next_offset"]; ok {
		n, valid := intValue(next)
		limit := 20
		if v, ok := d["limit"].(int); ok {
			limit = v
		}
		if !valid || outcome != "partial" || n <= 0 || n > limit || (len(items) > 0 && n != len(items)) {
			return false
		}
		if len(items) == 0 {
			er, _ := obj(m["error"])
			if er["code"] != "resource_limit" || n != 1 {
				return false
			}
		}
	}
	return true
}
