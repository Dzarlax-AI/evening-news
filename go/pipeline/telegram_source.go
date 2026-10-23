package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"net/url"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
	"github.com/Dzarlax-AI/evening-news/go/transport"
)

const TelegramNativeReadySelector = ".tgme_widget_message"
const telegramResponseArticleBudget = 5*1024*1024 - 4096
const telegramArticleArrayLimit = 4096

type TelegramOutcome struct {
	Articles     []extractor.TelegramArticle
	FinalURL     string
	AccessMethod string
	Outcome      string
	Error        string
	Retryable    bool
}

type TelegramSource struct {
	Admit  func(context.Context, string) error
	Fetch  func(context.Context, string, transport.Options) (transport.Document, error)
	Render func(context.Context, string, transport.BrowserOptions, string) (transport.RenderedDocument, error)
	Now    func() time.Time
}

// FetchChannel uses the guarded public HTTP path first. One native render is
// permitted only for transport/server failures or an empty parsed page.
func (s TelegramSource) FetchChannel(ctx context.Context, channelURL, sourceName string, limit int) TelegramOutcome {
	out := TelegramOutcome{Articles: []extractor.TelegramArticle{}, FinalURL: channelURL, Outcome: "failed"}
	if err := transport.ValidatePublicURL(channelURL); err != nil {
		return telegramFailure(out, "public_url_denied", false)
	}
	fetch := s.Fetch
	if fetch == nil {
		fetch = transport.FetchPublic
	}
	now := s.Now
	if now == nil {
		now = time.Now
	}
	doc, err := fetch(ctx, channelURL, transport.Options{Timeout: 30 * time.Second, MaxWireBytes: transport.MaxPageBytes, MaxDecodedBytes: transport.MaxPageBytes})
	if err == nil {
		final := doc.URL
		if final == "" {
			final = channelURL
		}
		if !sameTelegramChannel(channelURL, final) {
			return telegramFailure(out, "public_url_denied", false)
		}
		out.FinalURL, out.AccessMethod = canonicalTelegramURL(final), "http"
		articles, parseErr := extractor.ParseTelegramChannel(string(doc.HTML), final, sourceName, limit, now())
		if parseErr == nil {
			out.Articles, out.Outcome = articles, "success"
			return enforceTelegramResponseBudget(out)
		}
		if len(articles) > 0 {
			out.Articles, out.Outcome = articles, "partial"
			out.Error, out.Retryable = telegramParseError(parseErr), false
			return enforceTelegramResponseBudget(out)
		}
		if !errors.Is(parseErr, extractor.ErrTelegramEmpty) {
			return telegramFailure(out, telegramParseError(parseErr), false)
		}
		err = extractor.ErrTelegramEmpty
	}
	if ctx.Err() != nil {
		return telegramFailure(out, contextErrorCode(ctx.Err()), errors.Is(ctx.Err(), context.DeadlineExceeded))
	}
	if !telegramFallbackAllowed(err) || s.Render == nil {
		code, retryable := telegramFetchError(err)
		return telegramFailure(out, code, retryable)
	}
	admit := s.Admit
	if admit == nil {
		admit = transport.AdmitPublicURL
	}
	if err = admit(ctx, channelURL); err != nil {
		code, retryable := telegramFetchError(err)
		return telegramFailure(out, code, retryable)
	}
	page, renderErr := s.Render(ctx, channelURL, transport.BrowserOptions{Timeout: 30 * time.Second, MaxWireBytes: transport.MaxPageBytes, MaxDOMBytes: transport.MaxPageBytes}, TelegramNativeReadySelector)
	if renderErr != nil {
		if ctx.Err() != nil && !errors.Is(renderErr, transport.ErrNativeAdmissionDeadline) {
			renderErr = ctx.Err()
		}
		code, retryable := telegramNativeError(renderErr)
		return telegramFailure(out, code, retryable)
	}
	final := page.URL
	if final == "" {
		final = channelURL
	}
	if len(page.HTML) > transport.MaxPageBytes {
		return telegramFailure(out, "resource_limit", false)
	}
	if transport.IsChallengePage(page.HTML) {
		return telegramFailure(out, "upstream_challenge", false)
	}
	if !sameTelegramChannel(channelURL, final) {
		return telegramFailure(out, "public_url_denied", false)
	}
	out.FinalURL, out.AccessMethod = canonicalTelegramURL(final), "native_browser"
	articles, parseErr := extractor.ParseTelegramChannel(string(page.HTML), final, sourceName, limit, now())
	if parseErr != nil {
		if len(articles) > 0 {
			out.Articles, out.Outcome = articles, "partial"
			out.Error = telegramParseError(parseErr)
			return enforceTelegramResponseBudget(out)
		}
		return telegramFailure(out, telegramParseError(parseErr), false)
	}
	out.Articles, out.Outcome = articles, "success"
	return enforceTelegramResponseBudget(out)
}

func sameTelegramChannel(original, final string) bool {
	originalURL, finalURL := canonicalTelegramURL(original), canonicalTelegramURL(final)
	return originalURL != "" && originalURL == finalURL
}

func canonicalTelegramURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "https" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.ForceQuery || strings.ContainsAny(raw, "?#") || (u.Port() != "" && u.Port() != "443") || u.EscapedPath() != u.Path {
		return ""
	}
	host := strings.ToLower(u.Hostname())
	if host != "t.me" && host != "telegram.me" {
		return ""
	}
	if !strings.HasPrefix(u.Path, "/") || strings.HasSuffix(u.Path, "/") {
		return ""
	}
	parts := strings.Split(strings.TrimPrefix(u.Path, "/"), "/")
	username := ""
	if len(parts) == 2 && parts[0] == "s" {
		username = parts[1]
	} else if len(parts) == 1 {
		username = parts[0]
	}
	if username == "" || !telegramChannelUsername.MatchString(username) {
		return ""
	}
	return "https://t.me/s/" + strings.ToLower(username)
}

var telegramChannelUsername = regexp.MustCompile(`^[A-Za-z0-9_]{1,64}$`)

func telegramFallbackAllowed(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, transport.ErrUnsafePublicURL) || errors.Is(err, transport.ErrPublicAdmissionUnavailable) ||
		errors.Is(err, extractor.ErrWireLimit) || errors.Is(err, extractor.ErrOutputLimit) ||
		errors.Is(err, extractor.ErrTelegramByteLimit) || errors.Is(err, extractor.ErrTelegramTextLimit) {
		return false
	}
	var challenge *transport.PublicChallengeError
	if errors.As(err, &challenge) {
		return false
	}
	var status *transport.PublicHTTPStatusError
	if errors.As(err, &status) {
		return status.StatusCode >= 500 && status.StatusCode != 429
	}
	return true
}

func telegramFetchError(err error) (string, bool) {
	if err == nil {
		return "no_posts", false
	}
	if errors.Is(err, extractor.ErrTelegramEmpty) {
		return "no_posts", false
	}
	if errors.Is(err, context.Canceled) {
		return "cancelled", false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded", true
	}
	if errors.Is(err, transport.ErrUnsafePublicURL) {
		return "public_url_denied", false
	}
	if errors.Is(err, transport.ErrPublicAdmissionUnavailable) {
		return "admission_unavailable", true
	}
	if errors.Is(err, extractor.ErrWireLimit) || errors.Is(err, extractor.ErrOutputLimit) {
		return "resource_limit", false
	}
	var challenge *transport.PublicChallengeError
	if errors.As(err, &challenge) {
		return "upstream_challenge", false
	}
	var status *transport.PublicHTTPStatusError
	if errors.As(err, &status) {
		if status.StatusCode == 429 {
			return "upstream_rate_limited", true
		}
		if status.StatusCode == 401 || status.StatusCode == 407 {
			return "upstream_auth_required", false
		}
		if status.StatusCode == 403 {
			return "upstream_forbidden", false
		}
		if status.StatusCode >= 500 {
			return "upstream_unavailable", true
		}
		return "upstream_http_rejected", false
	}
	return "transport_failed", true
}

func telegramNativeError(err error) (string, bool) {
	switch {
	case errors.Is(err, transport.ErrBrowserEventOverflow):
		return "event_overflow", true
	case errors.Is(err, context.Canceled):
		return "cancelled", false
	case errors.Is(err, context.DeadlineExceeded):
		return "native_browser_timeout", true
	case errors.Is(err, transport.ErrNativePolicy):
		return "native_browser_policy_denied", false
	case errors.Is(err, transport.ErrNativeBusy):
		return "native_browser_busy", true
	case errors.Is(err, transport.ErrNativeOwnershipLost):
		return "native_browser_unavailable", false
	case errors.Is(err, transport.ErrUnsafePublicURL):
		return "public_url_denied", false
	case errors.Is(err, extractor.ErrWireLimit), errors.Is(err, extractor.ErrOutputLimit):
		return "resource_limit", false
	}
	var status *transport.PublicHTTPStatusError
	if errors.As(err, &status) {
		if status.StatusCode == 429 {
			return "upstream_rate_limited", true
		}
		if status.StatusCode == 401 || status.StatusCode == 407 {
			return "upstream_auth_required", false
		}
		return "native_browser_http_rejected", status.StatusCode >= 500
	}
	return "native_browser_unavailable", true
}

func telegramParseError(err error) string {
	switch {
	case errors.Is(err, extractor.ErrTelegramByteLimit), errors.Is(err, extractor.ErrTelegramTextLimit):
		return "resource_limit"
	case errors.Is(err, extractor.ErrTelegramMalformed):
		return "invalid_page"
	case errors.Is(err, extractor.ErrTelegramEmpty):
		return "no_posts"
	default:
		return "parse_failed"
	}
}

func contextErrorCode(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline_exceeded"
	}
	return "cancelled"
}

func telegramFailure(out TelegramOutcome, code string, retryable bool) TelegramOutcome {
	out.Outcome, out.Error, out.Retryable = "failed", code, retryable
	return out
}

func enforceTelegramResponseBudget(out TelegramOutcome) TelegramOutcome {
	kept := make([]extractor.TelegramArticle, 0, len(out.Articles))
	used := 2 // JSON array brackets.
	for _, article := range out.Articles {
		raw := article.RawData
		external, _ := raw["external_links"].([]string)
		hashtags, _ := raw["hashtags"].([]string)
		if len(article.MediaFiles) > telegramArticleArrayLimit || len(external) > telegramArticleArrayLimit || len(hashtags) > telegramArticleArrayLimit {
			return telegramLimitPrefix(out, kept, "resource_limit")
		}
		remaining := telegramResponseArticleBudget - used - 1
		estimated, ok := jsonSizeUpperBound(reflect.ValueOf(article), remaining, 0)
		if !ok || estimated > remaining {
			return telegramLimitPrefix(out, kept, "response_too_large")
		}
		// The estimator rejects amplification before encoding. Marshal only a
		// preflight-bounded article to verify the actual JSON response size.
		encoded, err := json.Marshal(article)
		if err != nil || len(encoded)+used+1 > telegramResponseArticleBudget {
			return telegramLimitPrefix(out, kept, "response_too_large")
		}
		used += len(encoded) + 1
		kept = append(kept, article)
	}
	out.Articles = kept
	return out
}

func telegramLimitPrefix(out TelegramOutcome, prefix []extractor.TelegramArticle, code string) TelegramOutcome {
	out.Articles = prefix
	out.Error, out.Retryable = code, false
	if len(prefix) == 0 {
		out.Outcome = "failed"
	} else {
		out.Outcome = "partial"
	}
	return out
}

// jsonSizeUpperBound computes a bounded preflight size before JSON marshaling.
// It rejects deep or unsupported values and stops at the remaining budget.
func jsonSizeUpperBound(value reflect.Value, cap, depth int) (int, bool) {
	if cap < 0 || depth > 32 {
		return 0, false
	}
	if !value.IsValid() {
		return 4, cap >= 4
	}
	for value.Kind() == reflect.Interface || value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return 4, cap >= 4
		}
		value = value.Elem()
		depth++
		if depth > 32 {
			return 0, false
		}
	}
	if value.Type() == reflect.TypeOf(time.Time{}) {
		encoded, err := value.Interface().(time.Time).MarshalJSON()
		return len(encoded), err == nil && len(encoded) <= cap
	}
	switch value.Kind() {
	case reflect.String:
		n := jsonStringSize(value.String(), cap)
		return n, n <= cap
	case reflect.Bool:
		n := 5
		if value.Bool() {
			n = 4
		}
		return n, n <= cap
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n := len(strconv.FormatInt(value.Int(), 10))
		return n, n <= cap
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n := len(strconv.FormatUint(value.Uint(), 10))
		return n, n <= cap
	case reflect.Float32, reflect.Float64:
		n := len(strconv.FormatFloat(value.Float(), 'g', -1, value.Type().Bits()))
		return n, n <= cap
	case reflect.Slice, reflect.Array:
		if value.Kind() == reflect.Slice && value.IsNil() {
			return 4, cap >= 4
		}
		used := 2
		for i := 0; i < value.Len(); i++ {
			if i > 0 {
				used++
			}
			item, ok := jsonSizeUpperBound(value.Index(i), cap-used, depth+1)
			if !ok {
				return 0, false
			}
			used += item
			if used > cap {
				return 0, false
			}
		}
		return used, true
	case reflect.Map:
		if value.IsNil() {
			return 4, cap >= 4
		}
		if value.Len() > telegramArticleArrayLimit {
			return 0, false
		}
		used, count := 2, 0
		iter := value.MapRange()
		for iter.Next() {
			if iter.Key().Kind() != reflect.String {
				return 0, false
			}
			if count > 0 {
				used++
			}
			keySize := jsonStringSize(iter.Key().String(), cap-used)
			if keySize > cap-used {
				return 0, false
			}
			used += keySize + 1 // colon
			item, ok := jsonSizeUpperBound(iter.Value(), cap-used, depth+1)
			if !ok {
				return 0, false
			}
			used += item
			if used > cap {
				return 0, false
			}
			count++
		}
		return used, true
	case reflect.Struct:
		used, count := 2, 0
		typ := value.Type()
		for i := 0; i < value.NumField(); i++ {
			field := typ.Field(i)
			if !field.IsExported() {
				continue
			}
			tag := strings.Split(field.Tag.Get("json"), ",")
			name := tag[0]
			if name == "-" {
				continue
			}
			if name == "" {
				name = field.Name
			}
			if len(tag) > 1 && strings.Contains(strings.Join(tag[1:], ","), "omitempty") && value.Field(i).IsZero() {
				continue
			}
			if count > 0 {
				used++
			}
			keySize := jsonStringSize(name, cap-used)
			if keySize > cap-used {
				return 0, false
			}
			used += keySize + 1
			item, ok := jsonSizeUpperBound(value.Field(i), cap-used, depth+1)
			if !ok {
				return 0, false
			}
			used += item
			if used > cap {
				return 0, false
			}
			count++
		}
		return used, true
	default:
		return 0, false
	}
}

func jsonStringSize(value string, cap int) int {
	used := 2 // quote characters
	for len(value) > 0 {
		r, size := utf8.DecodeRuneInString(value)
		if r == utf8.RuneError && size == 1 {
			used += 6 // encoding/json replaces malformed bytes with \ufffd
		} else {
			switch {
			case r == '"' || r == '\\' || r == '\b' || r == '\f' || r == '\n' || r == '\r' || r == '\t':
				used += 2
			case r < 0x20 || r == '<' || r == '>' || r == '&' || r == '\u2028' || r == '\u2029':
				used += 6
			default:
				used += size
			}
		}
		if used > cap {
			return used
		}
		value = value[size:]
	}
	return used
}
