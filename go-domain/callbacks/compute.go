package callbacks

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
)

var methods = map[string]bool{"source_content": true, "enhanced_selectors": true, "readability": true, "browser_rendering": true, "browser_heuristics": true, "fallback_extraction": true, "json_ld": true, "open_graph": true, "meta_description": true, "enhanced_heuristics": true, "reddit_json": true, "reddit_linked_article": true, "learned_pattern_browser_rendering": true, "learned_pattern_browser_heuristics": true}
var errorCodeRE = regexp.MustCompile(`^[a-z0-9_-]{1,40}$`)
var invalidInput = errors.New("invalid callback input")
var ErrNotImplemented = errors.New("callback operation is not implemented")

type Observation struct {
	Selector string `json:"selector"`
	Method   string `json:"method"`
	Success  bool   `json:"success"`
}
type Feedback struct {
	URL              string        `json:"url"`
	Method           string        `json:"method"`
	Success          bool          `json:"success"`
	Observations     []Observation `json:"observations"`
	SelectorUsed     *string       `json:"selector_used"`
	ContentLength    *int          `json:"content_length"`
	ExtractionTimeMS *int          `json:"extraction_time_ms"`
	QualityScore     *float64      `json:"quality_score"`
	ErrorCode        *string       `json:"error_code"`
}
type Pattern struct {
	Selector *string `json:"selector"`
	Method   *string `json:"method"`
}

func SanitizePattern(p Pattern) Pattern {
	if p.Selector != nil && len([]byte(*p.Selector)) > 2048 {
		p.Selector = nil
	}
	if p.Method != nil && !methods[*p.Method] {
		p.Method = nil
	}
	return p
}

// Learning owns the bounded learned-pattern cache and fenced feedback writes.
type Learning interface {
	Context(context.Context, Request, authority.Fence) (Pattern, error)
	Feedback(context.Context, Request, authority.Fence, Feedback) (bool, error)
}
type ArticleCompute struct {
	Runtime      *airuntime.Server
	Learning     Learning
	MaxTextChars int
}

func object(raw json.RawMessage, required, optional []string) (map[string]json.RawMessage, error) {
	var v map[string]json.RawMessage
	if json.Unmarshal(raw, &v) != nil || v == nil {
		return nil, invalidInput
	}
	allowed := map[string]bool{}
	for _, k := range required {
		allowed[k] = true
		if _, ok := v[k]; !ok {
			return nil, invalidInput
		}
	}
	for _, k := range optional {
		allowed[k] = true
	}
	for k := range v {
		if !allowed[k] {
			return nil, invalidInput
		}
	}
	return v, nil
}
func stringField(raw json.RawMessage) (string, bool) {
	var s string
	if bytes.Equal(raw, []byte("null")) || json.Unmarshal(raw, &s) != nil {
		return "", false
	}
	return s, true
}
func validURL(s string) bool {
	if len([]byte(s)) > 8192 {
		return false
	}
	_, ok := URLHostname(s)
	return ok
}

// URLHostname mirrors the callback's urlsplit-based scheme/hostname check;
// malformed path escapes and nonnumeric ports do not affect hostname there.
func URLHostname(raw string) (string, bool) {
	s := strings.TrimLeftFunc(raw, func(r rune) bool { return r <= 0x20 })
	s = strings.NewReplacer("\r", "", "\n", "", "\t", "").Replace(s)
	i := strings.Index(s, "://")
	if i < 0 {
		return "", false
	}
	scheme := strings.ToLower(s[:i])
	if scheme != "http" && scheme != "https" {
		return "", false
	}
	authority := s[i+3:]
	if end := strings.IndexAny(authority, "/?#"); end >= 0 {
		authority = authority[:end]
	}
	if at := strings.LastIndexByte(authority, '@'); at >= 0 {
		authority = authority[at+1:]
	}
	if strings.HasPrefix(authority, "[") {
		end := strings.IndexByte(authority, ']')
		if end < 0 {
			return "", false
		}
		host := authority[1:end]
		if net.ParseIP(strings.SplitN(host, "%", 2)[0]) == nil {
			return "", false
		}
		return strings.ToLower(host), true
	}
	if strings.ContainsAny(authority, "[]") {
		return "", false
	}
	if colon := strings.IndexByte(authority, ':'); colon >= 0 {
		authority = authority[:colon]
	}
	if authority == "" {
		return "", false
	}
	return strings.ToLower(authority), true
}
func (c *ArticleCompute) Validate(op string, raw json.RawMessage) error {
	switch op {
	case "analyze":
		m, e := object(raw, []string{"title", "content", "url"}, []string{"original_context", "locales"})
		if e != nil {
			return e
		}
		if rawLocales, ok := m["locales"]; ok {
			var locales []string
			if json.Unmarshal(rawLocales, &locales) != nil || !(len(locales) == 1 && locales[0] == "sr-Latn") && !(len(locales) == 2 && ((locales[0] == "ru" && locales[1] == "sr-Latn") || (locales[1] == "ru" && locales[0] == "sr-Latn"))) {
				return invalidInput
			}
		}
		max := c.MaxTextChars
		if max <= 0 {
			max = 100000
		}
		values := make(map[string]string, 4)
		for _, key := range []string{"title", "content", "url"} {
			s, ok := stringField(m[key])
			if !ok {
				return invalidInput
			}
			values[key] = s
		}
		if v, ok := m["original_context"]; ok && !bytes.Equal(v, []byte("null")) {
			s, ok := stringField(v)
			if !ok {
				return invalidInput
			}
			values["original_context"] = s
		}
		for _, key := range []string{"title", "content", "original_context"} {
			if utf8.RuneCountInString(values[key]) > max {
				return ErrTextLimit
			}
		}
		if !validURL(values["url"]) {
			return invalidInput
		}
	case "extraction_context":
		m, e := object(raw, []string{"url"}, nil)
		if e != nil {
			return e
		}
		s, ok := stringField(m["url"])
		if !ok || !validURL(s) {
			return invalidInput
		}
	case "extraction_feedback":
		m, e := object(raw, []string{"url", "method", "success", "observations"}, []string{"selector_used", "content_length", "extraction_time_ms", "quality_score", "error_code"})
		if e != nil {
			return e
		}
		s, ok := stringField(m["url"])
		if !ok || !validURL(s) {
			return invalidInput
		}
		method, ok := stringField(m["method"])
		if !ok || !methods[method] {
			return invalidInput
		}
		var flag bool
		if bytes.Equal(m["success"], []byte("null")) || json.Unmarshal(m["success"], &flag) != nil {
			return invalidInput
		}
		var entries []json.RawMessage
		if bytes.Equal(m["observations"], []byte("null")) || json.Unmarshal(m["observations"], &entries) != nil || len(entries) > 64 {
			return invalidInput
		}
		for _, rawEntry := range entries {
			entry, e := object(rawEntry, []string{"selector", "method", "success"}, nil)
			if e != nil {
				return e
			}
			selector, ok := stringField(entry["selector"])
			if !ok || len([]byte(selector)) > 2048 {
				return invalidInput
			}
			method, ok := stringField(entry["method"])
			if !ok || !methods[method] {
				return invalidInput
			}
			if bytes.Equal(entry["success"], []byte("null")) || json.Unmarshal(entry["success"], &flag) != nil {
				return invalidInput
			}
		}
		if v, ok := m["selector_used"]; ok && !bytes.Equal(v, []byte("null")) {
			s, ok := stringField(v)
			if !ok || len([]byte(s)) > 2048 {
				return invalidInput
			}
		}
		for _, k := range []string{"content_length", "extraction_time_ms"} {
			if v, ok := m[k]; ok && !bytes.Equal(v, []byte("null")) {
				var i int
				if json.Unmarshal(v, &i) != nil || i < 0 || i > 10000000 {
					return invalidInput
				}
			}
		}
		if v, ok := m["quality_score"]; ok && !bytes.Equal(v, []byte("null")) {
			var n float64
			if json.Unmarshal(v, &n) != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > 1 {
				return invalidInput
			}
		}
		if v, ok := m["error_code"]; ok && !bytes.Equal(v, []byte("null")) {
			s, ok := stringField(v)
			if !ok || !errorCodeRE.MatchString(s) {
				return invalidInput
			}
		}
	default:
		return invalidInput
	}
	return nil
}
func (c *ArticleCompute) Run(ctx context.Context, r Request, f authority.Fence) (any, error) {
	switch r.Operation {
	case "analyze":
		if c.Runtime == nil {
			return nil, errors.New("analysis runtime unavailable")
		}
		var v struct {
			Title    string   `json:"title"`
			Content  string   `json:"content"`
			URL      string   `json:"url"`
			Original *string  `json:"original_context"`
			Locales  []string `json:"locales"`
		}
		if json.Unmarshal(r.Input, &v) != nil {
			return nil, invalidInput
		}
		return c.Runtime.AnalyzeCallback(ctx, airuntime.ArticleInput{Title: v.Title, Content: v.Content, URL: v.URL, OriginalContext: v.Original, Locales: v.Locales, MaxTextChars: c.MaxTextChars})
	case "extraction_context":
		if c.Learning == nil {
			return nil, ErrNotImplemented
		}
		p, e := c.Learning.Context(ctx, r, f)
		return SanitizePattern(p), e
	case "extraction_feedback":
		if c.Learning == nil {
			return nil, ErrNotImplemented
		}
		var v Feedback
		if json.Unmarshal(r.Input, &v) != nil {
			return nil, invalidInput
		}
		ok, e := c.Learning.Feedback(ctx, r, f, v)
		return map[string]any{"recorded": ok}, e
	}
	return nil, invalidInput
}
