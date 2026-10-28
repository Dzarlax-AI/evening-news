package selectors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Analysis struct {
	ContentSelectors []string           `json:"content_selectors"`
	TitleSelectors   []string           `json:"title_selectors"`
	DateSelectors    []string           `json:"date_selectors"`
	ConfidenceScores map[string]float64 `json:"confidence_scores"`
	PageType         string             `json:"page_type"`
	Reasoning        string             `json:"reasoning"`
	SuggestedConfig  map[string]any     `json:"suggested_config"`
}
type TextGenerator interface {
	GenerateText(context.Context, string, string) (string, error)
}
type cacheEntry struct {
	value   []byte
	created time.Time
	size    int
}
type Analyzer struct {
	Generator TextGenerator
	mu        sync.Mutex
	entries   map[string]cacheEntry
	size      int
	now       func() time.Time
}

func NewAnalyzer(g TextGenerator) *Analyzer {
	return &Analyzer{Generator: g, entries: map[string]cacheEntry{}, now: time.Now}
}
func cacheKey(url, context string) string {
	if utf8.RuneCountInString(url)+utf8.RuneCountInString(context) > 4096 || len(url)+len(context) > 4096 {
		return ""
	}
	h := sha256.New()
	_, _ = h.Write([]byte(url))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(context))
	return hex.EncodeToString(h.Sum(nil))
}
func (a *Analyzer) prune(now time.Time) {
	for k, e := range a.entries {
		if now.Sub(e.created) >= 24*time.Hour {
			a.size -= e.size
			delete(a.entries, k)
		}
	}
}
func (a *Analyzer) get(key string) (*Analysis, bool) {
	if key == "" {
		return nil, false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.prune(a.now())
	e, ok := a.entries[key]
	if !ok {
		return nil, false
	}
	var v Analysis
	if json.Unmarshal(e.value, &v) != nil {
		return nil, false
	}
	return &v, true
}
func (a *Analyzer) put(key string, v *Analysis) {
	if key == "" {
		return
	}
	wire, err := json.Marshal(v)
	if err != nil {
		return
	}
	size := len(key) + len(wire)
	if size > 8*1024*1024 {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := a.now()
	a.prune(now)
	if old, ok := a.entries[key]; ok {
		a.size -= old.size
		delete(a.entries, key)
	}
	for len(a.entries) >= 128 || a.size+size > 8*1024*1024 {
		keys := make([]string, 0, len(a.entries))
		for k := range a.entries {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool {
			x, y := a.entries[keys[i]], a.entries[keys[j]]
			if x.created.Equal(y.created) {
				return keys[i] < keys[j]
			}
			return x.created.Before(y.created)
		})
		if len(keys) == 0 {
			return
		}
		old := a.entries[keys[0]]
		a.size -= old.size
		delete(a.entries, keys[0])
	}
	a.entries[key] = cacheEntry{value: wire, created: now, size: size}
	a.size += size
}
func parseAnalysis(text string) (*Analysis, error) {
	start, end := strings.IndexByte(text, '{'), strings.LastIndexByte(text, '}')
	if start < 0 || end < start {
		return nil, errors.New("invalid selector analysis")
	}
	var v struct {
		Content   *[]string          `json:"content_selectors"`
		Title     *[]string          `json:"title_selectors"`
		Date      []string           `json:"date_selectors"`
		Scores    map[string]float64 `json:"confidence_scores"`
		PageType  *string            `json:"page_type"`
		Reasoning string             `json:"reasoning"`
		Config    map[string]any     `json:"suggested_config"`
	}
	if json.Unmarshal([]byte(text[start:end+1]), &v) != nil || v.Content == nil || v.Title == nil || v.PageType == nil {
		return nil, errors.New("invalid selector analysis")
	}
	content, title := *v.Content, *v.Title
	if len(content) > 10 {
		content = content[:10]
	}
	if len(title) > 5 {
		title = title[:5]
	}
	if len(v.Date) > 5 {
		v.Date = v.Date[:5]
	}
	if v.Date == nil {
		v.Date = []string{}
	}
	if v.Scores == nil {
		v.Scores = map[string]float64{}
	}
	if v.Config == nil {
		v.Config = map[string]any{}
	}
	return &Analysis{ContentSelectors: content, TitleSelectors: title, DateSelectors: v.Date, ConfidenceScores: v.Scores, PageType: *v.PageType, Reasoning: v.Reasoning, SuggestedConfig: v.Config}, nil
}
func (a *Analyzer) Analyze(ctx context.Context, url, htmlText, contextName, domain string) (*Analysis, error) {
	return a.analyze(ctx, url, htmlText, contextName, domain, true)
}
func (a *Analyzer) analyze(ctx context.Context, url, htmlText, contextName, domain string, publish bool) (*Analysis, error) {
	key := cacheKey(url, contextName)
	if cached, ok := a.get(key); ok {
		return cached, nil
	}
	if a.Generator == nil {
		return nil, errors.New("selector generator unavailable")
	}
	prepared, err := PrepareHTML(htmlText)
	if err != nil {
		return nil, err
	}
	text, err := a.Generator.GenerateText(ctx, StructurePrompt(url, prepared, contextName), domain)
	if err != nil {
		return nil, err
	}
	value, err := parseAnalysis(text)
	if err != nil {
		return nil, err
	}
	if publish {
		a.put(key, value)
	}
	return value, nil
}
