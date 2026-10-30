package analysis

import (
	"math"
	"strconv"
	"strings"
	"unicode"
)

func validateAnalysisResult(raw map[string]any, r Request) map[string]any {
	categories := stringSlice(raw["categories"])
	if len(categories) == 0 {
		if c, ok := raw["category"].(string); ok && c != "" {
			categories = []string{c}
		}
	}
	var primary any
	if len(categories) > 0 {
		primary = categories[0]
	}
	content := r.Content
	if len([]rune(content)) > 500 {
		content = string([]rune(content)[:500])
	}
	valid := make([]string, 0, len(r.Categories))
	for _, c := range r.Categories {
		valid = append(valid, c.Name)
	}
	var isAd any
	if b, ok := raw["is_advertisement"].(bool); ok {
		isAd = b
	}
	result := map[string]any{
		"optimized_title":      nullableTrim(raw["optimized_title"]),
		"summary":              nullableTrim(raw["summary"]),
		"category":             primary,
		"categories":           categories,
		"category_confidences": arrayValue(raw["category_confidences"]),
		"category_confidence":  clamp(number(raw["category_confidence"], 0.8)),
		"summary_confidence":   clamp(number(raw["summary_confidence"], 0.8)),
		"categories_parsed":    parseCategory(primary, valid, r.Title, content),
		"original_categories":  arrayValue(raw["original_categories"]),
		"is_advertisement":     isAd,
		"ad_type":              defaultValue(raw["ad_type"], "news_article"),
		"ad_confidence":        clamp(number(raw["ad_confidence"], 0)),
		"ad_reasoning":         stringDefaultTrim(raw["ad_reasoning"], "Combined analysis"),
		"publication_date":     raw["publication_date"],
		"content_quality":      clamp(number(raw["content_quality"], 0.7)),
		"confidence":           clamp(number(raw["confidence"], 0.8)),
	}
	if r.Version == 2 {
		localizations := map[string]any{}
		rawLocales, _ := raw["localizations"].(map[string]any)
		for _, locale := range r.Locales {
			item, _ := rawLocales[locale].(map[string]any)
			localizations[locale] = map[string]any{"title": nullableTrim(item["title"]), "summary": nullableTrim(item["summary"])}
		}
		result["localizations"] = localizations
		result["analysis_version"] = 2
		if ru, ok := localizations["ru"].(map[string]any); ok {
			result["optimized_title"], result["summary"] = ru["title"], ru["summary"]
		}
	}
	return result
}

func nullableTrim(v any) any {
	s, _ := v.(string)
	if strings.TrimSpace(s) == "" {
		return nil
	}
	return strings.TrimSpace(s)
}
func stringDefaultTrim(v any, fallback string) string {
	s, _ := v.(string)
	if strings.TrimSpace(s) == "" {
		return fallback
	}
	return strings.TrimSpace(s)
}
func defaultValue(v any, fallback string) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fallback
}
func arrayValue(v any) []any {
	if a, ok := v.([]any); ok && a != nil {
		return a
	}
	return []any{}
}
func stringSlice(v any) []string {
	a, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]string, 0, len(a))
	for _, x := range a {
		if s, ok := x.(string); ok {
			out = append(out, s)
		}
	}
	return out
}
func number(v any, fallback float64) float64 {
	var f float64
	switch n := v.(type) {
	case float64:
		f = n
	case int:
		f = float64(n)
	case int64:
		f = float64(n)
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
		if err != nil {
			return fallback
		}
		f = parsed
	case jsonNumber:
		var err error
		f, err = n.Float64()
		if err != nil {
			return fallback
		}
	default:
		return fallback
	}
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return fallback
	}
	return f
}
func clamp(v float64) float64 { return math.Max(0, math.Min(1, v)) }

// jsonNumber is implemented by encoding/json.Number without coupling validation to the wire decoder.
type jsonNumber interface{ Float64() (float64, error) }

func parseCategory(raw any, valid []string, title, content string) any {
	s, ok := raw.(string)
	if !ok || s == "" {
		// The legacy parser returns this scalar before considering return_multiple.
		return "Other"
	}
	s = strings.TrimSpace(strings.ReplaceAll(s, "(choose ONE category only)", ""))
	s = strings.ReplaceAll(s, "OR", "|")
	parts := []string{s}
	separated := false
	for _, sep := range []string{"|", "/", ",", " and ", " & "} {
		if strings.Contains(s, sep) {
			separated = true
			parts = nil
			for _, p := range strings.Split(s, sep) {
				clean := lettersOnly(strings.TrimSpace(p))
				if has(valid, clean) {
					parts = append(parts, clean)
				}
			}
			break
		}
	}
	if !separated {
		clean := lettersOnly(s)
		if has(valid, clean) {
			parts = []string{clean}
		} else {
			parts = nil
		}
	}
	if len(parts) == 0 {
		return []any{map[string]any{"name": "Other", "confidence": 0.5}}
	}
	if len(parts) == 1 {
		return []any{map[string]any{"name": parts[0], "confidence": 1.0}}
	}
	text := strings.ToLower(title + " " + content)
	scores := make([]int, len(parts))
	total := 0
	for i, cat := range parts {
		for _, kw := range keywords[cat] {
			if strings.Contains(text, kw) {
				scores[i] += len(strings.Fields(kw))
			}
		}
		total += scores[i]
	}
	type scored struct {
		name  string
		score float64
		order int
	}
	items := make([]scored, len(parts))
	for i, cat := range parts {
		conf := 0.7 - float64(i)*0.1
		if total > 0 {
			conf = 0.3
			if scores[i] > 0 {
				conf = 0.6 + float64(scores[i])/float64(total)*0.4
			}
		}
		items[i] = scored{cat, math.Round(conf*100) / 100, i}
	}
	// Stable sort preserves AI ordering for equal confidence scores.
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if items[j].score > items[i].score {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, map[string]any{"name": item.name, "confidence": item.score})
	}
	return out
}

func lettersOnly(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}
func has(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

var keywords = map[string][]string{
	"Serbia":   {"serbia", "serbian", "belgrade", "beograd", "srbija", "srbije", "srbiju", "сербия", "сербии", "сербию", "сербский", "сербского", "сербской", "сербским", "белград", "београд", "београду", "београда", "vučić", "vucic", "вучич", "вучића", "president", "government", "parliament", "ministry", "президент", "правительство", "парламент", "министерство", "novi sad", "niš", "kragujevac", "subotica", "zemun", "vojvodina", "нови сад", "ниш", "крагуевац", "суботица", "земун", "воеводина", "kosovo", "metohija", "косово", "метохия", "метохию", "косова", "republika srpska", "bosnia", "республика srpska", "босния", "посольство", "посольство рф", "суверенитет", "территориальная целостность", "бомбардировка", "бомбардировке", "нато", "западные страны", "сербский народ"},
	"Tech":     {"technology", "tech", "software", "hardware", "computer", "digital", "internet", "web", "app", "application", "programming", "code", "artificial intelligence", "ai", "machine learning", "blockchain", "cryptocurrency", "bitcoin", "startup", "innovation", "gadget", "smartphone", "tablet", "laptop", "cloud", "data", "algorithm"},
	"Business": {"business", "company", "corporation", "enterprise", "market", "economy", "financial", "finance", "investment", "profit", "revenue", "sales", "trade", "commerce", "industry", "manufacturing", "retail", "banking", "stock", "shares", "merger", "acquisition", "ceo", "management", "strategy", "growth", "expansion"},
	"Science":  {"science", "research", "study", "experiment", "discovery", "scientist", "university", "academic", "journal", "publication", "theory", "medicine", "medical", "health", "biology", "chemistry", "physics", "mathematics", "engineering", "laboratory", "clinical", "trial"},
}
