package processing

import (
	"math"
	"net/url"
	"strings"
	"unicode/utf8"
)

type Category struct {
	Name       string  `json:"name"`
	Confidence float64 `json:"confidence"`
	AICategory string  `json:"ai_category"`
}

func NormalizeCategories(analysis map[string]any) []Category {
	raw, ok := analysis["categories"].([]any)
	if !ok {
		return nil
	}
	confidenceList, _ := analysis["category_confidences"].([]any)
	if confidenceList == nil {
		if single, exists := analysis["category_confidences"]; exists {
			confidenceList = []any{single}
		}
	}
	originals, _ := analysis["original_categories"].([]any)
	defaultConfidence := finiteConfidence(analysis["category_confidence"], .8)
	out := []Category{}
	for i, v := range raw {
		name := ""
		c := any(nil)
		original := ""
		if m, ok := v.(map[string]any); ok {
			name, _ = m["name"].(string)
			c = m["confidence"]
			original, _ = m["ai_category"].(string)
		} else {
			name, _ = v.(string)
		}
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		aligned := any(nil)
		if i < len(confidenceList) {
			aligned = confidenceList[i]
		}
		confidence := finiteConfidence(c, finiteConfidence(aligned, defaultConfidence))
		if strings.TrimSpace(original) == "" && i < len(originals) {
			original, _ = originals[i].(string)
		}
		if strings.TrimSpace(original) == "" {
			original = name
		}
		out = append(out, Category{Name: name, Confidence: confidence, AICategory: strings.TrimSpace(original)})
	}
	return out
}
func finiteConfidence(v any, fallback float64) float64 {
	var n float64
	switch x := v.(type) {
	case float64:
		n = x
	case float32:
		n = float64(x)
	case int:
		n = float64(x)
	case int64:
		n = float64(x)
	default:
		return fallback
	}
	if math.IsNaN(n) || math.IsInf(n, 0) {
		return fallback
	}
	return math.Max(0, math.Min(1, n))
}
func MapCategory(ai string) string {
	ai = strings.ToLower(strings.TrimSpace(ai))
	if ai == "" {
		return "Other"
	}
	for _, kv := range defaultCategoryMapping {
		if ai == kv.token {
			return kv.category
		}
	}
	for _, kv := range defaultCategoryMapping {
		if strings.Contains(ai, kv.token) || strings.Contains(kv.token, ai) {
			return kv.category
		}
	}
	return "Other"
}
func ExtractableURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, d := range []string{"t.me", "telegram.me", "twitter.com", "x.com", "instagram.com", "maps.app.goo.gl", "maps.google.com", "maps.google.ru", "2gis.ru", "2gis.com", "waze.com", "play.google.com", "apps.apple.com"} {
		if host == d || strings.HasSuffix(host, "."+d) {
			return false
		}
	}
	p := strings.ToLower(u.Path)
	if strings.Contains(p, "/maps") && (host == "google.com" || host == "google.ru" || strings.HasSuffix(host, ".google.com") || strings.HasSuffix(host, ".google.ru") || strings.Contains(host, "yandex")) {
		return false
	}
	return true
}
func EnforceTextLimit(s string, maxChars int) error {
	if maxChars < 1 {
		maxChars = 100000
	}
	if utf8.RuneCountInString(s) > maxChars {
		return ErrResourceLimit
	}
	return nil
}

// AnalyzeOutcome ports the successful-stage and terminal-advertisement policy from AIProcessor.
func AnalyzeOutcome(analysis map[string]any, summaryDone, categoryDone, adDone bool) Outcome {
	fields := map[string]any{}
	if e, ok := analysis["_processing_error"].(string); ok && e != "" {
		state := StateRetryWait
		if terminal, _ := analysis["_processing_terminal"].(bool); terminal {
			state = StateRejected
		}
		return Outcome{State: state, Fields: fields, Error: e, RetryAfterSeconds: analysis["_retry_after_seconds"]}
	}
	if analysis["analysis_version"] == float64(2) || analysis["analysis_version"] == 2 {
		if len(NormalizeCategories(analysis)) == 0 {
			return Outcome{State: StateRetryWait, Fields: fields, Error: "missing_categories"}
		}
		if _, ok := analysis["is_advertisement"].(bool); !ok {
			return Outcome{State: StateRetryWait, Fields: fields, Error: "missing_ad_classification"}
		}
		localizations, _ := analysis["localizations"].(map[string]any)
		validated := map[string]any{}
		for _, locale := range []string{"ru", "sr-Latn"} {
			raw, _ := localizations[locale].(map[string]any)
			title, _ := raw["title"].(string)
			summary, _ := raw["summary"].(string)
			title, summary = strings.TrimSpace(title), strings.TrimSpace(summary)
			valid := title != "" && utf8.RuneCountInString(title) <= 200 && summary != "" && !IsChallengeText(title) && !IsChallengeText(summary)
			if locale == "sr-Latn" {
				valid = valid && serbianLatinProse(title+"\n"+summary)
			}
			if locale != "ru" || !summaryDone {
				validated[locale] = map[string]any{"title": title, "summary": summary, "valid": valid}
			}
			if locale == "ru" {
				if valid {
					analysis["optimized_title"], analysis["summary"] = title, summary
					if !summaryDone {
						fields["title"], fields["summary"], fields["summary_processed"] = title, summary, true
					}
				} else {
					analysis["optimized_title"], analysis["summary"] = nil, nil
				}
			}
		}
		fields["localizations"] = validated
		fields["localization_prompt_version"] = analysis["localization_prompt_version"]
		fields["localization_model"] = analysis["localization_model"]
	}
	summary, _ := analysis["summary"].(string)
	summary = strings.TrimSpace(summary)
	if title, ok := analysis["optimized_title"].(string); ok && (!summaryDone || analysis["analysis_version"] == nil) {
		title = strings.TrimSpace(title)
		if title != "" && utf8.RuneCountInString(title) <= 200 && !IsChallengeText(title) {
			fields["title"] = title
		}
	}
	if analysis["is_advertisement"] == true {
		fields["ad_processed"] = true
		fields["is_advertisement"] = true
		fields["ad_confidence"] = finiteConfidence(analysis["ad_confidence"], 0)
		fields["ad_type"] = analysis["ad_type"]
		fields["ad_reasoning"] = analysis["ad_reasoning"]
		if !summaryDone && summary != "" && !IsChallengeText(summary) {
			fields["summary"] = summary
			fields["summary_processed"] = true
		}
		if !categoryDone {
			if cats := NormalizeCategories(analysis); len(cats) > 0 {
				fields["categories"] = cats
				fields["category_processed"] = true
			}
		}
		return Outcome{State: StateRejected, Fields: fields, Error: "advertisement"}
	}
	if !summaryDone {
		if summary == "" {
			return Outcome{State: StateRetryWait, Fields: fields, Error: "missing_summary"}
		}
		if IsChallengeText(summary) {
			return Outcome{State: StateRetryWait, Fields: fields, Error: "summary_describes_challenge"}
		}
		fields["summary"] = summary
		fields["summary_processed"] = true
	}
	if !categoryDone {
		cats := NormalizeCategories(analysis)
		if len(cats) == 0 {
			return Outcome{State: StateRetryWait, Fields: fields, Error: "missing_categories"}
		}
		fields["categories"] = cats
		fields["category_processed"] = true
	}
	if !adDone {
		isAd, ok := analysis["is_advertisement"].(bool)
		if !ok {
			return Outcome{State: StateRetryWait, Fields: fields, Error: "missing_ad_classification"}
		}
		fields["ad_processed"] = true
		fields["is_advertisement"] = isAd
		fields["ad_confidence"] = finiteConfidence(analysis["ad_confidence"], 0)
		fields["ad_type"] = analysis["ad_type"]
		fields["ad_reasoning"] = analysis["ad_reasoning"]
	}
	return Outcome{State: StateCompleted, Fields: fields}
}
