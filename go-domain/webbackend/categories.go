package webbackend

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
)

type categorySuggestion struct {
	Category   string  `json:"category"`
	Confidence float64 `json:"confidence"`
	Reasoning  string  `json:"reasoning"`
}
type categoryRow struct {
	Name     string
	Count    int
	Examples []string
}

func pythonRepr(s string) string {
	quote := '\''
	if strings.ContainsRune(s, '\'') && !strings.ContainsRune(s, '"') {
		quote = '"'
	}
	var b strings.Builder
	b.WriteRune(quote)
	for _, r := range s {
		switch {
		case r == quote || r == '\\':
			b.WriteRune('\\')
			b.WriteRune(r)
		case r == '\n':
			b.WriteString(`\n`)
		case r == '\r':
			b.WriteString(`\r`)
		case r == '\t':
			b.WriteString(`\t`)
		case !unicode.IsPrint(r):
			if r <= 255 {
				fmt.Fprintf(&b, `\x%02x`, r)
			} else if r <= 65535 {
				fmt.Fprintf(&b, `\u%04x`, r)
			} else {
				fmt.Fprintf(&b, `\U%08x`, r)
			}
		default:
			b.WriteRune(r)
		}
	}
	b.WriteRune(quote)
	return b.String()
}
func categoryPrompt(row categoryRow, categories []string) string {
	examples := row.Examples
	if len(examples) > 3 {
		examples = examples[:3]
	}
	q := make([]string, 0, len(examples))
	for _, e := range examples {
		q = append(q, pythonRepr(e))
	}
	names := make([]string, 0, len(categories))
	for _, name := range categories {
		names = append(names, "- "+name)
	}
	return fmt.Sprintf(`You are a news categorization expert. Analyze categories and provide accurate mappings. Always respond with valid JSON.

Analyze this AI-generated category and suggest the top 3 most appropriate mappings to our fixed categories.

AI CATEGORY: "%s"
USAGE COUNT: %d articles
EXAMPLE ARTICLES: [%s]

AVAILABLE FIXED CATEGORIES:
%s

TASK: Provide 3 ranked suggestions for mapping this AI category to fixed categories.

Response in JSON format:
{
    "suggestions": [
        {"category": "Most appropriate category name (exact match from list above)", "confidence": 0.85, "reasoning": "Brief explanation why this is the best match"},
        {"category": "Second best category name", "confidence": 0.65, "reasoning": "Brief explanation for second choice"},
        {"category": "Third alternative category name", "confidence": 0.45, "reasoning": "Brief explanation for third choice"}
    ]
}

Requirements:
- Always provide exactly 3 suggestions
- Categories must be exact matches from the available list
- Confidence scores should reflect semantic similarity
- Consider semantic similarity, example articles context, and categorization patterns
- Order suggestions by confidence (highest first)`, row.Name, row.Count, strings.Join(q, ", "), strings.Join(names, "\n"))
}
func suggestFixedCategory(name string) string {
	s := strings.ToLower(name)
	rules := []struct {
		name  string
		words []string
	}{{"Tech", []string{"tech", "software", "innovation", "digital", "ai", "computer", "programming"}}, {"Business", []string{"business", "economy", "finance", "market", "trade", "investment", "company"}}, {"Politics", []string{"politics", "government", "election", "policy", "law", "legal", "parliament"}}, {"International", []string{"international", "world", "global", "foreign", "europe", "russia", "china"}}, {"Serbia", []string{"serbia", "belgrade", "serbian", "вучич", "белград", "сербия"}}, {"Science", []string{"science", "research", "study", "health", "medicine", "environment", "education"}}}
	for _, r := range rules {
		for _, w := range r.words {
			if strings.Contains(s, w) {
				return r.name
			}
		}
	}
	return "Other"
}
func parseCategoryAnswer(answer string) ([]categorySuggestion, *categorySuggestion, error) {
	clean := strings.TrimSpace(answer)
	if !strings.HasPrefix(clean, "{") {
		i := strings.Index(clean, "{")
		if i < 0 {
			return nil, nil, fmt.Errorf("No JSON found in response")
		}
		clean = clean[i:]
	}
	var doc struct {
		Suggestions []map[string]any `json:"suggestions"`
	}
	if err := json.Unmarshal([]byte(clean), &doc); err != nil {
		return nil, nil, err
	}
	var out []categorySuggestion
	var primary *categorySuggestion
	for _, item := range doc.Suggestions {
		if len(out) == 3 {
			break
		}
		name := "Other"
		if v, ok := item["category"].(string); ok {
			name = v
		}
		name = strings.SplitN(name, " (", 2)[0]
		confidence := 0.0
		if v, ok := item["confidence"]; ok {
			switch n := v.(type) {
			case float64:
				confidence = n
			case string:
				var err error
				confidence, err = strconv.ParseFloat(n, 64)
				if err != nil {
					return nil, nil, err
				}
			default:
				return nil, nil, fmt.Errorf("invalid confidence")
			}
		}
		reason, _ := item["reasoning"].(string)
		if primary == nil {
			p := categorySuggestion{name, confidence, reason}
			if _, ok := item["confidence"]; !ok {
				p.Confidence = 0.5
			}
			if _, ok := item["reasoning"]; !ok {
				p.Reasoning = "AI analysis"
			}
			primary = &p
		}
		out = append(out, categorySuggestion{name, confidence, reason})
	}
	return out, primary, nil
}
func (s *Server) analyzeCategories(ctx context.Context, limit int, include bool) (map[string]any, error) {
	started := time.Now()
	rows, err := s.Pool.Query(ctx, `SELECT DISTINCT ac.ai_category, COUNT(*) AS usage_count, STRING_AGG(DISTINCT a.title, ' | ') AS example_titles FROM article_categories ac JOIN articles a ON ac.article_id=a.id LEFT JOIN category_mapping cm ON LOWER(ac.ai_category)=LOWER(cm.ai_category) WHERE ac.ai_category IS NOT NULL AND ac.ai_category != '' AND cm.ai_category IS NULL GROUP BY ac.ai_category ORDER BY usage_count DESC,ac.ai_category LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	values := []categoryRow{}
	for rows.Next() {
		var row categoryRow
		var examples *string
		if err = rows.Scan(&row.Name, &row.Count, &examples); err != nil {
			break
		}
		raw := ""
		if examples != nil {
			raw = *examples
		}
		row.Examples = strings.Split(raw, " | ")
		if len(row.Examples) > 5 {
			row.Examples = row.Examples[:5]
		}
		values = append(values, row)
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	output := []map[string]any{}
	if len(values) == 0 {
		return map[string]any{"suggestions": output, "total_analyzed": 0, "execution_time_seconds": time.Since(started).Seconds()}, nil
	}
	rows, err = s.Pool.Query(ctx, `SELECT name,display_name FROM categories ORDER BY name`)
	if err != nil {
		return nil, err
	}
	names := []string{}
	for rows.Next() {
		var name, display string
		if err = rows.Scan(&name, &display); err != nil {
			break
		}
		names = append(names, name+" ("+display+")")
	}
	rows.Close()
	if err != nil {
		return nil, err
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	for _, row := range values {
		answer, genErr := s.Runtime.GenerateSummary(ctx, categoryPrompt(row, names))
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		chosen, all := categoryDecision(row.Name, answer, genErr)
		examples := row.Examples
		if !include {
			examples = []string{}
		}
		output = append(output, map[string]any{"ai_category": row.Name, "suggested_fixed_category": chosen.Category, "confidence": chosen.Confidence, "usage_count": row.Count, "example_articles": examples, "reasoning": chosen.Reasoning, "all_suggestions": all})
	}
	sort.SliceStable(output, func(i, j int) bool { return output[i]["confidence"].(float64) > output[j]["confidence"].(float64) })
	return map[string]any{"suggestions": output, "total_analyzed": len(output), "execution_time_seconds": time.Since(started).Seconds()}, nil
}

func categoryDecision(name, answer string, genErr error) (categorySuggestion, []categorySuggestion) {
	suggestions, primary, parseErr := parseCategoryAnswer(answer)
	chosen := categorySuggestion{"Other", 0.3, "No AI suggestions provided"}
	all := []categorySuggestion{{"Other", 0.3, "Fallback suggestion"}}
	if genErr != nil {
		chosen = categorySuggestion{suggestFixedCategory(name), 0.3, "Keyword-based analysis (AI unavailable)"}
		all = []categorySuggestion{chosen}
	} else if parseErr != nil {
		chosen = categorySuggestion{suggestFixedCategory(name), 0.3, "Fallback to keyword-based analysis (invalid AI response)"}
		all = []categorySuggestion{chosen}
	} else if len(suggestions) > 0 {
		chosen = *primary
		all = suggestions
	}
	return chosen, all
}
