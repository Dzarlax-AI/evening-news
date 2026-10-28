package webbackend

import (
	"encoding/json"
	"errors"

	"os"
	"testing"
)

func TestCategoryPromptMatchesLegacyCapture(t *testing.T) {
	data, err := os.ReadFile("testdata/legacy-category-prompts.json")
	if err != nil {
		t.Fatal(err)
	}
	var capture struct {
		Cases []struct {
			Name       string   `json:"name"`
			Category   string   `json:"ai_category"`
			Count      int      `json:"usage_count"`
			Examples   []string `json:"examples"`
			Categories []string `json:"categories"`
			Prompt     string   `json:"full_prompt"`
		} `json:"cases"`
	}
	if err = json.Unmarshal(data, &capture); err != nil {
		t.Fatal(err)
	}
	for _, tc := range capture.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			got := categoryPrompt(categoryRow{Name: tc.Category, Count: tc.Count, Examples: tc.Examples}, tc.Categories)
			if got != tc.Prompt {
				t.Fatalf("prompt mismatch\ngot: %q\nwant: %q", got, tc.Prompt)
			}
		})
	}
}

func TestCategoryAnswerDefaultsMatchLegacy(t *testing.T) {
	all, primary, err := parseCategoryAnswer(`{"suggestions":[{"category":"Tech (Technology)"}]}`)
	if err != nil || primary == nil || primary.Category != "Tech" || primary.Confidence != 0.5 || primary.Reasoning != "AI analysis" || len(all) != 1 || all[0].Confidence != 0 || all[0].Reasoning != "" {
		t.Fatalf("primary=%+v all=%+v err=%v", primary, all, err)
	}
}

func TestCategoryFallbacksAreSanitized(t *testing.T) {
	for _, tc := range []struct {
		answer string
		err    error
		reason string
	}{{"", errors.New("secret provider detail"), "Keyword-based analysis (AI unavailable)"}, {"not json", nil, "Fallback to keyword-based analysis (invalid AI response)"}} {
		chosen, all := categoryDecision("Tech", tc.answer, tc.err)
		if chosen.Category != "Tech" || chosen.Confidence != 0.3 || chosen.Reasoning != tc.reason || len(all) != 1 || all[0] != chosen {
			t.Fatalf("chosen=%+v all=%+v", chosen, all)
		}
	}
}
