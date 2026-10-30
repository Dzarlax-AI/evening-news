package analysis

import (
	"strings"
	"testing"
)

func TestBilingualResultPreservesPartialLocales(t *testing.T) {
	r := Request{Version: 2, Locales: []string{"ru", "sr-Latn"}}
	raw := map[string]any{"localizations": map[string]any{"ru": map[string]any{"title": " Заголовок ", "summary": " Текст "}, "sr-Latn": map[string]any{"title": "Naslov"}}, "categories": []any{"Tech"}, "is_advertisement": false}
	result := validateAnalysisResult(raw, r)
	if result["summary"] != "Текст" || result["optimized_title"] != "Заголовок" {
		t.Fatalf("bad Russian projection: %#v", result)
	}
	sr := result["localizations"].(map[string]any)["sr-Latn"].(map[string]any)
	if sr["title"] != "Naslov" || sr["summary"] != nil {
		t.Fatalf("bad partial Serbian locale: %#v", sr)
	}
}

func TestBilingualPromptUsesSameEvidence(t *testing.T) {
	prompt := buildPrompt(Request{Version: 2, Locales: []string{"ru", "sr-Latn"}, Title: "Fixture", Content: "Synthetic evidence"})
	if !strings.Contains(prompt, "sr-Latn") || !strings.Contains(prompt, "Synthetic evidence") || strings.Contains(prompt, "ВСЕ результаты анализа должны быть на РУССКОМ") {
		t.Fatal("invalid bilingual prompt")
	}
}

func TestSerbianOnlyPromptUsesSavedRussian(t *testing.T) {
	p := buildPrompt(Request{Version: 2, Locales: []string{"sr-Latn"}, Title: "Русский заголовок", Content: "Русский текст"})
	if !strings.Contains(p, "Serbian Latin") || !strings.Contains(p, "Русский текст") || strings.Contains(p, "CATEGORIZATION") {
		t.Fatal("retry prompt is not Serbian-only")
	}
}
