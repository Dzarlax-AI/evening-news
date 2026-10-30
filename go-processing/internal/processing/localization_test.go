package processing

import "testing"

func TestBilingualOutcomeKeepsRussianWhenSerbianInvalid(t *testing.T) {
	input := map[string]any{"analysis_version": 2, "localizations": map[string]any{
		"ru":      map[string]any{"title": "Русский заголовок", "summary": "Русское содержание"},
		"sr-Latn": map[string]any{"title": "", "summary": ""},
	}, "categories": []any{"Tech"}, "is_advertisement": false}
	got := AnalyzeOutcome(input, false, false, false)
	if got.State != StateCompleted || got.Fields["summary"] != "Русское содержание" {
		t.Fatalf("Russian analysis did not complete: %#v", got)
	}
	locales := got.Fields["localizations"].(map[string]any)
	if locales["sr-Latn"].(map[string]any)["valid"] != false || locales["ru"].(map[string]any)["valid"] != true {
		t.Fatalf("incorrect independent locale validation: %#v", locales)
	}
}

func TestBilingualOutcomeRejectsMissingRussian(t *testing.T) {
	input := map[string]any{"analysis_version": 2, "localizations": map[string]any{
		"ru": map[string]any{"title": "", "summary": ""}, "sr-Latn": map[string]any{"title": "Naslov", "summary": "Sadržaj"},
	}, "categories": []any{"Tech"}, "is_advertisement": false}
	got := AnalyzeOutcome(input, false, false, false)
	if got.State != StateRetryWait || got.Error != "missing_summary" {
		t.Fatalf("expected Russian retry: %#v", got)
	}
	if got.Fields["localizations"].(map[string]any)["sr-Latn"].(map[string]any)["valid"] != true {
		t.Fatal("Serbian result lost")
	}
}

func TestBilingualOutcomeRequiresSharedFieldsBeforeLocales(t *testing.T) {
	for _, input := range []map[string]any{
		{"analysis_version": 2, "localizations": map[string]any{"ru": map[string]any{"title": "New", "summary": "Text"}, "sr-Latn": map[string]any{"title": "Naslov", "summary": "Tekst"}}, "categories": []any{}, "is_advertisement": false},
		{"analysis_version": 2, "localizations": map[string]any{"ru": map[string]any{"title": "New", "summary": "Text"}, "sr-Latn": map[string]any{"title": "Naslov", "summary": "Tekst"}}, "categories": []any{"Tech"}},
	} {
		got := AnalyzeOutcome(input, false, false, false)
		if got.State != StateRetryWait || len(got.Fields) != 0 {
			t.Fatalf("invalid shared result wrote partial locale: %#v", got)
		}
	}
}

func TestBilingualOutcomePreservesCompletedRussian(t *testing.T) {
	input := map[string]any{"analysis_version": 2, "localizations": map[string]any{"ru": map[string]any{"title": "New", "summary": "New summary"}, "sr-Latn": map[string]any{"title": "Naslov", "summary": "Tekst"}}, "categories": []any{"Tech"}, "is_advertisement": false}
	got := AnalyzeOutcome(input, true, false, false)
	if _, ok := got.Fields["title"]; ok {
		t.Fatal("replaced completed Russian title")
	}
	if _, ok := got.Fields["summary"]; ok {
		t.Fatal("replaced completed Russian summary")
	}
}

func TestLegacyProviderAcceptedOnlyWithoutBilingualRequest(t *testing.T) {
	legacy := map[string]any{"summary": "Russian text", "categories": []any{"Tech"}, "is_advertisement": false}
	if out := AnalyzeOutcome(requireBilingualResult(legacy, true), false, false, false); out.State != StateRetryWait || len(out.Fields) != 0 {
		t.Fatal("legacy response accepted for bilingual request")
	}
	if out := AnalyzeOutcome(requireBilingualResult(legacy, false), false, false, false); out.State != StateCompleted {
		t.Fatal("legacy Russian-only provider rejected")
	}
}

func TestSerbianProseScriptAllowsSourceQuotes(t *testing.T) {
	for _, tc := range []struct {
		text  string
		valid bool
	}{
		{"Это русский текст под сербской меткой", false},
		{"Grad je najavio obnovu mosta u Beogradu.", true},
		{"Govornik je citirao izvor: «Это исходная цитата на русском языке». Izgradnja počinje sutra.", true},
	} {
		if got := serbianLatinProse(tc.text); got != tc.valid {
			t.Errorf("script validation got=%v want=%v", got, tc.valid)
		}
	}
}
