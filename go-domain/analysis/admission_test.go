package analysis

import (
	"strings"
	"testing"
)

func TestTelegramPromptRequestsSharedContentAdmissionVerdict(t *testing.T) {
	ctx := "Все мы в рандомный момент нашей жизни"
	for _, tc := range []struct {
		name, context string
	}{
		{name: "reaction_phrase", context: ctx},
		{name: "useful_notice", context: "Улица закрыта у площади с 14:00 до 16:00 из-за ремонта."},
		{name: "context_with_attachment", context: ctx + " [вложение]"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			prompt := buildPrompt(Request{Version: 1, Title: "Synthetic Telegram", Content: tc.context,
				URL: "https://example.test/story", SourceType: "telegram", OriginalContext: &tc.context})
			for _, want := range []string{"TELEGRAM CONTENT ADMISSION", `"exclude"`, `"uncertain"`, "ORIGINAL SOURCE TEXT", tc.context} {
				if !strings.Contains(prompt, want) {
					t.Fatalf("prompt lacks %q", want)
				}
			}
		})
	}
}

func TestValidateTelegramAdmissionIsOptionalAndBounded(t *testing.T) {
	r := Request{Version: 1, URL: "https://example.test/story", SourceType: "telegram"}
	for _, tc := range []struct {
		name string
		raw  map[string]any
		want string
	}{
		{name: "absent verdict", raw: map[string]any{}, want: ""},
		{name: "unknown verdict", raw: map[string]any{"content_admission": map[string]any{"verdict": "maybe"}}, want: ""},
		{name: "valid exclude", raw: map[string]any{"content_admission": map[string]any{"verdict": "exclude", "reason": "Reaction without context"}}, want: "exclude"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := validateAnalysisResult(tc.raw, r)
			admission, ok := result["content_admission"].(map[string]any)
			if tc.want == "" {
				if ok {
					t.Fatalf("unexpected admission verdict: %#v", admission)
				}
				return
			}
			if !ok || admission["verdict"] != tc.want {
				t.Fatalf("validated admission = %#v, want %q", result["content_admission"], tc.want)
			}
		})
	}
}

func TestTruncatedTelegramContextCannotProduceExclude(t *testing.T) {
	original := strings.Repeat("synthetic context ", 150)
	result := validateAnalysisResult(map[string]any{"content_admission": map[string]any{"verdict": "exclude", "reason": "No context"}}, Request{
		Version: 1, URL: "https://example.test/story", SourceType: "telegram", OriginalContext: &original,
	})
	admission := result["content_admission"].(map[string]any)
	if admission["verdict"] != "uncertain" {
		t.Fatalf("truncated source context verdict = %v, want uncertain", admission["verdict"])
	}
}
