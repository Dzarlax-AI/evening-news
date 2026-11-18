package processing

import (
	"encoding/json"
	"testing"
)

func TestShouldRejectTelegramContentRequiresCertainTextOnlyVerdict(t *testing.T) {
	analysis := map[string]any{"content_admission": map[string]any{"verdict": "exclude"}}
	phrase := "Все мы в рандомный момент нашей жизни"
	tests := []struct {
		name       string
		source     string
		context    string
		metadata   telegramAdmissionMetadata
		analysis   map[string]any
		wantReject bool
	}{
		{name: "proven text only", source: "telegram", context: phrase, metadata: telegramAdmissionMetadata{Known: true}, analysis: analysis, wantReject: true},
		{name: "useful short notice kept", source: "telegram", context: "Улица закрыта у площади с 14:00 до 16:00", metadata: telegramAdmissionMetadata{Known: true}, analysis: map[string]any{"content_admission": map[string]any{"verdict": "keep"}}},
		{name: "video attachment", source: "telegram", context: phrase, metadata: telegramAdmissionMetadata{Known: true, HasAttachments: true}, analysis: analysis},
		{name: "same phrase with unavailable attachment and pseudo-empty media_files", source: "telegram", context: phrase, metadata: telegramAdmissionMetadata{}, analysis: analysis},
		{name: "hidden link from metadata", source: "telegram", context: phrase, metadata: telegramAdmissionMetadata{Known: true, ExternalLinks: []string{"https://example.test/story"}}, analysis: analysis},
		{name: "unverified inline link", source: "telegram", context: phrase + " https://example.test/story", metadata: telegramAdmissionMetadata{Known: true}, analysis: analysis},
		{name: "missing verdict", source: "telegram", context: phrase, metadata: telegramAdmissionMetadata{Known: true}, analysis: map[string]any{}},
		{name: "incomplete source evidence", source: "telegram", context: phrase, metadata: telegramAdmissionMetadata{Known: true, AdmissionUncertain: true}, analysis: analysis},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modelVerdict := tt.analysis["content_admission"]
			effective := effectiveTelegramAdmission(modelVerdict, tt.context, tt.metadata)
			if got := shouldRejectTelegramContent(effective); got != tt.wantReject {
				t.Fatalf("shouldRejectTelegramContent() = %v, want %v", got, tt.wantReject)
			}
			if tt.name == "video attachment" && effective["guard_reason"] != "attachment_present" {
				t.Fatalf("video guard reason = %v", effective["guard_reason"])
			}
			if tt.name == "incomplete source evidence" && effective["guard_reason"] != "source_evidence_incomplete" {
				t.Fatalf("incomplete evidence guard reason = %v", effective["guard_reason"])
			}
		})
	}
}

func TestParseTelegramContextNeedsPositiveMetadata(t *testing.T) {
	phrase := "Все мы в рандомный момент нашей жизни"
	for _, tc := range []struct {
		name  string
		raw   string
		allow bool
	}{
		{name: "pseudo-empty files with unknown metadata", raw: `{"source_text":"` + phrase + `","metadata_known":false,"has_media":false,"external_links":[]}`},
		{name: "video attachment", raw: `{"source_text":"` + phrase + `","metadata_known":true,"has_media":true,"external_links":[]}`},
		{name: "text only metadata", raw: `{"source_text":"` + phrase + `","metadata_known":true,"has_media":false,"admission_uncertain":false,"external_links":[]}`, allow: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			context, metadata := parseTelegramContext(json.RawMessage(tc.raw))
			effective := effectiveTelegramAdmission(map[string]any{"verdict": "exclude"}, context, metadata)
			got := shouldRejectTelegramContent(effective)
			if got != tc.allow {
				t.Fatalf("rejection = %v, want %v", got, tc.allow)
			}
		})
	}
}

func TestTelegramContentAdmissionIsDisabledByDefault(t *testing.T) {
	if (Processor{}).ApplyTelegramContentAdmission {
		t.Fatal("content admission enforcement must default to shadow-only")
	}
}
