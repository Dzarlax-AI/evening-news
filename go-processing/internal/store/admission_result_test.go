package store

import (
	"encoding/json"
	"testing"
)

func TestArticleProcessingItemResultPersistsBoundedAdmissionVerdict(t *testing.T) {
	reason := "Source text has no standalone information"
	raw := articleProcessingItemResult(Persisted{State: "completed", Fields: map[string]any{
		"content_admission": map[string]any{"verdict": "exclude", "reason": reason},
	}})
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	result, ok := got["content_admission"].(map[string]any)
	if !ok || result["verdict"] != "exclude" || result["reason"] != reason || got["state"] != "completed" {
		t.Fatalf("processing item result = %#v", got)
	}
}

func TestArticleProcessingItemResultOmitsInvalidAdmissionVerdict(t *testing.T) {
	raw := articleProcessingItemResult(Persisted{State: "completed", Fields: map[string]any{
		"content_admission": map[string]any{"verdict": "maybe", "reason": "invalid"},
	}})
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	if _, exists := got["content_admission"]; exists {
		t.Fatalf("invalid admission verdict persisted: %#v", got)
	}
}
