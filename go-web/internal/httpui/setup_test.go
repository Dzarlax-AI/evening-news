package httpui

import "testing"

func TestSetupStepMustBeOneExactKnownStep(t *testing.T) {
	for _, step := range []string{"storage", "ai", "telegram", "sources", "categories", "schedule", "finish"} {
		if !validSetupStep(step) {
			t.Errorf("known step rejected: %q", step)
		}
	}
	for _, step := range []string{"", "ai|telegram", "|", "AI", "unknown", " ai"} {
		if validSetupStep(step) {
			t.Errorf("invalid step accepted: %q", step)
		}
	}
}
