package selectors

import (
	"encoding/json"
	"os"
	"testing"
)

func TestPromptPythonGolden(t *testing.T) {
	raw, err := os.ReadFile("testdata/python-prompt-golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases map[string]string
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for kind, want := range cases {
		if got := StructurePrompt("https://example.test/news", "<p>fixture</p>", kind); got != want {
			t.Errorf("%s prompt differs from Python", kind)
		}
	}
}
