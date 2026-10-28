package selectors

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPreparedHTMLPythonCapturedNormalAndTruncation(t *testing.T) {
	raw, err := os.ReadFile("testdata/python-html-golden.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct{ Name, HTML, Prepared string }
	if err = json.Unmarshal(raw, &cases); err != nil {
		t.Fatal(err)
	}
	for _, tc := range cases {
		got, err := PrepareHTML(tc.HTML)
		if err != nil {
			t.Fatal(err)
		}
		for _, bad := range []string{"<script", "<style", "<noscript", "<iframe", "<embed"} {
			if strings.Contains(strings.ToLower(got), bad) {
				t.Errorf("%s retained %s", tc.Name, bad)
			}
		}
		if tc.Name == "fragment" || tc.Name == "long" {
			if got != tc.Prepared {
				t.Errorf("%s differs from Python captured output", tc.Name)
			}
		}
		if tc.Name == "long" && utf8.RuneCountInString(got) != 8023 {
			t.Errorf("truncated length %d", utf8.RuneCountInString(got))
		}
		if tc.Name == "malformed" && got != `<section><p>one</p><p>two</p><div>three</div></section>` {
			t.Errorf("HTML5 repaired tree: %q", got)
		}
		if tc.Name == "table" && got != `<table><tbody><tr><td>A</td><td>B</td></tr></tbody></table>` {
			t.Errorf("HTML5 table: %q", got)
		}
	}
}
func TestTopLevelNoiseIsRemoved(t *testing.T) {
	got, err := PrepareHTML(`<script>bad</script><p>kept</p><iframe>bad</iframe>`)
	if err != nil || got != `<p>kept</p>` {
		t.Fatalf("got %q err %v", got, err)
	}
}
