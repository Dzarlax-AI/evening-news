package jsonstrict

import "testing"

func TestRejectDuplicateKeys(t *testing.T) {
	for _, input := range []string{
		`{"a":1,"a":2}`,
		`{"outer":{"a":1,"a":2}}`,
		`{} {}`,
		`{"a":`,
	} {
		if err := RejectDuplicateKeys([]byte(input)); err == nil {
			t.Fatalf("expected invalid JSON rejection for %q", input)
		}
	}
	for _, input := range []string{`{"a":1,"b":{"a":2}}`, `[1,{"x":true}]`, `null`} {
		if err := RejectDuplicateKeys([]byte(input)); err != nil {
			t.Fatalf("valid JSON rejected for %q: %v", input, err)
		}
	}
}
