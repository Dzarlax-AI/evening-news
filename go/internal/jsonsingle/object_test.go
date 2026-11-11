package jsonsingle

import "testing"

func TestDecodeObjectRequiresUnambiguousFields(t *testing.T) {
	for _, raw := range []string{
		`null`, `[]`, `{}`, `{"value":null}`, `{"Value":"ok"}`,
		`{"value":"ok","value":"other"}`, `{"value":"ok","\u0076alue":"other"}`,
		`{"value":"ok","extra":false}`, `{"value":"ok"} {}`, `{"value":false}`,
	} {
		var out sample
		if DecodeObject([]byte(raw), &out, "value") == nil {
			t.Fatalf("ambiguous object accepted: %s", raw)
		}
	}
	var out sample
	if err := DecodeObject([]byte(" \n{\"value\":\"ok\"} \n"), &out, "value"); err != nil || out.Value != "ok" {
		t.Fatalf("valid object rejected: %v", err)
	}
}
