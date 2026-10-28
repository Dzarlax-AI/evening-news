package domainops

import (
	"encoding/json"
	"testing"
)

func TestDomainCanonicalUnicodeAndEscapes(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{`{"value":"\u2028\u2029 <&>"}`, "{\"value\":\"\u2028\u2029 <&>\"}"},
		{`{"value":"\\u2028"}`, `{"value":"\\u2028"}`},
		{`{"value":"\\\u2028"}`, "{\"value\":\"\\\\\u2028\"}"},
		{`{"value":1e-1}`, `{"value":0.1}`},
	} {
		got, e := canonicalDomainJSON(json.RawMessage(tc.input))
		if e != nil || string(got) != tc.want {
			t.Errorf("%s got %q want %q error %v", tc.input, got, tc.want, e)
		}
	}
}
