package privateapi

import "testing"

func TestDecodeJSONStrict(t *testing.T) {
	type value struct {
		Nested map[string]any `json:"nested"`
	}
	tests := []struct {
		input string
		valid bool
	}{
		{`{"nested":{"a":1,"b":[true,null]}}`, true},
		{`{"nested":{"a":1,"a":2}}`, false},
		{`{"nested":{"a":1,"\u0061":2}}`, false},
		{`{"nested":{},"nested":{}}`, false},
		{`{"nested":{"a":NaN}}`, false},
		{`{"nested":{"a":"\ud83d\ude80"}}`, true},
		{`{"nested":{"a":"\ud83d"}}`, false},
		{`{"nested":{"a":"\ude80"}}`, false},
		{`{"nested":{}} {}`, false},
		{`{"nested":{},"extra":true}`, false},
		{"{\"nested\":{\"a\":\"\xff\"}}", false},
	}
	for _, tc := range tests {
		var v value
		if ok := DecodeJSON([]byte(tc.input), &v) == nil; ok != tc.valid {
			t.Errorf("%q valid=%v", tc.input, ok)
		}
	}
}
