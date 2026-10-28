package callbacks

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestCallbackInputValidation(t *testing.T) {
	c := &ArticleCompute{MaxTextChars: 4}
	tests := []struct {
		op, input string
		want      error
	}{
		{"analyze", `{"title":"abcd","content":"ok","url":"https://example.test/"}`, nil},
		{"analyze", `{"title":"abcde","content":"ok","url":"https://example.test/"}`, ErrTextLimit},
		{"analyze", `{"title":"ok","content":"ok","url":"https://example.test/","original_context":null}`, nil},
		{"analyze", `{"title":"ok","content":"ok","url":"https://example.test/","extra":1}`, invalidInput},
		{"extraction_context", `{"url":"https://example.test/"}`, nil},
		{"extraction_feedback", `{"url":"https://example.test/","method":"readability","success":false,"observations":[{"selector":"main","method":"readability","success":false}],"quality_score":0}`, nil},
		{"extraction_feedback", `{"url":"https://example.test/","method":"readability","success":1,"observations":[]}`, invalidInput},
		{"extraction_feedback", `{"url":"https://example.test/","method":"readability","success":false,"observations":[],"content_length":1.5}`, invalidInput},
		{"extraction_feedback", `{"url":"https://example.test/","method":"invalid","success":true,"observations":[]}`, invalidInput},
	}
	for _, tc := range tests {
		if err := c.Validate(tc.op, json.RawMessage(tc.input)); !errors.Is(err, tc.want) {
			t.Errorf("%s %s: got %v want %v", tc.op, tc.input, err, tc.want)
		}
	}
}

// Captured from Python's compact sorted UTF-8 serialization at the original
// executor boundary. Absent and explicit null original context are distinct.
func TestAnalysisFingerprintPythonGolden(t *testing.T) {
	base := map[string]any{"title": "café <news> \u2028 \u2029 & 🚀", "content": "line one\nline two / \\", "url": "https://example.test/a?x=1&y=2"}
	cases := []struct {
		name, want string
		present    bool
		value      any
	}{
		{"absent", "65d3b761950688b3133f5ebe13ff29734661b27f43f9f0ab67c37297f230f51d", false, nil},
		{"null", "ef79d73b279ba1c44362a44066c6cb1865ea8faa0df987ea4a756a0fdf262461", true, nil},
		{"value", "1cb5b84d8a8d3d93311358e87af71af07d27d8986b4ca88e0155088cf117b2d9", true, "контекст > body"},
	}
	for _, tc := range cases {
		v := map[string]any{}
		for k, x := range base {
			v[k] = x
		}
		if tc.present {
			v["original_context"] = tc.value
		}
		got, err := analysisFingerprint(v)
		if err != nil || got != tc.want {
			t.Errorf("%s got %s err %v", tc.name, got, err)
		}
	}
}

func TestCanonicalPythonFloatSpellings(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{`{"quality_score":1e-1}`, `{"quality_score":0.1}`},
		{`{"quality_score":1.0}`, `{"quality_score":1.0}`},
		{`{"quality_score":1}`, `{"quality_score":1}`},
		{`{"quality_score":1e-5}`, `{"quality_score":1e-05}`},
		{`{"quality_score":1e-4}`, `{"quality_score":0.0001}`},
		{`{"quality_score":-0e0}`, `{"quality_score":-0.0}`},
	} {
		got, err := canonical(json.RawMessage(tc.input))
		if err != nil || string(got) != tc.want {
			t.Errorf("input %s: got %s err %v", tc.input, got, err)
		}
	}
}

func TestURLHostnameLegacyCases(t *testing.T) {
	for _, tc := range []struct {
		url, host string
		valid     bool
	}{
		{"https://EXAMPLE.test/a%zz", "example.test", true},
		{"https://example.test:abc/x", "example.test", true},
		{"HTTPS://example.test/a b", "example.test", true},
		{"http://example.test:99999/x", "example.test", true},
		{"http://[::1]/x", "::1", true},
		{"https:///missing", "", false},
		{"ftp://example.test/", "", false},
	} {
		got, ok := URLHostname(tc.url)
		if got != tc.host || ok != tc.valid {
			t.Errorf("%q got %q %v", tc.url, got, ok)
		}
	}
}
