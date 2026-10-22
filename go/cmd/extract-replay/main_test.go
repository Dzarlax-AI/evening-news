package main

import (
	"bytes"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
)

func corpusFile(t *testing.T, data string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "manifest.json")
	if e := os.WriteFile(p, []byte(data), 0600); e != nil {
		t.Fatal(e)
	}
	return p
}
func TestCLIEmitsEveryCaseEveryCycleAndFailsUnsupported(t *testing.T) {
	m := `{"version":1,"cases":[{"id":"ok","source_type":"article","input":{"url":"https://example.test/a","strategy":"source_content","content":"Captured source text"},"expected":{"state":"extracted","contains":["Captured"]}},{"id":"unsupported","input":{"url":"https://example.test/b","strategy":"missing_method","content":"Must not bypass dispatch"},"expected":{"state":"extracted"}}]}`
	var out, err bytes.Buffer
	code := run([]string{"-corpus", corpusFile(t, m), "-cycles", "3", "-workers", "2"}, &out, &err)
	if code != 1 {
		t.Fatalf("exit %d: %s", code, err.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 6 {
		t.Fatalf("records=%d", len(lines))
	}
	for _, line := range lines {
		var r record
		if e := json.Unmarshal([]byte(line), &r); e != nil {
			t.Fatal(e)
		}
		if r.ID == "unsupported" && r.Passed {
			t.Fatal("unsupported extraction passed")
		}
		if r.Memory.HeapInuseBytes == 0 {
			t.Fatal("missing heap metrics")
		}
	}
}
func TestCLIRejectsUnknownSchemaAndInvalidManifest(t *testing.T) {
	for _, data := range []string{`{"version":2,"cases":[]}`, `{"version":1,"cases":[{"id":"x","input":{"url":"https://example.test","unrecognized":true}}]}`, `{"version":1,"cases":[{"id":"x"},{"id":"x"}]}`, `{"version":1,"cases":[{"id":"x"}]} {}`} {
		var o, e bytes.Buffer
		if code := run([]string{"-corpus", corpusFile(t, data)}, &o, &e); code != 2 {
			t.Errorf("code=%d for %s", code, data)
		}
	}
}
func TestExpectedFieldsProjectionDoesNotIgnoreBadValues(t *testing.T) {
	r := extractor.Result{State: "extracted", Media: []extractor.Media{{Type: "poll", URL: "#stable_go_id", PollData: map[string]any{"question": "When?"}}}}
	e := expectation{Fields: map[string]any{"media": []any{map[string]any{"type": "poll", "poll_data": map[string]any{"question": "When?"}}}}}
	if f := check(r, e); len(f) != 0 {
		t.Fatal(f)
	}
	e.Fields["media"] = []any{map[string]any{"poll_data": map[string]any{"question": "Wrong"}}}
	if len(check(r, e)) == 0 {
		t.Fatal("bad nested field ignored")
	}
	e.Fields = map[string]any{"unknown": nil}
	if len(check(r, e)) == 0 {
		t.Fatal("unknown expected field ignored")
	}
}

func TestMissingExpectedStateIsInvalidManifest(t *testing.T) {
	for _, expected := range []string{"", `,"expected":{}`, `,"expected":{"contains":["Captured"]}`} {
		m := `{"version":1,"cases":[{"id":"no-expected","input":{"url":"https://example.test/a","strategy":"source_content","content":"Captured"}` + expected + `}]}`
		var out, err bytes.Buffer
		if code := run([]string{"-corpus", corpusFile(t, m)}, &out, &err); code != 2 {
			t.Errorf("missing explicit expected state accepted: exit=%d expected=%s", code, expected)
		}
	}
}

func TestNestedUnknownNullFieldFails(t *testing.T) {
	r := extractor.Result{State: "extracted", Media: []extractor.Media{{Type: "image", URL: "https://cdn.test/image.png"}}}
	e := expectation{Fields: map[string]any{"media": []any{map[string]any{"invented": nil}}}}
	if len(check(r, e)) == 0 {
		t.Fatal("unknown nested expected null field passed")
	}
}

func TestManifestHardByteBoundaryIncludesTrailingData(t *testing.T) {
	const limit = 64 * 1024 * 1024
	valid := `{"version":1,"cases":[{"id":"boundary","input":{"url":"https://example.test/a","strategy":"source_content","content":"Captured"},"expected":{"state":"extracted"}}]}`
	for _, tt := range []struct {
		name  string
		extra string
		want  int
	}{{"exact_boundary", "", 0}, {"overflow_whitespace", " ", 2}, {"overflow_trailing_garbage", " invalid", 2}} {
		t.Run(tt.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "manifest.json")
			f, e := os.Create(p)
			if e != nil {
				t.Fatal(e)
			}
			if _, e = io.WriteString(f, valid); e != nil {
				t.Fatal(e)
			}
			padding := strings.Repeat(" ", 64*1024)
			remaining := limit - len(valid)
			for remaining > 0 {
				n := min(remaining, len(padding))
				if _, e = io.WriteString(f, padding[:n]); e != nil {
					t.Fatal(e)
				}
				remaining -= n
			}
			if _, e = io.WriteString(f, tt.extra); e != nil {
				t.Fatal(e)
			}
			if e = f.Close(); e != nil {
				t.Fatal(e)
			}
			var out, err bytes.Buffer
			if code := run([]string{"-corpus", p}, &out, &err); code != tt.want {
				t.Fatalf("exit=%d want=%d: %s", code, tt.want, err.String())
			}
		})
	}
}
