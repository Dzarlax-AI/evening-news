package extractorcompare

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func article() map[string]any {
	return map[string]any{"id": "fixture.article", "source_type": "article", "input": map[string]any{"url": "https://fixture.test/article", "strategy": "readability", "html": "<p>capture</p>"}, "expected": map[string]any{"state": "extracted", "contains": []any{"capture"}}}
}
func TestCaseValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(map[string]any)
		want   string
	}{
		{"mixed", func(c map[string]any) { c["source_type"] = "rss" }, "unsupported_or_mixed_source_type"},
		{"injection", func(c map[string]any) { c["input"].(map[string]any)["rendered_html"] = "x" }, "unsupported_fixture_input_field"},
		{"auto", func(c map[string]any) { c["input"].(map[string]any)["strategy"] = "auto" }, "strategy_not_supported_for_offline_http_replay"},
		{"missing", func(c map[string]any) { delete(c["input"].(map[string]any), "html") }, "case_requires_exactly_one_captured_html_or_content_input"},
		{"oversized", func(c map[string]any) { c["input"].(map[string]any)["html"] = strings.Repeat("x", MaxInput+1) }, "captured_input_exceeds_5_mib"},
		{"empty", func(c map[string]any) { c["input"].(map[string]any)["html"] = " \n" }, "captured_html_must_not_be_empty"},
		{"challenge", func(c map[string]any) { c["input"].(map[string]any)["html"] = `<form id="challenge-form">` }, "captured_html_is_challenge_interstitial"},
		{"secret id", func(c map[string]any) { c["id"] = "article-token-secret" }, "case_id_must_be_bounded_safe_identifier"},
		{"expectation", func(c map[string]any) { c["expected"].(map[string]any)["unverified"] = "x" }, "unsupported_fixture_expectation_field"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := article()
			tc.change(c)
			_, e := validateCase(c)
			if e == nil || e.Error() != tc.want {
				t.Fatalf("error=%v", e)
			}
		})
	}
	if challenge("<article>challenge-form and cf_chl_ tokens</article>") {
		t.Fatal("false challenge")
	}
}
func TestExpectedFieldsAndFailure(t *testing.T) {
	result := map[string]any{"content": "failed", "method_used": "other", "title": "Wrong", "trace": []any{"other"}, "error": "invalid_html"}
	for _, e := range []map[string]any{{"state": "extracted", "contains": []any{"required"}}, {"state": "failed", "fields": map[string]any{"title": "Expected"}}, {"state": "failed", "method": "readability"}, {"state": "failed", "error": "quality_rejected"}, {"state": "failed", "trace_contains": []any{"readability"}}} {
		if expectedMatches(e, result, "failed") {
			t.Fatalf("false positive: %v", e)
		}
	}
	if !expectedMatches(map[string]any{"state": "failed", "fields": map[string]any{"title": ""}}, map[string]any{"title": nil}, "failed") {
		t.Fatal("legacy empty/null mismatch")
	}
}
func TestRealHTTPAndRedactedReport(t *testing.T) {
	secretURL := "https://fixture.example.test/article?private=fixture-query-secret"
	secretText := "Captured article text with distinct details."
	token := "test-token-never-reported"
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Path != "/v1/extract" || r.Header.Get("Authorization") != "Bearer "+token || r.Header.Get("Accept-Encoding") != "identity" {
			t.Error("incorrect request headers")
		}
		var p map[string]any
		if e := json.NewDecoder(r.Body).Decode(&p); e != nil {
			t.Error(e)
		}
		if p["url"] != secretURL || p["use_browser"] != false || p["strategy"] != "source_content" || p["content"] != secretText || p["html"] != nil || p["budget_ms"] != float64(30000) {
			t.Errorf("incorrect payload")
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"contract_version": "1", "request_id": p["request_id"], "original_url": secretURL, "outcome": "success", "result": map[string]any{"content": secretText, "method_used": "source_content", "media": []any{}}, "observations": []any{}})
	}))
	defer server.Close()
	dir := t.TempDir()
	path := filepath.Join(dir, "corpus.json")
	c := article()
	c["input"] = map[string]any{"url": secretURL, "strategy": "source_content", "content": secretText}
	c["expected"] = map[string]any{"state": "extracted", "contains": []any{"Captured article text"}}
	raw, _ := json.Marshal(map[string]any{"version": 1, "cases": []any{c}})
	if e := os.WriteFile(path, raw, 0600); e != nil {
		t.Fatal(e)
	}
	out := filepath.Join(dir, "report.json")
	report, e := Run(context.Background(), Options{Corpus: path, Endpoint: server.URL, Output: out, Token: token, Cycles: 2})
	if e != nil || calls != 2 || report.Mismatches != 0 {
		t.Fatalf("report=%+v calls=%d err=%v", report, calls, e)
	}
	sum := sha256.Sum256(raw)
	if report.CorpusSHA != hex.EncodeToString(sum[:]) {
		t.Fatal("wrong corpus hash")
	}
	encoded, e := os.ReadFile(out)
	if e != nil {
		t.Fatal(e)
	}
	for _, private := range []string{secretURL, "fixture-query-secret", secretText, token} {
		if strings.Contains(string(encoded), private) {
			t.Fatal("report leaked private content")
		}
	}
}
func TestSelectedCasesValidatedBeforeNetwork(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls++; _, _ = io.WriteString(w, `{}`) }))
	defer server.Close()
	dir := t.TempDir()
	a := article()
	b := article()
	b["id"] = "invalid"
	b["input"].(map[string]any)["html"] = `<form id="challenge-form">`
	raw, _ := json.Marshal(map[string]any{"version": 1, "cases": []any{a, b}})
	path := filepath.Join(dir, "c.json")
	_ = os.WriteFile(path, raw, 0600)
	_, err := Run(context.Background(), Options{Corpus: path, Endpoint: server.URL, Output: filepath.Join(dir, "out"), Token: "fixture", Cycles: 1})
	if err == nil || calls != 0 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}
func TestFrozenSourceContentCorpus(t *testing.T) {
	cases, digest, e := load("../../tests/fixtures/extractor-service/source-content-smoke.json", nil)
	if e != nil || len(cases) != 1 || digest != "564866bb28de5c5cdb95245b4b2e10e741eb02a08ee33e7c1610dbd467a26de2" || cases[0].ID != "retention.source_content" {
		t.Fatalf("cases=%v hash=%s err=%v", cases, digest, e)
	}
}

func TestLegacyMatchGolden(t *testing.T) {
	raw, e := os.ReadFile("testdata/legacy-match-golden.json")
	if e != nil {
		t.Fatal(e)
	}
	var corpus struct {
		Cases []struct {
			Expected map[string]any `json:"expected"`
			Result   map[string]any `json:"result"`
			State    string         `json:"state"`
			Matched  bool           `json:"matched"`
		} `json:"cases"`
	}
	if e = json.Unmarshal(raw, &corpus); e != nil {
		t.Fatal(e)
	}
	for i, c := range corpus.Cases {
		if got := expectedMatches(c.Expected, c.Result, c.State); got != c.Matched {
			t.Fatalf("case=%d got=%v want=%v", i, got, c.Matched)
		}
	}
}

func TestResponseContractRejectsMismatchAndMalformed(t *testing.T) {
	base := func() map[string]any {
		return map[string]any{"contract_version": "1", "request_id": "req", "original_url": "https://fixture.test/a", "outcome": "success", "result": map[string]any{"content": "fixture", "media": []any{}}, "observations": []any{}}
	}
	for _, change := range []func(map[string]any){func(p map[string]any) { p["request_id"] = "other" }, func(p map[string]any) { p["original_url"] = "https://other.test" }, func(p map[string]any) { p["outcome"] = "partial" }, func(p map[string]any) { p["result"].(map[string]any)["content"] = "" }, func(p map[string]any) { p["result"].(map[string]any)["media"] = []any{map[string]any{"type": "image"}} }, func(p map[string]any) {
		p["observations"] = []any{map[string]any{"selector": "x", "method": "x", "success": "true"}}
	}, func(p map[string]any) {
		p["error"] = map[string]any{"code": "x", "retryable": true, "retry_after_seconds": 86401.0}
	}} {
		p := base()
		change(p)
		if err := validateResponse(p, "req", "https://fixture.test/a"); err == nil {
			t.Fatal("invalid response accepted")
		}
	}
}
func TestRequestJSONBoundCountsPythonUnicodeEscapes(t *testing.T) {
	m := map[string]any{"a": "я😀<\n", "b": true}
	if got := pythonJSONSize(m); got != len(`{"a": "\u044f\ud83d\ude00<\n", "b": true}`) {
		t.Fatalf("python encoded length=%d", got)
	}
}
func TestRejectedServiceMatchesExpectedFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var p map[string]any
		_ = json.NewDecoder(r.Body).Decode(&p)
		_ = json.NewEncoder(w).Encode(map[string]any{"contract_version": "1", "request_id": p["request_id"], "original_url": p["url"], "outcome": "rejected", "result": map[string]any{"state": "failed", "content": ""}})
	}))
	defer server.Close()
	dir := t.TempDir()
	c := article()
	c["expected"] = map[string]any{"state": "failed"}
	raw, _ := json.Marshal(map[string]any{"version": 1, "cases": []any{c}})
	path := filepath.Join(dir, "corpus")
	_ = os.WriteFile(path, raw, 0600)
	report, err := Run(context.Background(), Options{Corpus: path, Endpoint: server.URL, Output: filepath.Join(dir, "report"), Token: "fixture", Cycles: 1})
	if err != nil || report.Mismatches != 0 || report.Records[0].State != "failed" {
		t.Fatalf("report=%+v err=%v", report, err)
	}
}

func TestOutputCannotAliasCorpus(t *testing.T) {
	dir := t.TempDir()
	corpus := filepath.Join(dir, "input.json")
	original := []byte(`{"version":1,"cases":[]}`)
	if err := os.WriteFile(corpus, original, 0600); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(dir, "link")
	hardlink := filepath.Join(dir, "hard")
	if err := os.Symlink(corpus, symlink); err != nil {
		t.Fatal(err)
	}
	if err := os.Link(corpus, hardlink); err != nil {
		t.Fatal(err)
	}
	calls := 0
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return nil, errors.New("unexpected") })}
	for _, path := range []string{corpus, symlink, hardlink} {
		_, err := Run(context.Background(), Options{Corpus: corpus, Output: path, Endpoint: "http://fixture.invalid", Token: "fixture", Cycles: 1, Client: client})
		if err == nil || err.Error() != "output_aliases_corpus" {
			t.Fatalf("alias was not rejected: %v", err)
		}
	}
	got, err := os.ReadFile(corpus)
	if err != nil || !bytes.Equal(got, original) || calls != 0 {
		t.Fatal("corpus changed or request issued")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestProjectedPythonBooleanNumberEquality(t *testing.T) {
	for _, pair := range [][2]any{{true, float64(1)}, {false, float64(0)}, {float64(1), true}, {float64(0), false}} {
		if !projected(pair[0], pair[1]) {
			t.Fatal("equivalent values rejected")
		}
	}
	if projected(true, float64(2)) || projected(false, float64(1)) {
		t.Fatal("different values accepted")
	}
}
