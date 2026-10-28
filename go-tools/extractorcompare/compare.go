// Package extractorcompare compares captured input against the private extractor API.
package extractorcompare

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	MaxCorpus = 64 << 20
	MaxInput  = 5 << 20
	MaxText   = 100000
)

var idPattern = regexp.MustCompile(`^[A-Za-z0-9._-]{1,96}$`)
var sensitiveID = regexp.MustCompile(`(?i)token|secret|api[_-]?key|bearer|https?://`)
var urlPattern = regexp.MustCompile(`^https?://[^\s/?#]+(?:/[^\s]*)?$`)
var strategies = map[string]bool{"source_content": true, "enhanced_selectors": true, "readability": true, "json_ld": true, "open_graph": true, "meta_description": true, "enhanced_heuristics": true, "fallback_extraction": true}
var inputFields = map[string]bool{"url": true, "html": true, "content": true, "title": true, "strategy": true}
var expectedFields = map[string]bool{"state": true, "contains": true, "not_contains": true, "fields": true, "method": true, "error": true, "trace_contains": true}
var states = map[string]bool{"extracted": true, "failed": true, "resource_limit": true}

type Options struct {
	Corpus, Endpoint, Output, Token string
	Cycles                          int
	CaseIDs                         []string
	Client                          *http.Client
}
type Record struct {
	ID             string `json:"id"`
	Cycle          int    `json:"cycle"`
	State          string `json:"state"`
	Outcome        string `json:"outcome"`
	Method         string `json:"method"`
	ContentChars   int    `json:"content_chars"`
	ExpectedMatch  bool   `json:"expected_match"`
	Mismatch       bool   `json:"mismatch"`
	Classification string `json:"classification"`
}
type Report struct {
	Version    int      `json:"version"`
	CorpusSHA  string   `json:"corpus_sha256"`
	CaseIDs    []string `json:"case_ids"`
	Cycles     int      `json:"cycles"`
	Records    []Record `json:"records"`
	Mismatches int      `json:"mismatches"`
	Scope      string   `json:"scope"`
}
type serviceError struct{ cause error }

func (e serviceError) Error() string { return "extractor service error" }
func (e serviceError) Unwrap() error { return e.cause }

// ErrorClass returns a finite legacy-compatible category without exposing details.
func ErrorClass(err error) string {
	var service serviceError
	if errors.As(err, &service) {
		return "ExtractionServiceError"
	}
	if errors.Is(err, os.ErrNotExist) {
		return "FileNotFoundError"
	}
	if errors.Is(err, os.ErrPermission) {
		return "PermissionError"
	}
	return "ValueError"
}

type Case struct {
	ID              string
	Source          string
	Input, Expected map[string]any
}

func safeID(s string) bool     { return idPattern.MatchString(s) && !sensitiveID.MatchString(s) }
func str(v any) (string, bool) { x, ok := v.(string); return x, ok }
func challenge(v string) bool {
	s := strings.ToLower(v)
	for _, x := range []string{`id="challenge-form"`, `id='challenge-form'`, `id="cf-challenge-running"`, `id='cf-challenge-running'`} {
		if strings.Contains(s, x) {
			return true
		}
	}
	return strings.Contains(s, "<title>just a moment") && strings.Contains(s, "cf_chl_")
}
func textList(v any, nonempty bool) bool {
	a, ok := v.([]any)
	if !ok {
		return false
	}
	for _, x := range a {
		s, ok := str(x)
		if !ok || (nonempty && s == "") {
			return false
		}
	}
	return true
}
func validateCase(raw map[string]any) (Case, error) {
	var c Case
	id, ok := str(raw["id"])
	if !ok || !safeID(id) {
		return c, errors.New("case_id_must_be_bounded_safe_identifier")
	}
	c.ID = id
	c.Source = "article"
	if v, exists := raw["source_type"]; exists {
		c.Source, ok = str(v)
		if !ok || !map[string]bool{"article": true, "web": true}[c.Source] {
			return c, errors.New("unsupported_or_mixed_source_type")
		}
	}
	c.Input, ok = raw["input"].(map[string]any)
	if !ok {
		return c, errors.New("case_input_must_be_object")
	}
	for k := range c.Input {
		if !inputFields[k] {
			return c, errors.New("unsupported_fixture_input_field")
		}
	}
	strategy, ok := str(c.Input["strategy"])
	if !ok || !strategies[strategy] {
		return c, errors.New("strategy_not_supported_for_offline_http_replay")
	}
	html, h := c.Input["html"]
	content, p := c.Input["content"]
	h = h && html != nil
	p = p && content != nil
	if h == p {
		return c, errors.New("case_requires_exactly_one_captured_html_or_content_input")
	}
	value := content
	if h {
		value = html
	}
	text, ok := str(value)
	if !ok {
		return c, errors.New("captured_input_must_be_text")
	}
	if h && strategy == "source_content" {
		return c, errors.New("source_content_requires_captured_content")
	}
	if p && strategy != "source_content" {
		return c, errors.New("captured_content_requires_source_content_strategy")
	}
	if len([]byte(text)) > MaxInput {
		return c, errors.New("captured_input_exceeds_5_mib")
	}
	if h && strings.TrimSpace(text) == "" {
		return c, errors.New("captured_html_must_not_be_empty")
	}
	if h && challenge(text) {
		return c, errors.New("captured_html_is_challenge_interstitial")
	}
	if v, exists := c.Input["title"]; exists {
		if _, ok = str(v); !ok {
			return c, errors.New("title_must_be_text")
		}
	}
	c.Expected, ok = raw["expected"].(map[string]any)
	if !ok {
		return c, errors.New("case_requires_supported_expected_state")
	}
	state, ok := str(c.Expected["state"])
	if !ok || !states[state] {
		return c, errors.New("case_requires_supported_expected_state")
	}
	for k := range c.Expected {
		if !expectedFields[k] {
			return c, errors.New("unsupported_fixture_expectation_field")
		}
	}
	if v, exists := c.Expected["fields"]; exists {
		if _, ok = v.(map[string]any); !ok {
			return c, errors.New("expected_fields_must_be_object")
		}
	}
	for _, key := range []string{"method", "error"} {
		if v, exists := c.Expected[key]; exists {
			if _, ok = str(v); !ok {
				return c, errors.New("expected_" + key + "_must_be_text")
			}
		}
	}
	if v, exists := c.Expected["trace_contains"]; exists && !textList(v, false) {
		return c, errors.New("expected_trace_contains_must_be_text_list")
	}
	for _, key := range []string{"contains", "not_contains"} {
		if v, exists := c.Expected[key]; exists && !textList(v, true) {
			return c, errors.New("expected_anchors_must_be_text_lists")
		}
	}
	rawURL, ok := str(c.Input["url"])
	if !ok || !urlPattern.MatchString(rawURL) {
		return c, errors.New("case_requires_http_url")
	}
	return c, nil
}
func load(path string, ids []string) ([]Case, string, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, "", e
	}
	defer f.Close()
	info, e := f.Stat()
	if e != nil || info.Size() > MaxCorpus {
		return nil, "", errors.New("corpus_exceeds_64_mib")
	}
	raw, e := io.ReadAll(io.LimitReader(f, MaxCorpus+1))
	if e != nil || len(raw) > MaxCorpus {
		return nil, "", errors.New("corpus_exceeds_64_mib")
	}
	sum := sha256.Sum256(raw)
	digest := hex.EncodeToString(sum[:])
	if !utf8.Valid(raw) {
		return nil, "", errors.New("invalid_corpus")
	}
	var root map[string]any
	if json.Unmarshal(raw, &root) != nil {
		return nil, "", errors.New("invalid_corpus")
	}
	version, ok := root["version"].(float64)
	cases, okCases := root["cases"].([]any)
	if !ok || version != 1 || !okCases || len(cases) == 0 {
		return nil, "", errors.New("expected_nonempty_version_1_corpus")
	}
	if len(cases) > 200 {
		return nil, "", errors.New("corpus_exceeds_200_cases")
	}
	byID := map[string]map[string]any{}
	order := []string{}
	for _, item := range cases {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, "", errors.New("case_id_must_be_bounded_safe_identifier")
		}
		id, ok := str(m["id"])
		if !ok || !safeID(id) {
			return nil, "", errors.New("case_id_must_be_bounded_safe_identifier")
		}
		if byID[id] != nil {
			return nil, "", errors.New("duplicate_case_id")
		}
		byID[id] = m
		order = append(order, id)
	}
	if len(ids) > 0 {
		order = ids
	}
	seen := map[string]bool{}
	selected := []Case{}
	for _, id := range order {
		if seen[id] {
			return nil, "", errors.New("duplicate_case_selection")
		}
		seen[id] = true
		m := byID[id]
		if m == nil {
			return nil, "", errors.New("unknown_case_id")
		}
		c, e := validateCase(m)
		if e != nil {
			return nil, "", e
		}
		selected = append(selected, c)
	}
	return selected, digest, nil
}
func normalized(s string) string { return strings.Join(strings.Fields(s), " ") }
func projected(e, a any) bool {
	if em, ok := e.(map[string]any); ok {
		am, ok := a.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range em {
			actual, ok := am[k]
			if !ok || !projected(v, actual) {
				return false
			}
		}
		return true
	}
	if el, ok := e.([]any); ok {
		al, ok := a.([]any)
		if !ok || len(el) != len(al) {
			return false
		}
		for i := range el {
			if !projected(el[i], al[i]) {
				return false
			}
		}
		return true
	}
	if e == nil && a == "" || a == nil && e == "" {
		return true
	}
	// Python's equality treats booleans as the numbers zero and one.
	if b, ok := e.(bool); ok {
		if n, ok := a.(float64); ok {
			return (b && n == 1) || (!b && n == 0)
		}
	}
	if b, ok := a.(bool); ok {
		if n, ok := e.(float64); ok {
			return (b && n == 1) || (!b && n == 0)
		}
	}
	return reflect.DeepEqual(e, a)
}

func distinctOutput(corpus, output string) error {
	if output == "" {
		return errors.New("invalid_output")
	}
	inputInfo, err := os.Stat(corpus)
	if err != nil {
		return errors.New("corpus_unavailable")
	}
	outputInfo, err := os.Stat(output)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return errors.New("output_unavailable")
	}
	if os.SameFile(inputInfo, outputInfo) {
		return errors.New("output_aliases_corpus")
	}
	return nil
}
func expectedMatches(e, r map[string]any, state string) bool {
	if e["state"] != state {
		return false
	}
	content, _ := str(r["content"])
	norm := normalized(content)
	for _, key := range []string{"contains", "not_contains"} {
		if vals, ok := e[key].([]any); ok {
			for _, v := range vals {
				found := strings.Contains(norm, normalized(v.(string)))
				if (key == "contains" && !found) || (key == "not_contains" && found) {
					return false
				}
			}
		}
	}
	if fields, ok := e["fields"].(map[string]any); ok {
		for k, v := range fields {
			a, ok := r[k]
			if !ok || !projected(v, a) {
				return false
			}
		}
	}
	method := methodValue(r)
	if m, ok := e["method"].(string); ok && m != method {
		return false
	}
	if er, ok := e["error"].(string); ok && er != "" {
		actual, _ := str(r["error"])
		if !strings.Contains(actual, er) {
			return false
		}
	}
	if trace, ok := e["trace_contains"].([]any); ok {
		actual, _ := r["trace"].([]any)
		for _, x := range trace {
			found := false
			for _, y := range actual {
				if reflect.DeepEqual(x, y) {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}
func methodValue(r map[string]any) string {
	v, _ := str(r["method_used"])
	if v == "" {
		v, _ = str(r["method"])
	}
	return v
}

func pythonJSONStringSize(s string) int {
	n := 2
	for _, r := range s {
		switch {
		case r == '"' || r == '\\' || r == '\b' || r == '\f' || r == '\n' || r == '\r' || r == '\t':
			n += 2
		case r < 0x20:
			n += 6
		case r <= 0x7e:
			n++
		case r <= 0xffff:
			n += 6
		default:
			n += 12
		}
	}
	return n
}
func pythonJSONSize(m map[string]any) int {
	n := 2
	count := 0
	for k, v := range m {
		if count > 0 {
			n += 2
		}
		count++
		n += pythonJSONStringSize(k) + 2
		switch x := v.(type) {
		case string:
			n += pythonJSONStringSize(x)
		case bool:
			if x {
				n += 4
			} else {
				n += 5
			}
		case int:
			n += len(fmt.Sprint(x))
		default:
			return MaxInput + 1
		}
	}
	return n
}
func requestID() (string, error) {
	var b [16]byte
	if _, e := rand.Read(b[:]); e != nil {
		return "", e
	}
	b[6] = (b[6] & 15) | 0x40
	b[8] = (b[8] & 63) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func validateResponse(p map[string]any, id, original string) error {
	if p["contract_version"] != "1" || p["request_id"] != id || p["original_url"] != original {
		return errors.New("invalid_payload")
	}
	outcome, ok := str(p["outcome"])
	if !ok || !map[string]bool{"success": true, "rejected": true, "failed": true, "needs_selector_study": true}[outcome] {
		return errors.New("invalid_payload")
	}
	r, ok := p["result"].(map[string]any)
	if !ok {
		return errors.New("invalid_payload")
	}
	if v, exists := p["error"]; exists && v != nil {
		e, ok := v.(map[string]any)
		if !ok {
			return errors.New("invalid_payload")
		}
		if _, ok = e["code"].(string); !ok {
			return errors.New("invalid_payload")
		}
		if _, ok = e["retryable"].(bool); !ok {
			return errors.New("invalid_payload")
		}
		if a, exists := e["retry_after_seconds"]; exists && a != nil {
			v, ok := a.(float64)
			if !ok || v < 0 || v > 86400 || v != float64(int(v)) {
				return errors.New("invalid_payload")
			}
		}
	}
	for _, name := range []string{"content", "title", "publication_date", "author", "description", "image_url", "method", "method_used", "selector_used"} {
		if v, exists := r[name]; exists && v != nil {
			if _, ok = v.(string); !ok {
				return errors.New("invalid_payload")
			}
		}
	}
	content, _ := str(r["content"])
	if utf8.RuneCountInString(content) > MaxText {
		return errors.New("response_too_large")
	}
	if outcome == "success" && strings.TrimSpace(content) == "" {
		return errors.New("invalid_payload")
	}
	if v := r["media"]; v != nil {
		a, ok := v.([]any)
		if !ok {
			return errors.New("invalid_payload")
		}
		for _, x := range a {
			m, ok := x.(map[string]any)
			if !ok {
				return errors.New("invalid_payload")
			}
			if _, ok = m["type"].(string); !ok {
				return errors.New("invalid_payload")
			}
			if _, ok = m["url"].(string); !ok {
				return errors.New("invalid_payload")
			}
		}
	}
	if v := p["observations"]; v != nil {
		a, ok := v.([]any)
		if !ok || len(a) > 64 {
			return errors.New("invalid_payload")
		}
		for _, x := range a {
			m, ok := x.(map[string]any)
			if !ok {
				return errors.New("invalid_payload")
			}
			if _, ok = m["selector"].(string); !ok {
				return errors.New("invalid_payload")
			}
			if _, ok = m["method"].(string); !ok {
				return errors.New("invalid_payload")
			}
			if _, ok = m["success"].(bool); !ok {
				return errors.New("invalid_payload")
			}
		}
	}
	return nil
}
func request(ctx context.Context, client *http.Client, endpoint, token string, c Case) (map[string]any, error) {
	id, e := requestID()
	if e != nil {
		return nil, e
	}
	rawURL := c.Input["url"].(string)
	p := map[string]any{"request_id": id, "url": rawURL, "strategy": c.Input["strategy"], "source_type": c.Source, "budget_ms": 30000, "max_bytes": MaxInput, "max_chars": MaxText, "use_browser": false}
	for _, k := range []string{"html", "content", "title"} {
		if v, ok := c.Input[k]; ok && v != nil {
			p[k] = v
		}
	}
	if pythonJSONSize(p) > MaxInput {
		return nil, errors.New("request_too_large")
	}
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	e = encoder.Encode(p)
	raw := bytes.TrimSuffix(encoded.Bytes(), []byte("\n"))
	if e != nil || len(raw) > MaxInput {
		return nil, errors.New("request_too_large")
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader(raw))
	if e != nil {
		return nil, errors.New("invalid_request")
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	resp, e := client.Do(req)
	if e != nil {
		return nil, errors.New("transport_error")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, errors.New("http_error")
	}
	encoding := strings.ToLower(resp.Header.Get("Content-Encoding"))
	if encoding != "" && encoding != "identity" {
		return nil, errors.New("invalid_payload")
	}
	b, e := io.ReadAll(io.LimitReader(resp.Body, MaxInput+1))
	if e != nil || len(b) > MaxInput || !utf8.Valid(b) {
		return nil, errors.New("invalid_payload")
	}
	var out map[string]any
	if json.Unmarshal(b, &out) != nil {
		return nil, errors.New("invalid_payload")
	}
	if e = validateResponse(out, id, rawURL); e != nil {
		return nil, e
	}
	return out, nil
}
func endpointURL(raw string) (string, error) {
	u, e := url.Parse(raw)
	if e != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(raw, " \t\n\r") {
		return "", errors.New("invalid_extractor_endpoint")
	}
	return strings.TrimRight(raw, "/") + "/v1/extract", nil
}

// Run validates every selected case before issuing HTTP requests and emits only a redacted report.
func Run(ctx context.Context, o Options) (Report, error) {
	var report Report
	if o.Cycles < 1 || o.Cycles > 6 {
		return report, errors.New("cycles_must_be_between_1_and_6")
	}
	if o.Token == "" {
		return report, errors.New("missing_token_environment_variable")
	}
	if err := distinctOutput(o.Corpus, o.Output); err != nil {
		return report, err
	}
	cases, digest, e := load(o.Corpus, o.CaseIDs)
	if e != nil {
		return report, e
	}
	endpoint, e := endpointURL(o.Endpoint)
	if e != nil {
		return report, e
	}
	client := &http.Client{Timeout: 30 * time.Second}
	if o.Client != nil {
		*client = *o.Client
		if client.Timeout == 0 {
			client.Timeout = 30 * time.Second
		}
	} else {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.Proxy = nil
		transport.DisableCompression = true
		client.Transport = transport
		defer transport.CloseIdleConnections()
	}
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	report = Report{Version: 1, CorpusSHA: digest, Cycles: o.Cycles, CaseIDs: []string{}, Records: []Record{}, Scope: "captured input checked against frozen expectations; independent HTTP service; browser disabled"}
	for _, c := range cases {
		report.CaseIDs = append(report.CaseIDs, c.ID)
	}
	for cycle := 1; cycle <= o.Cycles; cycle++ {
		for _, c := range cases {
			payload, e := request(ctx, client, endpoint, o.Token, c)
			if e != nil {
				return Report{}, serviceError{e}
			}
			result := payload["result"].(map[string]any)
			var state string
			if value, exists := result["state"]; !exists || value == nil {
				if payload["outcome"] == "success" {
					state = "extracted"
				} else {
					state = "failed"
				}
			} else {
				var ok bool
				state, ok = value.(string)
				if !ok {
					return Report{}, errors.New("service_result_requires_supported_state")
				}
			}
			if !states[state] {
				return Report{}, errors.New("service_result_requires_supported_state")
			}
			matched := expectedMatches(c.Expected, result, state)
			method := methodValue(result)
			if method != "" && !strategies[method] {
				method = "other"
			}
			content, _ := str(result["content"])
			record := Record{ID: c.ID, Cycle: cycle, State: state, Outcome: payload["outcome"].(string), Method: method, ContentChars: min(utf8.RuneCountInString(content), MaxText), ExpectedMatch: matched, Mismatch: !matched, Classification: "matched"}
			if !matched {
				report.Mismatches++
				record.Classification = "expected_mismatch"
			}
			report.Records = append(report.Records, record)
		}
	}
	data, e := json.MarshalIndent(report, "", "  ")
	if e != nil {
		return Report{}, e
	}
	if e = os.MkdirAll(filepath.Dir(o.Output), 0755); e != nil {
		return Report{}, e
	}
	if e = distinctOutput(o.Corpus, o.Output); e != nil {
		return Report{}, e
	}
	tmp, e := os.CreateTemp(filepath.Dir(o.Output), ".extractor-comparison-*")
	if e != nil {
		return Report{}, e
	}
	defer os.Remove(tmp.Name())
	_, e = tmp.Write(append(data, '\n'))
	if closeErr := tmp.Close(); e == nil {
		e = closeErr
	}
	if e != nil {
		return Report{}, e
	}
	if e = os.Rename(tmp.Name(), o.Output); e != nil {
		return Report{}, e
	}
	return report, nil
}
