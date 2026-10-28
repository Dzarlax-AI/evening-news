// Package replaycheck validates extractor output against source-backed goldens.
package replaycheck

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"reflect"
	"sort"
	"strings"
)

type obj = map[string]any
type key struct {
	ID    string
	Cycle int
}
type Report struct {
	CaseCount   int       `json:"case_count"`
	Cycles      int       `json:"cycles"`
	RecordCount int       `json:"record_count"`
	Failures    []Failure `json:"golden_failures"`
	Gate        string    `json:"gate"`
	Scope       string    `json:"scope"`
}
type Failure struct {
	ID    string `json:"id"`
	Cycle int    `json:"cycle"`
}

func decode(data []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(data))
	d.UseNumber()
	var v any
	if e := d.Decode(&v); e != nil {
		return nil, e
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return nil, errors.New("trailing_json")
	}
	return v, nil
}
func file(path string) (any, error) {
	b, e := os.ReadFile(path)
	if e != nil {
		return nil, e
	}
	return decode(b)
}
func integer(v any) (int, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	i, e := n.Int64()
	return int(i), e == nil && int64(int(i)) == i
}
func canonical(v any) ([]byte, error) {
	var b bytes.Buffer
	e := json.NewEncoder(&b)
	e.SetEscapeHTML(false)
	if err := e.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimSuffix(b.Bytes(), []byte("\n")), nil
}
func hash(v any) (string, error) {
	b, e := canonical(v)
	if e != nil {
		return "", e
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:]), nil
}
func match(a, b any) bool {
	switch x := a.(type) {
	case map[string]any:
		y, ok := b.(map[string]any)
		if !ok {
			return false
		}
		for k, v := range x {
			w, exists := y[k]
			if !exists || !match(v, w) {
				return false
			}
		}
		return true
	case []any:
		y, ok := b.([]any)
		if !ok || len(x) != len(y) {
			return false
		}
		for i := range x {
			if !match(x[i], y[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(a, b)
	}
}
func content(v any) string { s, _ := v.(string); return strings.Join(strings.Fields(s), " ") }
func loadCorpus(path string) (map[string]obj, error) {
	v, e := file(path)
	if e != nil {
		return nil, e
	}
	p, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("expected_nonempty_version_1_corpus")
	}
	n, ok := integer(p["version"])
	rows, arr := p["cases"].([]any)
	if !ok || n != 1 || !arr || len(rows) == 0 {
		return nil, errors.New("expected_nonempty_version_1_corpus")
	}
	cases := map[string]obj{}
	for _, row := range rows {
		c, ok := row.(map[string]any)
		if !ok {
			return nil, errors.New("corpus_cases_require_ids")
		}
		id, ok := c["id"].(string)
		if !ok || id == "" {
			return nil, errors.New("corpus_cases_require_ids")
		}
		if _, found := cases[id]; found {
			return nil, errors.New("duplicate_corpus_case_id")
		}
		cases[id] = c
	}
	return cases, nil
}
func loadGoldens(path string, cases map[string]obj) (map[string]obj, error) {
	v, e := file(path)
	if e != nil {
		return nil, e
	}
	p, ok := v.(map[string]any)
	if !ok {
		return nil, errors.New("expected_version_1_golden_cases")
	}
	n, ok := integer(p["version"])
	rows, arr := p["cases"].([]any)
	if !ok || n != 1 || !arr {
		return nil, errors.New("expected_version_1_golden_cases")
	}
	out := map[string]obj{}
	for _, row := range rows {
		g, ok := row.(map[string]any)
		if !ok || len(g) != 4 {
			return nil, errors.New("goldens_require_exact_fields")
		}
		for _, k := range []string{"id", "case_sha256", "evidence", "expected"} {
			if _, ok = g[k]; !ok {
				return nil, errors.New("goldens_require_exact_fields")
			}
		}
		id, ok := g["id"].(string)
		if !ok {
			return nil, errors.New("goldens_require_exact_fields")
		}
		if _, found := out[id]; found {
			return nil, errors.New("duplicate_golden_case_id")
		}
		c, exists := cases[id]
		if !exists {
			continue
		}
		digest, e := hash(c)
		if e != nil || g["case_sha256"] != digest {
			return nil, errors.New("golden_case_hash_mismatch")
		}
		evidence, ok := g["evidence"].(string)
		if !ok || strings.TrimSpace(evidence) == "" {
			return nil, errors.New("golden_requires_source_evidence")
		}
		expected, ok := g["expected"].(map[string]any)
		if !ok {
			return nil, errors.New("golden_requires_supported_expected_result")
		}
		for k := range expected {
			if k != "state" && k != "contains" && k != "not_contains" && k != "fields" && k != "media_contains" {
				return nil, errors.New("golden_requires_supported_expected_result")
			}
		}
		state, _ := expected["state"].(string)
		if state != "extracted" && state != "failed" && state != "resource_limit" {
			return nil, errors.New("golden_requires_supported_expected_result")
		}
		for _, k := range []string{"contains", "not_contains"} {
			if v, ok := expected[k]; ok {
				values, ok := v.([]any)
				if !ok {
					return nil, errors.New("golden_content_anchors_must_be_nonempty_text")
				}
				for _, value := range values {
					if str, ok := value.(string); !ok || str == "" {
						return nil, errors.New("golden_content_anchors_must_be_nonempty_text")
					}
				}
			}
		}
		if value, ok := expected["fields"]; ok {
			if _, ok = value.(map[string]any); !ok {
				return nil, errors.New("golden_fields_must_be_object")
			}
		}
		if value, ok := expected["media_contains"]; ok {
			media, ok := value.([]any)
			if !ok {
				return nil, errors.New("golden_media_contains_must_be_nonempty_object_list")
			}
			for _, item := range media {
				m, ok := item.(map[string]any)
				if !ok || len(m) == 0 {
					return nil, errors.New("golden_media_contains_must_be_nonempty_object_list")
				}
			}
		}
		if state == "extracted" && !hasAssertions(expected) {
			return nil, errors.New("successful_golden_requires_assertions")
		}
		out[id] = expected
	}
	if len(out) != len(cases) {
		return nil, errors.New("goldens_must_cover_every_corpus_case")
	}
	return out, nil
}
func hasAssertions(v obj) bool {
	for _, k := range []string{"contains", "fields", "media_contains"} {
		switch x := v[k].(type) {
		case []any:
			if len(x) > 0 {
				return true
			}
		case map[string]any:
			if len(x) > 0 {
				return true
			}
		}
	}
	return false
}
func loadReplay(path string) (map[key]obj, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 4096), 8*1024*1024)
	out := map[key]obj{}
	for scanner.Scan() {
		v, e := decode(scanner.Bytes())
		if e != nil {
			return nil, e
		}
		r, ok := v.(map[string]any)
		if !ok {
			return nil, errors.New("replay_requires_case_id_and_positive_cycle")
		}
		id, ok := r["case_id"].(string)
		if !ok {
			id, _ = r["id"].(string)
		}
		cycle, ok := integer(r["cycle"])
		if id == "" || !ok || cycle < 1 {
			return nil, errors.New("replay_requires_case_id_and_positive_cycle")
		}
		k := key{id, cycle}
		if _, found := out[k]; found {
			return nil, errors.New("duplicate_go_replay_record")
		}
		out[k] = r
	}
	if e = scanner.Err(); e != nil {
		return nil, e
	}
	if len(out) == 0 {
		return nil, errors.New("go_replay_must_not_be_empty")
	}
	return out, nil
}
func matches(expected, result obj) bool {
	if expected["state"] != result["state"] {
		return false
	}
	body := content(result["content"])
	for _, k := range []string{"contains", "not_contains"} {
		values, _ := expected[k].([]any)
		for _, v := range values {
			found := strings.Contains(body, content(v))
			if (k == "contains" && !found) || (k == "not_contains" && found) {
				return false
			}
		}
	}
	fields, _ := expected["fields"].(map[string]any)
	for k, v := range fields {
		x, ok := result[k]
		if !ok || !match(v, x) {
			return false
		}
	}
	wanted, _ := expected["media_contains"].([]any)
	items, _ := result["media"].([]any)
	for _, value := range wanted {
		found := false
		for _, item := range items {
			if match(value, item) {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}
func Check(replay, corpus, golden string, cycles int) (Report, error) {
	var out Report
	if cycles < 1 {
		return out, errors.New("cycles_must_be_positive")
	}
	cases, e := loadCorpus(corpus)
	if e != nil {
		return out, e
	}
	goldens, e := loadGoldens(golden, cases)
	if e != nil {
		return out, e
	}
	records, e := loadReplay(replay)
	if e != nil {
		return out, e
	}
	if len(records) != len(cases)*cycles {
		return out, errors.New("go_replay_case_or_cycle_set_mismatch")
	}
	ids := make([]string, 0, len(cases))
	for id := range cases {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	failures := []Failure{}
	for _, id := range ids {
		for cycle := 1; cycle <= cycles; cycle++ {
			r, ok := records[key{id, cycle}]
			if !ok {
				return out, errors.New("go_replay_case_or_cycle_set_mismatch")
			}
			result, ok := r["result"].(map[string]any)
			passed, _ := r["passed"].(bool)
			failure, exists := r["failures"]
			if !ok || !passed || (exists && truthy(failure)) || !matches(goldens[id], result) {
				failures = append(failures, Failure{id, cycle})
			}
		}
	}
	gate := "passed"
	if len(failures) > 0 {
		gate = "requires_review"
	}
	return Report{len(cases), cycles, len(records), failures, gate, "offline Go extractor replay checked against frozen source-backed expectations"}, nil
}
func truthy(v any) bool {
	switch x := v.(type) {
	case nil:
		return false
	case bool:
		return x
	case string:
		return x != ""
	case []any:
		return len(x) > 0
	case map[string]any:
		return len(x) > 0
	case json.Number:
		return x != "0"
	}
	return true
}
