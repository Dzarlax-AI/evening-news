package processing

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type golden struct {
	Version  int            `json:"version"`
	Case     string         `json:"case"`
	Article  map[string]any `json:"article"`
	Scenario struct {
		Pipeline     string         `json:"pipeline"`
		Analysis     map[string]any `json:"analysis"`
		Metadata     map[string]any `json:"metadata"`
		Extracted    map[string]any `json:"extracted"`
		FilterResult any            `json:"filter_result"`
		Attempts     int            `json:"attempts"`
		TextLimit    int            `json:"text_limit"`
	} `json:"scenario"`
	Expected struct {
		Outcome struct {
			State  string         `json:"state"`
			Error  any            `json:"error"`
			Fields map[string]any `json:"fields"`
		} `json:"outcome"`
		Persistence struct {
			State     string `json:"processing_state"`
			Attempts  int    `json:"processing_attempts"`
			Next      any    `json:"next_attempt_after_seconds"`
			Error     any    `json:"last_processing_error"`
			Processed bool   `json:"processed"`
		} `json:"persistence"`
	} `json:"expected"`
}

func TestFrozenPythonProcessingCorpus(t *testing.T) {
	files, err := filepath.Glob("../../../tests/fixtures/article-processing-v1/*.json")
	if err != nil {
		t.Fatal(err)
	}
	if len(files) == 0 {
		t.Fatal("Python golden corpus is missing")
	}
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	for _, path := range files {
		t.Run(filepath.Base(path), func(t *testing.T) {
			b, e := os.ReadFile(path)
			if e != nil {
				t.Fatal(e)
			}
			var g golden
			if e = json.Unmarshal(b, &g); e != nil {
				t.Fatal(e)
			}
			if g.Version != 1 {
				t.Fatalf("unsupported fixture version %d", g.Version)
			}
			content, _ := g.Article["content"].(string)
			attempt := g.Scenario.Attempts
			if attempt < 1 {
				attempt = 1
			}
			summaryDone, _ := g.Article["summary_processed"].(bool)
			categoryDone, _ := g.Article["category_processed"].(bool)
			adDone, _ := g.Article["ad_processed"].(bool)
			var o Outcome
			if adDone && g.Article["is_advertisement"] == true {
				o = Outcome{State: StateRejected, Error: "advertisement", Fields: map[string]any{}}
			} else if g.Scenario.Pipeline == "force_refresh" {
				fresh, _ := g.Scenario.Extracted["content"].(string)
				if fresh == "" || IsChallengeText(fresh) {
					o = Outcome{State: StateRetryWait, Error: "extraction_challenge", Fields: map[string]any{}}
				} else if g.Scenario.TextLimit > 0 && EnforceTextLimit(fresh, g.Scenario.TextLimit) != nil {
					o = Outcome{State: StateRejected, Error: "resource_limit", Fields: map[string]any{}}
				} else {
					o = AnalyzeOutcome(g.Scenario.Analysis, false, false, false)
					if len(fresh) > len(content) {
						o.Fields["content"] = fresh
					}
				}
			} else if g.Scenario.Pipeline == "summary_by_source_type" {
				if filterRejected(g.Scenario.FilterResult) {
					o = Outcome{State: StateRejected, Error: "smart_filter_rejected", Fields: map[string]any{}}
				} else if g.Scenario.Metadata != nil {
					if code, _ := g.Scenario.Metadata["_processing_error"].(string); code != "" {
						fields := map[string]any{}
						improved, _ := g.Scenario.Metadata["content"].(string)
						if len(improved) > len(content) {
							fields["content"] = improved
						}
						for _, key := range []string{"image_url", "media_files"} {
							if v := g.Scenario.Metadata[key]; v != nil {
								if key == "published_at" {
									if raw, ok := v.(string); ok {
										if d, e := time.Parse("2006-01-02", raw); e == nil {
											v = d.Format("2006-01-02T15:04:05")
										}
									}
								}
								fields[key] = v
							}
						}
						if raw, ok := g.Scenario.Metadata["publication_date"].(string); ok {
							if d, parseErr := time.Parse("2006-01-02", raw); parseErr == nil {
								fields["published_at"] = d.Format("2006-01-02T15:04:05")
							}
						}
						o = Outcome{State: StateRetryWait, Fields: fields, Error: code}
					} else {
						o = AnalyzeOutcome(g.Scenario.Analysis, summaryDone, categoryDone, adDone)
					}
				} else {
					o = AnalyzeOutcome(g.Scenario.Analysis, summaryDone, categoryDone, adDone)
				}
			} else if g.Scenario.TextLimit > 0 && EnforceTextLimit(content, g.Scenario.TextLimit) != nil {
				o = Outcome{State: StateRejected, Error: "resource_limit", Fields: map[string]any{}}
			} else if IsChallengeText(content) {
				o = Outcome{State: StateRetryWait, Error: "extraction_challenge", Fields: map[string]any{}}
			} else {
				o = AnalyzeOutcome(g.Scenario.Analysis, summaryDone, categoryDone, adDone)
			}
			if o.State != g.Expected.Outcome.State || o.Error != stringValue(g.Expected.Outcome.Error) {
				t.Fatalf("outcome mismatch got %s/%s want %s/%v", o.State, o.Error, g.Expected.Outcome.State, g.Expected.Outcome.Error)
			}
			if !reflect.DeepEqual(normalizeJSON(o.Fields), normalizeJSON(g.Expected.Outcome.Fields)) {
				t.Fatalf("fields mismatch\ngot: %#v\nwant:%#v", normalizeJSON(o.Fields), normalizeJSON(g.Expected.Outcome.Fields))
			}
			retry := any(nil)
			if v, ok := g.Scenario.Analysis["_retry_after_seconds"]; ok {
				retry = v
			}
			if v, ok := g.Scenario.Metadata["_retry_after_seconds"]; ok {
				retry = v
			}
			o.RetryAfterSeconds = retry
			p := o.Persist(attempt, now, Limits{MaxAttempts: 3, Initial: 30 * time.Minute, Later: 6 * time.Hour})
			if p.State != g.Expected.Persistence.State || p.Attempts != g.Expected.Persistence.Attempts || p.Processed != g.Expected.Persistence.Processed || stringPointer(p.Error) != stringValue(g.Expected.Persistence.Error) {
				t.Fatalf("persistence mismatch: %#v expected %#v", p, g.Expected.Persistence)
			}
			if p.NextAttemptAt != nil && g.Expected.Persistence.Next != nil {
				want, _ := g.Expected.Persistence.Next.(float64)
				if p.NextAttemptAt.Sub(now).Seconds() != want {
					t.Fatalf("retry delay %v want %v", p.NextAttemptAt.Sub(now).Seconds(), want)
				}
			} else if (p.NextAttemptAt == nil) != (g.Expected.Persistence.Next == nil) {
				t.Fatal("retry timestamp presence mismatch")
			}

		})
	}
}
func stringValue(v any) string {
	if v == nil {
		return ""
	}
	s, _ := v.(string)
	return s
}
func stringPointer(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
func normalizeJSON(v any) any {
	b, _ := json.Marshal(v)
	var out any
	_ = json.Unmarshal(b, &out)
	return out
}

func filterRejected(v any) bool {
	switch x := v.(type) {
	case []any:
		if len(x) > 0 {
			if nested, ok := x[0].([]any); ok {
				return filterRejected(nested)
			}
			b, _ := x[0].(bool)
			return !b
		}
	case bool:
		return !x
	}
	return false
}
