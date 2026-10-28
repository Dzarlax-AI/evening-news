package terminalevents

import (
	"encoding/json"
	"regexp"
)

var codePattern = regexp.MustCompile(`^[a-z0-9_-]{1,40}$`)

type sourceRecord struct {
	ID                  int64
	Type, Config, State string
	Code                *string
}
type observation struct {
	Sources           []sourceObservation
	Successful, Total int
	Warning           *sourceObservation
}

func intValue(m map[string]json.RawMessage, key string, max int) (int, bool) {
	var n int
	raw, ok := m[key]
	if !ok || json.Unmarshal(raw, &n) != nil || string(raw) == "null" || n < 0 || n > max {
		return 0, false
	}
	return n, true
}
func classifyObservation(jobState, runState string, jobRaw, runRaw []byte, records []sourceRecord) *observation {
	if jobState != "succeeded" || runState != "succeeded" || len(records) == 0 || len(records) > 64 {
		return nil
	}
	var job, run map[string]json.RawMessage
	if json.Unmarshal(jobRaw, &job) != nil || json.Unmarshal(runRaw, &run) != nil || len(job) == 0 || len(run) == 0 {
		return nil
	}
	goCount, ok := intValue(run, "errors_count", 1000000000)
	if !ok {
		return nil
	}
	jobCount, ok := intValue(job, "errors_count", 1000000000)
	if !ok {
		return nil
	}
	completed, ok := intValue(run, "sources_completed", 64)
	if !ok || completed != len(records) {
		return nil
	}
	total, ok := intValue(run, "sources_total", 64)
	if !ok || total != len(records) {
		return nil
	}
	outcomes := map[string]json.RawMessage{}
	if raw, exists := run["source_outcomes"]; exists {
		if json.Unmarshal(raw, &outcomes) != nil || outcomes == nil {
			return nil
		}
	} else if goCount != 0 {
		return nil
	}
	if len(outcomes) > 64 {
		return nil
	}
	counts := map[string]int{}
	sum := 0
	for k := range outcomes {
		if !codePattern.MatchString(k) {
			return nil
		}
		n, ok := intValue(outcomes, k, 1000)
		if !ok {
			return nil
		}
		counts[k] = n
		sum += n
	}
	if sum != goCount {
		return nil
	}
	seen := map[int64]bool{}
	actual := map[string]int{}
	items := []sourceObservation{}
	for _, r := range records {
		if r.ID <= 0 || seen[r.ID] || sourceNames[r.Type] == "" || !hashPattern.MatchString(r.Config) {
			return nil
		}
		seen[r.ID] = true
		item := sourceObservation{ID: r.ID, Type: r.Type, Config: r.Config}
		if r.State == "succeeded" && r.Code == nil {
			item.State = "healthy"
		} else {
			if !oneOf(r.State, "partial", "failed") || r.Code == nil || !codePattern.MatchString(*r.Code) {
				return nil
			}
			item.State = "warning"
			item.Code = *r.Code
			actual[item.Code]++
		}
		items = append(items, item)
	}
	if len(actual) != len(counts) || jobCount != goCount {
		return nil
	}
	for k, n := range actual {
		if counts[k] != n {
			return nil
		}
	}
	status := "success"
	if goCount > 0 {
		status = "warning"
	}
	var a, b string
	if json.Unmarshal(job["status"], &a) != nil || json.Unmarshal(run["status"], &b) != nil || a != status || b != status {
		return nil
	}
	out := &observation{Sources: items, Successful: len(items) - goCount, Total: len(items)}
	if goCount == 0 {
		return out
	}
	if goCount != 1 {
		return nil
	}
	for _, item := range items {
		if item.State == "warning" {
			if !oneOf(item.Code, "transport_failed", "deadline_exceeded", "upstream_unavailable", "upstream_rate_limited") {
				return nil
			}
			v := item
			out.Warning = &v
			return out
		}
	}
	return nil
}
