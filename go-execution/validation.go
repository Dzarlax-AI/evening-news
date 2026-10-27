package execution

import (
	"bytes"
	"encoding/json"
	"io"
	"math"
	"strconv"
	"strings"
	"time"
)

func rawInt(raw json.RawMessage) (int64, bool) {
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&value) != nil {
		return 0, false
	}
	n, ok := value.(json.Number)
	if !ok {
		return 0, false
	}
	var trailing any
	if d.Decode(&trailing) != io.EOF {
		return 0, false
	}
	v, e := strconv.ParseInt(n.String(), 10, 64)
	return v, e == nil && !strings.ContainsAny(n.String(), ".eE+")
}

func validISODateTime(value string) bool {
	if value == "" {
		return false
	}
	layouts := []string{
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05.000000",
		"2006-01-02T15:04:05-07:00",
		"2006-01-02T15:04:05.000000-07:00",
		"2006-01-02T15:04:05-07:00:00",
		"2006-01-02T15:04:05.000000-07:00:00",
	}
	for _, layout := range layouts {
		parsed, err := time.Parse(layout, value)
		if err != nil || parsed.Format(layout) != value {
			continue
		}
		if strings.Contains(layout, ".000000") && parsed.Nanosecond() == 0 {
			continue
		}
		if strings.Contains(layout, "-07:00:00") {
			_, offset := parsed.Zone()
			if offset%60 == 0 {
				continue
			}
		}
		return true
	}
	return false
}

func validatePayload(kind string, payload json.RawMessage) (json.RawMessage, error) {
	var p map[string]json.RawMessage
	if len(payload) > 4096 || json.Unmarshal(payload, &p) != nil || p == nil {
		return nil, ErrInvalidPayload
	}
	allowed := map[string]map[string]bool{
		"news_processing": {"schedule_id": true, "scheduled_for": true, "timeout_seconds": true}, "news_digest": {"schedule_id": true, "scheduled_for": true, "run_processing": true, "send_telegram": true, "timeout_seconds": true}, "telegram_digest": {"schedule_id": true, "scheduled_for": true, "timeout_seconds": true}, "backup": {"schedule_id": true, "scheduled_for": true, "timeout_seconds": true}, "reprocess_failed": {"schedule_id": true, "scheduled_for": true, "limit": true, "dry_run": true, "timeout_seconds": true}, "article_reprocess": {"article_ids": true, "force": true, "reextract_media": true}, "summaries_generate": {}, "source_test": {"source_id": true},
	}[kind]
	if allowed == nil {
		return nil, ErrInvalidPayload
	}
	for k := range p {
		if !allowed[k] {
			return nil, ErrInvalidPayload
		}
	}
	for _, k := range []string{"schedule_id", "scheduled_for"} {
		_, a := p["schedule_id"]
		_, b := p["scheduled_for"]
		if a != b {
			return nil, ErrInvalidPayload
		}
		_ = k
	}
	if raw, ok := p["schedule_id"]; ok {
		n, yes := rawInt(raw)
		if !yes || n <= 0 {
			return nil, ErrInvalidPayload
		}
		var stamp string
		if json.Unmarshal(p["scheduled_for"], &stamp) != nil || !validISODateTime(stamp) {
			return nil, ErrInvalidPayload
		}
	}
	if raw, ok := p["timeout_seconds"]; ok {
		n, yes := rawInt(raw)
		if !yes || n < 1 || n > 86400 {
			return nil, ErrInvalidPayload
		}
	}
	for _, k := range []string{"run_processing", "send_telegram", "dry_run", "force", "reextract_media"} {
		if raw, ok := p[k]; ok {
			var value any
			if json.Unmarshal(raw, &value) != nil {
				return nil, ErrInvalidPayload
			}
			if _, ok := value.(bool); !ok {
				return nil, ErrInvalidPayload
			}
		}
	}
	for _, k := range []string{"limit", "source_id"} {
		if raw, ok := p[k]; ok {
			n, yes := rawInt(raw)
			if !yes || n <= 0 || k == "limit" && n > 100 {
				return nil, ErrInvalidPayload
			}
		}
	}
	if kind == "article_reprocess" {
		var ids []json.RawMessage
		if json.Unmarshal(p["article_ids"], &ids) != nil || len(ids) < 1 || len(ids) > 100 {
			return nil, ErrInvalidPayload
		}
		seen := map[int64]bool{}
		vals := make([]int64, 0, len(ids))
		for _, v := range ids {
			n, ok := rawInt(v)
			if !ok || n <= 0 || seen[n] {
				return nil, ErrInvalidPayload
			}
			seen[n] = true
			vals = append(vals, n)
		} // normalize sort order
		for i := 0; i < len(vals); i++ {
			for j := i + 1; j < len(vals); j++ {
				if vals[j] < vals[i] {
					vals[i], vals[j] = vals[j], vals[i]
				}
			}
		}
		p["article_ids"], _ = json.Marshal(vals)
		if _, ok := p["force"]; !ok {
			p["force"] = json.RawMessage("true")
		}
		if _, ok := p["reextract_media"]; !ok {
			p["reextract_media"] = json.RawMessage("false")
		}
	}
	if kind == "source_test" {
		if _, ok := p["source_id"]; !ok {
			return nil, ErrInvalidPayload
		}
	}
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	if enc.Encode(p) != nil {
		return nil, ErrInvalidPayload
	}
	out := bytes.TrimSpace(b.Bytes())
	return out, nil
}

func safeResult(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if len(raw) > MaxResultBytes {
		return nil, ErrInvalidPayload
	}
	m, ok := strictRawObject(raw)
	if !ok || m == nil {
		return nil, ErrInvalidPayload
	}
	allowed := map[string]bool{"status": true, "success": true, "settled": true, "retryable": true, "articles_fetched": true, "articles_processed": true, "articles_summarized": true, "articles_categorized": true, "articles_requeued": true, "articles_rejected": true, "articles_retrying": true, "articles_exhausted": true, "total_candidates": true, "found_candidates": true, "processed": true, "improved": true, "summaries_generated": true, "parts_sent": true, "errors_count": true, "duration_seconds": true, "backup_bytes": true, "requeued_count": true, "dry_run": true, "connection_ok": true, "article_outcomes": true, "article_outcomes_total": true, "article_outcomes_omitted": true}
	if err := validateArticleOutcomes(m); err != nil {
		return nil, err
	}
	for k, v := range m {
		if !allowed[k] {
			return nil, ErrInvalidPayload
		}
		if k == "status" {
			var st string
			if json.Unmarshal(v, &st) != nil || !strings.Contains("|success|warning|deferred|dry_run|completed|rejected|exhausted|retry_wait|incomplete|settled|busy|", "|"+st+"|") {
				return nil, ErrInvalidPayload
			}
			continue
		}
		if k == "article_outcomes" {
			// The array was validated against the closed item schema above.
			continue
		}
		var x any
		d := json.NewDecoder(bytes.NewReader(v))
		d.UseNumber()
		if d.Decode(&x) != nil {
			return nil, ErrInvalidPayload
		}
		if strings.Contains("|success|settled|retryable|dry_run|connection_ok|", "|"+k+"|") {
			if _, ok := x.(bool); !ok {
				return nil, ErrInvalidPayload
			}
			continue
		}
		z, ok := x.(json.Number)
		if !ok {
			return nil, ErrInvalidPayload
		}
		f, e := z.Float64()
		if e != nil || math.IsNaN(f) || math.IsInf(f, 0) || f < 0 || f > 1e9 {
			return nil, ErrInvalidPayload
		}
	}
	return raw, nil
}

// strictRawObject decodes one JSON object while rejecting duplicate keys and
// trailing values. Result projections are persisted receipts, so ambiguous
// duplicate fields must not change how readers interpret the same bytes.
func strictRawObject(raw []byte) (map[string]json.RawMessage, bool) {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, false
	}
	object := make(map[string]json.RawMessage)
	for d.More() {
		token, err = d.Token()
		key, isString := token.(string)
		if err != nil || !isString {
			return nil, false
		}
		if _, exists := object[key]; exists {
			return nil, false
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil, false
		}
		object[key] = value
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') {
		return nil, false
	}
	var trailing any
	if err = d.Decode(&trailing); err != io.EOF {
		return nil, false
	}
	return object, true
}

func validateArticleOutcomes(result map[string]json.RawMessage) error {
	raw, hasOutcomes := result["article_outcomes"]
	_, hasTotal := result["article_outcomes_total"]
	_, hasOmitted := result["article_outcomes_omitted"]
	if !hasOutcomes {
		if hasTotal || hasOmitted {
			return ErrInvalidPayload
		}
		return nil
	}
	if !hasTotal || !hasOmitted {
		return ErrInvalidPayload
	}
	var outcomes []json.RawMessage
	if json.Unmarshal(raw, &outcomes) != nil || outcomes == nil || len(outcomes) > 100 {
		return ErrInvalidPayload
	}
	seen := make(map[int64]bool, len(outcomes))
	for _, rawOutcome := range outcomes {
		outcome, ok := strictRawObject(rawOutcome)
		if !ok || len(outcome) != 3 {
			return ErrInvalidPayload
		}
		for key := range outcome {
			if key != "article_id" && key != "state" && key != "reason" {
				return ErrInvalidPayload
			}
		}
		id, ok := rawInt(outcome["article_id"])
		if !ok || id <= 0 || id > math.MaxInt32 || seen[id] {
			return ErrInvalidPayload
		}
		seen[id] = true
		var state, reason string
		if json.Unmarshal(outcome["state"], &state) != nil ||
			!strings.Contains("|completed|rejected|exhausted|pending|retry_wait|processing|", "|"+state+"|") ||
			json.Unmarshal(outcome["reason"], &reason) != nil ||
			!strings.Contains("|advertisement|resource_limit|category_mapping_failed|missing_summary|smart_filter_rejected|extraction_challenge|provider_timeout|other|", "|"+reason+"|") {
			return ErrInvalidPayload
		}
	}
	total, ok := rawInt(result["article_outcomes_total"])
	if !ok || total < int64(len(outcomes)) || total > 100 {
		return ErrInvalidPayload
	}
	omitted, ok := rawInt(result["article_outcomes_omitted"])
	if !ok || omitted != total-int64(len(outcomes)) {
		return ErrInvalidPayload
	}
	return nil
}

func mayDeliver(kind string, p json.RawMessage) bool {
	if kind == "telegram_digest" {
		return true
	}
	if kind != "news_digest" {
		return false
	}
	var m map[string]json.RawMessage
	_ = json.Unmarshal(p, &m)
	var b bool
	return json.Unmarshal(m["send_telegram"], &b) != nil || b
}
