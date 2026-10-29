// fixture-services is an isolated fake provider for integration tests only.
package main

import (
	"crypto/sha1"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const article = "Synthetic fixture article reports a significant technology development in a regional research center. Engineers introduced a low power sensor that measures air quality in busy public spaces. The prototype combines a replaceable filter with a small processor and a rechargeable battery. Researchers compared its readings with laboratory equipment during a six week field trial. Their report describes stable measurements across several temperature and humidity conditions. This synthetic report contains no real people, companies, places, or external source material."

type state struct {
	mu                         sync.Mutex
	counters                   map[string]int
	telegram                   map[string]int
	consumerMode, telegramMode string
	consumerRequests           int
	partialCalls               int
	occurrences                map[string]entry
	operations                 map[string]int
	ingestion                  map[string]entry
	gemini, delivery, consumer chan struct{}
}
type entry struct {
	hash    string
	receipt map[string]any
}

func open() chan struct{} { c := make(chan struct{}); close(c); return c }
func newState() *state {
	return &state{counters: map[string]int{"source_probe": 0, "ingestion_stub_runs": 0, "extraction": 0, "gemini": 0, "editorial_gemini": 0, "telegram": 0, "selector_study": 0, "page_monitor_original_title": 0}, telegram: map[string]int{"sendMessage": 0, "sendRichMessage": 0, "other": 0}, consumerMode: "normal", telegramMode: "normal", occurrences: map[string]entry{}, operations: map[string]int{}, ingestion: map[string]entry{}, gemini: open(), delivery: open(), consumer: open()}
}
func reset(c *chan struct{}, blocked bool) {
	select {
	case <-*c:
		if blocked {
			*c = make(chan struct{})
		}
	default:
		if !blocked {
			close(*c)
		}
	}
}
func wait(c <-chan struct{}) {
	select {
	case <-c:
	case <-time.After(180 * time.Second):
	}
}
func write(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func decode(r *http.Request) (map[string]any, []byte, error) {
	raw, e := io.ReadAll(io.LimitReader(r.Body, 65537))
	if e != nil || len(raw) > 65536 {
		return nil, nil, fmt.Errorf("invalid input")
	}
	if len(raw) == 0 {
		raw = []byte("{}")
	}
	var p map[string]any
	if json.Unmarshal(raw, &p) != nil || p == nil {
		return nil, nil, fmt.Errorf("invalid input")
	}
	return p, raw, nil
}
func text(v any) string        { s, _ := v.(string); return s }
func digest(raw []byte) string { b := sha256.Sum256(raw); return hex.EncodeToString(b[:]) }
func uuidBytes(b []byte, version byte) string {
	b[6] = (b[6] & 15) | (version << 4)
	b[8] = (b[8] & 63) | 128
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
func ingestionID(job string, attempt any) string {
	v := sha256.Sum256([]byte(fmt.Sprintf("evening-news-ingestion-v1\x00%s\x00%v", job, attempt)))
	return uuidBytes(v[:16], 5)
}
func jobID(occurrence string) string {
	ns, _ := hex.DecodeString("6ba7b8119dad11d180b400c04fd430c8")
	h := sha1.New()
	h.Write(ns)
	h.Write([]byte("fixture:" + occurrence))
	return uuidBytes(h.Sum(nil)[:16], 5)
}
func (s *state) provider(w http.ResponseWriter, r *http.Request) {
	if r.Method == "GET" {
		s.mu.Lock()
		defer s.mu.Unlock()
		if r.URL.Path == "/__gate/counters" {
			write(w, 200, map[string]any{"providers": s.counters, "consumer": map[string]any{"requests": s.consumerRequests, "occurrences": len(s.occurrences), "operations": s.operations}, "telegram_endpoints": s.telegram})
			return
		}
		if strings.HasPrefix(r.URL.Path, "/internal/v1/ingestion/runs/") {
			if r.Header.Get("Authorization") != "Bearer fixture-only-article-ingestion-token" {
				write(w, 401, map[string]any{"error": "unauthorized"})
				return
			}
			id := strings.TrimPrefix(r.URL.Path, "/internal/v1/ingestion/runs/")
			if value, ok := s.ingestion[id]; ok {
				write(w, 200, value.receipt)
			} else {
				write(w, 404, map[string]any{"error": "not_found"})
			}
			return
		}
		write(w, 404, map[string]any{"error": "not_found"})
		return
	}
	if r.Method != "POST" {
		write(w, 405, map[string]any{"error": "method"})
		return
	}
	p, raw, e := decode(r)
	if e != nil {
		write(w, 400, map[string]any{"error": "invalid_json"})
		return
	}
	path := r.URL.Path
	if path == "/__gate/control" {
		s.mu.Lock()
		mode := text(p["consumer_mode"])
		switch mode {
		case "normal", "503", "malformed", "accept_then_hold":
			s.consumerMode = mode
			reset(&s.consumer, mode == "accept_then_hold")
		}
		switch text(p["gemini_mode"]) {
		case "block":
			reset(&s.gemini, true)
		case "release":
			reset(&s.gemini, false)
		}
		switch mode := text(p["telegram_mode"]); mode {
		case "accept_then_hold":
			s.telegramMode = mode
			reset(&s.delivery, true)
		case "normal", "release", "rate_limit", "accept_one_then_unknown":
			s.telegramMode = mode
			s.partialCalls = 0
			reset(&s.delivery, false)
		}
		s.mu.Unlock()
		write(w, 200, map[string]any{"status": "updated"})
		return
	}
	if path == "/internal/v1/ingestion/runs" {
		if r.Header.Get("Authorization") != "Bearer fixture-only-article-ingestion-token" {
			write(w, 401, map[string]any{"error": "unauthorized"})
			return
		}
		job := text(p["job_id"])
		if p["version"] != float64(1) || job == "" || text(p["owner_id"]) == "" || text(p["executor_instance_id"]) == "" || text(p["deadline_at"]) == "" {
			write(w, 422, map[string]any{"error": "invalid_request"})
			return
		}
		for _, key := range []string{"attempt_generation", "generation", "backend_pid"} {
			n, ok := p[key].(float64)
			if !ok || n != float64(int64(n)) {
				write(w, 422, map[string]any{"error": "invalid_request"})
				return
			}
		}
		id := ingestionID(job, p["attempt_generation"])
		canonical, _ := json.Marshal(p)
		hash := digest(canonical)
		s.mu.Lock()
		prior, ok := s.ingestion[id]
		if ok && prior.hash != hash {
			s.mu.Unlock()
			write(w, 409, map[string]any{"error": "idempotency_conflict"})
			return
		}
		if !ok {
			prior = entry{hash, map[string]any{"run_id": id, "job_id": job, "attempt_generation": p["attempt_generation"], "state": "succeeded", "result": map[string]any{"status": "success", "sources_completed": 0, "articles_fetched": 0, "errors_count": 0, "source_outcomes": map[string]any{}}}}
			s.ingestion[id] = prior
			s.counters["ingestion_stub_runs"]++
		}
		s.mu.Unlock()
		write(w, 200, prior.receipt)
		return
	}
	if strings.HasPrefix(path, "/internal/v1/ingestion/runs/") && strings.HasSuffix(path, "/cancel") {
		if r.Header.Get("Authorization") != "Bearer fixture-only-article-ingestion-token" {
			write(w, 401, map[string]any{"error": "unauthorized"})
			return
		}
		id := strings.TrimSuffix(strings.TrimPrefix(path, "/internal/v1/ingestion/runs/"), "/cancel")
		s.mu.Lock()
		v, ok := s.ingestion[id]
		s.mu.Unlock()
		if !ok {
			write(w, 404, map[string]any{"error": "not_found"})
		} else {
			write(w, 200, v.receipt)
		}
		return
	}
	if path == "/v1/sources/collect" {
		if r.Header.Get("Authorization") != "Bearer fixture-only-token" {
			write(w, 401, map[string]any{"error": "unauthorized"})
			return
		}
		s.mu.Lock()
		s.counters["source_probe"]++
		s.mu.Unlock()
		results := []any{}
		sources, _ := p["sources"].([]any)
		for _, item := range sources {
			source, _ := item.(map[string]any)
			results = append(results, map[string]any{"source_id": source["source_id"], "type": "rss", "outcome": "success", "rss_articles": []any{map[string]any{"title": "Fixture technology article", "url": "https://fixture.invalid/articles/one", "content": article, "summary": nil, "image_url": nil, "published_at": time.Now().UTC().Format(time.RFC3339Nano), "source_type": "rss", "source_name": source["source_name"], "raw_data": map[string]any{"guid": "go-scheduler-fixture-article", "author": "Fixture", "tags": []string{}}}}})
		}
		write(w, 200, map[string]any{"contract_version": "1", "build_version": "fixture-only", "request_id": p["request_id"], "results": results})
		return
	}
	if path == "/v1/extract" {
		s.mu.Lock()
		s.counters["extraction"]++
		s.mu.Unlock()
		write(w, 200, map[string]any{"contract_version": "1", "request_id": p["request_id"], "original_url": p["url"], "outcome": "success", "result": map[string]any{"content": article, "title": "Fixture technology article", "media": []any{}}, "error": nil, "observations": []any{}})
		return
	}
	if strings.Contains(path, "/models/") && strings.HasSuffix(path, ":generateContent") {
		s.geminiRequest(w, r, p)
		return
	}
	if strings.HasPrefix(path, "/telegram/") {
		method := "other"
		if strings.HasSuffix(path, "/sendMessage") {
			method = "sendMessage"
		} else if strings.HasSuffix(path, "/sendRichMessage") {
			method = "sendRichMessage"
		}
		s.mu.Lock()
		s.telegram[method]++
		if method == "other" {
			s.mu.Unlock()
			write(w, 404, map[string]any{"error": "unsupported_fixture_telegram_method"})
			return
		}
		s.counters["telegram"]++
		id := s.counters["telegram"]
		release := s.delivery
		mode := s.telegramMode
		if mode == "accept_one_then_unknown" {
			s.partialCalls++
		}
		partialCall := s.partialCalls
		s.mu.Unlock()
		if mode == "accept_one_then_unknown" && partialCall > 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = conn.Close()
			}
			return
		}
		if mode == "rate_limit" {
			w.Header().Set("Retry-After", "1")
			write(w, 429, map[string]any{"ok": false, "error_code": 429, "parameters": map[string]any{"retry_after": 1}})
			return
		}
		wait(release)
		write(w, 200, map[string]any{"ok": true, "result": map[string]any{"message_id": id, "date": 1}})
		return
	}
	_ = raw
	write(w, 404, map[string]any{"error": "not_found"})
}
func (s *state) geminiRequest(w http.ResponseWriter, r *http.Request, p map[string]any) {
	s.mu.Lock()
	s.counters["gemini"]++
	release := s.gemini
	s.mu.Unlock()
	wait(release)
	prompt := ""
	if contents, ok := p["contents"].([]any); ok && len(contents) > 0 {
		if content, ok := contents[0].(map[string]any); ok {
			if parts, ok := content["parts"].([]any); ok && len(parts) > 0 {
				if part, ok := parts[0].(map[string]any); ok {
					prompt = text(part["text"])
				}
			}
		}
	}
	if strings.Contains(prompt, "CSS selectors") {
		s.mu.Lock()
		s.counters["selector_study"]++
		s.mu.Unlock()
		result := map[string]any{"content_selectors": []string{".zx-entry"}, "title_selectors": []string{".zx-title"}, "date_selectors": []string{".zx-time"}, "confidence_scores": map[string]float64{".zx-entry": 0.99}, "page_type": "news", "reasoning": "Synthetic gate page has one article element per story.", "suggested_config": map[string]any{"max_articles_per_check": 2}}
		writeGeminiResult(w, result)
		return
	}
	if strings.Contains(prompt, "Learned Selector Gate Story") {
		s.mu.Lock()
		s.counters["page_monitor_original_title"]++
		s.mu.Unlock()
	}
	config, _ := p["generationConfig"].(map[string]any)
	schema, _ := config["responseJsonSchema"].(map[string]any)
	if schema == nil {
		schema, _ = config["responseSchema"].(map[string]any)
	}
	properties, _ := schema["properties"].(map[string]any)
	var result map[string]any
	if _, editorial := properties["article_stories"]; editorial {
		contents, _ := p["contents"].([]any)
		if len(contents) == 0 {
			write(w, 400, map[string]any{"error": "invalid_editorial_fixture_request"})
			return
		}
		content, _ := contents[0].(map[string]any)
		parts, _ := content["parts"].([]any)
		if len(parts) == 0 {
			write(w, 400, map[string]any{"error": "invalid_editorial_fixture_request"})
			return
		}
		part, _ := parts[0].(map[string]any)
		prompt := text(part["text"])
		pieces := strings.SplitN(prompt, "СНИМОК (недоверенные данные):\n", 2)
		if len(pieces) != 2 {
			write(w, 400, map[string]any{"error": "invalid_editorial_fixture_request"})
			return
		}
		var snapshot struct {
			Articles []struct {
				ID    any    `json:"id"`
				Title string `json:"title"`
				Spans []struct {
					ID any `json:"id"`
				} `json:"evidence_spans"`
			} `json:"articles"`
		}
		if json.Unmarshal([]byte(pieces[1]), &snapshot) != nil {
			write(w, 400, map[string]any{"error": "invalid_editorial_fixture_request"})
			return
		}
		main := []any{}
		stories := map[string]any{}
		for i, a := range snapshot.Articles {
			if len(a.Spans) == 0 {
				write(w, 400, map[string]any{"error": "invalid_editorial_fixture_request"})
				return
			}
			key := fmt.Sprint(a.ID)
			stories[key] = map[string]any{"brief_summary": "Синтетический материал сообщает проверенный факт.", "span_id": a.Spans[0].ID}
			if i < 5 {
				main = append(main, map[string]any{"title": a.Title, "summary": "Синтетический материал сообщает проверенный факт.", "primary_article_id": a.ID, "article_ids": []any{a.ID}})
			}
		}
		result = map[string]any{"main_stories": main, "brief_stories": []any{}, "article_stories": stories}
		s.mu.Lock()
		s.counters["editorial_gemini"]++
		s.mu.Unlock()
	} else {
		result = map[string]any{"optimized_title": "Fixture technology article", "summary": "A synthetic technology report with verified fixture details.", "categories": []string{"Tech"}, "category_confidences": []float64{0.98}, "category": "Tech", "category_confidence": 0.98, "summary_confidence": 0.98, "original_categories": []string{"technology"}, "is_advertisement": false, "ad_type": "news_article", "ad_confidence": 0.0, "ad_reasoning": "Synthetic fixture is editorial content.", "publication_date": nil, "content_quality": 0.95, "confidence": 0.98}
	}
	writeGeminiResult(w, result)
}

func writeGeminiResult(w http.ResponseWriter, result map[string]any) {
	raw, _ := json.Marshal(result)
	write(w, 200, map[string]any{
		"candidates":    []any{map[string]any{"finishReason": "STOP", "content": map[string]any{"parts": []any{map[string]any{"text": string(raw)}}}}},
		"usageMetadata": map[string]any{"promptTokenCount": 100, "candidatesTokenCount": 100, "thoughtsTokenCount": 10, "cachedContentTokenCount": 20, "totalTokenCount": 210},
	})
}

func (s *state) consumerHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" || r.URL.Path != "/commands" {
		write(w, 404, map[string]any{"error": "not_found"})
		return
	}
	s.mu.Lock()
	s.consumerRequests++
	mode := s.consumerMode
	release := s.consumer
	s.mu.Unlock()
	if r.Header.Get("Authorization") != "Bearer fixture-generic-token" {
		write(w, 401, map[string]any{"error": "unauthorized"})
		return
	}
	p, raw, e := decode(r)
	if e != nil {
		write(w, 400, map[string]any{"error": "invalid_json"})
		return
	}
	id := text(p["occurrence_id"])
	if id == "" || r.Header.Get("Idempotency-Key") != id {
		write(w, 422, map[string]any{"error": "invalid_command"})
		return
	}
	if mode == "503" {
		write(w, 503, map[string]any{"error": "fixture_unavailable"})
		return
	}
	hash := digest(raw)
	s.mu.Lock()
	prior, exists := s.occurrences[id]
	if exists && prior.hash != hash {
		s.mu.Unlock()
		write(w, 409, map[string]any{"error": "idempotency_conflict"})
		return
	}
	if !exists {
		prior = entry{hash, map[string]any{"version": 1, "occurrence_id": id, "job_id": jobID(id), "status": "accepted"}}
		s.occurrences[id] = prior
		op := text(p["operation"])
		if op == "" {
			op = "unknown"
		}
		s.operations[op]++
	}
	s.mu.Unlock()
	if mode == "accept_then_hold" {
		wait(release)
	}
	if mode == "malformed" {
		write(w, 202, map[string]any{"status": "accepted", "occurrence_id": id})
		return
	}
	write(w, 202, prior.receipt)
}
func main() {
	s := newState()
	go func() { log.Fatal(http.ListenAndServe(":8090", http.HandlerFunc(s.provider))) }()
	log.Fatal(http.ListenAndServe(":8091", http.HandlerFunc(s.consumerHandler)))
}
