package webbackend

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

const shadowPolicy = "qdrant-jev-topics-v1"

// topicsRead exposes only operator-facing fields, never the stored model trace.
func (s *Server) topicsRead(w http.ResponseWriter, r *http.Request) {
	if s.Pool == nil {
		reject(w, 503, "Topics unavailable")
		return
	}
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil {
		limit = 25
	}
	if limit < 1 || limit > 100 {
		reject(w, 422, "Invalid limit")
		return
	}
	offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
	if err != nil {
		offset = 0
	}
	if offset < 0 || offset > 100000 {
		reject(w, 422, "Invalid offset")
		return
	}
	state := r.URL.Query().Get("state")
	if state != "" && state != "active" && state != "dormant" && state != "candidate" && state != "review" {
		reject(w, 422, "Invalid state")
		return
	}
	id := r.URL.Query().Get("id")
	if id != "" && (len(id) != 36 || strings.Trim(id, "0123456789abcdef-") != "") {
		reject(w, 422, "Invalid topic")
		return
	}
	q := r.URL.Query().Get("q")
	if len(q) > 200 {
		reject(w, 422, "Invalid search")
		return
	}
	var raw []byte
	err = s.Pool.QueryRow(r.Context(), `SELECT json_build_object(
 'index', (SELECT json_build_object('started_at',started_at,'scanned_articles',scanned_articles,'completed_at',completed_at,'embedding_tokens',embedding_tokens,'embedding_requests_today',CASE WHEN embedding_day=(clock_timestamp() AT TIME ZONE 'UTC')::date THEN embedding_requests ELSE 0 END) FROM topic_shadow_semantic_index WHERE policy_version=$1),
 'checkpoint', (SELECT json_build_object('started_at',started_at,'scan_after',scan_after,'scan_article_id',scan_article_id,'covered_through',covered_through,'replay_in_progress_at',replay_in_progress_at) FROM topic_shadow_checkpoints WHERE policy_version=$1),
 'topics', COALESCE((SELECT json_agg(x) FROM (SELECT topic_id::text AS id,state,card->>'Title' AS title,card->>'Context' AS context,opened_at,last_material_at,quieted_at,material_count FROM topic_shadow_topics WHERE policy_version=$1 AND ($2='' OR state=$2) AND ($3='' OR card->>'Title' ILIKE '%' || $3 || '%' OR card->>'Context' ILIKE '%' || $3 || '%') AND ($4='' OR topic_id::text=$4) ORDER BY last_material_at DESC NULLS LAST, topic_id LIMIT $5 OFFSET $6) x),'[]'::json),
 'events', COALESCE((SELECT json_agg(x) FROM (SELECT e.kind,e.event_at,e.available_at,e.evidence->>'Reason' AS reason,d.decision->>'fact' AS fact,COALESCE(d.input->>'Title',a.title) AS title,COALESCE(d.input->>'Summary',a.summary) AS summary,a.url FROM topic_shadow_events e LEFT JOIN topic_shadow_decisions d ON d.policy_version=e.policy_version AND d.article_id=e.article_id AND d.claim_token=e.claim_token LEFT JOIN articles a ON a.id=e.article_id WHERE e.policy_version=$1 AND $4<>'' AND e.topic_id::text=$4 ORDER BY e.available_at,e.topic_revision LIMIT 100) x),'[]'::json),
 'decisions', COALESCE((SELECT json_agg(x) FROM (SELECT d.state,d.ready_at,d.decision->>'novelty' AS novelty,d.decision->>'review_reason' AS reason,d.decision->>'fact' AS fact,COALESCE(d.input->>'Title',a.title) AS title,COALESCE(d.input->>'Summary',a.summary) AS summary,a.url,d.topic_id::text AS topic_id FROM topic_shadow_decisions d JOIN articles a ON a.id=d.article_id WHERE d.policy_version=$1 AND (($2='review' AND d.state='review' AND ($3='' OR a.title ILIKE '%' || $3 || '%' OR a.summary ILIKE '%' || $3 || '%')) OR ($4<>'' AND d.topic_id::text=$4 AND d.state IN ('accepted','review'))) ORDER BY d.ready_at DESC,d.article_id DESC LIMIT $5 OFFSET $6) x),'[]'::json))`, shadowPolicy, state, q, id, limit, offset).Scan(&raw)
	if err != nil || !json.Valid(raw) {
		reject(w, 503, "Topics unavailable")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(raw)
}
