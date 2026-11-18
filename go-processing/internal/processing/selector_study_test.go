package processing

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

func TestExtractionStudyUsesSingleSnapshotAndKeepsFallback(t *testing.T) {
	const url = "https://publisher.example/news"
	const html = "<main>captured page</main>"
	digest := sha256.Sum256([]byte(html))
	fingerprint := hex.EncodeToString(digest[:])
	for _, tc := range []struct {
		name    string
		enabled bool
		layout  string
		applied bool
	}{
		{name: "off"},
		{name: "listing", enabled: true, layout: "listing", applied: true},
		{name: "unknown", enabled: true, layout: "unknown"},
		{name: "not applied", enabled: true, layout: "article"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var studies, feedback, replays atomic.Int32
			ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req aiEnvelope
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("decode AI wire: %v", err)
					return
				}
				result := map[string]any{}
				switch req.Operation {
				case "extraction_context":
				case "extraction_study":
					studies.Add(1)
					if req.Input["html"] != html || req.Input["snapshot_digest"] != fingerprint || req.Input["document_url"] != url {
						t.Error("study did not use original snapshot identity")
					}
					result = map[string]any{"snapshot_digest": fingerprint, "layout": map[string]any{"page_kind": tc.layout, "content_selectors": []string{"a.card"}}}
				case "extraction_feedback":
					feedback.Add(1)
				default:
					t.Errorf("unexpected operation %q", req.Operation)
				}
				_ = json.NewEncoder(w).Encode(analysisResponse(req, result))
			}))
			defer ai.Close()
			extract := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req map[string]any
				if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
					t.Errorf("decode extraction wire: %v", err)
					return
				}
				resp := map[string]any{"contract_version": "1", "request_id": req["request_id"], "original_url": url, "outcome": "success", "result": map[string]any{"content": syntheticExtractedArticle}}
				if replay, ok := req["reparse"].(map[string]any); ok {
					replays.Add(1)
					if replay["snapshot_digest"] != fingerprint || replay["html"] != html || replay["document_url"] != url || req["enable_page_study"] != nil {
						t.Error("reparse did not use original snapshot identity")
					}
					resp["outcome"] = "rejected"
					resp["result"] = map[string]any{}
					resp["applied"] = tc.applied
					resp["error"] = map[string]any{"code": "unsupported_input", "reason": "non_article_listing", "retryable": false}
				} else {
					if (req["enable_page_study"] == true) != tc.enabled {
						t.Error("unexpected study opt-in")
					}
					resp["study_html"], resp["study_document_url"], resp["snapshot_digest"] = html, url, fingerprint
				}
				_ = json.NewEncoder(w).Encode(resp)
			}))
			defer extract.Close()
			client := NewExtractorClient(extract.URL, "fixture-token")
			client.EnablePageStudy = tc.enabled
			p := &Processor{AI: NewAIClient(ai.URL, "fixture-token"), Extractor: client}
			f := store.Fence{RunID: "10000000-0000-4000-8000-000000000001", Envelope: store.Envelope{Version: 1}}
			got, err := p.extract(context.Background(), f, store.Article{ID: 7, URL: url}, 1, fingerprint, "")
			if err != nil {
				t.Fatal(err)
			}
			if tc.applied {
				if !got.Terminal || got.Error != "non_article_listing" || got.Content != "" || feedback.Load() != 0 {
					t.Fatal("validated listing was not terminal")
				}
			} else if got.Content != syntheticExtractedArticle || feedback.Load() != 1 {
				t.Fatal("baseline should survive unavailable study or reparse")
			}
			var wantStudy, wantReplay int32
			if tc.enabled {
				wantStudy = 1
			}
			if tc.layout == "listing" || tc.layout == "article" {
				wantReplay = 1
			}
			if studies.Load() != wantStudy || replays.Load() != wantReplay {
				t.Fatalf("studies=%d replays=%d", studies.Load(), replays.Load())
			}
		})
	}
}
