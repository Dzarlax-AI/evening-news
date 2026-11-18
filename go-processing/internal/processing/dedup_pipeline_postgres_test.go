package processing

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

func TestProcessingHandlerPostgresDedupTrialKeepsAdmittedCopies(t *testing.T) {
	pg := newPipelinePG(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	exec := func(query string, args ...any) {
		t.Helper()
		if _, err := pg.pool.Exec(ctx, query, args...); err != nil {
			t.Fatal("prepare synthetic dedup pipeline scenario failed")
		}
	}
	exec(`CREATE TABLE article_localizations(article_id bigint, locale text, status text, title text, summary text,
		PRIMARY KEY(article_id,locale)); INSERT INTO sources VALUES(21,'telegram','Synthetic source'),(22,'telegram','Second synthetic source')`)

	const sourceTitle = "City authority issues a bridge announcement"
	const original = "The city closed the bridge on Monday morning and redirected all traffic through the northern tunnel while two independent teams inspected the structure."
	const different = "The city opened a pedestrian bridge on the other side of the river. The new crossing connects two parks after a year of construction."
	const revised = "The city reopened the original bridge on Tuesday afternoon after the independent teams completed the structural inspection and removed the temporary diversions."
	const displayTitle = "Translated headline from synthetic analysis"
	seed := func(id, source int64, body string) {
		t.Helper()
		exec(`INSERT INTO articles(id,title,source_title,url,source_id,content)
			VALUES($1,$2,$2,'https://t.me/synthetic/' || $1::bigint::text,$3,$4)`, id, sourceTitle, source, body)
	}
	seed(711, 21, original)
	seed(712, 21, original)
	seed(713, 22, original)
	seed(714, 21, different)
	seed(715, 21, different)
	exec("UPDATE articles SET fetched_at=NULL,source_title=NULL WHERE id=715")
	seed(716, 21, original)
	seed(717, 21, original)

	var calls atomic.Int32
	ai := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request aiEnvelope
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Operation != "analyze" {
			http.Error(w, "unexpected fixture callback", http.StatusBadRequest)
			return
		}
		calls.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(analysisResponse(request, map[string]any{
			"summary": "Synthetic bridge report with sufficient evidence.", "optimized_title": displayTitle,
			"categories": []string{"technology"}, "is_advertisement": request.ArticleID == 711,
		}))
	}))
	defer ai.Close()
	processor := newIntegrationProcessor(pg, ai.URL, "")
	processor.DedupTrial = true

	var logs bytes.Buffer
	logger := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(logger)

	run := func(handler *Handler, f store.Fence) {
		t.Helper()
		body, err := json.Marshal(f.Envelope)
		if err != nil {
			t.Fatal("encode synthetic processing envelope failed")
		}
		req := httptest.NewRequest(http.MethodPost, "/internal/v1/processing/runs", bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer fixture-processing-token")
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		handler.Routes().ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("processing route returned %d", rec.Code)
		}
		for {
			var state string
			if err := pg.pool.QueryRow(ctx, `SELECT state FROM article_processing_runs WHERE id=$1`, f.RunID).Scan(&state); err != nil {
				t.Fatal("read processing state failed")
			}
			if state == "succeeded" {
				break
			}
			if state != "running" {
				t.Fatalf("unexpected processing run state %s", state)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	handler := NewHandler(pg.db, processor, Config{Token: "fixture-processing-token"})
	defer handler.Close(ctx)
	first := pg.newRun(t, 711)
	run(handler, first)
	run(handler, first)
	// Establish the first admitted representative before starting the peers.
	run(handler, pg.newRun(t, 712))
	run(handler, pg.newRunWithPayload(t, 713, "news_processing", map[string]any{"article_ids": []int64{713, 714, 715}}))
	if calls.Load() != 5 {
		t.Fatalf("expected 5 callback calls including the first rejected copy: got %d", calls.Load())
	}

	read := func(id int64) (state string, claim, source *string, hash []byte) {
		t.Helper()
		err := pg.pool.QueryRow(ctx, `SELECT a.processing_state,d.claim_token::text,a.source_title,d.exact_hash
			FROM articles a LEFT JOIN article_dedup_observations d ON d.article_id=a.id WHERE a.id=$1`, id).Scan(&state, &claim, &source, &hash)
		if err != nil {
			t.Fatal("read trial observation failed")
		}
		return
	}
	if state, claim, _, _ := read(711); state != "rejected" || claim != nil {
		t.Fatalf("failed first copy entered exact registry: state=%s observation=%t", state, claim != nil)
	}
	state, firstClaim, source, firstHash := read(712)
	if state != "completed" || firstClaim == nil || source == nil || *source != sourceTitle || len(firstHash) != 32 {
		t.Fatal("first admitted copy did not retain original source headline and exact observation")
	}
	if state, _, _, hash := read(713); state != "completed" || !bytes.Equal(hash, firstHash) {
		t.Fatal("exact copy was hidden or used a different fingerprint")
	}
	if state, _, _, hash := read(714); state != "completed" || len(hash) != 32 || bytes.Equal(hash, firstHash) {
		t.Fatal("different article with the same title was hidden or joined the exact group")
	}
	if state, _, source, hash := read(715); state != "completed" || source != nil || len(hash) != 32 {
		t.Fatal("legacy row with no fetched date or source title lost its exact observation")
	}

	// Mirror the job's reset of processing_attempts, then run the real processor again.
	exec(`UPDATE articles SET content=$1,processing_state='pending',processing_attempts=0,processed=false,
		summary_processed=false,category_processed=false,ad_processed=false WHERE id=712`, revised)
	reprocess := pg.newRunWithPayload(t, 712, "article_reprocess", map[string]any{"article_ids": []int64{712}})
	run(handler, reprocess)
	state, claim, source, changedHash := read(712)
	if state != "completed" || claim == nil || *claim == *firstClaim || source == nil || *source != sourceTitle ||
		!bytes.Equal(changedHash, dedupObservation(revised).ExactHash) {
		t.Fatal("updated article reused the old observation or lost the source headline")
	}

	disabled := *processor
	disabled.DedupTrial = false
	disabledHandler := NewHandler(pg.db, &disabled, Config{Token: "fixture-processing-token"})
	defer disabledHandler.Close(ctx)
	run(disabledHandler, pg.newRun(t, 716))
	if state, claim, _, _ := read(716); state != "completed" || claim != nil {
		t.Fatal("disabled trial affected processing or recorded a fingerprint")
	}

	// Simulate unavailable diagnostics after admission has already succeeded.
	exec("ALTER TABLE article_dedup_observations RENAME TO unavailable_dedup_observations")
	run(handler, pg.newRun(t, 717))
	var stateAfterFailure string
	if err := pg.pool.QueryRow(ctx, "SELECT processing_state FROM articles WHERE id=717").Scan(&stateAfterFailure); err != nil || stateAfterFailure != "completed" {
		t.Fatal("trial failure affected the committed article outcome")
	}

	if err := handler.Close(ctx); err != nil {
		t.Fatal("drain dedup pipeline handler failed")
	}
	if err := disabledHandler.Close(ctx); err != nil {
		t.Fatal("drain disabled-trial pipeline handler failed")
	}

	var sawExact, sawMissingDate, sawFailure bool
	scanner := json.NewDecoder(bytes.NewReader(logs.Bytes()))
	for scanner.More() {
		var entry struct {
			Message     string `json:"msg"`
			ArticleID   int64  `json:"article_id"`
			Exact       int64  `json:"exact_representative_id"`
			MissingDate bool   `json:"missing_fetched_at"`
		}
		if err := scanner.Decode(&entry); err != nil {
			t.Fatal("trial emitted an invalid structured log")
		}
		sawExact = sawExact || (entry.ArticleID == 713 && entry.Exact == 712)
		sawMissingDate = sawMissingDate || (entry.ArticleID == 715 && entry.MissingDate)
		sawFailure = sawFailure || (entry.ArticleID == 717 && entry.Message == "article_dedup_trial_failed")
	}
	if !sawExact || !sawMissingDate || !sawFailure || strings.Contains(logs.String(), sourceTitle) {
		t.Fatalf("trial diagnostics incorrect: exact=%t missing_date=%t fail_open=%t", sawExact, sawMissingDate, sawFailure)
	}
}
