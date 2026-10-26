package processing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

type Processor struct {
	DB                                           *store.Store
	AI                                           *AIClient
	Extractor                                    *ExtractorClient
	MaxWorkers, MaxBatch, MaxAttempts, TextLimit int
	CycleMax, PerSourceLimit                     int
	DateFrom, DateTo                             *time.Time
	Duplicates                                   *DuplicateCache
	AttemptTimeout, RetryInitial, RetryLater     time.Duration
}
type jobPayload struct {
	ArticleIDs     []int64 `json:"article_ids"`
	Force          *bool   `json:"force"`
	PreserveStages bool    `json:"preserve_completed_stages"`
	DryRun         bool    `json:"dry_run"`
	Limit          int     `json:"limit"`
	MaxArticles    int     `json:"max_articles"`
}

func (p *Processor) Run(ctx context.Context, f store.Fence) (map[string]any, error) {
	if p.Duplicates == nil {
		p.Duplicates = NewDuplicateCache()
	}
	kind, raw, err := p.DB.CanonicalJob(ctx, f)
	if err != nil {
		return nil, withPhase("canonical_job", err)
	}
	var payload jobPayload
	if err = json.Unmarshal(raw, &payload); err != nil {
		return nil, errors.New("invalid canonical processing payload")
	}
	dryRun := kind == "reprocess_failed" && payload.DryRun
	if !dryRun {
		if err := p.DB.RecoverClaims(ctx, f); err != nil {
			return nil, withPhase("recover_claims", err)
		}
		if err := p.DB.RecoverExhausted(ctx, f, p.maxAttempts(), time.Now().UTC()); err != nil {
			return nil, withPhase("recover_exhausted", err)
		}
	}
	force := payload.Force != nil && *payload.Force
	if kind == "article_reprocess" && payload.Force == nil {
		force = true
	}
	batchSize := p.MaxBatch
	if batchSize < 1 || batchSize > 100 {
		batchSize = 25
	}
	cycleLimit := p.CycleMax
	if kind == "reprocess_failed" {
		limit := payload.Limit
		if limit < 1 {
			limit = 20
		}
		if cycleLimit == 0 || limit < cycleLimit {
			cycleLimit = limit
		}
	}
	if payload.MaxArticles > 0 && (cycleLimit == 0 || payload.MaxArticles < cycleLimit) {
		cycleLimit = payload.MaxArticles
	}
	if len(payload.ArticleIDs) > 0 && (cycleLimit == 0 || len(payload.ArticleIDs) < cycleLimit) {
		cycleLimit = len(payload.ArticleIDs)
	}
	if dryRun {
		limit := payload.Limit
		if limit < 1 {
			limit = 20
		}
		candidates, err := p.DB.SelectDue(ctx, f, limit, 0, nil, p.DateFrom, p.DateTo, p.maxAttempts())
		if err != nil {
			return nil, withPhase("select_dry_run", err)
		}
		return map[string]any{"status": "dry_run", "total_candidates": len(candidates), "found_candidates": len(candidates), "processed": 0, "improved": 0, "errors": []string{}}, nil
	}
	counts := map[string]int{"articles_processed": 0, "articles_summarized": 0, "articles_categorized": 0, "articles_rejected": 0, "articles_retrying": 0, "articles_exhausted": 0}
	var cursor int64
	sourceCounts := map[int64]int{}
	selectedCount := 0
	const outcomeSampleLimit = 20
	outcomeSample := make([]store.ArticleOutcome, 0, outcomeSampleLimit)
	outcomesTotal := 0
	categoryMappingErrors := 0
	categoryErrorSample := make([]string, 0, outcomeSampleLimit)
	explicitSelection := kind == "article_reprocess"
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		remaining := batchSize
		if cycleLimit > 0 {
			remaining = cycleLimit - selectedCount
			if remaining <= 0 {
				break
			}
			if remaining > batchSize {
				remaining = batchSize
			}
		}
		dateFrom, dateTo := p.DateFrom, p.DateTo
		if len(payload.ArticleIDs) > 0 {
			dateFrom, dateTo = nil, nil
		}
		batch, err := p.DB.SelectDue(ctx, f, remaining, cursor, payload.ArticleIDs, dateFrom, dateTo, p.maxAttempts())
		if err != nil {
			return nil, withPhase("select_due", err)
		}
		if len(batch) == 0 {
			break
		}
		cursor = batch[len(batch)-1].ID
		eligible := make([]store.Article, 0, len(batch))
		for _, row := range batch {
			sourceID := int64(0)
			if row.SourceID != nil {
				sourceID = *row.SourceID
			}
			if p.sourceLimit() > 0 && sourceCounts[sourceID] >= p.sourceLimit() {
				continue
			}
			sourceCounts[sourceID]++
			eligible = append(eligible, row)
			selectedCount++
		}
		sem := make(chan struct{}, max(1, p.MaxWorkers))
		batchCtx, cancelBatch := context.WithCancel(ctx)
		var wg sync.WaitGroup
		var mu sync.Mutex
		var firstErr error
		for _, row := range eligible {
			wg.Add(1)
			go func(a store.Article) {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-batchCtx.Done():
					return
				}
				defer func() { <-sem }()
				preserveStages := payload.PreserveStages || kind == "article_reprocess"
				out, _, e := p.processOne(batchCtx, f, a, force, preserveStages)
				mu.Lock()
				if e != nil {
					if firstErr == nil {
						firstErr = e
					}
					cancelBatch()
					mu.Unlock()
					return
				}
				switch out.State {
				case StateCompleted:
					counts["articles_processed"]++
				case StateRejected:
					counts["articles_rejected"]++
				case StateRetryWait:
					counts["articles_retrying"]++
				case StateExhausted:
					counts["articles_exhausted"]++
				}
				if _, ok := out.Fields["summary"]; ok {
					counts["articles_summarized"]++
				}
				if _, ok := out.Fields["categories"]; ok {
					counts["articles_categorized"]++
				}
				if out.Error == "category_mapping_failed" {
					categoryMappingErrors++
					if len(categoryErrorSample) < outcomeSampleLimit {
						categoryErrorSample = append(categoryErrorSample, fmt.Sprintf("Article %d category mapping failed", a.ID))
					}
				}
				if !explicitSelection {
					outcomesTotal++
					if len(outcomeSample) < outcomeSampleLimit {
						var reason *string
						if out.Error != "" {
							value := out.Error
							reason = &value
						}
						outcomeSample = append(outcomeSample, store.ArticleOutcome{ArticleID: a.ID, State: out.State, Reason: reason})
					}
				}
				mu.Unlock()
			}(row)
		}
		wg.Wait()
		cancelBatch()
		if firstErr != nil {
			return nil, withPhase("process_batch", firstErr)
		}
		if len(batch) < remaining {
			break
		}
	}
	outcomes := outcomeSample
	status := "success"
	settled := counts["articles_retrying"] == 0
	retryable := counts["articles_retrying"] > 0
	success := categoryMappingErrors == 0
	if categoryMappingErrors > 0 {
		status = "warning"
	}
	if explicitSelection {
		requested := uniqueSortedIDs(payload.ArticleIDs)
		outcomes, err = p.DB.ArticleOutcomes(ctx, requested)
		if err != nil {
			return nil, withPhase("read_selected_outcomes", err)
		}
		outcomesTotal = len(outcomes)
		settled = len(outcomes) == len(requested)
		retryable = false
		for _, item := range outcomes {
			if item.State == "pending" || item.State == "retry_wait" {
				retryable = true
			}
			if item.State != "completed" && item.State != "rejected" {
				settled = false
			}
		}
		success = settled && categoryMappingErrors == 0
		if len(outcomes) == 1 {
			status = outcomes[0].State
		} else if settled {
			status = "settled"
		} else {
			status = "incomplete"
		}
	} else {
		sort.Slice(outcomes, func(i, j int) bool { return outcomes[i].ArticleID < outcomes[j].ArticleID })
	}
	receipt := map[string]any{"status": status, "articles_processed": counts["articles_processed"], "articles_summarized": counts["articles_summarized"], "articles_categorized": counts["articles_categorized"], "articles_rejected": counts["articles_rejected"], "articles_retrying": counts["articles_retrying"], "articles_exhausted": counts["articles_exhausted"], "errors_count": categoryMappingErrors, "article_outcomes": outcomes, "article_outcomes_total": outcomesTotal, "article_outcomes_omitted": max(0, outcomesTotal-len(outcomes)), "settled": settled, "success": success, "retryable": retryable}
	if kind == "reprocess_failed" {
		receipt["processed"] = counts["articles_processed"]
		receipt["improved"] = counts["articles_summarized"]
		receipt["errors"] = categoryErrorSample
		receipt["errors_count"] = categoryMappingErrors
	}
	return receipt, nil
}
func (p *Processor) sourceLimit() int { return p.PerSourceLimit }

func uniqueSortedIDs(ids []int64) []int64 {
	if len(ids) == 0 {
		return nil
	}
	out := append([]int64(nil), ids...)
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	unique := out[:0]
	for _, id := range out {
		if len(unique) == 0 || unique[len(unique)-1] != id {
			unique = append(unique, id)
		}
	}
	return unique
}

func (p *Processor) processOne(ctx context.Context, f store.Fence, a store.Article, force, preserve bool) (Outcome, int, error) {
	retryDelay := p.RetryLater
	if a.ProcessingAttempts == 0 {
		retryDelay = p.RetryInitial
	}
	if retryDelay <= 0 {
		if a.ProcessingAttempts == 0 {
			retryDelay = 30 * time.Minute
		} else {
			retryDelay = 6 * time.Hour
		}
	}
	attempt, fp, token, err := p.DB.ClaimArticle(ctx, f, a, force, p.timeout(), retryDelay)
	if err != nil {
		return Outcome{}, 0, err
	}
	a.ClaimToken = token
	workCtx, cancel := context.WithTimeout(ctx, p.timeout())
	defer cancel()
	summaryDone := !force && a.SummaryProcessed && strings.TrimSpace(a.Summary) != ""
	categoryDone := !force && a.CategoryProcessed
	adDone := !force && a.AdProcessed
	if preserve && force {
		summaryDone = a.SummaryProcessed && strings.TrimSpace(a.Summary) != ""
		categoryDone = a.CategoryProcessed
		adDone = a.AdProcessed
	}
	if adDone && a.IsAdvertisement != nil && *a.IsAdvertisement {
		return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: map[string]any{}, Error: "advertisement"})
	}
	if summaryDone && categoryDone && adDone {
		return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateCompleted, Fields: map[string]any{}})
	}
	mediaContent := a.Content
	content := a.Content
	if summaryDone {
		content = a.Summary
	}
	fields := map[string]any{}
	forceMode := force && (!preserve || (!summaryDone && !categoryDone && !adDone))
	forceExtract := forceMode && ExtractableURL(a.URL)
	if !forceExtract {
		if err = EnforceTextLimit(content, p.textLimit()); err != nil {
			return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "resource_limit"})
		}
	}
	if forceExtract && EnforceTextLimit(mediaContent, p.textLimit()) != nil {
		mediaContent = ""
	}
	if !summaryDone && !hasMediaFiles(a.MediaFiles) && strings.TrimSpace(mediaContent) != "" {
		if extractedMedia := extractMediaFiles(mediaContent, a.URL); len(extractedMedia) > 0 {
			fields["media_files"] = extractedMedia
		}
	}
	ordinaryFreshAccepted := false
	extractNeeded := forceExtract
	if !forceMode && !summaryDone {
		if a.ProcessingAttempts == 0 {
			hash, duplicate, cacheHit := p.Duplicates.Lookup(content, a.ID, time.Now())
			if cacheHit && duplicate {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "smart_filter_rejected"})
			}
			if !cacheHit && hash != "" {
				exists, lookupErr := p.DB.HasDuplicateHash(workCtx, hash, a.ID)
				if lookupErr == nil {
					if exists {
						p.Duplicates.Remember(hash, nil, time.Now())
						return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "smart_filter_rejected"})
					}
					owner := a.ID
					p.Duplicates.Remember(hash, &owner, time.Now())
				}
			}
		}
		ok, reason := shouldProcess(a.Title, content, a.URL, a.SourceType, true)
		if !ok {
			extractNeeded = reasonNeedsExtraction(reason)
			if !extractNeeded {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "smart_filter_rejected"})
			}
		}
	}
	if !forceMode && !summaryDone && isSummarySourceType(a.SourceType) && ExtractableURL(a.URL) {
		extractNeeded = true
	}
	if extractNeeded {
		if !ExtractableURL(a.URL) {
			if forceExtract {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "content_refresh_unavailable"})
			}
		}
		contextContent := content
		if forceExtract {
			contextContent = ""
		}
		contextInput := map[string]any{"title": a.Title, "content": contextContent, "url": a.URL}
		if a.SourceType == "telegram" && len([]rune(strings.TrimSpace(mediaContent))) >= 30 && !forceExtract {
			contextInput["original_context"] = mediaContent
		}
		contextFP := InputFingerprint(contextInput)
		if err = p.DB.UpdateItemFingerprint(workCtx, f, a.ID, attempt, a.ClaimToken, contextFP); err != nil {
			if attemptTimedOut(workCtx, ctx) {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "timeout"})
			}
			return Outcome{}, attempt, err
		}
		fp = contextFP
		fresh, extractErr := p.extract(workCtx, f, a, attempt, contextFP, contextContent)
		if extractErr != nil {
			if isFatal(extractErr) {
				return Outcome{}, attempt, extractErr
			}
			if attemptTimedOut(workCtx, ctx) {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "timeout"})
			}
			if errors.Is(extractErr, ErrResourceLimit) {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "resource_limit"})
			}
			return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "extraction_unavailable"})
		}
		if fresh.Error != "" {
			if fresh.Terminal {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: fresh.Error})
			}
			if forceExtract || (fresh.Error != "no_content" && fresh.Error != "needs_selector_study" && fresh.Error != "invalid_content") {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: fresh.Error, RetryAfterSeconds: fresh.RetryAfterSeconds})
			}
			if len(strings.TrimSpace(a.Content)) < 30 {
				if scientificTitleFallback(a.Title, a.URL) {
					content = a.Title
				} else {
					return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "smart_filter_rejected"})
				}
			}
		}
		if strings.TrimSpace(fresh.Content) == "" {
			if forceExtract {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "content_refresh_unavailable"})
			}
		}
		if fresh.Content != "" {
			if IsChallengeText(fresh.Content) {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "extraction_challenge"})
			}
			if EnforceTextLimit(fresh.Content, p.textLimit()) != nil {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "resource_limit"})
			}
			candidate := fresh.Content
			candidateAccepted := true
			if !isGoodExtractionContent(candidate, p.textLimit()) {
				candidateAccepted = false
			}
			if candidateAccepted && !forceExtract && len(strings.TrimSpace(candidate)) < 30 {
				candidateAccepted = false
			}
			if candidateAccepted && !forceMode {
				if ok, _ := shouldProcess(a.Title, candidate, a.URL, a.SourceType, false); !ok {
					candidateAccepted = false
				}
			}
			if candidateAccepted {
				content = candidate
				ordinaryFreshAccepted = !forceExtract && !forceMode && !summaryDone
				if len([]rune(strings.TrimSpace(candidate))) > len([]rune(a.Content)) {
					fields["content"] = candidate
				}
				if a.ImageURL == "" && fresh.ImageURL != "" {
					fields["image_url"] = fresh.ImageURL
				}
				if !hasMediaFiles(a.MediaFiles) && len(fresh.MediaFiles) > 0 {
					fields["media_files"] = fresh.MediaFiles
				}
				if a.PublishedAt == nil || a.PublishedAt.Year() < 2020 {
					if d := parseDate(fresh.PublicationDate); d != nil {
						fields["published_at"] = d
					}
				}
			} else if forceExtract {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "content_refresh_unavailable"})
			} else {
				if len(strings.TrimSpace(a.Content)) >= 30 {
					content = a.Content
				} else if scientificTitleFallback(a.Title, a.URL) {
					content = a.Title
				} else {
					return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "smart_filter_rejected"})
				}
			}
		}
	}
	if IsChallengeText(content) {
		return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "extraction_challenge"})
	}
	if extractNeeded && !forceExtract && !summaryDone {
		ok, _ := shouldProcess(a.Title, content, a.URL, a.SourceType, false)
		if !ok {
			return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "smart_filter_rejected"})
		}
	}
	input := map[string]any{"title": a.Title, "content": content, "url": a.URL}
	if ordinaryFreshAccepted && strings.TrimSpace(mediaContent) != "" {
		input["original_context"] = mediaContent
	} else if forceExtract && a.SourceType == "telegram" && len([]rune(strings.TrimSpace(mediaContent))) >= 30 && EnforceTextLimit(mediaContent, p.textLimit()) == nil {
		input["original_context"] = mediaContent
	}
	if summaryDone {
		input["content"] = a.Summary
	}
	inputFP := InputFingerprint(input)
	if err = p.DB.UpdateItemFingerprint(workCtx, f, a.ID, attempt, a.ClaimToken, inputFP); err != nil {
		if attemptTimedOut(workCtx, ctx) {
			return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "timeout"})
		}
		return Outcome{}, attempt, err
	}
	fp = inputFP
	analysis, err := p.AI.Call(workCtx, f, a, attempt, fp, "analyze", input)
	if err != nil {
		if isFatal(err) {
			return Outcome{}, attempt, err
		}
		if errors.Is(err, ErrResourceLimit) {
			return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "resource_limit"})
		}
		if attemptTimedOut(workCtx, ctx) {
			return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "timeout"})
		}
		return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: errorCode(err)})
	}
	if ordinaryFreshAccepted && shouldFallbackSummary(analysis) {
		fallbackInput := map[string]any{"title": a.Title, "content": content, "url": a.URL}
		fallbackFP := InputFingerprint(fallbackInput)
		if err = p.DB.UpdateItemFingerprint(workCtx, f, a.ID, attempt, a.ClaimToken, fallbackFP); err != nil {
			if attemptTimedOut(workCtx, ctx) {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "timeout"})
			}
			return Outcome{}, attempt, err
		}
		fp = fallbackFP
		fallback, fallbackErr := p.AI.Call(workCtx, f, a, attempt, fp, "analyze", fallbackInput)
		if fallbackErr != nil {
			if isFatal(fallbackErr) {
				return Outcome{}, attempt, fallbackErr
			}
			if errors.Is(fallbackErr, ErrResourceLimit) {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRejected, Fields: fields, Error: "resource_limit"})
			}
			if attemptTimedOut(workCtx, ctx) {
				return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: "timeout"})
			}
			return p.finish(ctx, f, a, attempt, fp, Outcome{State: StateRetryWait, Fields: fields, Error: errorCode(fallbackErr)})
		}
		if fallback != nil {
			merged := make(map[string]any, len(analysis)+len(fallback))
			for key, value := range analysis {
				merged[key] = value
			}
			for key, value := range fallback {
				merged[key] = value
			}
			analysis = merged
		}
	}
	if analysis == nil {
		analysis = map[string]any{}
	}
	out := AnalyzeOutcome(analysis, summaryDone, categoryDone, adDone)
	for k, v := range fields {
		out.Fields[k] = v
	}
	return p.finish(ctx, f, a, attempt, fp, out)
}
func (p *Processor) extract(ctx context.Context, f store.Fence, a store.Article, attempt int, fp, content string) (Extracted, error) {
	if p.Extractor == nil {
		return Extracted{}, fatal("extractor_not_configured")
	}
	ctxResult, err := p.AI.Call(ctx, f, a, attempt, fp, "extraction_context", map[string]any{"url": a.URL})
	if err != nil {
		return Extracted{}, err
	}
	if code, _ := ctxResult["_processing_error"].(string); code != "" {
		terminal, _ := ctxResult["_processing_terminal"].(bool)
		return Extracted{Error: code, RetryAfterSeconds: ctxResult["_retry_after_seconds"], Terminal: terminal}, nil
	}
	selector, _ := ctxResult["selector"].(string)
	method, _ := ctxResult["method"].(string)
	extractionStarted := time.Now()
	out, err := p.Extractor.Extract(ctx, a.URL, a.Title, a.SourceType, selector, method, p.textLimit())
	if err != nil {
		return out, err
	}
	observations := make([]map[string]any, 0, len(out.Observations))
	for _, o := range out.Observations {
		if len(observations) == 64 {
			break
		}
		observations = append(observations, map[string]any{"selector": o.Selector, "method": articleExtractionMethod(o.Method), "success": o.Success})
	}
	feedbackMethod := articleExtractionMethod(out.MethodUsed)
	if feedbackMethod == "fallback_extraction" && out.Method != "" {
		feedbackMethod = articleExtractionMethod(out.Method)
	}
	success := out.Outcome == "success" && strings.TrimSpace(out.Content) != ""
	feedback := map[string]any{"url": a.URL, "method": feedbackMethod, "success": success, "observations": observations}
	if out.SelectorUsed != "" {
		feedback["selector_used"] = out.SelectorUsed
	}
	if n := len([]rune(out.Content)); n > 0 {
		feedback["content_length"] = n
	}
	if elapsed := time.Since(extractionStarted).Milliseconds(); elapsed >= 0 && elapsed <= 10_000_000 {
		feedback["extraction_time_ms"] = int(elapsed)
	}
	if success {
		feedback["quality_score"] = 0.8
	}
	if out.Error != "" {
		feedback["error_code"] = callbackErrorCode(out.Error)
	}
	if _, err = p.AI.Call(ctx, f, a, attempt, fp, "extraction_feedback", feedback); isFatal(err) {
		return Extracted{}, err
	}
	return out, nil
}
func (p *Processor) finish(ctx context.Context, f store.Fence, a store.Article, attempt int, fp string, o Outcome) (Outcome, int, error) {
	cleanupCtx, cleanupCancel := context.WithTimeout(ctx, 5*time.Second)
	defer cleanupCancel()
	persisted := o.Persist(attempt, time.Now().UTC(), Limits{MaxAttempts: p.maxAttempts(), Initial: p.RetryInitial, Later: p.RetryLater})
	if cats, ok := persisted.Fields["categories"].([]Category); ok {
		converted := make([]store.Category, 0, len(cats))
		for _, c := range cats {
			converted = append(converted, store.Category{Name: c.Name, Confidence: c.Confidence, AICategory: c.AICategory, DisplayCategory: MapCategory(c.Name)})
		}
		persisted.Fields["categories"] = converted
	}
	dbOutcome := store.Persisted{State: persisted.State, Fields: persisted.Fields, Attempts: persisted.Attempts, NextAttemptAt: persisted.NextAttemptAt, Error: persisted.Error, Processed: persisted.Processed}
	if err := p.DB.SaveOutcome(cleanupCtx, f, a, attempt, a.ClaimToken, fp, dbOutcome); err != nil {
		if errors.Is(err, store.ErrCategoryMapping) {
			if recordErr := p.DB.RecordCategoryMappingFailure(cleanupCtx, f, a, attempt, a.ClaimToken, fp); recordErr != nil {
				return o, attempt, recordErr
			}
			return Outcome{State: StateRetryWait, Fields: map[string]any{}, Error: "category_mapping_failed"}, attempt, nil
		}
		return o, attempt, err
	}
	o.State = persisted.State
	return o, attempt, nil
}

func attemptTimedOut(workCtx, parentCtx context.Context) bool {
	return errors.Is(workCtx.Err(), context.DeadlineExceeded) && parentCtx.Err() == nil
}

func (p *Processor) timeout() time.Duration {
	if p.AttemptTimeout > 0 {
		return p.AttemptTimeout
	}
	return 10 * time.Minute
}
func (p *Processor) textLimit() int {
	if p.TextLimit > 0 {
		return p.TextLimit
	}
	return 100000
}
func (p *Processor) maxAttempts() int {
	if p.MaxAttempts > 0 {
		return p.MaxAttempts
	}
	return 3
}
func parseDate(raw string) *time.Time {
	for _, layout := range []string{
		time.RFC3339Nano, time.RFC1123Z, time.RFC1123, time.RFC822Z, time.RFC822,
		"2006-01-02T15:04:05.999999999Z07:00", "2006-01-02T15:04:05Z0700",
		"2006-01-02T15:04:05.999999999", "2006-01-02T15:04:05", "2006-01-02T15:04",
		"2006-01-02 15:04:05.999999999", "2006-01-02 15:04:05", "2006-01-02 15:04",
		"2006-01-02",
	} {
		if t, e := time.Parse(layout, raw); e == nil {
			// Python's dateutil path strips timezone metadata without converting
			// the wall clock. PostgreSQL stores this field as a naive timestamp.
			x := time.Date(t.Year(), t.Month(), t.Day(), t.Hour(), t.Minute(), t.Second(), t.Nanosecond(), time.UTC)
			return &x
		}
	}
	return nil
}

func isSummarySourceType(sourceType string) bool {
	return sourceType == "rss" || sourceType == "custom" || sourceType == "telegram"
}

func shouldFallbackSummary(analysis map[string]any) bool {
	if analysis == nil || analysis["is_advertisement"] == true {
		return false
	}
	if code, _ := analysis["_processing_error"].(string); code != "" {
		return false
	}
	summary, _ := analysis["summary"].(string)
	return strings.TrimSpace(summary) == "" || IsChallengeText(summary)
}

func scientificTitleFallback(title, rawURL string) bool {
	if len([]rune(strings.TrimSpace(title))) <= 20 {
		return false
	}
	u, err := url.Parse(rawURL)
	if err != nil || u.Hostname() == "" {
		return false
	}
	host := strings.ToLower(u.Hostname())
	for _, domain := range []string{"nplus1.ru", "arxiv.org", "nature.com", "sciencedirect.com"} {
		if host == domain || strings.HasSuffix(host, "."+domain) {
			return true
		}
	}
	return false
}

var subscriptionPricePattern = regexp.MustCompile(`(?i)\$\s*\d+(?:\.\d+)?\s*/\s*(?:year|month|week)`)

func isShortBotOrConsentContent(content string) bool {
	if len([]rune(strings.TrimSpace(content))) >= 500 {
		return false
	}
	lower := strings.ToLower(content)
	for _, marker := range []string{
		"включить javascript и файлы cookie", "проверки безопасности", "является ли пользователь ботом",
		"just a moment", "checking if the site connection is secure", "enable javascript and cookies",
		"attention required", "checking your browser before accessing", "before you continue to google",
		"согласие на использование файлов cookie", "consent to the use of cookies", "we use cookies and data to",
		"uses cookies to deliver its services", "cookies help us deliver our services", "javascript is not available",
		"javascript не доступен", "требуется включить javascript",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func isGoodExtractionContent(content string, maxChars int) bool {
	if content == "" || EnforceTextLimit(content, maxChars) != nil {
		return false
	}
	if isShortBotOrConsentContent(content) {
		return false
	}
	lower := strings.ToLower(content)
	prefix := lower
	if len(prefix) > 100 {
		prefix = prefix[:100]
	}
	ending := strings.TrimRight(lower, " .→\n\t")
	if strings.Contains(prefix, "membership") && len(subscriptionPricePattern.FindAllStringIndex(lower, -1)) >= 2 && strings.Count(lower, "billed annually") >= 2 && strings.Contains(lower, "cancel anytime") && strings.HasSuffix(ending, "continue with free") {
		return false
	}
	stripped := strings.TrimSpace(content)
	if len([]rune(stripped)) < 200 {
		return false
	}
	words := strings.Fields(content)
	whitespaceRatio := float64(len(words)) / float64(max(utf8.RuneCountInString(content), 1))
	if whitespaceRatio < 0.05 || whitespaceRatio > 0.5 {
		return false
	}
	sentences := []string{}
	for _, sentence := range strings.Split(content, ".") {
		sentence = strings.TrimSpace(sentence)
		if utf8.RuneCountInString(sentence) > 10 {
			sentences = append(sentences, sentence)
		}
	}
	if len(sentences) < 2 {
		return false
	}
	sentenceRunes := 0
	for _, sentence := range sentences {
		sentenceRunes += utf8.RuneCountInString(sentence)
	}
	if float64(sentenceRunes)/float64(len(sentences)) < 15 || len(words) < 10 {
		return false
	}
	unique := map[string]struct{}{}
	wordCounts := map[string]int{}
	for _, word := range words {
		if utf8.RuneCountInString(word) > 3 {
			unique[strings.ToLower(word)] = struct{}{}
		}
		if utf8.RuneCountInString(word) > 4 {
			wordCounts[strings.ToLower(word)]++
		}
	}
	if float64(len(unique))/float64(len(words)) < 0.3 {
		return false
	}
	navIndicators := []string{"menu", "navigation", "footer", "sidebar", "advertisement", "cookie", "subscribe", "newsletter", "copyright", "all rights reserved", "terms of service", "privacy policy", "follow us", "social media"}
	navCount := 0
	for _, word := range words {
		wordLower := strings.ToLower(word)
		for _, marker := range navIndicators {
			if strings.Contains(wordLower, marker) {
				navCount++
				break
			}
		}
	}
	if float64(navCount) > float64(len(words))*0.1 {
		return false
	}
	for _, count := range wordCounts {
		if float64(count) > float64(len(words))*0.15 {
			return false
		}
	}
	for _, marker := range []string{"404 not found", "page not found", "access denied", "error 404", "error 500", "internal server error", "page not available", "content not found", "please enable javascript", "javascript is required"} {
		if strings.Contains(strings.ToLower(stripped), marker) {
			return false
		}
	}
	return true
}

func hasMediaFiles(raw json.RawMessage) bool {
	var files []any
	if len(raw) == 0 || json.Unmarshal(raw, &files) != nil {
		return false
	}
	return len(files) > 0
}

func articleExtractionMethod(method string) string {
	switch method {
	case "source_content", "enhanced_selectors", "readability", "browser_rendering",
		"browser_heuristics", "fallback_extraction", "json_ld", "open_graph",
		"meta_description", "enhanced_heuristics", "reddit_json", "reddit_linked_article",
		"learned_pattern_browser_rendering", "learned_pattern_browser_heuristics":
		return method
	default:
		return "fallback_extraction"
	}
}

func callbackErrorCode(code string) string {
	if len(code) <= 40 && regexp.MustCompile(`^[a-z0-9_-]+$`).MatchString(code) {
		return code
	}
	return "extractor_failed"
}

func errorCode(err error) string {
	if err == nil {
		return "processing_error"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "processing_error"
}
