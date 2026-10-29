package airuntime

import (
	"context"
	"errors"

	ai "github.com/Dzarlax-AI/evening-news/go-ai"
	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
)

// AnalyzeCallback preserves the article processor's legacy fallback result
// while using the in-process runtime and its shared admission controls.
func (s *Server) AnalyzeCallback(ctx context.Context, input ArticleInput) (map[string]any, error) {
	result, _, err := s.AnalyzeArticle(ctx, input)
	if err == nil {
		return result, nil
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	var status *int
	var retry *float64
	var pe *providerFailure
	var ae *analysis.AnalysisError
	var aie *ai.Error
	if errors.As(err, &pe) {
		status, retry = pe.status, pe.retryAfter
	} else if errors.As(err, &aie) {
		if aie.StatusCode != 0 {
			status = &aie.StatusCode
		}
		if aie.RetryAfter > 0 {
			seconds := aie.RetryAfter.Seconds()
			retry = &seconds
		}
	} else if errors.As(err, &ae) {
		status, retry = ae.Detail.StatusCode, ae.Detail.RetryAfterSeconds
	}
	terminal := status != nil && (*status == 400 || *status == 413 || *status == 422)
	return map[string]any{"_processing_error": "APIError", "_processing_terminal": terminal, "_retry_after_seconds": retry,
		"optimized_title": nil, "summary": nil, "categories": []string{"Other"}, "category_confidences": []float64{0.1}, "category": "Other", "category_confidence": 0.1, "summary_confidence": 0.0, "is_advertisement": false, "ad_type": "news_article", "ad_confidence": 0.0, "ad_reasoning": "Fallback analysis - AI unavailable", "publication_date": nil, "content_quality": 0.2, "confidence": 0.1}, nil
}
