package processing

import (
	"context"
	"errors"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
	"github.com/jackc/pgx/v5/pgconn"
)

func TestArticleFailureDetails(t *testing.T) {
	for _, test := range []struct {
		name, operation, wantOperation, wantReason string
		cause                                      error
	}{
		{"timeout", "claim_article", "claim_article", "timeout", context.DeadlineExceeded},
		{"claim conflict", "claim_article", "claim_check_snapshot", "claim_conflict", &store.ArticleOperationError{Operation: "claim_check_snapshot"}},
		{"database", "save_article_outcome", "save_article_outcome", "lock_unavailable", &pgconn.PgError{Code: "55P03", Message: "private SQL context"}},
		{"fence", "claim_article", "claim_article", "stale_fence", store.ErrStaleFence},
		{"integration", "extract_article", "extract_article", "extractor_protocol", fatal("extractor_protocol")},
		{"arbitrary", "analyze_article", "analyze_article", "unexpected_error", errors.New("private payload")},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := withPhase("process_batch", &articleError{id: 19313, err: withPhase(test.operation, test.cause)})
			id, operation, reason := articleFailureDetails(err)
			if diagnosticPhase(err) != "process_batch" || id != 19313 || operation != test.wantOperation || reason != test.wantReason {
				t.Fatalf("diagnostics = %s %d %s %s", diagnosticPhase(err), id, operation, reason)
			}
		})
	}
}
