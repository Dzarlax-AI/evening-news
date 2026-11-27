package processing

import (
	"context"
	"errors"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// phaseError carries only a finite phase name across the asynchronous HTTP
// boundary. Its underlying error is never written to service logs.
type phaseError struct {
	phase string
	err   error
}

func (e *phaseError) Error() string { return e.phase + " failed" }
func (e *phaseError) Unwrap() error { return e.err }

func withPhase(phase string, err error) error {
	if err == nil {
		return nil
	}
	return &phaseError{phase: phase, err: err}
}

func diagnosticPhase(err error) string {
	var phase *phaseError
	if errors.As(err, &phase) {
		return phase.phase
	}
	return "processing"
}

// articleError attaches only the numeric article ID to a failed batch item.
type articleError struct {
	id  int64
	err error
}

func (e *articleError) Error() string { return e.err.Error() }
func (e *articleError) Unwrap() error { return e.err }

func articleFailureDetails(err error) (id int64, operation, reason string) {
	var article *articleError
	if errors.As(err, &article) {
		id, operation = article.id, diagnosticPhase(article.err)
	} else {
		operation = diagnosticPhase(err)
	}
	var storeError *store.ArticleOperationError
	if errors.As(err, &storeError) {
		operation = storeError.Operation
	}
	switch {
	case errors.Is(err, store.ErrStaleFence):
		return id, operation, "stale_fence"
	case errors.Is(err, store.ErrRunConflict):
		return id, operation, "run_conflict"
	case errors.Is(err, store.ErrCancelled), errors.Is(err, context.Canceled):
		return id, operation, "cancelled"
	case errors.Is(err, context.DeadlineExceeded), pgconn.Timeout(err):
		return id, operation, "timeout"
	case errors.Is(err, pgx.ErrNoRows):
		return id, operation, "record_missing"
	case storeError != nil && storeError.Unwrap() == nil:
		return id, operation, "claim_conflict"
	}
	var pgError *pgconn.PgError
	if errors.As(err, &pgError) {
		switch pgError.Code {
		case "55P03":
			return id, operation, "lock_unavailable"
		case "40P01":
			return id, operation, "deadlock"
		case "40001":
			return id, operation, "serialization_failure"
		case "57014":
			return id, operation, "database_cancelled"
		case "42501":
			return id, operation, "database_permission_denied"
		default:
			return id, operation, "database_error"
		}
	}
	var integration FatalProcessingError
	if errors.As(err, &integration) {
		switch integration.Code {
		case "extractor_not_configured", "extractor_rejected", "extractor_protocol",
			"article_ai_rejected", "article_ai_protocol":
			return id, operation, integration.Code
		}
	}
	if storeError != nil {
		return id, operation, "store_error"
	}
	return id, operation, "unexpected_error"
}
