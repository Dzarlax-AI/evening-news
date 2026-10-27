package execution

import (
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
)

// logExecutionFailure records only a stable phase/reason and PostgreSQL SQLSTATE.
// It intentionally omits raw driver errors, identifiers, SQL, and payloads.
func logExecutionFailure(phase, reason string, err error) {
	attrs := []any{"phase", phase}
	if reason != "" {
		attrs = append(attrs, "reason_code", reason)
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code != "" {
		attrs = append(attrs, "sqlstate", pgErr.Code)
	}
	slog.Warn("execution operation failed", attrs...)
}
