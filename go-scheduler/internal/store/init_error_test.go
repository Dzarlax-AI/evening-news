package store

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestInitErrorHasOnlyBoundedDiagnostics(t *testing.T) {
	err := initError("migration_statement", 3, &pgconn.PgError{
		Code:    "42501",
		Message: "permission denied; secret-dsn-must-not-appear",
	})
	var initErr *InitError
	if !errors.As(err, &initErr) {
		t.Fatalf("expected InitError, got %T", err)
	}
	if initErr.Phase != "migration_statement" || initErr.Statement != 3 || initErr.SQLState != "42501" {
		t.Fatalf("unexpected sanitized error: %#v", initErr)
	}
	if strings.Contains(err.Error(), "secret-dsn-must-not-appear") || strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("raw database detail leaked: %q", err.Error())
	}
}

func TestInitErrorOmitsInvalidSQLState(t *testing.T) {
	err := initError("db_connect", 0, &pgconn.PgError{Code: "credential=secret"})
	if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), "sqlstate=") {
		t.Fatalf("unexpected diagnostic: %q", err.Error())
	}
}
