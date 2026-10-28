package migration

import (
	"errors"
	"strings"
	"testing"
)

func TestMigrationErrorsDoNotExposeDatabaseText(t *testing.T) {
	got := safeMigrationError("006_feed_performance_optimization", errors.New("sensitive-fixture-database-detail"))
	if strings.Contains(got, "sensitive-fixture") || !strings.Contains(got, "006_feed_performance_optimization") {
		t.Fatalf("error=%q", got)
	}
}
