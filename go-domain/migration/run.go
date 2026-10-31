package migration

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Bootstrap SQL is captured from the original SQLAlchemy PostgreSQL metadata
// in dependency order, before its conditional migrations execute.
//
//go:embed sql/bootstrap.json
var bootstrapJSON []byte

type bootstrapStatement struct {
	Kind  string `json:"kind"`
	Name  string `json:"name"`
	Table string `json:"table"`
	SQL   string `json:"sql"`
}
type Result struct {
	MigrationsRun     []map[string]any `json:"migrations_run"`
	MigrationsSkipped []map[string]any `json:"migrations_skipped"`
	Errors            []string         `json:"errors"`
	TotalTime         float64          `json:"total_time"`
	DryRun            bool             `json:"dry_run"`
}

func bootstrap(ctx context.Context, tx pgx.Tx) error {
	var statements []bootstrapStatement
	if err := json.Unmarshal(bootstrapJSON, &statements); err != nil {
		return err
	}
	created := map[string]bool{}
	for _, s := range statements {
		switch s.Kind {
		case "table":
			var exists bool
			err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=current_schema() AND c.relname=$1 AND c.relkind IN ('r','p','v','m','f'))`, s.Name).Scan(&exists)
			if err != nil {
				return err
			}
			if exists {
				continue
			}
			if err := execute(ctx, tx, s.SQL); err != nil {
				return fmt.Errorf("bootstrap %s: %w", s.Name, err)
			}
			created[s.Name] = true
		case "index":
			if created[s.Table] {
				if err := execute(ctx, tx, s.SQL); err != nil {
					return fmt.Errorf("bootstrap %s: %w", s.Name, err)
				}
			}
		default:
			return fmt.Errorf("unknown bootstrap statement")
		}
	}
	return nil
}
func allMigrations() []migration {
	return append(earlyMigrations(), backgroundMigration(), schedulerMigration(), executionMigration(), ingestionMigration(), processingMigration(), editorialMigration(), deliveryMigration(), managedConfigurationMigration(), localizationMigration(), editorialLocalizationMigration(), serbianDigestRetryMigration(), viberMigration())
}

// Run is explicitly invoked by maintenance, never by service startup. Each
// conditional migration and its privilege changes commit together.
func Run(ctx context.Context, pool *pgxpool.Pool, allowCreateAll, dryRun bool) (Result, error) {
	return runMigrations(ctx, pool, allMigrations(), allowCreateAll, dryRun)
}

// RunAdoption applies only the additive managed-configuration migration.
// Adoption must never run legacy migrations against an existing installation.
func RunAdoption(ctx context.Context, pool *pgxpool.Pool) (Result, error) {
	return runMigrations(ctx, pool, []migration{managedConfigurationMigration()}, false, false)
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool, migrations []migration, allowCreateAll, dryRun bool) (Result, error) {
	started := time.Now()
	result := Result{MigrationsRun: []map[string]any{}, MigrationsSkipped: []map[string]any{}, Errors: []string{}, DryRun: dryRun}
	if pool == nil {
		return result, errors.New("migration database unavailable")
	}
	if allowCreateAll && !dryRun {
		tx, err := pool.Begin(ctx)
		if err != nil {
			return result, err
		}
		err = bootstrap(ctx, tx)
		if err != nil {
			_ = tx.Rollback(ctx)
			return result, err
		}
		if err = tx.Commit(ctx); err != nil {
			return result, err
		}
	}
	for _, m := range migrations {
		tx, err := pool.Begin(ctx)
		if err != nil {
			result.Errors = append(result.Errors, "Migration system error: database unavailable")
			break
		}
		needed, err := m.Needed(ctx, tx)
		if err != nil {
			_ = tx.Rollback(ctx)
			result.Errors = append(result.Errors, safeMigrationError(m.ID, err))
			continue
		}
		if !needed {
			_ = tx.Rollback(ctx)
			result.MigrationsSkipped = append(result.MigrationsSkipped, map[string]any{"id": m.ID, "reason": "Already applied or not needed"})
			continue
		}
		var detail map[string]any
		if dryRun {
			detail = map[string]any{"dry_run": true}
			_ = tx.Rollback(ctx)
		} else {
			detail, err = m.Apply(ctx, tx)
			if err == nil {
				err = tx.Commit(ctx)
			} else {
				_ = tx.Rollback(ctx)
			}
		}
		if err != nil {
			result.Errors = append(result.Errors, safeMigrationError(m.ID, err))
			continue
		}
		result.MigrationsRun = append(result.MigrationsRun, map[string]any{"id": m.ID, "description": m.Description, "result": detail})
	}
	result.TotalTime = time.Since(started).Seconds()
	for _, e := range result.Errors {
		if strings.HasPrefix(e, "Migration 006_feed_performance_optimization failed:") {
			continue
		}
		return result, errors.New("required migration failed")
	}
	return result, nil
}

// safeMigrationError deliberately excludes database-provided text because
// optional migration errors are returned through the private admin API.
func safeMigrationError(id string, _ error) string {
	return "Migration " + id + " failed: database operation failed"
}
