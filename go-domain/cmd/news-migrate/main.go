// news-migrate is an explicit one-shot schema maintenance command.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "migration failed")
		os.Exit(1)
	}
}
func run() error {
	dsn := strings.Replace(os.Getenv("DATABASE_URL"), "postgresql+asyncpg://", "postgresql://", 1)
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL required")
	}
	allow := true
	if raw := strings.ToLower(strings.TrimSpace(os.Getenv("ALLOW_CREATE_ALL"))); raw != "" {
		switch raw {
		case "1", "true", "yes", "on", "y", "t":
			allow = true
		case "0", "false", "no", "off", "n", "f":
			allow = false
		default:
			return fmt.Errorf("invalid ALLOW_CREATE_ALL")
		}
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}
	cfg.MaxConns = 1
	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	result, err := migration.Run(ctx, pool, allow, false)
	fmt.Printf("migrations_run=%d errors=%d\n", len(result.MigrationsRun), len(result.Errors))
	if err != nil {
		return err
	}
	if len(result.Errors) > 0 {
		return fmt.Errorf("migrations reported errors")
	}
	return nil
}
