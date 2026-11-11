package repositories

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

// EditorialDigestCatalog returns the date index and latest immutable edition
// using a single repeatable-read snapshot.
func (p *Postgres) EditorialDigestCatalog(ctx context.Context, locale, month string, now time.Time) (EditorialDigestCatalog, error) {
	result := EditorialDigestCatalog{Issues: []EditorialDigestIssue{}}
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return result, err
	}
	defer tx.Rollback(ctx)
	var tableName *string
	if err := tx.QueryRow(ctx, `SELECT to_regclass('editorial_digest_editions')::text`).Scan(&tableName); err != nil {
		return result, err
	}
	if tableName == nil {
		if month == "" {
			result.Month = now.UTC().Format("2006-01")
		} else {
			result.Month = month
		}
		return result, tx.Commit(ctx)
	}
	var earliest, latest, latestID *string
	err = tx.QueryRow(ctx, `SELECT
		(SELECT min(edition_date)::text FROM editorial_digest_editions WHERE locale=$1),
		(SELECT edition_date::text FROM editorial_digest_editions WHERE locale=$1 ORDER BY edition_date DESC,created_at DESC,id DESC LIMIT 1),
		(SELECT id::text FROM editorial_digest_editions WHERE locale=$1 ORDER BY edition_date DESC,created_at DESC,id DESC LIMIT 1)`, locale).Scan(&earliest, &latest, &latestID)
	if err != nil {
		return result, err
	}
	if earliest != nil {
		result.EarliestDate = *earliest
	}
	if latest != nil {
		result.LatestDate = *latest
	}
	if latestID != nil {
		result.LatestID = *latestID
	}
	if month == "" {
		if latest != nil {
			result.Month = (*latest)[:7]
		} else {
			result.Month = now.UTC().Format("2006-01")
		}
	} else {
		result.Month = month
	}
	rows, err := tx.Query(ctx, `SELECT DISTINCT ON (edition_date) edition_date::text,id::text
		FROM editorial_digest_editions WHERE locale=$1 AND edition_date >= ($2::date) AND edition_date < (($2::date + interval '1 month')::date)
		ORDER BY edition_date DESC,created_at DESC,id DESC`, locale, result.Month+"-01")
	if err != nil {
		return result, err
	}
	for rows.Next() {
		var issue EditorialDigestIssue
		if err = rows.Scan(&issue.Date, &issue.ID); err != nil {
			rows.Close()
			return result, err
		}
		result.Issues = append(result.Issues, issue)
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return result, err
	}
	rows.Close()
	if err = tx.Commit(ctx); err != nil {
		return EditorialDigestCatalog{}, fmt.Errorf("commit digest catalog snapshot: %w", err)
	}
	return result, nil
}
