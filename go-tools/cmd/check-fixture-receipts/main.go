// check-fixture-receipts reads aggregate receipts from the disposable gate DB.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"regexp"
	"sort"
	"time"
)

var jobPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

func run(job string) error {
	if !jobPattern.MatchString(job) || os.Getenv("NEWS_FIXTURE_CONFIRM") != "disposable-fixture-only" || os.Getenv("FIXTURE_DATABASE_URL") == "" {
		return errors.New("invalid fixture configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, e := pgx.Connect(ctx, os.Getenv("FIXTURE_DATABASE_URL"))
	if e != nil {
		return e
	}
	defer conn.Close(ctx)
	var db string
	if e = conn.QueryRow(ctx, "SELECT current_database()").Scan(&db); e != nil || db != "gate_db" {
		return errors.New("invalid fixture database")
	}
	rows, e := conn.Query(ctx, `SELECT sr.source_type,sr.state,sr.error_code FROM article_ingestion_source_runs sr JOIN article_ingestion_runs ir ON ir.id=sr.run_id JOIN sources s ON s.id=sr.source_id WHERE ir.job_id=$1::uuid AND s.name LIKE 'Go Web Gate %'`, job)
	if e != nil {
		return e
	}
	counts := map[string]int{"rss": 0, "telegram": 0, "custom": 0}
	states := map[string]bool{}
	valid := true
	total, errorsCount := 0, 0
	for rows.Next() {
		var typ, state string
		var code *string
		if e = rows.Scan(&typ, &state, &code); e != nil {
			rows.Close()
			return e
		}
		total++
		if _, ok := counts[typ]; !ok {
			valid = false
		} else {
			counts[typ]++
		}
		states[state] = true
		if state != "succeeded" && state != "partial" {
			valid = false
		}
		if code != nil {
			valid = false
			errorsCount++
		}
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return e
	}
	valid = valid && total == 8 && counts["rss"] == 4 && counts["telegram"] == 1 && counts["custom"] == 3
	var processed int
	if e = conn.QueryRow(ctx, `SELECT count(*) FROM articles a JOIN sources s ON s.id=a.source_id WHERE s.name LIKE 'Go Web Gate %' AND a.processed=true AND a.summary_processed=true AND a.category_processed=true`).Scan(&processed); e != nil {
		return e
	}
	stateList := make([]string, 0, len(states))
	for s := range states {
		stateList = append(stateList, s)
	}
	sort.Strings(stateList)
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"valid": valid, "counts": counts, "states": stateList, "error_count": errorsCount, "processed": processed})
}
func main() {
	if len(os.Args) != 2 {
		fmt.Fprintln(os.Stderr, "fixture receipt check failed")
		os.Exit(1)
	}
	if err := run(os.Args[1]); err != nil {
		fmt.Fprintln(os.Stderr, "fixture receipt check failed")
		os.Exit(1)
	}
}
