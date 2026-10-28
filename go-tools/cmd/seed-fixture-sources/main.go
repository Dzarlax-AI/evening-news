// seed-fixture-sources seeds eight synthetic sources in an isolated test database.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"os"
	"time"
)

type source struct{ name, kind, url, config string }

func run() error {
	dsn := os.Getenv("FIXTURE_DATABASE_URL")
	if dsn == "" || os.Getenv("NEWS_FIXTURE_CONFIRM") != "disposable-fixture-only" {
		return errors.New("fixture database required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, e := pgx.Connect(ctx, dsn)
	if e != nil {
		return errors.New("fixture database unavailable")
	}
	defer conn.Close(ctx)
	var database string
	if e = conn.QueryRow(ctx, "SELECT current_database()").Scan(&database); e != nil || database != "gate_db" {
		return errors.New("fixture database required")
	}
	tx, e := conn.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, `UPDATE sources SET enabled=false`); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, `INSERT INTO categories(name,display_name,description,color,is_local,created_at) VALUES ('Tech','Technology','Synthetic Go web gate category','#334155',false,now()) ON CONFLICT(name) DO NOTHING`); e != nil {
		return e
	}
	sources := []source{}
	for i := 1; i <= 4; i++ {
		edition := "one"
		if i%2 == 0 {
			edition = "two"
		}
		sources = append(sources, source{fmt.Sprintf("Go Web Gate RSS %d", i), "rss", "https://rss.fixture.test/rss.xml?edition=" + edition, `{"limit":2}`})
	}
	sources = append(sources, source{"Go Web Gate Telegram", "telegram", "https://t.me/s/fixture_channel", `{"limit":2}`})
	for i := 1; i <= 3; i++ {
		sources = append(sources, source{fmt.Sprintf("Go Web Gate Page Monitor %d", i), "custom", "https://pm.fixture.test/news", `{"article_selectors":["main"],"title_selectors":[],"date_selectors":[],"link_selectors":[],"enable_ai_analysis":true,"max_articles_per_check":2,"use_browser":false,"wait_for_js":false}`})
	}
	ids := map[string]int{}
	for _, s := range sources {
		var id int
		e = tx.QueryRow(ctx, `SELECT id FROM sources WHERE name=$1 ORDER BY id LIMIT 1`, s.name).Scan(&id)
		if errors.Is(e, pgx.ErrNoRows) {
			e = tx.QueryRow(ctx, `INSERT INTO sources(name,source_type,url,enabled,config,fetch_interval,error_count,created_at,updated_at) VALUES ($1,$2,$3,true,$4::json,3600,0,now(),now()) RETURNING id`, s.name, s.kind, s.url, s.config).Scan(&id)
		} else if e == nil {
			_, e = tx.Exec(ctx, `UPDATE sources SET enabled=true WHERE name=$1`, s.name)
		}
		if e != nil {
			return e
		}
		ids[s.name] = id
	}
	if e = tx.Commit(ctx); e != nil {
		return e
	}
	return json.NewEncoder(os.Stdout).Encode(map[string]any{"source_ids": ids})
}
func main() {
	if e := run(); e != nil {
		fmt.Fprintln(os.Stderr, "fixture seeding failed")
		os.Exit(1)
	}
}
