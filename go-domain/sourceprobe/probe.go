// Package sourceprobe performs bounded connection tests through the existing collector.
package sourceprobe

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/digestworkflow"
	"github.com/Dzarlax-AI/evening-news/go-domain/extractorclient"
	"github.com/jackc/pgx/v5"
	"math"
	"strconv"
	"time"
)

type Probe struct{ Client *extractorclient.Client }
type Source struct {
	ID              int64
	Name, Type, URL string
	Config          map[string]any
}

var articleSelectors = []string{"article", ".article", ".news-item", ".post", ".entry", ".changelog-item", ".update-item", ".release-note", ".content li", ".main li", "ul.updates li", "ul.news li", ".content > div", ".main > div", ".updates > div", `[class*="item"]`, `[class*="post"]`, `[class*="article"]`, "h2, h3, h4", ".title", ".headline", ".heading"}
var titleSelectors = []string{"h1", "h2", "h3", "h4", ".title", ".headline", ".heading", "a[href]", ".link", ".post-title", ".article-title"}
var dateSelectors = []string{"time", "time[datetime]", "[datetime]", ".date", ".timestamp", ".published", ".created", ".updated", ".publish-date", ".published-date", ".creation-date", ".post-date", ".article-date", ".news-date", ".entry-date", ".blog-date", ".meta .date", ".meta time", ".metadata .date", ".info .date", ".post-meta .date", ".article-meta .date", ".entry-meta .date", ".commit-date", ".release-date", ".version-date", ".update-date", ".changelog-date", ".timeline .date", ".byline .date", ".author-date", ".publish-info .date", `[class*="date"]`, `[class*="time"]`, `[class*="publish"]`, `[id*="date"]`, `[id*="time"]`}

func intValue(v any) (int, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	i, e := strconv.ParseInt(string(n), 10, 32)
	return int(i), e == nil
}
func descriptor(s Source) (map[string]any, bool) {
	d := map[string]any{"source_id": s.ID, "type": s.Type, "source_name": s.Name, "url": s.URL, "enabled": true, "per_source_budget_ms": 30000}
	limit, hasLimit := intValue(s.Config["limit"])
	switch s.Type {
	case "rss":
		if hasLimit && limit > 0 {
			d["limit"] = limit
		}
	case "telegram":
		tg := map[string]any{}
		if value, present := s.Config["limit"]; present && value != nil && !hasLimit {
			return nil, false
		}
		if hasLimit {
			if limit <= 0 {
				return nil, false
			}
			if limit > 100 {
				limit = 100
			}
			d["limit"] = limit
			tg["limit"] = limit
		}
		d["telegram"] = tg
	case "custom":
		if v, ok := s.Config["wait_timeout_ms"]; ok {
			n, ok := number(v)
			if raw, yes := v.(string); yes {
				var err error
				n, err = strconv.ParseFloat(raw, 64)
				ok = err == nil
			}
			if flag, yes := v.(bool); yes {
				ok = true
				if flag {
					n = 1
				} else {
					n = 0
				}
			}
			if !ok || n <= 0 || math.IsNaN(n) || math.IsInf(n, 0) {
				return nil, false
			}
		}
		c := map[string]any{}
		for _, pair := range []struct {
			key string
			def any
		}{{"use_browser", true}, {"wait_for_js", true}, {"min_title_length", json.Number("10")}, {"article_selectors", articleSelectors}, {"title_selectors", titleSelectors}, {"date_selectors", dateSelectors}, {"link_selectors", []string{"a[href]", ".link", ".read-more", ".permalink"}}} {
			v, ok := s.Config[pair.key]
			if !ok || v == nil {
				v = pair.def
			}
			c[pair.key] = v
		}
		value, present := s.Config["max_articles_per_check"]
		if !present {
			value = json.Number("20")
		}
		n, ok := intValue(value)
		if ok && n > 0 {
			if n > 100 {
				n = 100
			}
			d["limit"] = n
		}
		d["page_monitor"] = c
	default:
		return nil, false
	}
	return d, true
}
func number(v any) (float64, bool) {
	n, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	f, e := strconv.ParseFloat(string(n), 64)
	return f, e == nil
}
func (p *Probe) Test(ctx context.Context, f authority.Fence, id int64) (bool, error) {
	tx, e := f.Pool.Begin(ctx)
	if e != nil {
		return false, e
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	if e = f.Validate(ctx, tx); e != nil {
		return false, e
	}
	var s Source
	s.ID = id
	var raw []byte
	e = tx.QueryRow(ctx, `SELECT name,source_type,url,config FROM sources WHERE id=$1`, id).Scan(&s.Name, &s.Type, &s.URL, &raw)
	if errors.Is(e, pgx.ErrNoRows) {
		return false, errors.New("source missing")
	}
	if e != nil {
		return false, e
	}
	dec := json.NewDecoder(bytesReader(raw))
	dec.UseNumber()
	if dec.Decode(&s.Config) != nil || s.Config == nil {
		s.Config = map[string]any{}
	}
	if e = tx.Rollback(ctx); e != nil {
		return false, e
	}
	if p.Client == nil {
		return false, nil
	}
	d, ok := descriptor(s)
	if !ok {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	for attempt := 0; attempt < 2; attempt++ {
		id, e := digestworkflow.NewUUID()
		if e != nil {
			return false, e
		}
		var response json.RawMessage
		e = p.Client.Post(ctx, "/v1/sources/collect", map[string]any{"contract_version": "1", "request_id": id, "batch_budget_ms": 27000, "sources": []any{d}}, &response)
		if e != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			return false, nil
		}
		outcome, delay, valid := validateResponse(response, id, s, d)
		if !valid {
			return false, nil
		}
		if outcome == "deferred" && attempt == 0 && delay > 0 && delay < 5 {
			timer := time.NewTimer(time.Duration(delay) * time.Second)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				return false, ctx.Err()
			}
			continue
		}
		return outcome == "success" || outcome == "partial", nil
	}
	return false, nil
}
func bytesReader(b []byte) *bytes.Reader { return bytes.NewReader(b) }
