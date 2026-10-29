// Package digestworkflow owns the bounded immutable editorial snapshot.
package digestworkflow

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/telegramdelivery"
	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/text/cases"
)

const maxArticles = 500

var ErrInvalid = errors.New("editorial_invalid_result")
var ErrLimit = errors.New("editorial_resource_limit")

type Workflow struct {
	Pool            *pgxpool.Pool
	Fence           telegramdelivery.TransactionFence
	Provider        editorial.ProviderConfig
	ResolveProvider func(context.Context) (editorial.ProviderConfig, error)
	RecordUsage     func(context.Context, pgx.Tx, editorial.ProviderUsage) error
	PriceUsage      func(editorial.ProviderUsage) *float64
	SiteBase        string
}

var categories = map[string]string{"serbia": "Сербия", "tech": "Технологии", "technology": "Технологии", "ai": "Искусственный интеллект", "politics": "Политика", "business": "Бизнес", "media": "Медиа", "science": "Наука", "health": "Здоровье", "world": "Мир", "culture": "Культура", "other": "Другое"}

func categoryLabel(name, display string) string {
	if s := categories[cases.Fold().String(strings.TrimSpace(name))]; s != "" {
		return s
	}
	if strings.TrimSpace(display) != "" {
		return strings.TrimSpace(display)
	}
	return strings.TrimSpace(name)
}

// EditionID mirrors Python uuid5 namespace and canonical UTF-8 input.
func EditionID(job string, attempt int, operation string) string {
	ns, _ := hex.DecodeString("9e341f33e4dc5ee89013817f41e9d442")
	h := sha1.New()
	h.Write(ns)
	fmt.Fprintf(h, "%s:%d:%s:ru", job, attempt, operation)
	b := h.Sum(nil)[:16]
	b[6] = (b[6] & 15) | 0x50
	b[8] = (b[8] & 63) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:])
}
func (w Workflow) Snapshot(ctx context.Context, cutoff time.Time, id string) (editorial.Snapshot, []byte, error) {
	var empty editorial.Snapshot
	base := strings.TrimRight(w.SiteBase, "/")
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || strings.ContainsAny(base, " \t\n\r") {
		return empty, nil, ErrInvalid
	}
	cutoff = cutoff.UTC().Truncate(time.Microsecond)
	date := cutoff.Format("2006-01-02")
	snap := editorial.Snapshot{Date: date, CutoffUTC: cutoff, SiteBaseURL: base, EditionURL: base + "/digest/" + date + "?edition=" + id, Articles: []editorial.Article{}}
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return empty, nil, err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return empty, nil, err
	}
	// Full article content is intentionally excluded: digest generation is
	// grounded in the saved summary, while the edition owns source links.
	rows, err := tx.Query(ctx, `SELECT a.id, COALESCE(a.title,''), COALESCE(a.summary,''), COALESCE(a.url,''),
 COALESCE(s.name,''), c.id, COALESCE(c.name,''), COALESCE(c.display_name,'')
 FROM articles a LEFT JOIN sources s ON s.id=a.source_id
 JOIN (SELECT article_id,category_id,row_number() OVER(PARTITION BY article_id ORDER BY COALESCE(confidence,0) DESC,category_id ASC) AS rank FROM article_categories) r ON r.article_id=a.id AND r.rank=1
 LEFT JOIN categories c ON c.id=r.category_id
 WHERE date(a.fetched_at)=$1::date AND a.processing_state='completed' AND a.is_advertisement IS NOT TRUE
 ORDER BY a.fetched_at DESC,a.id ASC LIMIT 501`, date)
	if err != nil {
		return empty, nil, err
	}
	for rows.Next() {
		var aid int
		var cid *int
		var a editorial.Article
		var name, display string
		if err = rows.Scan(&aid, &a.Title, &a.Summary, &a.URL, &a.SourceName, &cid, &name, &display); err != nil {
			rows.Close()
			return empty, nil, err
		}
		if aid <= 0 || cid == nil || *cid <= 0 || strings.TrimSpace(name) == "" || strings.TrimSpace(a.SourceName) == "" {
			rows.Close()
			return empty, nil, ErrInvalid
		}
		a.ID = strconv.Itoa(aid)
		a.CategoryID = strconv.Itoa(*cid)
		a.CategoryName = categoryLabel(name, display)
		snap.Articles = append(snap.Articles, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return empty, nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return empty, nil, err
	}
	if len(snap.Articles) > maxArticles {
		return empty, nil, ErrLimit
	}
	if len(snap.Articles) == 0 {
		return snap, nil, nil
	}
	if _, err = editorial.ValidateSnapshot(snap, "ru"); err != nil {
		return empty, nil, ErrInvalid
	}
	// Persist all fields including empty summary/content, matching the Python snapshot.
	articles := make([]map[string]string, 0, len(snap.Articles))
	for _, a := range snap.Articles {
		articles = append(articles, map[string]string{"id": a.ID, "title": a.Title, "summary": a.Summary, "content": a.Content, "source_name": a.SourceName, "url": a.URL, "category_id": a.CategoryID, "category_name": a.CategoryName})
	}
	raw, err := json.Marshal(map[string]any{"date": date, "cutoff_utc": cutoff.Format("2006-01-02T15:04:05.000000Z"), "site_base_url": base, "edition_url": snap.EditionURL, "articles": articles})
	if err != nil {
		return empty, nil, ErrInvalid
	}
	if len(raw) > 4<<20 {
		return empty, nil, ErrLimit
	}
	return snap, raw, nil
}
