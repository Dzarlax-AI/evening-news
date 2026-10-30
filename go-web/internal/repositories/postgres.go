package repositories

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Postgres is the bounded-pool read adapter for the public site.
type Postgres struct{ pool *pgxpool.Pool }

func (*Postgres) SupportsPublicLocale(locale string) bool {
	return locale == "ru" || locale == "sr-Latn"
}

// Pool exposes the shared bounded connection pool to command and scheduler adapters.
func (p *Postgres) Pool() *pgxpool.Pool { return p.pool }

func OpenPostgres(ctx context.Context, databaseURL string, maxConns int32) (*Postgres, error) {
	databaseURL = strings.Replace(databaseURL, "postgresql+asyncpg://", "postgres://", 1)
	cfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, errors.New("invalid database configuration")
	}
	if maxConns <= 0 {
		maxConns = 8
	}
	cfg.MaxConns = maxConns
	cfg.MinConns = 0
	cfg.MaxConnLifetime = 30 * time.Minute
	cfg.MaxConnIdleTime = 5 * time.Minute
	p, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, errors.New("database pool could not be opened")
	}
	if err := p.Ping(ctx); err != nil {
		p.Close()
		return nil, errors.New("database is unavailable")
	}
	return &Postgres{pool: p}, nil
}

func (p *Postgres) Close() {
	if p != nil && p.pool != nil {
		p.pool.Close()
	}
}

const articleProjection = `a.id,a.source_id,a.title,a.summary,a.content,a.url,a.image_url,a.media_files,
 a.published_at,a.fetched_at,a.is_advertisement,a.ad_confidence,a.ad_type,a.ad_reasoning,
 s.name, COALESCE((SELECT c.display_name FROM article_categories ac JOIN categories c ON c.id=ac.category_id WHERE ac.article_id=a.id ORDER BY ac.id LIMIT 1),'Other'),
 COALESCE((SELECT jsonb_agg(jsonb_build_object('name',c.name,'display_name',c.display_name,'confidence',ac.confidence,'color',c.color) ORDER BY ac.id) FROM article_categories ac JOIN categories c ON c.id=ac.category_id WHERE ac.article_id=a.id),'[]'::jsonb),
 COALESCE((SELECT jsonb_agg(jsonb_build_object('ai_category',COALESCE(ac.ai_category,'Other'),'confidence',COALESCE(NULLIF(ac.confidence,0),1.0)) ORDER BY ac.id) FROM article_categories ac JOIN categories c ON c.id=ac.category_id WHERE ac.article_id=a.id),'[]'::jsonb)`

const serbianReady = `EXISTS (SELECT 1 FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed' AND NULLIF(btrim(l.title),'') IS NOT NULL AND NULLIF(btrim(l.summary),'') IS NOT NULL)`

func localizedProjection(ctx context.Context) string {
	if !isSerbian(ctx) {
		return articleProjection
	}
	projection := strings.Replace(articleProjection, "a.id,a.source_id,a.title,a.summary,a.content", `a.id,a.source_id,(SELECT l.title FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed'),(SELECT l.summary FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed'),a.content`, 1)
	projection = strings.ReplaceAll(projection, "c.display_name", `COALESCE((SELECT cl.display_name FROM category_localizations cl WHERE cl.category_id=c.id AND cl.locale='sr-Latn'),'Ostale teme')`)
	return strings.Replace(projection, "'Other'", "'Ostale teme'", 1)
}

func published(includeAds bool) string {
	if includeAds {
		return `(a.processing_state='completed' OR (a.processing_state='rejected' AND a.ad_processed IS TRUE AND a.is_advertisement IS TRUE))`
	}
	return `a.processing_state='completed'`
}

func (p *Postgres) Feed(ctx context.Context, q FeedQuery) (FeedResponse, error) {
	where := []string{published(q.Category == "advertisements" || !q.HideAds)}
	if isSerbian(ctx) {
		where = append(where, serbianReady)
	}
	args := []any{}
	add := func(expr string, val any) {
		args = append(args, val)
		where = append(where, fmt.Sprintf(expr, len(args)))
	}
	if q.SinceHours != nil {
		add(`a.published_at >= now() AT TIME ZONE 'UTC' - ($%d * interval '1 hour')`, *q.SinceHours)
	}
	if q.HideAds && strings.ToLower(q.Category) != "advertisements" {
		where = append(where, `a.is_advertisement IS FALSE`)
	}
	if c := strings.ToLower(strings.TrimSpace(q.Category)); c != "" && c != "all" {
		if c == "advertisements" {
			where = append(where, `a.is_advertisement IS TRUE`)
		} else {
			add(`EXISTS (SELECT 1 FROM article_categories ac JOIN categories c ON c.id=ac.category_id WHERE ac.article_id=a.id AND lower(c.name)=$%d)`, c)
		}
	}
	if src := strings.TrimSpace(q.Source); src != "" && strings.ToLower(src) != "all" {
		ids := []int64{}
		names := []string{}
		for _, part := range strings.Split(src, ",") {
			part = strings.TrimSpace(part)
			if part == "" {
				continue
			}
			var id int64
			if _, err := fmt.Sscan(part, &id); err == nil && id > 0 {
				ids = append(ids, id)
			} else {
				names = append(names, part)
			}
		}
		if len(ids) > 0 {
			add(`a.source_id=ANY($%d::bigint[])`, ids)
		} else if len(names) > 0 {
			add(`s.name=ANY($%d::text[])`, names)
		}
	}
	args = append(args, q.Limit, q.Offset)
	sql := `SELECT ` + localizedProjection(ctx) + ` FROM articles a LEFT JOIN sources s ON s.id=a.source_id WHERE ` + strings.Join(where, " AND ") + fmt.Sprintf(` ORDER BY a.published_at DESC OFFSET $%d LIMIT $%d`, len(args), len(args)-1)
	rows, err := p.pool.Query(ctx, sql, args...)
	if err != nil {
		return FeedResponse{}, err
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		item, e := scanArticle(rows, false, true)
		if e != nil {
			return FeedResponse{}, e
		}
		delete(item, "primary_image")
		delete(item, "is_advertisement_value")
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return FeedResponse{}, err
	}
	return FeedResponse{Articles: items, Total: len(items), Limit: q.Limit, Offset: q.Offset}, nil
}

func (p *Postgres) Article(ctx context.Context, id int64, includeUnpublished bool) (map[string]any, error) {
	where := `a.id=$1`
	if isSerbian(ctx) {
		where += ` AND ` + serbianReady
	}
	if !includeUnpublished {
		where += ` AND ` + published(true)
	}
	row := p.pool.QueryRow(ctx, `SELECT `+localizedProjection(ctx)+` FROM articles a LEFT JOIN sources s ON s.id=a.source_id WHERE `+where, id)
	item, err := scanArticle(row, true, true)
	if err == pgx.ErrNoRows {
		return nil, nil
	}
	delete(item, "primary_image")
	delete(item, "is_advertisement_value")
	return item, err
}

func scanArticle(row interface{ Scan(...any) error }, includeContent, clean bool) (map[string]any, error) {
	var id int64
	var sourceID *int64
	var title, summary, content, url, imageURL, sourceName, category *string
	var media []byte
	var publishedAt, fetchedAt *time.Time
	var isAd *bool
	var adConf *float64
	var adType, adReasoning *string
	var cats, aiCats []byte
	if err := row.Scan(&id, &sourceID, &title, &summary, &content, &url, &imageURL, &media, &publishedAt, &fetchedAt, &isAd, &adConf, &adType, &adReasoning, &sourceName, &category, &cats, &aiCats); err != nil {
		return nil, err
	}
	if clean && title != nil {
		v := cleanText(*title)
		title = &v
	}
	if clean && summary != nil {
		v := cleanText(cleanSummary(*summary))
		summary = &v
	}
	if clean && content != nil {
		v := cleanText(*content)
		content = &v
	}
	var mediaValue any = []any{}
	if len(media) > 0 {
		_ = json.Unmarshal(media, &mediaValue)
	}
	var catValue any = []any{}
	if len(cats) > 0 {
		_ = json.Unmarshal(cats, &catValue)
	}
	var aiValue any = []any{}
	if len(aiCats) > 0 {
		_ = json.Unmarshal(aiCats, &aiValue)
	}
	domain := "unknown"
	if url != nil {
		if u, err := urlParse(*url); err == nil && u != "" {
			domain = u
		}
	}
	var pAt, fAt any
	if publishedAt != nil {
		pAt = pythonISO(*publishedAt)
	}
	if fetchedAt != nil {
		fAt = pythonISO(*fetchedAt)
	}
	var sid any
	if sourceID != nil {
		sid = *sourceID
	}
	var ad bool
	if isAd != nil {
		ad = *isAd
	}
	conf := 0.0
	if adConf != nil {
		conf = *adConf
	}
	var source any = "Unknown"
	if sourceName != nil {
		source = *sourceName
	}
	var titleV, summaryV, urlV, imageV, adTypeV, adReasoningV any
	titleV = stringOrNil(title)
	summaryV = stringOrNil(summary)
	urlV = stringOrNil(url)
	imageV = stringOrNil(imageURL)
	adTypeV = stringOrNil(adType)
	adReasoningV = stringOrNil(adReasoning)
	var primary *string
	if files, ok := mediaValue.([]any); ok {
		for _, f := range files {
			if m, ok := f.(map[string]any); ok && m["type"] == "image" {
				if u, ok := m["url"].(string); ok {
					primary = &u
					break
				}
			}
		}
	}
	if primary == nil {
		primary = imageURL
	}
	item := map[string]any{"id": id, "article_id": id, "title": titleV, "summary": summaryV, "image_url": imageV, "primary_image": stringOrNil(primary), "media_files": mediaValue, "url": urlV, "domain": domain, "category": valueOr(category, "Other"), "categories": catValue, "ai_categories": aiValue, "published_at": pAt, "fetched_at": fAt, "is_advertisement": ad, "is_advertisement_value": boolOrNil(isAd), "ad_confidence": conf, "ad_type": adTypeV, "ad_reasoning": adReasoningV, "source_id": sid, "source_name": source}
	if includeContent {
		item["content"] = stringOrNil(content)
	} else if content != nil {
		item["content"] = truncateRunes(*content, 500)
	} else {
		item["content"] = nil
	}
	return item, nil
}

func stringOrNil(value *string) any {
	if value == nil {
		return nil
	}
	return *value
}
func boolOrNil(value *bool) any {
	if value == nil {
		return nil
	}
	return *value
}

func valueOr(v *string, fallback string) string {
	if v == nil || *v == "" {
		return fallback
	}
	return *v
}
func urlParse(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	return strings.ToLower(u.Host), nil
}
func truncateRunes(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "..."
}
func pythonISO(value time.Time) string {
	value = value.UTC()
	if value.Nanosecond() == 0 {
		return value.Format("2006-01-02T15:04:05")
	}
	return value.Format("2006-01-02T15:04:05.000000")
}
func cleanText(s string) string {
	s = htmlUnescape(s)
	s = stripTags(s)
	return strings.Join(strings.Fields(strings.NewReplacer("<", " ", ">", " ").Replace(s)), " ")
}
func htmlUnescape(s string) string {
	return html.UnescapeString(s)
}
func stripTags(s string) string {
	var b strings.Builder
	inside := false
	for _, r := range s {
		if r == '<' {
			inside = true
			continue
		}
		if r == '>' {
			inside = false
			continue
		}
		if !inside {
			b.WriteRune(r)
		}
	}
	return b.String()
}
func cleanSummary(s string) string {
	prefixes := []string{"Краткое содержание статьи на русском языке с основными тезисами:", "Краткое содержание статьи на русском языке:", "Краткое содержание:", "Основные тезисы статьи:", "Основные тезисы:", "Суммаризация статьи:", "Пересказ статьи:", "Содержание статьи:", "Вот краткое содержание:", "Вот основные тезисы:", "Статья содержит следующие основные моменты:", "Статья рассказывает о следующем:", "ПЕРЕСКАЗ:"}
	trimmed := strings.TrimSpace(s)
	lower := strings.ToLower(trimmed)
	for _, p := range prefixes {
		if strings.HasPrefix(lower, strings.ToLower(p)) {
			trimmed = strings.TrimSpace(trimmed[len(p):])
			break
		}
	}
	trimmed = summaryBullet.ReplaceAllString(trimmed, "$1")
	trimmed = emptyNumberLine.ReplaceAllString(trimmed, "")
	trimmed = blankLines.ReplaceAllString(trimmed, "\n\n")
	trimmed = trailingColon.ReplaceAllString(trimmed, "")
	return strings.TrimSpace(trimmed)
}

var summaryBullet = regexp.MustCompile(`(?m)^[-•·*]\s*`)
var emptyNumberLine = regexp.MustCompile(`(?m)^\d+\.\s*$`)
var blankLines = regexp.MustCompile(`\n\s*\n`)
var trailingColon = regexp.MustCompile(`:\s*$`)

func (p *Postgres) CategoriesConfig(ctx context.Context) (map[string]any, error) {
	query := `SELECT name,display_name,color FROM categories ORDER BY id`
	if isSerbian(ctx) {
		query = `SELECT c.name,COALESCE(cl.display_name,'Ostale teme'),c.color FROM categories c LEFT JOIN category_localizations cl ON cl.category_id=c.id AND cl.locale='sr-Latn' ORDER BY c.id`
	}
	rows, err := p.pool.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]any{}
	for rows.Next() {
		var name, display, color *string
		if err := rows.Scan(&name, &display, &color); err != nil {
			return nil, err
		}
		if name != nil {
			out[strings.ToLower(*name)] = map[string]any{"name": valueOr(display, *name), "color": valueOr(color, "#6c757d")}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"categories": out}, nil
}

func (p *Postgres) Categories(ctx context.Context, q StatsQuery) (map[string]any, error) {
	conditions, args := statsConditions(q)
	if isSerbian(ctx) {
		conditions = append(conditions, serbianReady)
	}
	query := `SELECT count(*) FROM articles a WHERE ` + strings.Join(conditions, " AND ")
	var total int
	if err := p.pool.QueryRow(ctx, query, args...).Scan(&total); err != nil {
		return nil, err
	}
	conditions = append(conditions, `ac.category_id IS NOT NULL`)
	rows, err := p.pool.Query(ctx, `SELECT lower(c.name),count(ac.article_id) FROM article_categories ac JOIN categories c ON c.id=ac.category_id JOIN articles a ON a.id=ac.article_id WHERE `+strings.Join(conditions, " AND ")+` GROUP BY lower(c.name)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts := map[string]any{"all": total}
	for rows.Next() {
		var n string
		var c int
		if err := rows.Scan(&n, &c); err != nil {
			return nil, err
		}
		counts[n] = c
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"categories": counts, "total": total, "filters": map[string]any{"since_hours": q.SinceHours, "hide_ads": q.HideAds}}, nil
}
func (p *Postgres) Sources(ctx context.Context, q StatsQuery) (map[string]any, error) {
	conditions, args := statsConditions(q)
	if isSerbian(ctx) {
		conditions = append(conditions, serbianReady)
	}
	var total int
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM articles a WHERE `+strings.Join(conditions, " AND "), args...).Scan(&total); err != nil {
		return nil, err
	}
	rows, err := p.pool.Query(ctx, `SELECT s.id,s.name,count(a.id) FROM sources s JOIN articles a ON a.source_id=s.id WHERE s.enabled IS TRUE AND `+strings.Join(conditions, " AND ")+` GROUP BY s.id,s.name ORDER BY count(a.id) DESC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	counts, names := map[string]any{}, map[string]any{}
	for rows.Next() {
		var id int64
		var n string
		var c int
		if err := rows.Scan(&id, &n, &c); err != nil {
			return nil, err
		}
		key := fmt.Sprint(id)
		counts[key] = c
		names[key] = n
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"sources": counts, "source_names": names, "total": total, "filters": map[string]any{"since_hours": q.SinceHours, "hide_ads": q.HideAds}}, nil
}
func statsConditions(q StatsQuery) ([]string, []any) {
	c := []string{published(!q.HideAds)}
	a := []any{}
	if q.HideAds {
		c = append(c, `a.is_advertisement IS FALSE`)
	}
	if q.SinceHours != nil {
		a = append(a, *q.SinceHours)
		c = append(c, fmt.Sprintf(`a.published_at >= now() AT TIME ZONE 'UTC' - ($%d * interval '1 hour')`, len(a)))
	}
	return c, a
}

func (p *Postgres) Search(ctx context.Context, q SearchQuery) (map[string]any, error) {
	query := strings.TrimSpace(q.Query)
	words := strings.Fields(query)
	conditions := []string{published(!q.HideAds)}
	if isSerbian(ctx) {
		conditions = append(conditions, serbianReady)
	}
	args := []any{}
	for _, word := range words {
		if len([]rune(word)) < 2 {
			continue
		}
		args = append(args, "%"+word+"%")
		n := len(args)
		conditions = append(conditions, fmt.Sprintf(`(a.title ILIKE $%d OR COALESCE(a.summary,'') ILIKE $%d OR COALESCE(a.content,'') ILIKE $%d)`, n, n, n))
	}
	if isSerbian(ctx) {
		for i, condition := range conditions {
			condition = strings.ReplaceAll(condition, "a.title ILIKE", "(SELECT l.title FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed') ILIKE")
			condition = strings.ReplaceAll(condition, "COALESCE(a.summary,'') ILIKE", "COALESCE((SELECT l.summary FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed'),'') ILIKE")
			condition = strings.ReplaceAll(condition, " OR COALESCE(a.content,'') ILIKE", " OR '' ILIKE")
			conditions[i] = condition
		}
	}
	if q.HideAds {
		conditions = append(conditions, `a.is_advertisement IS NOT TRUE`)
	}
	if q.Category != "" && strings.ToLower(q.Category) != "all" {
		cats := []string{}
		for _, c := range strings.Split(q.Category, ",") {
			c = strings.ToLower(strings.TrimSpace(c))
			if c != "" {
				cats = append(cats, c)
			}
		}
		if len(cats) == 1 && cats[0] == "advertisements" {
			conditions = append(conditions, `a.is_advertisement IS TRUE`)
		} else if len(cats) > 0 {
			args = append(args, cats)
			conditions = append(conditions, fmt.Sprintf(`EXISTS(SELECT 1 FROM article_categories ac JOIN categories c ON c.id=ac.category_id WHERE ac.article_id=a.id AND lower(c.name)=ANY($%d::text[]))`, len(args)))
		}
	}
	if q.SinceHours != nil {
		args = append(args, *q.SinceHours)
		conditions = append(conditions, fmt.Sprintf(`a.published_at >= now() AT TIME ZONE 'UTC' - ($%d * interval '1 hour')`, len(args)))
	}
	where := strings.Join(conditions, " AND ")
	order := `a.published_at DESC`
	if q.Sort == "date" {
		order = `a.published_at DESC`
	} else if q.Sort == "title" {
		order = `a.title`
	}
	if isSerbian(ctx) && q.Sort == "title" {
		order = `(SELECT l.title FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed')`
	}
	countArgs := append([]any(nil), args...)
	if q.Sort != "date" && q.Sort != "title" {
		// Bind the relevance query separately; user input remains data, never SQL.
		args = append(args, "%"+query+"%")
		rankIndex := len(args)
		order = fmt.Sprintf(`CASE WHEN a.title ILIKE $%d THEN 3 WHEN COALESCE(a.summary,'') ILIKE $%d THEN 2 ELSE 1 END DESC,a.published_at DESC`, rankIndex, rankIndex)
	}
	if isSerbian(ctx) {
		order = strings.ReplaceAll(order, "a.title", "(SELECT l.title FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed')")
		order = strings.ReplaceAll(order, "a.summary", "(SELECT l.summary FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed')")
	}
	var total int
	if err := p.pool.QueryRow(ctx, `SELECT count(*) FROM articles a LEFT JOIN sources s ON s.id=a.source_id WHERE `+where, countArgs...).Scan(&total); err != nil {
		return nil, err
	}
	args = append(args, q.Offset, q.Limit)
	rows, err := p.pool.Query(ctx, `SELECT `+localizedProjection(ctx)+` FROM articles a LEFT JOIN sources s ON s.id=a.source_id WHERE `+where+` ORDER BY `+order+fmt.Sprintf(` OFFSET $%d LIMIT $%d`, len(args)-1, len(args)), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	articles := []map[string]any{}
	for rows.Next() {
		item, e := scanArticle(rows, true, false)
		if e != nil {
			return nil, e
		}
		result := searchArticle(item, query)
		if isSerbian(ctx) {
			result["category"] = item["category"]
		}
		articles = append(articles, result)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"articles": articles, "pagination": map[string]any{"total": total, "limit": q.Limit, "offset": q.Offset, "has_more": q.Offset+q.Limit < total}, "query": query, "filters": map[string]any{"category": nullable(q.Category), "since_hours": q.SinceHours, "sort": q.Sort, "hide_ads": q.HideAds}, "results_count": len(articles)}, nil
}

var contentImageTag = regexp.MustCompile(`(?is)<img[^>]+src=["']([^"']+)["'][^>]*>`)

func searchArticle(item map[string]any, query string) map[string]any {
	media, _ := item["media_files"].([]any)
	if media == nil {
		media = []any{}
	}
	content := stringValue(item["content"])
	for index, match := range contentImageTag.FindAllStringSubmatch(content, -1) {
		if len(match) < 2 {
			continue
		}
		u := html.UnescapeString(match[1])
		media = append(media, map[string]any{"type": "image", "url": u, "thumbnail_url": u, "filename": fmt.Sprintf("content_image_%d.jpg", index+1), "source": "content_html"})
	}
	images := []any{}
	for _, entry := range media {
		if m, ok := entry.(map[string]any); ok && m["type"] == "image" {
			images = append(images, m)
		}
	}
	primary := item["image_url"]
	if primary == nil && len(images) > 0 {
		if m, ok := images[0].(map[string]any); ok {
			primary = m["url"]
		}
	}
	qLower := strings.ToLower(query)
	relevance := 0
	if strings.Contains(strings.ToLower(stringValue(item["title"])), qLower) {
		relevance += 3
	}
	if strings.Contains(strings.ToLower(stringValue(item["summary"])), qLower) {
		relevance += 2
	}
	if strings.Contains(strings.ToLower(content), qLower) {
		relevance++
	}
	return map[string]any{"id": item["id"], "title": item["title"], "summary": item["summary"], "url": item["url"], "image_url": item["image_url"], "primary_image": primary, "source_id": item["source_id"], "source_name": item["source_name"], "category": bestAIcategory(item["ai_categories"]), "categories": item["categories"], "published_at": item["published_at"], "fetched_at": item["fetched_at"], "is_advertisement": item["is_advertisement_value"], "media_files": media, "images": images, "relevance_score": relevance}
}
func bestAIcategory(value any) string {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return "Other"
	}
	bestName := "Other"
	bestConfidence := -1.0
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name, _ := m["ai_category"].(string)
		confidence, _ := m["confidence"].(float64)
		if confidence > bestConfidence {
			bestName = name
			bestConfidence = confidence
		}
	}
	if bestName == "" {
		return "Other"
	}
	return bestName
}
func stringValue(v any) string {
	if s, ok := v.(*string); ok && s != nil {
		return *s
	}
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}
func nullable(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func (p *Postgres) APIFeed(ctx context.Context, q APIFeedQuery) (APIFeedResponse, error) {
	conditions := []string{published(strings.EqualFold(q.Category, "advertisements"))}
	if isSerbian(ctx) {
		conditions = append(conditions, serbianReady)
	}
	args := []any{}
	if q.SinceHours != nil {
		args = append(args, *q.SinceHours)
		conditions = append(conditions, fmt.Sprintf(`a.published_at >= now() AT TIME ZONE 'UTC' - ($%d * interval '1 hour')`, len(args)))
	}
	if c := strings.ToLower(q.Category); c != "" && c != "all" {
		if c == "advertisements" {
			conditions = append(conditions, `a.is_advertisement IS TRUE`)
		} else {
			args = append(args, c)
			conditions = append(conditions, fmt.Sprintf(`EXISTS(SELECT 1 FROM article_categories ac JOIN categories c ON c.id=ac.category_id WHERE ac.article_id=a.id AND lower(c.name)=$%d)`, len(args)))
		}
	}
	args = append(args, q.Offset, q.Limit)
	rows, err := p.pool.Query(ctx, `SELECT `+localizedProjection(ctx)+` FROM articles a LEFT JOIN sources s ON s.id=a.source_id WHERE `+strings.Join(conditions, " AND ")+fmt.Sprintf(` ORDER BY a.fetched_at DESC OFFSET $%d LIMIT $%d`, len(args)-1, len(args)), args...)
	if err != nil {
		return APIFeedResponse{}, err
	}
	defer rows.Close()
	items := []map[string]any{}
	for rows.Next() {
		item, e := scanArticle(rows, false, true)
		if e != nil {
			return APIFeedResponse{}, e
		}
		items = append(items, feedItem(item))
	}
	if err := rows.Err(); err != nil {
		return APIFeedResponse{}, err
	}
	var cat *string
	if q.Category != "" {
		v := q.Category
		cat = &v
	}
	now := pythonISO(time.Now().UTC())
	title := "Evening News v2 Feed"
	if isSerbian(ctx) {
		title = "Evening News v2 — vesti na srpskom"
	}
	return APIFeedResponse{Version: "2.0", Title: title, UpdatedAt: now, Items: items, Pagination: APIFeedPagination{Limit: q.Limit, Offset: q.Offset, Count: len(items), Category: cat}}, nil
}
func feedItem(m map[string]any) map[string]any {
	categories := []any{}
	if values, ok := m["categories"].([]any); ok {
		for _, value := range values {
			if category, ok := value.(map[string]any); ok {
				categories = append(categories, map[string]any{"name": category["name"], "display_name": category["display_name"]})
			}
		}
	}
	return map[string]any{"article_id": m["article_id"], "title": m["title"], "summary": m["summary"], "content": m["content"], "image_url": m["image_url"], "media_files": m["media_files"], "url": m["url"], "domain": m["domain"], "category": m["category"], "categories": categories, "ai_categories": m["ai_categories"], "published_at": m["published_at"], "fetched_at": m["fetched_at"], "is_advertisement": m["is_advertisement"], "ad_confidence": m["ad_confidence"], "ad_type": m["ad_type"], "ad_reasoning": m["ad_reasoning"], "source_id": m["source_id"]}
}
func (p *Postgres) RSS(ctx context.Context, limit int) ([]RSSArticle, error) {
	projection := `a.id,a.title,a.url,a.summary,a.published_at`
	condition := published(false)
	if isSerbian(ctx) {
		projection = `a.id,(SELECT l.title FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed'),a.url,(SELECT l.summary FROM article_localizations l WHERE l.article_id=a.id AND l.locale='sr-Latn' AND l.status='completed'),a.published_at`
		condition += ` AND ` + serbianReady
	}
	rows, err := p.pool.Query(ctx, `SELECT `+projection+` FROM articles a WHERE `+condition+` AND a.is_advertisement IS FALSE ORDER BY a.published_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []RSSArticle{}
	for rows.Next() {
		var a RSSArticle
		if err := rows.Scan(&a.ID, &a.Title, &a.URL, &a.Summary, &a.PublishedAt); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (p *Postgres) Digest(ctx context.Context, date time.Time) (DigestPage, error) {
	page := DigestPage{Date: date.UTC()}
	rows, err := p.pool.Query(ctx, `SELECT category,summary_text,articles_count FROM daily_summaries WHERE date=$1 ORDER BY articles_count DESC`, date.Format("2006-01-02"))
	if err != nil {
		return page, err
	}
	for rows.Next() {
		var s DigestSummary
		if err := rows.Scan(&s.Category, &s.Text, &s.ArticlesCount); err != nil {
			rows.Close()
			return page, err
		}
		page.Summaries = append(page.Summaries, s)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return page, err
	}
	rows.Close()
	rows, err = p.pool.Query(ctx, `SELECT id,title,summary,url,source_name,image_url,fetched_at,category,confidence FROM (SELECT DISTINCT ON (a.id) a.id,a.title,a.summary,a.url,COALESCE(s.name,'Unknown') AS source_name,COALESCE((SELECT item->>'url' FROM jsonb_array_elements(COALESCE(a.media_files::jsonb,'[]'::jsonb)) item WHERE item->>'type'='image' LIMIT 1),a.image_url) AS image_url,a.fetched_at,COALESCE(c.name,'Other') AS category,COALESCE(ac.confidence,0) AS confidence FROM articles a JOIN article_categories ac ON ac.article_id=a.id JOIN categories c ON c.id=ac.category_id LEFT JOIN sources s ON s.id=a.source_id WHERE a.fetched_at::date=$1 AND a.processing_state='completed' AND a.is_advertisement IS NOT TRUE ORDER BY a.id,ac.confidence DESC NULLS LAST) chosen ORDER BY fetched_at DESC`, date.Format("2006-01-02"))
	if err != nil {
		return page, err
	}
	byCategory := map[string][]DigestArticle{}
	order := []string{}
	for rows.Next() {
		var a DigestArticle
		var category string
		var confidence float64
		if err := rows.Scan(&a.ID, &a.Title, &a.Summary, &a.URL, &a.SourceName, &a.ImageURL, &a.FetchedAt, &category, &confidence); err != nil {
			rows.Close()
			return page, err
		}
		if _, ok := byCategory[category]; !ok {
			order = append(order, category)
		}
		byCategory[category] = append(byCategory[category], a)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return page, err
	}
	rows.Close()
	ordered := make([]string, 0, len(page.Summaries)+len(order))
	seen := map[string]bool{}
	for _, summary := range page.Summaries {
		if !seen[summary.Category] {
			ordered = append(ordered, summary.Category)
			seen[summary.Category] = true
		}
	}
	for _, category := range order {
		if !seen[category] {
			ordered = append(ordered, category)
			seen[category] = true
		}
	}
	for _, c := range ordered {
		page.Categories = append(page.Categories, DigestCategory{Name: c, Articles: byCategory[c]})
	}
	return page, nil
}

// EditorialDigest returns the requested immutable edition, or the latest
// edition for the date and locale when editionID is empty. A missing table is
// treated as an installation that has not yet enabled editorial editions, so
// callers can continue to show the historical daily summaries.
func (p *Postgres) EditorialDigest(ctx context.Context, date time.Time, editionID, locale string) (*EditorialDigestEdition, error) {
	var tableName *string
	if err := p.pool.QueryRow(ctx, `SELECT to_regclass('editorial_digest_editions')::text`).Scan(&tableName); err != nil {
		return nil, err
	}
	if tableName == nil {
		return nil, nil
	}

	var row pgx.Row
	if editionID != "" {
		row = p.pool.QueryRow(ctx, `SELECT id::text,edition_date::text,locale,snapshot_hash,snapshot,edition
			FROM editorial_digest_editions WHERE edition_date=$1 AND locale=$2 AND id=$3::uuid`, date.Format("2006-01-02"), locale, editionID)
	} else {
		row = p.pool.QueryRow(ctx, `SELECT id::text,edition_date::text,locale,snapshot_hash,snapshot,edition
			FROM editorial_digest_editions WHERE edition_date=$1 AND locale=$2 ORDER BY created_at DESC,id DESC LIMIT 1`, date.Format("2006-01-02"), locale)
	}
	var edition EditorialDigestEdition
	var snapshotJSON, editionJSON []byte
	if err := row.Scan(&edition.ID, &edition.Date, &edition.Locale, &edition.SnapshotHash, &snapshotJSON, &editionJSON); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(snapshotJSON, &edition.Snapshot); err != nil {
		return nil, fmt.Errorf("decode editorial digest snapshot: %w", err)
	}
	if err := json.Unmarshal(editionJSON, &edition.Edition); err != nil {
		return nil, fmt.Errorf("decode editorial digest content: %w", err)
	}
	return &edition, nil
}

// EditorialDigestStatus looks up only ready-edition metadata for the admin
// reader. It does not consult or mutate legacy daily_summaries rows.
func (p *Postgres) EditorialDigestCounterpart(ctx context.Context, date time.Time, editionID, locale, target string) (string, error) {
	rows, err := p.pool.Query(ctx, `SELECT other.id::text FROM editorial_digest_editions base JOIN editorial_digest_editions other ON other.job_id=base.job_id AND other.attempt_generation=base.attempt_generation AND other.edition_date=base.edition_date WHERE base.id=$1::uuid AND base.edition_date=$2 AND base.locale=$3 AND other.locale=$4 AND base.job_id IS NOT NULL AND base.attempt_generation IS NOT NULL ORDER BY other.id LIMIT 2`, editionID, date.Format("2006-01-02"), locale, target)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return "", err
		}
		ids = append(ids, id)
	}
	if err = rows.Err(); err != nil {
		return "", err
	}
	if len(ids) != 1 {
		return "", nil
	}
	return ids[0], nil
}

func (p *Postgres) EditorialDigestStatus(ctx context.Context, date time.Time, locale string) (EditorialDigestStatus, error) {
	status := EditorialDigestStatus{Date: date.Format("2006-01-02"), Locale: locale}
	var tableName *string
	if err := p.pool.QueryRow(ctx, `SELECT to_regclass('editorial_digest_editions')::text`).Scan(&tableName); err != nil {
		return status, err
	}
	if tableName == nil {
		return status, nil
	}
	err := p.pool.QueryRow(ctx, `
		SELECT e.id::text,
			COUNT(story.value) FILTER (WHERE story.value->>'kind'='main')::integer,
			COUNT(story.value) FILTER (WHERE story.value->>'kind'='brief')::integer,
			COUNT(story.value)::integer,
			jsonb_array_length(CASE WHEN jsonb_typeof(e.snapshot->'articles')='array' THEN e.snapshot->'articles' ELSE '[]'::jsonb END)::integer
		FROM editorial_digest_editions e
		LEFT JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(e.edition->'stories')='array' THEN e.edition->'stories' ELSE '[]'::jsonb END) AS story(value) ON TRUE
		WHERE e.edition_date=$1 AND e.locale=$2
		GROUP BY e.id,e.created_at,e.snapshot
		ORDER BY e.created_at DESC,e.id DESC LIMIT 1`, status.Date, locale).Scan(&status.EditionID, &status.MainCount, &status.BriefCount, &status.StoryCount, &status.ArticleCount)
	if errors.Is(err, pgx.ErrNoRows) {
		return status, nil
	}
	if err != nil {
		return status, err
	}
	status.Ready = true
	return status, nil
}
