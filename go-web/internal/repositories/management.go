package repositories

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-web/internal/management"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ManagementRepository contains SQL-only CRUD/read operations for the web admin API.
type ManagementRepository struct{ pool *pgxpool.Pool }

func NewManagementRepository(pool *pgxpool.Pool) *ManagementRepository {
	return &ManagementRepository{pool: pool}
}

func isoNaive(v *time.Time) any {
	if v == nil {
		return nil
	}
	t := v.UTC()
	out := t.Format("2006-01-02T15:04:05")
	if us := t.Nanosecond() / 1000; us != 0 {
		out += fmt.Sprintf(".%06d", us)
	}
	return out
}
func pgConflict(err error) bool {
	var pg *pgconn.PgError
	return errors.As(err, &pg) && pg.Code == "23505"
}

func (r *ManagementRepository) Sources(ctx context.Context) (map[string]any, error) {
	rows, err := r.pool.Query(ctx, `SELECT s.id,s.name,s.source_type,s.url,s.enabled,s.last_fetch,s.last_success,s.error_count,s.last_error,(SELECT count(*) FROM articles a WHERE a.source_id=s.id) FROM sources s ORDER BY s.id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id int64
		var name, typ, raw string
		var active bool
		var lastFetch, lastSuccess *time.Time
		var errorsCount int
		var lastError *string
		var count int64
		if err = rows.Scan(&id, &name, &typ, &raw, &active, &lastFetch, &lastSuccess, &errorsCount, &lastError, &count); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"id": id, "name": name, "source_type": typ, "url": raw, "is_active": active, "last_updated": isoNaive(lastFetch), "last_success": isoNaive(lastSuccess), "articles_count": count, "error_count": errorsCount, "last_error": lastError})
	}
	return map[string]any{"sources": out}, rows.Err()
}
func (r *ManagementRepository) CreateSource(ctx context.Context, in management.SourceCreate) (map[string]any, error) {
	active := true
	if in.IsActive != nil {
		active = *in.IsActive
	}
	var id int64
	var created *time.Time
	err := r.pool.QueryRow(ctx, `INSERT INTO sources(name,source_type,url,enabled,config,fetch_interval,error_count,created_at,updated_at) VALUES($1,$2,$3,$4,'{}'::jsonb,1800,0,timezone('UTC',now()),timezone('UTC',now())) RETURNING id,created_at`, in.Name, in.SourceType, in.URL, active).Scan(&id, &created)
	if pgConflict(err) {
		return nil, management.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "name": in.Name, "source_type": in.SourceType, "url": in.URL, "is_active": active, "created_at": isoNaive(created)}, nil
}
func (r *ManagementRepository) UpdateSource(ctx context.Context, id int64, in management.SourceUpdate) (map[string]any, error) {
	sets := []string{}
	args := []any{id}
	add := func(col string, v any) {
		args = append(args, v)
		sets = append(sets, fmt.Sprintf("%s=$%d", col, len(args)))
	}
	if in.Name != nil {
		add("name", *in.Name)
	}
	if in.SourceType != nil {
		add("source_type", *in.SourceType)
	}
	if in.URL != nil {
		add("url", *in.URL)
	}
	if in.IsActive != nil {
		add("enabled", *in.IsActive)
	}
	if in.UpdateInterval != nil {
		add("fetch_interval", *in.UpdateInterval)
	}
	if len(sets) > 0 {
		sets = append(sets, "updated_at=timezone('UTC',now())")
	}
	var row pgx.Row
	if len(sets) == 0 {
		row = r.pool.QueryRow(ctx, `SELECT id,name,source_type,url,enabled,fetch_interval,updated_at FROM sources WHERE id=$1`, id)
	} else {
		row = r.pool.QueryRow(ctx, `UPDATE sources SET `+strings.Join(sets, ",")+` WHERE id=$1 RETURNING id,name,source_type,url,enabled,fetch_interval,updated_at`, args...)
	}
	var sid int64
	var name, typ, raw string
	var active bool
	var interval int
	var updated *time.Time
	err := row.Scan(&sid, &name, &typ, &raw, &active, &interval, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &management.APIError{Status: 404, Detail: "Source not found"}
	}
	if pgConflict(err) {
		return nil, management.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": sid, "name": name, "source_type": typ, "url": raw, "is_active": active, "update_interval": interval, "updated_at": isoNaive(updated)}, nil
}
func (r *ManagementRepository) ToggleSource(ctx context.Context, id int64) (map[string]any, error) {
	var sid int64
	var name string
	var active bool
	err := r.pool.QueryRow(ctx, `UPDATE sources SET enabled=NOT enabled,updated_at=timezone('UTC',now()) WHERE id=$1 RETURNING id,name,enabled`, id).Scan(&sid, &name, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &management.APIError{Status: 404, Detail: "Source not found"}
	}
	if err != nil {
		return nil, err
	}
	state := "disabled"
	if active {
		state = "enabled"
	}
	return map[string]any{"id": sid, "name": name, "is_active": active, "message": "Source " + state}, nil
}
func (r *ManagementRepository) DeleteSource(ctx context.Context, id int64, purge bool) (map[string]any, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var name string
	if err = tx.QueryRow(ctx, `SELECT name FROM sources WHERE id=$1 FOR UPDATE`, id).Scan(&name); errors.Is(err, pgx.ErrNoRows) {
		return nil, &management.APIError{Status: 404, Detail: "Source not found"}
	} else if err != nil {
		return nil, err
	}
	var count int64
	if err = tx.QueryRow(ctx, `SELECT count(*) FROM articles WHERE source_id=$1`, id).Scan(&count); err != nil {
		return nil, err
	}
	if count > 0 && !purge {
		return nil, &management.APIError{Status: 400, Detail: fmt.Sprintf("Cannot delete source with %d articles. Disable it instead or pass delete_articles=true to purge.", count)}
	}
	if purge {
		if _, err = tx.Exec(ctx, `DELETE FROM articles WHERE source_id=$1`, id); err != nil {
			return nil, err
		}
	}
	if _, err = tx.Exec(ctx, `DELETE FROM sources WHERE id=$1`, id); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	deleted := int64(0)
	if purge {
		deleted = count
	}
	return map[string]any{"message": fmt.Sprintf("Source '%s' deleted successfully", name), "deleted_source_id": id, "deleted_articles": deleted}, nil
}

func (r *ManagementRepository) Categories(ctx context.Context) (map[string]any, error) {
	rows, err := r.pool.Query(ctx, `SELECT c.id,c.name,c.display_name,c.description,c.color,c.is_local,count(ac.article_id),(SELECT cl.display_name FROM category_localizations cl WHERE cl.category_id=c.id AND cl.locale='sr-Latn') FROM categories c LEFT JOIN article_categories ac ON ac.category_id=c.id GROUP BY c.id ORDER BY count(ac.article_id) DESC`)
	if err != nil {
		return nil, err
	}
	cats := []map[string]any{}
	var total int64
	for rows.Next() {
		var id int64
		var name, display, color string
		var description, serbian *string
		var local bool
		var count int64
		if err = rows.Scan(&id, &name, &display, &description, &color, &local, &count, &serbian); err != nil {
			rows.Close()
			return nil, err
		}
		cats = append(cats, map[string]any{"id": id, "category": name, "display_name": display, "serbian_display_name": serbian, "description": description, "color": color, "is_local": local, "count": count})
		total += count
	}
	if err = rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	var ads int64
	if err = r.pool.QueryRow(ctx, `SELECT count(*) FROM articles WHERE is_advertisement IS TRUE`).Scan(&ads); err != nil {
		return nil, err
	}
	if ads > 0 {
		cats = append(cats, map[string]any{"category": "advertisements", "count": ads})
	}
	return map[string]any{"categories": cats, "total_articles": total, "advertisements": ads}, nil
}
func setSerbianCategory(ctx context.Context, tx pgx.Tx, id int64, name *string) error {
	if name == nil {
		return nil
	}
	value := strings.TrimSpace(*name)
	if value == "" {
		_, err := tx.Exec(ctx, `DELETE FROM category_localizations WHERE category_id=$1 AND locale='sr-Latn'`, id)
		return err
	}
	_, err := tx.Exec(ctx, `INSERT INTO category_localizations(category_id,locale,display_name) VALUES($1,'sr-Latn',$2) ON CONFLICT(category_id,locale) DO UPDATE SET display_name=EXCLUDED.display_name`, id, value)
	return err
}
func (r *ManagementRepository) CreateCategory(ctx context.Context, in management.CategoryCreate) (map[string]any, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var id int64
	var created *time.Time
	err = tx.QueryRow(ctx, `INSERT INTO categories(name,display_name,description,color,created_at) VALUES($1,$2,$3,$4,timezone('UTC',now())) RETURNING id,created_at`, in.Name, in.DisplayName, in.Description, *in.Color).Scan(&id, &created)
	if pgConflict(err) {
		return nil, &management.APIError{Status: 400, Detail: "Category already exists"}
	}
	if err != nil {
		return nil, err
	}
	if err = setSerbianCategory(ctx, tx, id, in.SerbianDisplayName); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "name": in.Name, "display_name": in.DisplayName, "serbian_display_name": in.SerbianDisplayName, "description": in.Description, "color": *in.Color, "created_at": isoNaive(created)}, nil
}
func (r *ManagementRepository) UpdateCategory(ctx context.Context, id int64, in management.CategoryUpdate) (map[string]any, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	if in.IsLocal != nil && *in.IsLocal {
		if _, err = tx.Exec(ctx, `UPDATE categories SET is_local=false WHERE is_local=true AND id!=$1`, id); err != nil {
			return nil, err
		}
	}
	var color any
	if in.Color != nil && *in.Color != "" {
		color = *in.Color
	}
	var sid int64
	var name, display, storedColor string
	var description *string
	var local bool
	var created *time.Time
	err = tx.QueryRow(ctx, `UPDATE categories SET display_name=$2,color=COALESCE(NULLIF($3,''),color),description=COALESCE($4,description),is_local=COALESCE($5,is_local) WHERE id=$1 RETURNING id,name,display_name,description,color,is_local,created_at`, id, in.DisplayName, color, in.Description, in.IsLocal).Scan(&sid, &name, &display, &description, &storedColor, &local, &created)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &management.APIError{Status: 404, Detail: "Category not found"}
	}
	if err != nil {
		return nil, err
	}
	if err = setSerbianCategory(ctx, tx, id, in.SerbianDisplayName); err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"id": sid, "name": name, "display_name": display, "description": description, "color": storedColor, "is_local": local, "created_at": isoNaive(created)}, nil
}
func mappingMap(id int64, ai, fixed string, threshold *float64, description, createdBy *string, usage *int64, last, created, updated *time.Time, active bool) map[string]any {
	if threshold == nil {
		v := 0.7
		threshold = &v
	}
	return map[string]any{"id": id, "ai_category": ai, "fixed_category": fixed, "confidence_threshold": *threshold, "description": description, "created_by": createdBy, "usage_count": usage, "last_used": isoNaive(last), "is_active": active, "created_at": isoNaive(created), "updated_at": isoNaive(updated)}
}
func (r *ManagementRepository) Mappings(ctx context.Context, activeOnly bool) ([]map[string]any, error) {
	q := `SELECT id,ai_category,fixed_category,confidence_threshold,description,created_by,usage_count,last_used,is_active,created_at,updated_at FROM category_mapping`
	if activeOnly {
		q += ` WHERE is_active=true`
	}
	q += ` ORDER BY usage_count DESC`
	rows, err := r.pool.Query(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id int64
		var ai, fixed string
		var threshold *float64
		var description, by *string
		var usage *int64
		var last, created, updated *time.Time
		var active bool
		if err = rows.Scan(&id, &ai, &fixed, &threshold, &description, &by, &usage, &last, &active, &created, &updated); err != nil {
			return nil, err
		}
		out = append(out, mappingMap(id, ai, fixed, threshold, description, by, usage, last, created, updated, active))
	}
	return out, rows.Err()
}
func (r *ManagementRepository) UpsertMapping(ctx context.Context, in management.MappingRequest) (map[string]any, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	var id int64
	err = tx.QueryRow(ctx, `SELECT id FROM category_mapping WHERE ai_category=$1 FOR UPDATE`, in.AICategory).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		err = tx.QueryRow(ctx, `INSERT INTO category_mapping(ai_category,fixed_category,confidence_threshold,description,created_by,usage_count,is_active,created_at,updated_at) VALUES($1,$2,$3,$4,'admin',0,true,timezone('UTC',now()),timezone('UTC',now())) RETURNING id`, in.AICategory, in.FixedCategory, *in.ConfidenceThreshold, in.Description).Scan(&id)
	} else if err == nil {
		_, err = tx.Exec(ctx, `UPDATE category_mapping SET fixed_category=$2,confidence_threshold=$3,description=$4,is_active=true,updated_at=timezone('UTC',now()) WHERE id=$1`, id, in.FixedCategory, *in.ConfidenceThreshold, in.Description)
	}
	if err != nil {
		if pgConflict(err) {
			return nil, management.ErrConflict
		}
		return nil, err
	}
	result, err := selectMapping(ctx, tx, id)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}
func selectMapping(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, id int64) (map[string]any, error) {
	var ai, fixed string
	var threshold *float64
	var description, by *string
	var usage *int64
	var last, created, updated *time.Time
	var active bool
	err := q.QueryRow(ctx, `SELECT id,ai_category,fixed_category,confidence_threshold,description,created_by,usage_count,last_used,is_active,created_at,updated_at FROM category_mapping WHERE id=$1`, id).Scan(&id, &ai, &fixed, &threshold, &description, &by, &usage, &last, &active, &created, &updated)
	if err != nil {
		return nil, err
	}
	return mappingMap(id, ai, fixed, threshold, description, by, usage, last, created, updated, active), nil
}
func (r *ManagementRepository) UpdateMapping(ctx context.Context, id int64, in management.MappingRequest) (map[string]any, error) {
	var ai, fixed string
	var threshold float64
	var desc *string
	var active bool
	var updated *time.Time
	err := r.pool.QueryRow(ctx, `UPDATE category_mapping SET ai_category=$2,fixed_category=$3,confidence_threshold=$4,description=$5,updated_at=timezone('UTC',now()) WHERE id=$1 RETURNING ai_category,fixed_category,confidence_threshold,description,is_active,updated_at`, id, in.AICategory, in.FixedCategory, *in.ConfidenceThreshold, in.Description).Scan(&ai, &fixed, &threshold, &desc, &active, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &management.APIError{Status: 404, Detail: "Mapping not found"}
	}
	if pgConflict(err) {
		return nil, management.ErrConflict
	}
	if err != nil {
		return nil, err
	}
	return map[string]any{"id": id, "ai_category": ai, "fixed_category": fixed, "confidence_threshold": threshold, "description": desc, "is_active": active, "updated_at": isoNaive(updated)}, nil
}
func (r *ManagementRepository) DeleteMapping(ctx context.Context, id int64) (map[string]any, error) {
	tag, err := r.pool.Exec(ctx, `DELETE FROM category_mapping WHERE id=$1`, id)
	if err != nil {
		return nil, err
	}
	if tag.RowsAffected() == 0 {
		return nil, &management.APIError{Status: 404, Detail: "Mapping not found"}
	}
	return map[string]any{"message": "Mapping deleted successfully"}, nil
}
func (r *ManagementRepository) ToggleMapping(ctx context.Context, id int64) (map[string]any, error) {
	var active bool
	err := r.pool.QueryRow(ctx, `UPDATE category_mapping SET is_active=NOT is_active,updated_at=timezone('UTC',now()) WHERE id=$1 RETURNING is_active`, id).Scan(&active)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &management.APIError{Status: 404, Detail: "Mapping not found"}
	}
	if err != nil {
		return nil, err
	}
	verb := "deactivated"
	if active {
		verb = "activated"
	}
	return map[string]any{"message": "Mapping " + verb, "is_active": active}, nil
}
func (r *ManagementRepository) FixedCategoryMappings(ctx context.Context) (map[string]any, error) {
	rows, err := r.pool.Query(ctx, `SELECT name,display_name FROM categories ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var name, display string
		if err = rows.Scan(&name, &display); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"key": name, "display_name": display})
	}
	return map[string]any{"categories": out}, rows.Err()
}
func (r *ManagementRepository) FixedCategories(ctx context.Context) (map[string]any, error) {
	rows, err := r.pool.Query(ctx, `SELECT name FROM categories ORDER BY name`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var name string
		if err = rows.Scan(&name); err != nil {
			return nil, err
		}
		out = append(out, name)
	}
	return map[string]any{"fixed_categories": out}, rows.Err()
}
func suggestCategory(v string) string {
	s := strings.ToLower(v)
	groups := map[string][]string{"Tech": {"tech", "software", "innovation", "digital", "ai", "computer", "programming"}, "Business": {"business", "economy", "finance", "market", "trade", "investment", "company"}, "Politics": {"politics", "government", "election", "policy", "law", "legal", "parliament"}}
	for _, name := range []string{"Tech", "Business", "Politics"} {
		for _, k := range groups[name] {
			if strings.Contains(s, k) {
				return name
			}
		}
	}
	return "Other"
}
func (r *ManagementRepository) UnmappedCategories(ctx context.Context, limit int) (map[string]any, error) {
	rows, err := r.pool.Query(ctx, `SELECT ac.ai_category,count(*) FROM article_categories ac LEFT JOIN category_mapping cm ON lower(ac.ai_category)=lower(cm.ai_category) WHERE ac.ai_category IS NOT NULL AND ac.ai_category<>'' AND cm.ai_category IS NULL GROUP BY ac.ai_category ORDER BY count(*) DESC,ac.ai_category LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var ai string
		var count int64
		if err = rows.Scan(&ai, &count); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"ai_category": ai, "usage_count": count, "suggested_fixed_category": suggestCategory(ai), "examples": []any{}})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"message": "Unmapped AI categories analysis", "unmapped_categories": out, "total_unmapped": len(out), "note": "These AI categories need manual mapping to fixed categories"}, nil
}

func (r *ManagementRepository) SourceExists(ctx context.Context, id int64) (bool, error) {
	var found bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sources WHERE id=$1)`, id).Scan(&found)
	return found, err
}
func (r *ManagementRepository) ArticleExists(ctx context.Context, id int64) (bool, error) {
	var found bool
	err := r.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM articles WHERE id=$1)`, id).Scan(&found)
	return found, err
}
func (r *ManagementRepository) Article(ctx context.Context, id int64) (map[string]any, error) {
	var aid int64
	var sid *int64
	var title, url string
	var summary, content, image *string
	var mediaRaw []byte
	var published, fetched, next *time.Time
	var processed, summaryProcessed, categoryProcessed, adProcessed, isAd bool
	var state string
	var attempts int
	var adConf float64
	var adType, adReason *string
	var markersRaw []byte
	var sourceName, sourceType *string
	err := r.pool.QueryRow(ctx, `SELECT a.id,a.source_id,a.title,a.summary,a.content,a.url,a.image_url,a.media_files,a.published_at,a.fetched_at,a.processed,a.processing_state,a.processing_attempts,a.next_attempt_at,a.summary_processed,a.category_processed,a.ad_processed,a.is_advertisement,a.ad_confidence,a.ad_type,a.ad_reasoning,a.ad_markers,s.name,s.source_type FROM articles a LEFT JOIN sources s ON s.id=a.source_id WHERE a.id=$1`, id).Scan(&aid, &sid, &title, &summary, &content, &url, &image, &mediaRaw, &published, &fetched, &processed, &state, &attempts, &next, &summaryProcessed, &categoryProcessed, &adProcessed, &isAd, &adConf, &adType, &adReason, &markersRaw, &sourceName, &sourceType)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &management.APIError{Status: 404, Detail: "Article not found"}
	}
	if err != nil {
		return nil, err
	}
	media := []any{}
	if len(mediaRaw) > 0 {
		_ = json.Unmarshal(mediaRaw, &media)
	}
	markers := []any{}
	if len(markersRaw) > 0 {
		_ = json.Unmarshal(markersRaw, &markers)
	}
	rows, err := r.pool.Query(ctx, `SELECT COALESCE(ac.ai_category,'Other'),COALESCE(ac.confidence,0.0),COALESCE(c.color,'#6c757d') FROM article_categories ac LEFT JOIN categories c ON c.id=ac.category_id WHERE ac.article_id=$1 ORDER BY ac.confidence DESC NULLS LAST,c.id`, id)
	if err != nil {
		return nil, err
	}
	categories := []map[string]any{}
	primary := "Other"
	for rows.Next() {
		var name, color string
		var confidence float64
		if err = rows.Scan(&name, &confidence, &color); err != nil {
			rows.Close()
			return nil, err
		}
		if len(categories) == 0 {
			primary = name
		}
		n := name
		categories = append(categories, map[string]any{"name": name, "display_name": n, "confidence": confidence, "color": color, "ai_category": name})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	source := map[string]any{"id": sid, "name": sourceName, "type": sourceType}
	return map[string]any{"id": aid, "title": title, "summary": summary, "content": content, "url": url, "image_url": image, "media_files": media, "published_at": isoNaive(published), "fetched_at": isoNaive(fetched), "processed": processed, "processing_state": state, "processing_attempts": attempts, "next_attempt_at": isoNaive(next), "summary_processed": summaryProcessed, "category_processed": categoryProcessed, "ad_processed": adProcessed, "source": source, "categories": categories, "primary_category": primary, "is_advertisement": isAd, "ad_confidence": adConf, "ad_type": adType, "ad_reasoning": adReason, "ad_markers": markers}, nil
}

func (r *ManagementRepository) DailySummaries(ctx context.Context, q management.SummaryQuery) (map[string]any, error) {
	args := []any{}
	where := []string{"true"}
	if q.Category != "" {
		args = append(args, q.Category)
		where = append(where, fmt.Sprintf("category=$%d", len(args)))
	}
	if q.Date != nil {
		if len(*q.Date) == 10 {
			if _, err := time.Parse("2006-01-02", *q.Date); err == nil {
				args = append(args, *q.Date)
				where = append(where, fmt.Sprintf("date=$%d", len(args)))
			}
		}
	}
	args = append(args, q.Limit)
	limitP := len(args)
	args = append(args, q.Offset)
	offsetP := len(args)
	rows, err := r.pool.Query(ctx, `SELECT id,date,category,summary_text,articles_count,created_at,updated_at FROM daily_summaries WHERE `+strings.Join(where, " AND ")+fmt.Sprintf(" ORDER BY date DESC,created_at DESC LIMIT $%d OFFSET $%d", limitP, offsetP), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var id int64
		var date time.Time
		var category, text string
		var count int64
		var created, updated *time.Time
		if err = rows.Scan(&id, &date, &category, &text, &count, &created, &updated); err != nil {
			return nil, err
		}
		u := updated
		if u == nil {
			u = created
		}
		out = append(out, map[string]any{"id": id, "date": date.Format("2006-01-02"), "category": category, "summary_text": text, "articles_count": count, "created_at": isoNaive(created), "updated_at": isoNaive(u)})
	}
	if err = rows.Err(); err != nil {
		return nil, err
	}
	return map[string]any{"summaries": out, "limit": q.Limit, "offset": q.Offset, "total": len(out)}, nil
}
func (r *ManagementRepository) SummaryCategories(ctx context.Context) (map[string]any, error) {
	rows, err := r.pool.Query(ctx, `SELECT category,count(*) FROM daily_summaries GROUP BY category ORDER BY count(*) DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []map[string]any{}
	for rows.Next() {
		var category string
		var count int64
		if err = rows.Scan(&category, &count); err != nil {
			return nil, err
		}
		out = append(out, map[string]any{"category": category, "summaries_count": count})
	}
	return map[string]any{"categories": out, "total_categories": len(out)}, rows.Err()
}
func (r *ManagementRepository) Summary(ctx context.Context, id int64) (map[string]any, error) {
	var date time.Time
	var category, text string
	var count int64
	var created, updated *time.Time
	err := r.pool.QueryRow(ctx, `SELECT date,category,summary_text,articles_count,created_at,updated_at FROM daily_summaries WHERE id=$1`, id).Scan(&date, &category, &text, &count, &created, &updated)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, &management.APIError{Status: 404, Detail: "Summary not found"}
	}
	if err != nil {
		return nil, err
	}
	if updated == nil {
		updated = created
	}
	return map[string]any{"id": id, "date": date.Format("2006-01-02"), "category": category, "summary_text": text, "articles_count": count, "created_at": isoNaive(created), "updated_at": isoNaive(updated)}, nil
}
func (r *ManagementRepository) ProcessingStatus(ctx context.Context) (map[string]any, error) {
	var queued, active int64
	if err := r.pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE state IN ('queued','interrupted')),count(*) FILTER (WHERE state='running') FROM background_jobs`).Scan(&queued, &active); err != nil {
		return nil, err
	}
	var healthy bool
	err := r.pool.QueryRow(ctx, `SELECT status='running' AND heartbeat_at >= (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')-interval '20 seconds' FROM background_worker_status WHERE id=1`).Scan(&healthy)
	if errors.Is(err, pgx.ErrNoRows) {
		healthy = false
	} else if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	stamp := now.Format("2006-01-02T15:04:05")
	return map[string]any{"busy": active > 0, "is_running": active > 0, "worker_available": healthy, "queued_jobs": queued, "active_jobs": active, "timestamp": stamp}, nil
}
