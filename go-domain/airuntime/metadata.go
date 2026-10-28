package airuntime

import (
	"context"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/analysis"
	"github.com/jackc/pgx/v5/pgxpool"
)

var fallbackCategories = []analysis.CategoryMetadata{
	{Name: "Business", DisplayName: "Бизнес"}, {Name: "Tech", DisplayName: "Технологии"},
	{Name: "Science", DisplayName: "Наука"}, {Name: "Serbia", DisplayName: "Сербия"}, {Name: "Other", DisplayName: "Прочее"},
}

type metadataSnapshot struct {
	categories []analysis.CategoryMetadata
	local      *string
	loaded     time.Time
}
type MetadataStore struct {
	db       *pgxpool.Pool
	ttl      time.Duration
	now      func() time.Time
	load     func(context.Context) ([]analysis.CategoryMetadata, *string, error)
	mu       sync.Mutex
	refresh  chan struct{}
	epoch    uint64
	snapshot *metadataSnapshot
}

func NewMetadataStore(db *pgxpool.Pool) *MetadataStore {
	m := &MetadataStore{db: db, ttl: 300 * time.Second, now: time.Now, refresh: make(chan struct{}, 1)}
	if db != nil {
		m.load = m.loadDB
	}
	return m
}

func (m *MetadataStore) Invalidate() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.epoch++
	m.snapshot = nil
}
func (m *MetadataStore) Load(ctx context.Context) ([]analysis.CategoryMetadata, string) {
	m.mu.Lock()
	if m.snapshot != nil && m.now().Sub(m.snapshot.loaded) < m.ttl {
		result, local := cloneCategories(m.snapshot.categories), stringValue(m.snapshot.local)
		m.mu.Unlock()
		return result, local
	}
	m.mu.Unlock()
	// Waiting for another metadata refresh must be cancelable independently of
	// the database request that currently owns the refresh slot.
	select {
	case m.refresh <- struct{}{}:
		defer func() { <-m.refresh }()
	case <-ctx.Done():
		return cloneCategories(fallbackCategories), ""
	}
	m.mu.Lock()
	if m.snapshot != nil && m.now().Sub(m.snapshot.loaded) < m.ttl {
		result, local := cloneCategories(m.snapshot.categories), stringValue(m.snapshot.local)
		m.mu.Unlock()
		return result, local
	}
	epoch := m.epoch
	load := m.load
	m.mu.Unlock()
	if load != nil {
		cats, local, err := load(ctx)
		if err == nil {
			m.mu.Lock()
			if m.epoch == epoch {
				m.snapshot = &metadataSnapshot{categories: cats, local: local, loaded: m.now()}
			}
			m.mu.Unlock()
			return cloneCategories(cats), stringValue(local)
		}
	}
	return cloneCategories(fallbackCategories), ""
}
func (m *MetadataStore) loadDB(ctx context.Context) ([]analysis.CategoryMetadata, *string, error) {
	rows, err := m.db.Query(ctx, `SELECT name, display_name, COALESCE(is_local,false) FROM categories ORDER BY name`)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	cats := make([]analysis.CategoryMetadata, 0)
	local := (*string)(nil)
	for rows.Next() {
		var c analysis.CategoryMetadata
		var isLocal bool
		if err := rows.Scan(&c.Name, &c.DisplayName, &isLocal); err != nil {
			return nil, nil, err
		}
		cats = append(cats, c)
		if isLocal && local == nil {
			v := c.Name
			local = &v
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	mapping, err := m.db.Query(ctx, `SELECT ai_category, fixed_category FROM category_mapping ORDER BY fixed_category, usage_count DESC`)
	if err != nil {
		return nil, nil, err
	}
	defer mapping.Close()
	byName := make(map[string]int, len(cats))
	for i, c := range cats {
		byName[c.Name] = i
	}
	counts := make(map[string]int)
	for mapping.Next() {
		var example, fixed string
		if err := mapping.Scan(&example, &fixed); err != nil {
			return nil, nil, err
		}
		i, ok := byName[fixed]
		if !ok || counts[fixed] >= 5 {
			continue
		}
		cats[i].Examples = append(cats[i].Examples, example)
		counts[fixed]++
	}
	if err := mapping.Err(); err != nil {
		return nil, nil, err
	}
	return cats, local, nil
}
func cloneCategories(input []analysis.CategoryMetadata) []analysis.CategoryMetadata {
	out := make([]analysis.CategoryMetadata, len(input))
	for i, c := range input {
		out[i] = c
		out[i].Examples = append([]string(nil), c.Examples...)
	}
	return out
}
func stringValue(v *string) string {
	if v == nil {
		return ""
	}
	return *v
}
