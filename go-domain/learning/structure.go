package learning

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/selectors"
)

func (m *Memory) PageStructure(ctx context.Context, domain string) (selectors.Structure, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.load(ctx)
	if ctx.Err() != nil {
		return selectors.Structure{}, ctx.Err()
	}
	result := selectors.Structure{Container: []string{}, Title: []string{}, Link: []string{}, Date: []string{}}
	for _, entry := range []struct {
		method string
		target *[]string
	}{{"source_container", &result.Container}, {"source_title", &result.Title}, {"source_link", &result.Link}, {"source_date", &result.Date}} {
		all := []string{}
		good := []string{}
		for _, p := range m.patterns[domain] {
			if p.method == entry.method {
				all = append(all, p.selector)
				if p.rate() >= 0.5 {
					good = append(good, p.selector)
				}
			}
		}
		if len(good) > 0 {
			*entry.target = good
		} else {
			*entry.target = all
		}
	}
	return result, nil
}

type discovery struct {
	selector, method string
	confidence       float64
}

func (m *Memory) RecordStructure(ctx context.Context, domain string, a *selectors.Analysis, f selectors.SourceFence) (bool, error) {
	if a == nil {
		return false, nil
	}
	select {
	case m.feedbackSlot <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	defer func() { <-m.feedbackSlot }()
	patterns := []discovery{}
	for _, s := range a.ContentSelectors {
		confidence, ok := a.ConfidenceScores[s]
		if !ok {
			confidence = 0.8
		}
		patterns = append(patterns, discovery{s, "source_container", confidence})
	}
	for _, s := range a.TitleSelectors {
		patterns = append(patterns, discovery{s, "source_title", 0.9})
	}
	for _, s := range a.DateSelectors {
		patterns = append(patterns, discovery{s, "source_date", 0.8})
	}
	for _, p := range patterns {
		if strings.TrimSpace(p.selector) == "" || len([]byte(p.selector)) > 2048 {
			return false, nil
		}
	}
	m.mu.Lock()
	_ = m.load(ctx)
	m.mu.Unlock()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	if _, err = f.ValidateTx(ctx, tx); err != nil {
		return false, err
	}
	if m.Guard != nil && m.Guard.Check(ctx) != nil {
		return false, authority.ErrStale
	}
	for _, p := range patterns {
		_, err = tx.Exec(ctx, `INSERT INTO extraction_patterns(domain,selector_pattern,extraction_strategy,quality_score_avg,discovered_by) VALUES($1,$2,$3,$4,'ai') ON CONFLICT(domain,selector_pattern,extraction_strategy) DO UPDATE SET quality_score_avg=CASE WHEN COALESCE(extraction_patterns.success_count,0)+COALESCE(extraction_patterns.failure_count,0)=0 THEN GREATEST(COALESCE(extraction_patterns.quality_score_avg,0),EXCLUDED.quality_score_avg) ELSE extraction_patterns.quality_score_avg END,discovered_by=CASE WHEN COALESCE(extraction_patterns.success_count,0)+COALESCE(extraction_patterns.failure_count,0)=0 THEN 'ai' ELSE extraction_patterns.discovered_by END,updated_at=NOW()`, domain, p.selector, p.method, p.confidence)
		if err != nil {
			return false, err
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	// Cache publication is fenced again after the durable commit.
	next, err := m.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = next.Rollback(c)
	}()
	if _, err = f.ValidateTx(ctx, next); err != nil {
		return false, err
	}
	if m.Guard != nil && m.Guard.Check(ctx) != nil {
		return false, authority.ErrStale
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	original := m.patterns
	staged := make(map[string][]*pattern, len(original))
	for d, list := range original {
		clones := make([]*pattern, len(list))
		for i, p := range list {
			copy := *p
			clones[i] = &copy
		}
		staged[d] = clones
	}
	m.patterns = staged
	for _, entry := range patterns {
		var found *pattern
		for _, p := range m.patterns[domain] {
			if p.selector == entry.selector && p.method == entry.method {
				found = p
				break
			}
		}
		if found == nil {
			found = &pattern{domain: domain, selector: entry.selector, method: entry.method, discovered: "ai", quality: entry.confidence}
			m.patterns[domain] = append(m.patterns[domain], found)
		} else if found.success+found.failure == 0 {
			if found.discovered == "ai" && found.quality > entry.confidence {
				entry.confidence = found.quality
			}
			found.quality = entry.confidence
			found.discovered = "ai"
		}
	}
	m.enforceLimits()
	if ctx.Err() != nil {
		m.patterns = original
		return false, ctx.Err()
	}
	// The read-only transaction holds the source row fence until publication.
	if err = next.Rollback(ctx); err != nil {
		m.patterns = original
		if errors.Is(err, context.Canceled) {
			return false, err
		}
		return false, authority.ErrStale
	}
	return true, nil
}
