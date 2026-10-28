// Package learning owns learned extraction patterns and fenced feedback.
package learning

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/Dzarlax-AI/evening-news/go-domain/callbacks"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pattern struct {
	domain, selector, method, discovered                     string
	success, failure, consecutiveSuccess, consecutiveFailure int
	quality                                                  float64
	length                                                   int
	last                                                     *time.Time
}

func (p *pattern) rate() float64 {
	total := p.success + p.failure
	if total > 0 {
		return float64(p.success) / float64(total)
	}
	if p.discovered == "ai" {
		return p.quality
	}
	return 0
}
func better(a, b *pattern) bool {
	if a.rate() != b.rate() {
		return a.rate() > b.rate()
	}
	if a.success != b.success {
		return a.success > b.success
	}
	if a.consecutiveFailure != b.consecutiveFailure {
		return a.consecutiveFailure < b.consecutiveFailure
	}
	if a.consecutiveSuccess != b.consecutiveSuccess {
		return a.consecutiveSuccess > b.consecutiveSuccess
	}
	if a.last == nil && b.last != nil {
		return false
	}
	if a.last != nil && b.last == nil {
		return true
	}
	if a.last != nil && b.last != nil && !a.last.Equal(*b.last) {
		return a.last.After(*b.last)
	}
	if a.quality != b.quality {
		return a.quality > b.quality
	}
	if a.selector != b.selector {
		return a.selector < b.selector
	}
	if a.method != b.method {
		return a.method < b.method
	}
	return a.domain < b.domain
}

type remembered struct {
	key     string
	pattern callbacks.Pattern
}
type Memory struct {
	Pool         *pgxpool.Pool
	Guard        interface{ Check(context.Context) error }
	mu           sync.Mutex
	loaded       bool
	patterns     map[string][]*pattern
	contexts     map[string]*list.Element
	lru          *list.List
	feedbackSlot chan struct{}
	stability    *stability
}

func New(pool *pgxpool.Pool) *Memory {
	return &Memory{Pool: pool, patterns: make(map[string][]*pattern), contexts: make(map[string]*list.Element), lru: list.New(), feedbackSlot: make(chan struct{}, 1), stability: newStability("/app/data/domain_stats.json")}
}
func key(r callbacks.Request) string {
	return r.RunID + "/" + strconv.FormatInt(r.ArticleID, 10) + "/" + strconv.FormatInt(int64(r.ArticleAttempt), 10) + "/" + r.InputFingerprint
}

// load preserves the legacy bounded, quality-ranked startup snapshot.
func (m *Memory) load(ctx context.Context) error {
	if m.loaded {
		return nil
	}
	rows, err := m.Pool.Query(ctx, `WITH pattern_stats AS (
 SELECT domain,selector_pattern,extraction_strategy,COALESCE(success_count,0) AS success_count,COALESCE(failure_count,0) AS failure_count,
 CASE WHEN COALESCE(success_count,0)+COALESCE(failure_count,0)>0 THEN COALESCE(success_count,0)*1.0/(COALESCE(success_count,0)+COALESCE(failure_count,0)) WHEN discovered_by='ai' THEN COALESCE(quality_score_avg,0) ELSE 0 END AS success_rate,
 COALESCE(quality_score_avg,0) AS quality_score_avg,COALESCE(content_length_avg,0) AS content_length_avg,COALESCE(discovered_by,'manual') AS discovered_by,COALESCE(consecutive_successes,0) AS consecutive_successes,COALESCE(consecutive_failures,0) AS consecutive_failures,last_success_at
 FROM extraction_patterns WHERE success_count>0 OR failure_count<5), eligible AS (
 SELECT *,ROW_NUMBER() OVER(PARTITION BY domain ORDER BY success_rate DESC,success_count DESC,consecutive_failures ASC,consecutive_successes DESC,last_success_at DESC NULLS LAST,quality_score_avg DESC,selector_pattern ASC,extraction_strategy ASC) AS rank FROM pattern_stats), domain_scores AS (
 SELECT domain,MAX(success_rate) FILTER(WHERE rank=1) AS rate,MAX(success_count) FILTER(WHERE rank=1) AS success,MIN(consecutive_failures) FILTER(WHERE rank=1) AS failures,MAX(consecutive_successes) FILTER(WHERE rank=1) AS consecutive,MAX(last_success_at) FILTER(WHERE rank=1) AS last,MAX(quality_score_avg) FILTER(WHERE rank=1) AS quality,MAX(selector_pattern) FILTER(WHERE rank=1) AS selector,MAX(extraction_strategy) FILTER(WHERE rank=1) AS method FROM eligible WHERE rank<=10 GROUP BY domain), ranked_domains AS (
 SELECT domain,ROW_NUMBER() OVER(ORDER BY rate DESC,success DESC,failures ASC,consecutive DESC,last DESC NULLS LAST,quality DESC,selector ASC,method ASC,domain ASC) AS rank FROM domain_scores)
 SELECT e.domain,e.selector_pattern,e.extraction_strategy,e.success_count,e.failure_count,e.quality_score_avg,e.content_length_avg,e.discovered_by,e.consecutive_successes,e.consecutive_failures,e.last_success_at FROM eligible e JOIN ranked_domains d ON d.domain=e.domain WHERE e.rank<=10 AND d.rank<=500 ORDER BY e.success_rate DESC,e.success_count DESC,e.consecutive_failures ASC,e.consecutive_successes DESC,e.last_success_at DESC NULLS LAST,e.quality_score_avg DESC,e.selector_pattern ASC,e.extraction_strategy ASC,e.domain ASC LIMIT 2000`)
	if err != nil {
		if ctx.Err() == nil {
			m.loaded = true
		}
		return err
	}
	defer rows.Close()
	entries := make(map[string][]*pattern)
	for rows.Next() {
		p := &pattern{}
		if err = rows.Scan(&p.domain, &p.selector, &p.method, &p.success, &p.failure, &p.quality, &p.length, &p.discovered, &p.consecutiveSuccess, &p.consecutiveFailure, &p.last); err != nil {
			if ctx.Err() == nil {
				m.loaded = true
			}
			return err
		}
		entries[p.domain] = append(entries[p.domain], p)
	}
	if err = rows.Err(); err != nil {
		if ctx.Err() == nil {
			m.loaded = true
		}
		return err
	}
	m.patterns = entries
	m.loaded = true
	return nil
}
func (m *Memory) Context(ctx context.Context, r callbacks.Request, _ authority.Fence) (callbacks.Pattern, error) {
	var input struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal(r.Input, &input); err != nil {
		return callbacks.Pattern{}, err
	}
	domain, ok := callbacks.URLHostname(input.URL)
	if !ok {
		return callbacks.Pattern{}, errors.New("invalid article URL")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_ = m.load(ctx)
	if ctx.Err() != nil {
		return callbacks.Pattern{}, ctx.Err()
	}
	var best *pattern
	for _, p := range m.patterns[domain] {
		if p.success > 0 && p.rate() > 0.5 && (best == nil || p.rate() > best.rate() || p.rate() == best.rate() && p.success > best.success) {
			best = p
		}
	}
	result := callbacks.Pattern{}
	if best != nil {
		selector, method := best.selector, best.method
		result.Selector = &selector
		result.Method = &method
	}
	result = callbacks.SanitizePattern(result)
	k := key(r)
	if old := m.contexts[k]; old != nil {
		m.lru.Remove(old)
	}
	m.contexts[k] = m.lru.PushBack(remembered{k, result})
	for m.lru.Len() > 128 {
		old := m.lru.Front()
		delete(m.contexts, old.Value.(remembered).key)
		m.lru.Remove(old)
	}
	return result, nil
}
func (m *Memory) take(r callbacks.Request) callbacks.Pattern {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := key(r)
	if e := m.contexts[k]; e != nil {
		delete(m.contexts, k)
		m.lru.Remove(e)
		return e.Value.(remembered).pattern
	}
	return callbacks.Pattern{}
}

func (m *Memory) Feedback(ctx context.Context, r callbacks.Request, f authority.Fence, v callbacks.Feedback) (bool, error) {
	select {
	case m.feedbackSlot <- struct{}{}:
	case <-ctx.Done():
		return false, ctx.Err()
	}
	defer func() { <-m.feedbackSlot }()
	// Load the snapshot before the first commit. Loading afterwards would read
	// the committed update and then apply it a second time in memory.
	m.mu.Lock()
	_ = m.load(ctx)
	m.mu.Unlock()
	if ctx.Err() != nil {
		return false, ctx.Err()
	}
	learned := m.take(r)
	domain, ok := callbacks.URLHostname(v.URL)
	if !ok {
		return false, errors.New("invalid article URL")
	}
	quality := 0.0
	if v.Success {
		quality = 0.8
	}
	if v.QualityScore != nil {
		quality = *v.QualityScore
	}
	length, elapsed := 0, 0
	if v.ContentLength != nil {
		length = *v.ContentLength
	}
	if v.ExtractionTimeMS != nil {
		elapsed = *v.ExtractionTimeMS
	}
	tx, err := m.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	envelope, _ := json.Marshal(r.Envelope)
	sum := sha256.Sum256(envelope)
	claim := callbacks.Claim{Fence: f, RunID: r.RunID, EnvelopeDigest: hex.EncodeToString(sum[:]), ArticleID: r.ArticleID, ArticleAttempt: r.ArticleAttempt, InputFingerprint: r.InputFingerprint, URL: v.URL}
	if err = claim.ValidateTx(ctx, tx, true); err != nil {
		return false, err
	}
	var storedQuality any
	if v.Success {
		storedQuality = quality
	}
	_, err = tx.Exec(ctx, `INSERT INTO extraction_attempts(article_url,domain,extraction_strategy,selector_used,success,content_length,quality_score,extraction_time_ms,error_message) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)`, v.URL, domain, v.Method, v.SelectorUsed, v.Success, length, storedQuality, elapsed, v.ErrorCode)
	if err != nil {
		return false, err
	}
	for _, ob := range v.Observations {
		var code *string
		if !ob.Success {
			code = v.ErrorCode
		}
		if _, err = tx.Exec(ctx, `INSERT INTO extraction_attempts(article_url,domain,extraction_strategy,selector_used,success,content_length,quality_score,extraction_time_ms,error_message) VALUES($1,$2,$3,$4,$5,NULL,NULL,NULL,$6)`, v.URL, domain, ob.Method, ob.Selector, ob.Success, code); err != nil {
			return false, err
		}
	}
	if v.Success && v.SelectorUsed != nil && *v.SelectorUsed != "" {
		_, err = tx.Exec(ctx, `INSERT INTO extraction_patterns(domain,selector_pattern,extraction_strategy,success_count,failure_count,quality_score_avg,content_length_avg,discovered_by,is_stable,consecutive_successes,consecutive_failures,first_success_at,last_success_at)
 VALUES($1,$2,$3,1,0,$4,$5,'manual',false,1,0,NOW(),NOW()) ON CONFLICT(domain,selector_pattern,extraction_strategy) DO UPDATE SET
 success_count=COALESCE(extraction_patterns.success_count,0)+1,
 quality_score_avg=(COALESCE(extraction_patterns.quality_score_avg,0)*COALESCE(extraction_patterns.success_count,0)+EXCLUDED.quality_score_avg)/(COALESCE(extraction_patterns.success_count,0)+1),
 content_length_avg=(COALESCE(extraction_patterns.content_length_avg,0)*COALESCE(extraction_patterns.success_count,0)+EXCLUDED.content_length_avg)/(COALESCE(extraction_patterns.success_count,0)+1),
 consecutive_successes=COALESCE(extraction_patterns.consecutive_successes,0)+1,consecutive_failures=0,is_stable=COALESCE(extraction_patterns.consecutive_successes,0)+1>=3,first_success_at=COALESCE(extraction_patterns.first_success_at,NOW()),last_success_at=NOW(),updated_at=NOW()`, domain, *v.SelectorUsed, v.Method, quality, length)
		if err != nil {
			return false, err
		}
	}
	failed := false
	if learned.Selector != nil && learned.Method != nil && *learned.Selector != "" && *learned.Method != "" {
		for _, ob := range v.Observations {
			if !ob.Success && ob.Selector == *learned.Selector && ob.Method == *learned.Method {
				failed = true
				break
			}
		}
		if failed {
			_, err = tx.Exec(ctx, `UPDATE extraction_patterns SET failure_count=COALESCE(failure_count,0)+1,consecutive_failures=COALESCE(consecutive_failures,0)+1,consecutive_successes=0,is_stable=false,updated_at=NOW() WHERE domain=$1 AND selector_pattern=$2 AND extraction_strategy=$3`, domain, *learned.Selector, *learned.Method)
			if err != nil {
				return false, err
			}
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return false, err
	}
	m.mu.Lock()
	_ = m.load(ctx)
	if ctx.Err() != nil {
		m.mu.Unlock()
		return false, ctx.Err()
	}
	if v.Success && v.SelectorUsed != nil && *v.SelectorUsed != "" {
		var found *pattern
		for _, p := range m.patterns[domain] {
			if p.selector == *v.SelectorUsed && p.method == v.Method {
				found = p
				break
			}
		}
		if found == nil {
			found = &pattern{domain: domain, selector: *v.SelectorUsed, method: v.Method, discovered: "manual"}
			m.patterns[domain] = append(m.patterns[domain], found)
		}
		found.success++
		found.consecutiveSuccess++
		found.consecutiveFailure = 0
		found.quality = (found.quality*float64(found.success-1) + quality) / float64(found.success)
		found.length = (found.length*(found.success-1) + length) / found.success
	}
	if failed {
		for _, p := range m.patterns[domain] {
			if p.selector == *learned.Selector && p.method == *learned.Method {
				p.failure++
				p.consecutiveFailure++
				p.consecutiveSuccess = 0
				break
			}
		}
	}
	m.enforceLimits()
	m.mu.Unlock()
	if v.Success && m.stability != nil {
		err = m.stability.success(domain, v.Method, elapsed, length, quality, func(staged, target string) error {
			tx, e := m.Pool.Begin(ctx)
			if e != nil {
				return e
			}
			defer tx.Rollback(context.Background())
			if e = claim.ValidateTx(ctx, tx, true); e != nil {
				return e
			}
			if e = ctx.Err(); e != nil {
				return e
			}
			if m.Guard != nil && m.Guard.Check(ctx) != nil {
				return authority.ErrStale
			}
			if e = os.Rename(staged, target); e != nil {
				return e
			}
			return tx.Commit(ctx)
		})
		if errors.Is(err, authority.ErrStale) || ctx.Err() != nil {
			return false, authority.ErrStale
		}
	}
	return true, nil
}
func (m *Memory) enforceLimits() {
	domains := make([]string, 0, len(m.patterns))
	for domain, entries := range m.patterns {
		sort.SliceStable(entries, func(i, j int) bool { return better(entries[i], entries[j]) })
		if len(entries) > 10 {
			m.patterns[domain] = entries[:10]
		}
		if len(entries) > 0 {
			domains = append(domains, domain)
		}
	}
	sort.SliceStable(domains, func(i, j int) bool { return better(m.patterns[domains[i]][0], m.patterns[domains[j]][0]) })
	if len(domains) > 500 {
		domains = domains[:500]
	}
	all := make([]*pattern, 0, 2000)
	for _, domain := range domains {
		all = append(all, m.patterns[domain]...)
	}
	sort.SliceStable(all, func(i, j int) bool { return better(all[i], all[j]) })
	if len(all) > 2000 {
		all = all[:2000]
	}
	m.patterns = make(map[string][]*pattern)
	for _, p := range all {
		m.patterns[p.domain] = append(m.patterns[p.domain], p)
	}
}
