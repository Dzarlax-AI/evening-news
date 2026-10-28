package digestworkflow

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-editorial/editorial"
	"github.com/jackc/pgx/v5"
)

var ErrNotReady = errors.New("editorial_not_ready")
var ErrPrepared = errors.New("prepared_digest_missing")

type Identity struct {
	JobID, DomainInstanceID string
	Attempt                 int
	Deadline                time.Time
}
type Prepared struct {
	Identity  Identity
	EditionID string
	Edition   editorial.Edition
	Rendered  editorial.Rendered
	claimed   bool
	size      int
}
type PreparedCache struct {
	mu      sync.Mutex
	entries map[string]*Prepared
	total   int
}

func randomUUID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	b[6] = (b[6] & 15) | 0x40
	b[8] = (b[8] & 63) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}
func (c *PreparedCache) add(p Prepared) (string, error) {
	data, err := json.Marshal(p)
	if err != nil {
		return "", ErrInvalid
	}
	p.size = len(data)
	if p.size > 1<<20 {
		return "", ErrLimit
	}
	token, err := randomUUID()
	if err != nil {
		return "", err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]*Prepared)
	}
	now := time.Now()
	for k, v := range c.entries {
		if !v.Identity.Deadline.Add(5 * time.Minute).After(now) {
			delete(c.entries, k)
			c.total -= v.size
		}
	}
	if _, exists := c.entries[token]; exists {
		return "", ErrLimit
	}
	if len(c.entries) >= 64 || c.total+p.size > 5<<20 {
		return "", ErrLimit
	}
	c.entries[token] = &p
	c.total += p.size
	return token, nil
}
func (c *PreparedCache) Claim(token string, id Identity) (Prepared, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	p := c.entries[token]
	if p == nil || p.claimed || p.Identity.JobID != id.JobID || p.Identity.Attempt != id.Attempt || p.Identity.DomainInstanceID != id.DomainInstanceID || !p.Identity.Deadline.Equal(id.Deadline) || !p.Identity.Deadline.After(time.Now()) {
		return Prepared{}, ErrPrepared
	}
	p.claimed = true
	return *p, nil
}
func (w Workflow) Count(ctx context.Context, date time.Time) (int, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return 0, err
	}
	var n int
	err = tx.QueryRow(ctx, `SELECT count(*) FROM editorial_digest_editions WHERE edition_date=$1::date AND locale='ru'`, date.UTC().Format("2006-01-02")).Scan(&n)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, err
	}
	return n, nil
}
func (w Workflow) Prepare(ctx context.Context, cache *PreparedCache, id Identity, now time.Time) (string, error) {
	tx, err := w.Pool.Begin(ctx)
	if err != nil {
		return "", err
	}
	defer tx.Rollback(ctx)
	if err = w.Fence.Validate(ctx, tx); err != nil {
		return "", err
	}
	var editionID string
	var snapshotRaw, editionRaw, renderedRaw []byte
	err = tx.QueryRow(ctx, `SELECT id::text,snapshot,edition,rendered FROM editorial_digest_editions WHERE edition_date=$1::date AND locale='ru' ORDER BY created_at DESC,id DESC LIMIT 1`, now.UTC().Format("2006-01-02")).Scan(&editionID, &snapshotRaw, &editionRaw, &renderedRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", ErrNotReady
	}
	if err != nil {
		return "", err
	}
	if err = tx.Commit(ctx); err != nil {
		return "", err
	}
	var edition editorial.Edition
	var snapshot editorial.Snapshot
	var rendered editorial.Rendered
	if json.Unmarshal(snapshotRaw, &snapshot) != nil || json.Unmarshal(editionRaw, &edition) != nil || json.Unmarshal(renderedRaw, &rendered) != nil || edition.Locale != "ru" || edition.EditionURL == "" || rendered.RichHTML == "" || len(rendered.PlainParts) == 0 {
		return "", ErrInvalid
	}
	if editorial.ValidateEdition(snapshot, "ru", edition) != nil {
		return "", ErrInvalid
	}
	expected, err := editorial.Render(snapshot, "ru", edition)
	if err != nil || !reflect.DeepEqual(expected, rendered) {
		return "", ErrInvalid
	}
	return cache.add(Prepared{Identity: id, EditionID: editionID, Edition: edition, Rendered: rendered})
}

// NewUUID creates a canonical opaque identifier for a persisted intent.
func NewUUID() (string, error) { return randomUUID() }
