package selectors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"github.com/jackc/pgx/v5"
)

type Source struct {
	Name, URL string
	Config    map[string]any
}
type SourceFence struct {
	Fence       authority.Fence
	SourceID    int64
	Fingerprint string
}

func ingestionRun(job string, attempt int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("evening-news-ingestion-v1\x00%s\x00%d", job, attempt)))
	b := sum[:16]
	b[6] = (b[6] & 0x0f) | 0x50
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
func SourceFingerprint(id int64, name, kind, url string, enabled bool, configText string) string {
	parts := []string{strconv.FormatInt(id, 10), name, kind, url, strconv.FormatBool(enabled), configText}
	h := sha256.New()
	for _, part := range parts {
		_, _ = fmt.Fprintf(h, "%d:%s", len([]byte(part)), part)
	}
	return hex.EncodeToString(h.Sum(nil))
}
func (s SourceFence) ValidateTx(ctx context.Context, tx pgx.Tx) (Source, error) {
	if err := s.Fence.Validate(ctx, tx); err != nil {
		return Source{}, err
	}
	var state string
	err := tx.QueryRow(ctx, `SELECT state FROM article_ingestion_runs WHERE id=$1::uuid AND job_id=$2::uuid AND attempt_generation=$3 FOR UPDATE`, ingestionRun(s.Fence.JobID, s.Fence.AttemptGeneration), s.Fence.JobID, s.Fence.AttemptGeneration).Scan(&state)
	if err != nil || state != "running" {
		return Source{}, authority.ErrStale
	}
	var source Source
	var typ, configText string
	var enabled bool
	var raw []byte
	err = tx.QueryRow(ctx, `SELECT name,source_type,url,enabled,config,config::text FROM sources WHERE id=$1 FOR SHARE`, s.SourceID).Scan(&source.Name, &typ, &source.URL, &enabled, &raw, &configText)
	if err != nil || typ != "custom" || !enabled || SourceFingerprint(s.SourceID, source.Name, typ, source.URL, enabled, configText) != s.Fingerprint {
		return Source{}, authority.ErrStale
	}
	if json.Unmarshal(raw, &source.Config) != nil || source.Config == nil {
		source.Config = map[string]any{}
	}
	return source, nil
}
func (s SourceFence) Validate(ctx context.Context) (Source, error) {
	tx, err := s.Fence.Pool.Begin(ctx)
	if err != nil {
		return Source{}, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	return s.ValidateTx(ctx, tx)
}
