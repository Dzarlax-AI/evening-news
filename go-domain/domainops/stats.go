package domainops

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"github.com/Dzarlax-AI/evening-news/go-domain/authority"
	"math"
	"strconv"
	"strings"
	"time"
)

func publishStats(ctx context.Context, f authority.Fence, raw json.RawMessage) (map[string]any, error) {
	if f.Kind != "news_processing" && f.Kind != "news_digest" && f.Kind != "article_reprocess" && f.Kind != "reprocess_failed" {
		return nil, errors.New("invalid job kind")
	}
	var input map[string]json.RawMessage
	if json.Unmarshal(raw, &input) != nil || len(input) != 1 {
		return nil, errors.New("invalid stats")
	}
	data, ok := input["stats"]
	if !ok {
		return nil, errors.New("invalid stats")
	}
	var rawValues map[string]json.RawMessage
	if json.Unmarshal(data, &rawValues) != nil || rawValues == nil {
		return nil, errors.New("invalid stats")
	}
	for _, value := range rawValues {
		v := bytes.TrimSpace(value)
		if len(v) == 0 || (v[0] != '-' && (v[0] < '0' || v[0] > '9')) {
			return nil, errors.New("invalid stats")
		}
	}
	var values map[string]json.Number
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.UseNumber()
	if dec.Decode(&values) != nil || len(values) == 0 {
		return nil, errors.New("invalid stats")
	}
	fields := map[string]int64{"articles_fetched": 0, "articles_processed": 0, "api_calls_made": 0, "errors_count": 0, "duration_seconds": 0}
	for k, n := range values {
		if _, ok = fields[k]; !ok {
			return nil, errors.New("invalid stats")
		}
		v, e := strconv.ParseFloat(string(n), 64)
		if e != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > 1e9 {
			return nil, errors.New("invalid stats")
		}
		if k != "errors_count" && k != "duration_seconds" && strings.ContainsAny(string(n), ".eE") {
			return nil, errors.New("invalid count")
		}
		fields[k] = int64(v)
	}
	tx, err := f.Pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer func() {
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = tx.Rollback(c)
	}()
	if err = f.Validate(ctx, tx); err != nil {
		return nil, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO processing_stats(date,articles_fetched,articles_processed,api_calls_made,errors_count,processing_time_seconds) VALUES((clock_timestamp() AT TIME ZONE 'UTC')::date,$1,$2,$3,$4,$5) ON CONFLICT(date) DO UPDATE SET articles_fetched=processing_stats.articles_fetched+EXCLUDED.articles_fetched,articles_processed=processing_stats.articles_processed+EXCLUDED.articles_processed,api_calls_made=processing_stats.api_calls_made+EXCLUDED.api_calls_made,errors_count=processing_stats.errors_count+EXCLUDED.errors_count,processing_time_seconds=processing_stats.processing_time_seconds+EXCLUDED.processing_time_seconds`, fields["articles_fetched"], fields["articles_processed"], fields["api_calls_made"], fields["errors_count"], fields["duration_seconds"])
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, err
	}
	return map[string]any{"published": true}, nil
}
