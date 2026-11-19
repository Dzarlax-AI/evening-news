package runner

import (
	"errors"
	"testing"
)

func TestLoadConfigUsesLookupAndReturnsErrorsForInvalidValues(t *testing.T) {
	v := map[string]string{"DATABASE_URL": "postgres://processor:pw@db/news", "ARTICLE_PROCESSING_TOKEN": "1234567890123456", "ARTICLE_AI_URL": "http://executor:8100", "ARTICLE_AI_TOKEN": "1234567890123456", "EXTRACTION_SERVICE_URL": "http://extractor:8092", "EXTRACTION_SERVICE_TOKEN": "1234567890123456"}
	c, e := LoadConfig(func(k string) string { return v[k] })
	if e != nil {
		t.Fatal(e)
	}
	if c.DatabaseURL != v["DATABASE_URL"] || c.DBMaxConns != 5 || c.MaxWorkers != 5 || c.DedupTrial || c.TelegramContentFilter {
		t.Fatalf("unexpected config: %+v", c)
	}
	v["ARTICLE_DEDUP_TRIAL"] = "true"
	if c, e = LoadConfig(func(k string) string { return v[k] }); e != nil || !c.DedupTrial {
		t.Fatalf("dedup trial flag missing from processing config: %v", e)
	}
	v["ARTICLE_TELEGRAM_CONTENT_FILTER"] = "true"
	if c, e = LoadConfig(func(k string) string { return v[k] }); e != nil || !c.TelegramContentFilter {
		t.Fatalf("Telegram content filter missing from processing config: %v", e)
	}
	v["ARTICLE_TELEGRAM_CONTENT_FILTER"] = "invalid"
	if _, e = LoadConfig(func(k string) string { return v[k] }); !errors.Is(e, ErrInvalidConfig) {
		t.Fatalf("invalid Telegram content filter error = %v", e)
	}
	delete(v, "ARTICLE_TELEGRAM_CONTENT_FILTER")
	v["MAX_WORKERS"] = "oops"
	if _, e = LoadConfig(func(k string) string { return v[k] }); !errors.Is(e, ErrInvalidConfig) {
		t.Fatalf("invalid config error = %v", e)
	}
}
