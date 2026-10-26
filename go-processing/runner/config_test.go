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
	if c.DatabaseURL != v["DATABASE_URL"] || c.DBMaxConns != 5 || c.MaxWorkers != 5 {
		t.Fatalf("unexpected config: %+v", c)
	}
	v["MAX_WORKERS"] = "oops"
	if _, e = LoadConfig(func(k string) string { return v[k] }); !errors.Is(e, ErrInvalidConfig) {
		t.Fatalf("invalid config error = %v", e)
	}
}
