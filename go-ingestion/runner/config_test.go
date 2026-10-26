package runner

import (
	"errors"
	"testing"
)

func TestLoadConfigUsesLookupAndRejectsInvalidLimits(t *testing.T) {
	v := map[string]string{"DATABASE_URL": "postgres://ingest:pw@db/news", "ARTICLE_INGESTION_TOKEN": "1234567890123456", "EXTRACTION_SERVICE_URL": "http://extractor:8092", "EXTRACTION_SERVICE_TOKEN": "1234567890123456"}
	c, e := LoadConfig(func(k string) string { return v[k] })
	if e != nil {
		t.Fatal(e)
	}
	if c.DatabaseURL != v["DATABASE_URL"] || c.DBMaxConns != 5 || c.ListenAddr != ":8093" {
		t.Fatalf("unexpected config: %+v", c)
	}
	v["ARTICLE_INGESTION_DB_MAX_CONNS"] = "nope"
	if _, e = LoadConfig(func(k string) string { return v[k] }); !errors.Is(e, ErrInvalidConfig) {
		t.Fatalf("invalid config error = %v", e)
	}
}
