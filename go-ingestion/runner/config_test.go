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

func TestSelectorServiceURLOverridesLegacyExecutorURLAndFallsBack(t *testing.T) {
	v := map[string]string{"DATABASE_URL": "postgres://ingest:pw@db/news", "ARTICLE_INGESTION_TOKEN": "1234567890123456", "EXTRACTION_SERVICE_URL": "http://extractor:8092", "EXTRACTION_SERVICE_TOKEN": "1234567890123456", "JOB_EXECUTOR_URL": "http://background:8101"}
	load := func() Config {
		c, err := LoadConfig(func(k string) string { return v[k] })
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	if got := load().SelectorURL; got != "http://background:8101" {
		t.Fatalf("legacy selector fallback = %q", got)
	}
	v["SELECTOR_SERVICE_URL"] = "http://executor:8100"
	if got := load().SelectorURL; got != "http://executor:8100" {
		t.Fatalf("explicit selector URL = %q", got)
	}
}
