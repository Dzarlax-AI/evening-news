package config

import (
	"testing"
	"time"
)

func TestLoadFromUsesOnlyLookupAndValidates(t *testing.T) {
	v := map[string]string{"DATABASE_URL": "postgresql://worker:pw@db/news", "JOB_EXECUTOR_URL": "http://executor:8100", "JOB_EXECUTOR_TOKEN": "1234567890123456"}
	c, e := LoadFrom(func(k string) string { return v[k] })
	if e != nil {
		t.Fatal(e)
	}
	if c.DatabaseURL != v["DATABASE_URL"] || c.PollInterval != time.Second {
		t.Fatalf("unexpected config: %+v", c)
	}
	v["WORKER_POLL_INTERVAL_SECONDS"] = "invalid"
	if _, e = LoadFrom(func(k string) string { return v[k] }); e == nil {
		t.Fatal("invalid config accepted")
	}
}
