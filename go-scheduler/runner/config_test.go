package runner

import (
	"errors"
	"testing"
	"time"
)

func TestLoadConfigIsScopedAndDeterministic(t *testing.T) {
	v := map[string]string{"SCHEDULER_DATABASE_URL": "postgres://sched:pw@db/schedule", "SCHEDULER_DB_SCHEMA": "news", "SCHEDULER_API_TOKEN": "1234567890123456", "SCHEDULER_TARGET_ALIASES": "worker", "SCHEDULER_TARGET_WORKER_URL": "https://worker.internal", "SCHEDULER_TARGET_WORKER_TOKEN": "worker-token"}
	c, e := LoadConfig(func(k string) string { return v[k] })
	if e != nil {
		t.Fatal(e)
	}
	if c.DatabaseURL != v["SCHEDULER_DATABASE_URL"] || c.PollEvery != time.Second || c.Targets["worker"].URL != v["SCHEDULER_TARGET_WORKER_URL"] {
		t.Fatalf("unexpected config: %+v", c)
	}
	v["SCHEDULER_POLL_INTERVAL"] = "broken"
	if _, e = LoadConfig(func(k string) string { return v[k] }); !errors.Is(e, ErrInvalidConfig) {
		t.Fatalf("invalid config error = %v", e)
	}
}
