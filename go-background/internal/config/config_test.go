package config

import (
	"errors"
	"strings"
	"testing"
)

func validEnvironment() map[string]string {
	return map[string]string{
		"DATABASE_URL":        "postgres://generic:secret@db/news",
		"WORKER_DATABASE_URL": "postgres://worker:secret@db/news", "JOB_EXECUTOR_URL": "http://executor:8100", "JOB_EXECUTOR_TOKEN": "executor-token-123456",
		"SCHEDULER_DATABASE_URL": "postgres://scheduler:secret@db/schedules", "SCHEDULER_DB_SCHEMA": "news", "SCHEDULER_API_TOKEN": "scheduler-token-123456", "SCHEDULER_TARGET_ALIASES": "news-worker", "SCHEDULER_TARGET_NEWS_WORKER_URL": "https://web.internal/jobs", "SCHEDULER_TARGET_NEWS_WORKER_TOKEN": "web-target-token",
		"INGESTION_DATABASE_URL": "postgres://ingestion:secret@db/news", "ARTICLE_INGESTION_TOKEN": "ingestion-token-123456", "EXTRACTION_SERVICE_URL": "http://extractor:8092", "EXTRACTION_SERVICE_TOKEN": "extractor-token-123456",
		"PROCESSING_DATABASE_URL": "postgres://processing:secret@db/news", "ARTICLE_PROCESSING_TOKEN": "processing-token-123456", "ARTICLE_AI_URL": "http://executor:8100", "ARTICLE_AI_TOKEN": "fixture-only-ai-token-123456",
	}
}

func TestLoadUsesRoleScopedDatabaseURLsAndPreservesLimits(t *testing.T) {
	env := validEnvironment()
	env["WORKER_POLL_INTERVAL_SECONDS"] = "3"
	env["SCHEDULER_MAX_INFLIGHT"] = "7"
	env["ARTICLE_INGESTION_DB_MAX_CONNS"] = "3"
	env["MAX_WORKERS"] = "9"
	c, err := Load(func(k string) string { return env[k] }, "release-123")
	if err != nil {
		t.Fatal(err)
	}
	if c.Worker.DatabaseURL != env["WORKER_DATABASE_URL"] || c.Scheduler.DatabaseURL != env["SCHEDULER_DATABASE_URL"] || c.Ingestion.DatabaseURL != env["INGESTION_DATABASE_URL"] || c.Processing.DatabaseURL != env["PROCESSING_DATABASE_URL"] {
		t.Fatal("a component received the wrong database URL")
	}
	if c.Worker.DatabaseURL == env["DATABASE_URL"] || c.Scheduler.BuildID != "release-123" || c.BuildID != "release-123" {
		t.Fatal("generic DSN or build ID contract violated")
	}
	if c.Worker.DBMaxConns != 4 || c.Worker.PollInterval.Seconds() != 3 || c.Scheduler.DBMaxConns != 8 || c.Scheduler.MaxInflight != 7 || c.Ingestion.DBMaxConns != 3 || c.Processing.DBMaxConns != 5 || c.Processing.MaxWorkers != 9 {
		t.Fatal("component defaults changed")
	}
	if c.Scheduler.ListenAddr != ":8080" || c.Ingestion.ListenAddr != ":8093" || c.Processing.ListenAddr != ":8094" || c.HealthAddr != ":8095" {
		t.Fatal("listener defaults changed")
	}
}

func TestLoadDiagnosticsIdentifyFieldWithoutEchoingSecretValues(t *testing.T) {
	env := validEnvironment()
	env["WORKER_DATABASE_URL"] = "mysql://private-user:private-password@db/news?token=secret"
	_, err := Load(func(k string) string { return env[k] }, "")
	if err == nil {
		t.Fatal("invalid worker DSN was accepted")
	}
	message := err.Error()
	if !strings.Contains(message, "component=worker") || !strings.Contains(message, "DATABASE_URL") {
		t.Fatalf("missing safe component diagnostic: %s", message)
	}
	for _, secret := range []string{"private-user", "private-password", "token=secret"} {
		if strings.Contains(message, secret) {
			t.Fatalf("configuration error leaked %q", secret)
		}
	}
}

func TestLoadDoesNotFallbackToGenericDatabaseURL(t *testing.T) {
	env := validEnvironment()
	delete(env, "WORKER_DATABASE_URL")
	_, err := Load(func(k string) string { return env[k] }, "")
	if !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("missing role DSN error = %v", err)
	}
}

func TestLoadRejectsListenerCollisionsAndInvalidShutdownBound(t *testing.T) {
	for name, change := range map[string]func(map[string]string){
		"port collision": func(e map[string]string) { e["BACKGROUND_HEALTH_LISTEN"] = "127.0.0.1:8094" },
		"shutdown bound": func(e map[string]string) { e["BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS"] = "181" },
		"backoff order":  func(e map[string]string) { e["BACKGROUND_WORKER_BACKOFF_MIN"] = "6s" },
	} {
		t.Run(name, func(t *testing.T) {
			env := validEnvironment()
			change(env)
			if _, err := Load(func(k string) string { return env[k] }, ""); !errors.Is(err, ErrInvalidConfig) {
				t.Fatalf("Load error = %v", err)
			}
		})
	}
}
