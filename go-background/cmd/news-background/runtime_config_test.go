package main

import (
	"github.com/Dzarlax-AI/evening-news/go-background/internal/config"
	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"os"
	"path/filepath"
	"testing"
)

func TestPreparedManifestLoadsBackgroundAndFailsClosed(t *testing.T) {
	root := t.TempDir()
	l := managedsetup.Layout{Setup: filepath.Join(root, "setup"), Web: filepath.Join(root, "web"), Domain: filepath.Join(root, "domain"), Background: filepath.Join(root, "background"), Extractor: filepath.Join(root, "extractor"), Postgres: filepath.Join(root, "pg"), PostgresSecrets: filepath.Join(root, "pgsecret"), WebUID: -1, DomainUID: -1, BackgroundUID: -1, ExtractorUID: -1}
	if err := os.Mkdir(l.Postgres, 0700); err != nil {
		t.Fatal(err)
	}
	if _, err := managedsetup.Prepare(l, "http://localhost:8000"); err != nil {
		t.Fatal(err)
	}
	t.Setenv("NEWS_RUNTIME_FILE", filepath.Join(l.Background, "runtime.json"))
	get, err := configLookup()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = config.Load(get, "fixture"); err != nil {
		t.Fatal(err)
	}
	os.Remove(filepath.Join(l.Background, "runtime.json"))
	t.Setenv("WORKER_DATABASE_URL", "legacy")
	if _, err = configLookup(); err == nil {
		t.Fatal("missing state fell back to environment")
	}
}
