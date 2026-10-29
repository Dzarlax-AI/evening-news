package managedsetup

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadRenderedComposePrivateAndNameOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compose.json")
	raw := `{"name":"fixture","services":{"web":{"environment":{"DATABASE_URL":"postgres://private:secret@db/news","JWT_SECRET":"hidden"}},"executor":{"environment":{"DATABASE_URL":"postgres://private:secret@db/news"}},"background":{"environment":{"WORKER_DATABASE_URL":"postgres://private:secret@db/news"}},"extractor":{"environment":{"EXTRACTOR_SERVICE_TOKEN":"hidden"}}}}`
	if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := ReadRenderedCompose(path)
	if err != nil {
		t.Fatal(err)
	}
	if c.Services["web"]["JWT_SECRET"] != "hidden" {
		t.Fatal("compose value was not retained in memory")
	}
	keys := c.Keys()
	joined := strings.Join(keys["web"], ",")
	if strings.Contains(joined, "hidden") || strings.Contains(joined, "secret") || !strings.Contains(joined, "JWT_SECRET") {
		t.Fatalf("unsafe or incomplete key listing: %s", joined)
	}
	if len(c.Digest) != 64 {
		t.Fatalf("digest length %d", len(c.Digest))
	}
}

func TestReadRenderedComposeRejectsUnsafeAndUnsupportedInputs(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "compose.json")
	valid := `{"services":{"web":{"environment":{"A":"1"}},"executor":{"environment":{"A":"1"}},"background":{"environment":{"A":"1"}},"extractor":{"environment":{"A":"1"}}}}`
	if err := os.WriteFile(path, []byte(valid), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRenderedCompose(path); err == nil {
		t.Fatal("accepted world-readable config")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(valid, `"A":"1"`, `"A":null`, 1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRenderedCompose(path); err == nil {
		t.Fatal("accepted non-string environment value")
	}
}

func TestStageAndActivateAdoptionAreImmutable(t *testing.T) {
	root := t.TempDir()
	l := Layout{Setup: filepath.Join(root, "setup"), Web: filepath.Join(root, "web"), Domain: filepath.Join(root, "domain"), Background: filepath.Join(root, "background"), Extractor: filepath.Join(root, "extractor"), WebUID: -1, DomainUID: -1, BackgroundUID: -1, ExtractorUID: -1}
	for _, dir := range []string{l.Setup, l.Web, l.Domain, l.Background, l.Extractor} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	roles := map[string]map[string]string{
		"web":        {"DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "JWT_SECRET": "signing", "WEB_BACKEND_URL": "http://domain", "WEB_BACKEND_TOKEN": "a", "SCHEDULED_JOBS_TOKEN": "b", "SCHEDULER_API_URL": "http://worker", "SCHEDULER_API_TOKEN": "c"},
		"domain":     {"DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "JOB_EXECUTOR_TOKEN": "a", "ARTICLE_AI_TOKEN": "b", "EXTRACTION_SERVICE_URL": "http://extract", "EXTRACTION_SERVICE_TOKEN": "d", "WEB_BACKEND_TOKEN": "e", "NEWS_CREDENTIAL_KEY": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "NEWS_CREDENTIAL_KEY_ID": "id"},
		"background": {"WORKER_DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "EXECUTION_DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "INGESTION_DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "PROCESSING_DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "SCHEDULER_DATABASE_URL": "postgres://role:pw@127.0.0.1/scheduler", "JOB_EXECUTOR_TOKEN": "a", "ARTICLE_AI_TOKEN": "b", "ARTICLE_INGESTION_TOKEN": "i", "ARTICLE_PROCESSING_TOKEN": "p", "SCHEDULER_API_TOKEN": "c", "SCHEDULER_TARGET_NEWS_WORKER_TOKEN": "s", "EXTRACTION_SERVICE_TOKEN": "d"},
		"extractor":  {"EXTRACTOR_SERVICE_TOKEN": "d"},
	}
	policies := map[string]struct{ Allowed, Required []string }{
		"web":        {nil, []string{"DATABASE_URL", "JWT_SECRET", "WEB_BACKEND_URL", "WEB_BACKEND_TOKEN", "SCHEDULED_JOBS_TOKEN", "SCHEDULER_API_URL", "SCHEDULER_API_TOKEN"}},
		"domain":     {nil, []string{"DATABASE_URL", "JOB_EXECUTOR_TOKEN", "ARTICLE_AI_TOKEN", "EXTRACTION_SERVICE_URL", "EXTRACTION_SERVICE_TOKEN", "WEB_BACKEND_TOKEN", "NEWS_CREDENTIAL_KEY", "NEWS_CREDENTIAL_KEY_ID"}},
		"background": {nil, []string{"WORKER_DATABASE_URL", "EXECUTION_DATABASE_URL", "INGESTION_DATABASE_URL", "PROCESSING_DATABASE_URL", "SCHEDULER_DATABASE_URL", "JOB_EXECUTOR_TOKEN", "ARTICLE_AI_TOKEN", "ARTICLE_INGESTION_TOKEN", "ARTICLE_PROCESSING_TOKEN", "SCHEDULER_API_TOKEN", "SCHEDULER_TARGET_NEWS_WORKER_TOKEN", "EXTRACTION_SERVICE_TOKEN"}},
		"extractor":  {nil, []string{"EXTRACTOR_SERVICE_TOKEN"}},
	}
	// Allowed fields must match test fixture values while exercising the same
	// service validation surface as runtime readers.
	for role, policy := range policies {
		policy.Allowed = make([]string, 0, len(roles[role]))
		for key := range roles[role] {
			policy.Allowed = append(policy.Allowed, key)
		}
		policies[role] = policy
	}
	digest := strings.Repeat("a", 64)
	dir, bundle, err := StageAdoption(l.Setup, strings.Repeat("1", 32), digest, roles, policies, map[string][]byte{"import.json": []byte(`{"admin_hash":"phc"}`)})
	if err != nil {
		t.Fatal(err)
	}
	if bundle.Revision != 1 {
		t.Fatalf("revision=%d", bundle.Revision)
	}
	if err = ActivateAdoption(l, dir, bundle.InstallationID, filepath.Join(root, "override.yml"), policies); err != nil {
		t.Fatal(err)
	}
	if err = ActivateAdoption(l, dir, bundle.InstallationID, filepath.Join(root, "override.yml"), policies); err != nil {
		t.Fatalf("idempotent activation: %v", err)
	}
	if _, _, err = StageAdoption(l.Setup, bundle.InstallationID, digest, roles, policies, nil); err != nil {
		t.Fatalf("idempotent stage: %v", err)
	}
	if err = os.WriteFile(filepath.Join(l.Web, "runtime.json"), []byte("changed"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = ActivateAdoption(l, dir, bundle.InstallationID, filepath.Join(root, "override.yml"), policies); err == nil {
		t.Fatal("activation overwrote conflicting role state")
	}
}
