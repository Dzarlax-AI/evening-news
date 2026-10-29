package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestManagedManifestDoesNotFallBackToEnvironment(t *testing.T) {
	t.Setenv("ADMIN_PASSWORD", "legacy-secret")
	t.Setenv("DATABASE_URL", "legacy-db")
	path := filepath.Join(t.TempDir(), "web.json")
	t.Setenv("NEWS_RUNTIME_FILE", path)
	data := `{"version":1,"service":"web","installation_id":"fixture","values":{"DATABASE_URL":"managed-db","JWT_SECRET":"key","WEB_BACKEND_URL":"http://domain","WEB_BACKEND_TOKEN":"backend","SCHEDULED_JOBS_TOKEN":"jobs","SCHEDULER_API_URL":"http://scheduler","SCHEDULER_API_TOKEN":"scheduler"}}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.Managed || c.DatabaseURL != "managed-db" || c.AdminPassword != "" || os.Getenv("DATABASE_URL") != "legacy-db" {
		t.Fatal("managed settings leaked or fell back")
	}
	if err = os.WriteFile(path, []byte(`{"version":1,"service":"web","installation_id":"fixture","values":{"DATABASE_URL":"managed-db"}}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = Load(); err == nil {
		t.Fatal("incomplete manifest accepted")
	}
}
