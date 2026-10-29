package managedsetup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRestoreRequiresEmptyTargetsAndPreservesKeys(t *testing.T) {
	source := testLayout(t)
	if _, err := Prepare(source, "http://localhost:8000"); err != nil {
		t.Fatal(err)
	}
	stage := t.TempDir()
	for name, path := range map[string]string{"installation.json": filepath.Join(source.Setup, "installation.json"), "web.json": filepath.Join(source.Web, "runtime.json"), "domain.json": filepath.Join(source.Domain, "runtime.json"), "background.json": filepath.Join(source.Background, "runtime.json"), "extractor.json": filepath.Join(source.Extractor, "runtime.json"), "postgres-password": filepath.Join(source.PostgresSecrets, "postgres-password")} {
		b, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		if e = os.WriteFile(filepath.Join(stage, name), b, 0600); e != nil {
			t.Fatal(e)
		}
	}
	for _, name := range []string{"news.sql", "scheduler.sql"} {
		if err := os.WriteFile(filepath.Join(stage, name), []byte("-- synthetic restore fixture\n"), 0600); err != nil {
			t.Fatal(err)
		}
	}
	target := testLayout(t)
	if err := RestoreState(target, stage); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(source.Domain, "runtime.json"))
	if err != nil {
		t.Fatal(err)
	}
	restored, err := os.ReadFile(filepath.Join(target.Domain, "runtime.json"))
	if err != nil || string(original) != string(restored) {
		t.Fatal("key state changed during restore")
	}
	if _, err := Prepare(target, "http://localhost:8000"); err != nil {
		t.Fatal(err)
	}
	if err := RestoreState(target, stage); err == nil {
		t.Fatal("restore overwrote nonempty state")
	}
	if err := os.WriteFile(filepath.Join(stage, "web.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	empty := testLayout(t)
	if err := RestoreState(empty, stage); err == nil {
		t.Fatal("invalid role state accepted")
	}
	if _, err := os.Stat(empty.Setup); !os.IsNotExist(err) {
		t.Fatal("target mutated before validation")
	}
}
