package managedsetup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
)

func TestNewExternalStateOnlyReplacesDatabaseEndpoints(t *testing.T) {
	roles := map[string]string{
		"web":        "postgres://web:one@db:5432/news?search_path=news",
		"domain":     "postgres://domain:two@db:5432/news?search_path=news",
		"worker":     "postgres://worker:three@db:5432/news?search_path=news",
		"execution":  "postgres://execution:four@db:5432/news?search_path=news",
		"ingestion":  "postgres://ingestion:five@db:5432/news?search_path=news",
		"processing": "postgres://processing:six@db:5432/news?search_path=news",
		"scheduler":  "postgres://scheduler:seven@db:5432/scheduler?search_path=scheduler_service",
	}
	s, files, err := NewExternalState("http://localhost:8000", Options{}, roles)
	if err != nil {
		t.Fatal(err)
	}
	if !s.ExternalDB || s.InstallationID == "" {
		t.Fatal("external identity not created")
	}
	if len(files) != 4 {
		t.Fatalf("got %d runtime files", len(files))
	}
	values := map[string]map[string]string{}
	for role, raw := range files {
		var m runtimeconfig.Manifest
		if err := json.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		if m.Service != role || m.InstallationID != s.InstallationID {
			t.Fatalf("invalid %s manifest identity", role)
		}
		values[role] = m.Values
	}
	checks := [][3]string{{"web", "DATABASE_URL", "web"}, {"domain", "DATABASE_URL", "domain"}, {"background", "WORKER_DATABASE_URL", "worker"}, {"background", "EXECUTION_DATABASE_URL", "execution"}, {"background", "INGESTION_DATABASE_URL", "ingestion"}, {"background", "PROCESSING_DATABASE_URL", "processing"}, {"background", "SCHEDULER_DATABASE_URL", "scheduler"}}
	for _, c := range checks {
		if values[c[0]][c[1]] != roles[c[2]] {
			t.Fatalf("%s/%s endpoint was not preserved", c[0], c[1])
		}
	}
	if values["web"]["JWT_SECRET"] == "" || values["domain"]["NEWS_CREDENTIAL_KEY"] == "" || values["background"]["SCHEDULER_API_TOKEN"] == "" {
		t.Fatal("generated purpose secrets missing")
	}
}

func TestNewExternalStateRejectsPartialRoleBundle(t *testing.T) {
	if _, _, err := NewExternalState("http://localhost:8000", Options{}, map[string]string{"web": "postgres://example"}); err == nil {
		t.Fatal("partial external role bundle accepted")
	}
}

func TestInstallAndVerifyExternalState(t *testing.T) {
	root := t.TempDir()
	l := Layout{Setup: filepath.Join(root, "setup"), Web: filepath.Join(root, "web"), Domain: filepath.Join(root, "domain"), Background: filepath.Join(root, "background"), Extractor: filepath.Join(root, "extractor"), WebUID: -1, DomainUID: -1, BackgroundUID: -1, ExtractorUID: -1}
	for _, d := range []string{l.Setup, l.Web, l.Domain, l.Background, l.Extractor} {
		if err := os.Mkdir(d, 0700); err != nil {
			t.Fatal(err)
		}
	}
	dsns := map[string]string{"web": "postgres://web:p@db/news?search_path=news", "domain": "postgres://domain:p@db/news?search_path=news", "worker": "postgres://worker:p@db/news?search_path=news", "execution": "postgres://execution:p@db/news?search_path=news", "ingestion": "postgres://ingestion:p@db/news?search_path=news", "processing": "postgres://processing:p@db/news?search_path=news", "scheduler": "postgres://scheduler:p@db/scheduler?search_path=scheduler_service"}
	s, files, err := NewExternalState("http://localhost:8000", Options{}, dsns)
	if err != nil {
		t.Fatal(err)
	}
	if err = InstallExternalState(l, s, files, "postgres://installer:p@db/news?search_path=news"); err != nil {
		t.Fatal(err)
	}
	if err = VerifyExternalState(l, s); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(l.Web, "runtime.json"), []byte("{}"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = VerifyExternalState(l, s); err == nil {
		t.Fatal("changed runtime manifest verified")
	}
}
