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

func TestNativeExternalStateVerifiesAndPreparesIdempotently(t *testing.T) {
	root := t.TempDir()
	l := Layout{
		Setup: filepath.Join(root, "setup"), Web: filepath.Join(root, "web"), Domain: filepath.Join(root, "domain"),
		Background: filepath.Join(root, "background"), Extractor: filepath.Join(root, "extractor"), Egress: filepath.Join(root, "egress"),
		Postgres: filepath.Join(root, "pg"), PostgresSecrets: filepath.Join(root, "pg-secret"),
		WebUID: -1, DomainUID: -1, BackgroundUID: -1, ExtractorUID: -1, EgressUID: -1,
	}
	for _, dir := range []string{l.Setup, l.Web, l.Domain, l.Background, l.Extractor, l.Egress, l.Postgres} {
		if err := os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	dsns := map[string]string{
		"web": "postgres://web:p@db/news?search_path=news", "domain": "postgres://domain:p@db/news?search_path=news",
		"worker": "postgres://worker:p@db/news?search_path=news", "execution": "postgres://execution:p@db/news?search_path=news",
		"ingestion": "postgres://ingestion:p@db/news?search_path=news", "processing": "postgres://processing:p@db/news?search_path=news",
		"scheduler": "postgres://scheduler:p@db/scheduler?search_path=scheduler_service",
	}
	native := NativeBrowserOptions{
		ExtractorValues: map[string]string{
			"LISTEN_ADDR": ":8080", "EXTRACTOR_HTTP_ONLY": "false", "EXTRACTOR_BROWSER_BACKEND": "native",
			"EXTRACTOR_NATIVE_NETWORK_ISOLATED": "true", "CHROME_ENDPOINT": "http://10.77.0.2:9222",
			"EXTRACTOR_NATIVE_PROXY_URL": "http://10.77.0.5:8081", "EXTRACTOR_NATIVE_PROXY_TOKEN": "fixture-proxy-token-01",
			"EXTRACTOR_MAX_CONCURRENT": "2", "EXTRACTOR_BROWSER_CONCURRENT": "2",
		},
		EgressValues:    map[string]string{"EGRESS_LISTEN_ADDR": "10.77.0.5:8081", "EGRESS_TOKEN": "fixture-proxy-token-01", "EGRESS_MAX_CONCURRENT": "4"},
		ExtractorLimits: ContainerLimits{Init: true, ReadOnly: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges:true"}, PidsLimit: 128, MemLimit: "256m", CPUs: 1, StopGracePeriod: "55s"},
		EgressLimits:    ContainerLimits{Init: true, ReadOnly: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges:true"}, PidsLimit: 64, MemLimit: "128m", CPUs: 0.25, StopGracePeriod: "10s"},
		ControlNetwork:  "control", ControlName: "fixture-control", OutboundNetwork: "outbound", OutboundName: "fixture-outbound",
		ExtractorIP: "10.77.0.4", EgressIP: "10.77.0.5",
	}
	options := Options{NativeBrowser: &native}
	state, manifests, err := NewExternalState("http://localhost:8000", options, dsns)
	if err != nil {
		t.Fatal(err)
	}
	if len(state.RuntimeValues) != 5 || len(manifests) != 5 {
		t.Fatal("native external state did not include the egress role")
	}
	if err = InstallExternalState(l, state, manifests, "postgres://bootstrap:p@db/news?search_path=news"); err != nil {
		t.Fatal(err)
	}
	if err = VerifyExternalState(l, state); err != nil {
		t.Fatal(err)
	}
	if _, err = PrepareWithOptions(l, "http://localhost:8000", options); err != nil {
		t.Fatalf("native external prepare did not verify idempotently: %v", err)
	}
}
