package managedsetup

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
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
	ambiguous := `{"services":{"app":{"environment":{"DATABASE_URL":"postgres://app:pw@db/news"}},"web":{"environment":{}},"executor":{"environment":{"DATABASE_URL":"postgres://domain:pw@db/news"}},"background":{"environment":{"WORKER_DATABASE_URL":"postgres://worker:pw@db/news"}},"extractor":{"environment":{"EXTRACTOR_SERVICE_TOKEN":"token"}}}}`
	if err := os.WriteFile(path, []byte(ambiguous), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadRenderedCompose(path); err == nil {
		t.Fatal("accepted ambiguous app/web aliases")
	}
}

func TestRenderedComposeReportsLoopbackPublishedPorts(t *testing.T) {
	for _, tt := range []struct {
		name string
		port string
		want bool
	}{
		{name: "loopback", port: `{"target":8000,"published":"8000","host_ip":"127.0.0.1","protocol":"tcp"}`, want: true},
		{name: "public bind", port: `{"target":8000,"published":"8000","host_ip":"0.0.0.0","protocol":"tcp"}`},
		{name: "host mode", port: `{"target":8000,"published":"8000","host_ip":"127.0.0.1","mode":"host","protocol":"tcp"}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "compose.json")
			raw := `{"services":{"app":{"environment":{"DATABASE_URL":"postgres://x:x@db/news"},"ports":[` + tt.port + `]},"executor":{"environment":{"DATABASE_URL":"postgres://x:x@db/news"}},"background":{"environment":{"WORKER_DATABASE_URL":"postgres://x:x@db/news"}},"extractor":{"environment":{"EXTRACTOR_SERVICE_TOKEN":"x"}}}}`
			if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
				t.Fatal(err)
			}
			compose, err := ReadRenderedCompose(path)
			if err != nil {
				t.Fatal(err)
			}
			if got := compose.HasLoopbackPublishedPorts("app"); got != tt.want {
				t.Fatalf("HasLoopbackPublishedPorts()=%t want %t", got, tt.want)
			}
		})
	}
}

func TestReadNativeBrowserOptionsUsesExistingIsolatedTopology(t *testing.T) {
	path := filepath.Join(t.TempDir(), "compose.json")
	config := `{"services":{"app":{"environment":{"DATABASE_URL":"postgres://web:pw@db/news"}},"executor":{"environment":{"DATABASE_URL":"postgres://domain:pw@db/news"}},"background":{"environment":{"WORKER_DATABASE_URL":"postgres://worker:pw@db/news"}},"extractor":{"init":true,"read_only":true,"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"],"pids_limit":128,"mem_limit":"256m","cpus":1,"stop_grace_period":"55s","environment":{"EXTRACTOR_SERVICE_TOKEN":"service-secret","LISTEN_ADDR":":8080","EXTRACTOR_MAX_CONCURRENT":"2","EXTRACTOR_BROWSER_CONCURRENT":"2","CHROME_ENDPOINT":"http://10.77.0.2:9222","EXTRACTOR_HTTP_ONLY":"false","EXTRACTOR_BROWSER_BACKEND":"native","EXTRACTOR_NATIVE_PROXY_URL":"http://10.77.0.5:8081","EXTRACTOR_NATIVE_PROXY_TOKEN":"fixture-proxy-token-01","EXTRACTOR_NATIVE_NETWORK_ISOLATED":"true"},"networks":{"private-control":{"ipv4_address":"10.77.0.4"},"news-outbound":{}}},"egress":{"init":true,"read_only":true,"cap_drop":["ALL"],"security_opt":["no-new-privileges:true"],"pids_limit":64,"mem_limit":"128m","cpus":0.25,"stop_grace_period":"10s","environment":{"EGRESS_LISTEN_ADDR":"10.77.0.5:8081","EGRESS_TOKEN":"fixture-proxy-token-01","EGRESS_MAX_CONCURRENT":"4"},"networks":{"private-control":{"ipv4_address":"10.77.0.5"},"news-outbound":{}}}},"networks":{"private-control":{"name":"shared-news-control","external":true},"news-outbound":{"name":"news-extractor-outbound"}}}`
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	options, err := ReadNativeBrowserOptions(path)
	if err != nil {
		t.Fatal(err)
	}
	if options.ExtractorValues["EXTRACTOR_BROWSER_CONCURRENT"] != "2" || options.ControlName != "shared-news-control" || options.OutboundName != "news-extractor-outbound" {
		t.Fatal("rendered native settings were not preserved")
	}
	if options.ExtractorLimits.PidsLimit != 128 || options.EgressLimits.PidsLimit != 64 || options.ExtractorLimits.CPUs != 1 || options.EgressLimits.CPUs != 0.25 {
		t.Fatal("native container hardening and resource limits were not imported")
	}
	override, err := NativeBrowserComposeOverride(options)
	if err != nil {
		t.Fatal(err)
	}
	text := string(override)
	if !strings.Contains(text, "extraction-call") || !strings.Contains(text, "external: true") || strings.Contains(text, "chrome:") || strings.Contains(text, "readeck:") || strings.Contains(text, "fixture-proxy-token-01") {
		t.Fatal("native Compose override changed shared services or exposed credentials")
	}
	if err = os.WriteFile(path, []byte(strings.Replace(config, `"EXTRACTOR_NATIVE_NETWORK_ISOLATED":"true"`, `"EXTRACTOR_NATIVE_NETWORK_ISOLATED":"false"`, 1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = ReadNativeBrowserOptions(path); err == nil {
		t.Fatal("accepted native mode without the isolation assertion")
	}
}

func TestNativeBrowserComposeOverrideRoundTripsWithoutDatabaseAccess(t *testing.T) {
	docker, err := exec.LookPath("docker")
	if err != nil {
		t.Skip("Docker Compose is required for the Compose merge regression")
	}
	options := NativeBrowserOptions{
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
	override, err := NativeBrowserComposeOverride(options)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	base := filepath.Join(dir, "compose.yml")
	additional := filepath.Join(dir, "native.yml")
	baseYAML := `services:
  postgres:
    image: postgres:17
  executor:
    image: fixture/executor
    environment:
      DATABASE_URL: postgres://role:private@postgres/news
  background:
    image: fixture/background
    environment:
      WORKER_DATABASE_URL: postgres://role:private@postgres/news
  extractor:
    image: fixture/extractor
  egress:
    image: fixture/egress
  chrome:
    image: fixture/chrome
  readeck:
    image: fixture/readeck
networks:
  control:
    external: true
    name: fixture-control
  outbound:
    external: true
    name: fixture-outbound
`
	if err = os.WriteFile(base, []byte(baseYAML), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(additional, override, 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(docker, "compose", "--project-name", "news-native-fixture", "-f", base, "-f", additional, "config", "--format", "json")
	cmd.Env = append(os.Environ(), "COMPOSE_DISABLE_ENV_FILE=1")
	raw, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("docker compose config failed: %v: %s", err, raw)
	}
	var rendered struct {
		Services map[string]struct {
			Networks  map[string]map[string]any `json:"networks"`
			PidsLimit int                       `json:"pids_limit"`
			MemLimit  string                    `json:"mem_limit"`
			CPUs      float64                   `json:"cpus"`
			ReadOnly  bool                      `json:"read_only"`
			Init      bool                      `json:"init"`
		} `json:"services"`
		Networks map[string]struct {
			Name     string `json:"name"`
			Internal bool   `json:"internal"`
			External bool   `json:"external"`
		} `json:"networks"`
	}
	if err = json.Unmarshal(raw, &rendered); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"chrome", "readeck"} {
		if _, ok := rendered.Services[name]; !ok {
			t.Fatalf("shared service %s disappeared", name)
		}
	}
	for _, service := range []string{"executor", "background"} {
		networks := rendered.Services[service].Networks
		if _, ok := networks["default"]; !ok {
			t.Errorf("%s lost the database/default network", service)
		}
		if _, ok := networks["extraction-call"]; !ok {
			t.Errorf("%s lacks extraction-call network", service)
		}
	}
	for _, service := range []string{"extractor", "egress"} {
		networks := rendered.Services[service].Networks
		if _, ok := networks["default"]; ok {
			t.Errorf("%s received database/default network", service)
		}
	}
	for _, service := range []string{"extractor", "egress"} {
		networks := rendered.Services[service].Networks
		if _, ok := networks["control"]; !ok {
			t.Errorf("%s lost control network", service)
		}
		if _, ok := networks["outbound"]; !ok {
			t.Errorf("%s lost outbound network", service)
		}
	}
	if extractor := rendered.Services["extractor"]; extractor.PidsLimit != 128 || extractor.MemLimit != "268435456" || extractor.CPUs != 1 || !extractor.Init || !extractor.ReadOnly {
		t.Fatal("extractor hardening/resource limits changed")
	}
	if egress := rendered.Services["egress"]; egress.PidsLimit != 64 || egress.MemLimit != "134217728" || egress.CPUs != 0.25 || !egress.Init || !egress.ReadOnly {
		t.Fatal("egress hardening/resource limits changed")
	}
	if rendered.Networks["extraction-call"].Internal != true || rendered.Networks["control"].Name != "fixture-control" || rendered.Networks["outbound"].Name != "fixture-outbound" {
		t.Fatal("network boundary or shared external network identity changed")
	}
}

func TestStageAndActivateAdoptionAreImmutable(t *testing.T) {
	root := t.TempDir()
	l := Layout{Setup: filepath.Join(root, "setup"), Web: filepath.Join(root, "web"), Domain: filepath.Join(root, "domain"), Background: filepath.Join(root, "background"), Extractor: filepath.Join(root, "extractor"), Egress: filepath.Join(root, "egress"), WebUID: -1, DomainUID: -1, BackgroundUID: -1, ExtractorUID: -1, EgressUID: -1}
	checkRuntimeOwner := os.Geteuid() == 0
	if checkRuntimeOwner {
		l.WebUID = 65534
	}
	for _, dir := range []string{l.Setup, l.Web, l.Domain, l.Background, l.Extractor, l.Egress} {
		if err := os.MkdirAll(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(l.Setup, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(l.Web, 0755); err != nil {
		t.Fatal(err)
	}
	roles := map[string]map[string]string{
		"web":        {"DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "JWT_SECRET": "signing", "WEB_BACKEND_URL": "http://domain", "WEB_BACKEND_TOKEN": "a", "SCHEDULED_JOBS_TOKEN": "b", "SCHEDULER_API_URL": "http://worker", "SCHEDULER_API_TOKEN": "c"},
		"domain":     {"DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "JOB_EXECUTOR_TOKEN": "a", "ARTICLE_AI_TOKEN": "b", "EXTRACTION_SERVICE_URL": "http://extract", "EXTRACTION_SERVICE_TOKEN": "d", "WEB_BACKEND_TOKEN": "e", "NEWS_CREDENTIAL_KEY": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA", "NEWS_CREDENTIAL_KEY_ID": "id"},
		"background": {"WORKER_DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "EXECUTION_DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "INGESTION_DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "PROCESSING_DATABASE_URL": "postgres://role:pw@127.0.0.1/news", "SCHEDULER_DATABASE_URL": "postgres://role:pw@127.0.0.1/scheduler", "JOB_EXECUTOR_TOKEN": "a", "ARTICLE_AI_TOKEN": "b", "ARTICLE_INGESTION_TOKEN": "i", "ARTICLE_PROCESSING_TOKEN": "p", "SCHEDULER_API_TOKEN": "c", "SCHEDULER_TARGET_NEWS_WORKER_TOKEN": "s", "EXTRACTION_SERVICE_TOKEN": "d"},
		"extractor":  {"EXTRACTOR_SERVICE_TOKEN": "d"},
		"egress":     {"EGRESS_TOKEN": "proxy", "EGRESS_LISTEN_ADDR": "10.0.0.5:8081"},
	}
	policies := map[string]struct{ Allowed, Required []string }{
		"web":        {nil, []string{"DATABASE_URL", "JWT_SECRET", "WEB_BACKEND_URL", "WEB_BACKEND_TOKEN", "SCHEDULED_JOBS_TOKEN", "SCHEDULER_API_URL", "SCHEDULER_API_TOKEN"}},
		"domain":     {nil, []string{"DATABASE_URL", "JOB_EXECUTOR_TOKEN", "ARTICLE_AI_TOKEN", "EXTRACTION_SERVICE_URL", "EXTRACTION_SERVICE_TOKEN", "WEB_BACKEND_TOKEN", "NEWS_CREDENTIAL_KEY", "NEWS_CREDENTIAL_KEY_ID"}},
		"background": {nil, []string{"WORKER_DATABASE_URL", "EXECUTION_DATABASE_URL", "INGESTION_DATABASE_URL", "PROCESSING_DATABASE_URL", "SCHEDULER_DATABASE_URL", "JOB_EXECUTOR_TOKEN", "ARTICLE_AI_TOKEN", "ARTICLE_INGESTION_TOKEN", "ARTICLE_PROCESSING_TOKEN", "SCHEDULER_API_TOKEN", "SCHEDULER_TARGET_NEWS_WORKER_TOKEN", "EXTRACTION_SERVICE_TOKEN"}},
		"extractor":  {nil, []string{"EXTRACTOR_SERVICE_TOKEN"}},
		"egress":     {nil, []string{"EGRESS_TOKEN"}},
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
	override, err := AdoptionComposeOverride(map[string]string{"web": "app", "domain": "executor", "background": "background", "extractor": "extractor", "egress": "egress", "migrate": "migrate"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(override), "egress:\n    env_file: !override []") || !strings.Contains(string(override), "profiles: !override []") {
		t.Fatal("native adoption does not ensure the managed egress service starts")
	}
	dir, bundle, err := StageAdoption(l.Setup, strings.Repeat("1", 32), digest, roles, policies, map[string][]byte{"import.json": []byte(`{"admin_hash":"phc"}`), "compose-override.yml": override})
	if err != nil {
		t.Fatal(err)
	}
	if info, statErr := os.Stat(l.Setup); statErr != nil || info.Mode().Perm() != 0700 {
		t.Fatal("empty setup volume was not made private before staging")
	}
	if bundle.Revision != 1 {
		t.Fatalf("revision=%d", bundle.Revision)
	}
	unexpected := filepath.Join(l.Web, "keep.txt")
	if err = os.WriteFile(unexpected, []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = ActivateAdoption(l, dir, bundle.InstallationID, filepath.Join(root, "override.yml"), policies); err == nil {
		t.Fatal("activation accepted unexpected runtime volume content")
	}
	if info, statErr := os.Stat(l.Web); statErr != nil || info.Mode().Perm() != 0755 {
		t.Fatal("failed preflight changed runtime volume permissions")
	}
	if err = os.Remove(unexpected); err != nil {
		t.Fatal(err)
	}
	if err = ActivateAdoption(l, dir, bundle.InstallationID, filepath.Join(root, "override.yml"), policies); err != nil {
		t.Fatal(err)
	}
	if info, statErr := os.Stat(l.Web); statErr != nil || info.Mode().Perm() != 0700 {
		t.Fatal("adopted runtime volume was not made private")
	}
	if checkRuntimeOwner {
		for _, path := range []string{l.Web, filepath.Join(l.Web, "runtime.json")} {
			info, statErr := os.Stat(path)
			if statErr != nil {
				t.Fatalf("stat adopted path %s: %v", path, statErr)
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || int(stat.Uid) != l.WebUID {
				t.Fatalf("runtime UID does not own adopted path %s", path)
			}
		}
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
	unsafeSetup := filepath.Join(root, "unsafe-setup")
	if err = os.Mkdir(unsafeSetup, 0755); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(unsafeSetup, "keep.txt")
	if err = os.WriteFile(keep, []byte("preserve"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, err = StageAdoption(unsafeSetup, strings.Repeat("2", 32), digest, roles, policies, nil); err == nil {
		t.Fatal("staging accepted a nonempty public setup volume")
	}
	if info, statErr := os.Stat(unsafeSetup); statErr != nil || info.Mode().Perm() != 0755 {
		t.Fatal("refused setup volume had its permissions changed")
	}
	if _, statErr := os.Stat(keep); statErr != nil {
		t.Fatal("refused setup volume contents were changed")
	}
}
