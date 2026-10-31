package ciimpact

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestHistoricalChanges(t *testing.T) {
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		base, head    string
		mods, targets []string
	}{
		{"ce57a97", "e99fe76", []string{"go-background", "go-domain", "go-execution"}, []string{"background"}},
		{"e99fe76", "2ca1726", []string{"go-domain"}, []string{"executor"}},
	} {
		t.Run(tc.head, func(t *testing.T) {
			p, e := PlanChanges(root, tc.base, tc.head)
			if e != nil {
				t.Fatal(e)
			}
			if !reflect.DeepEqual(p.Modules, tc.mods) || !reflect.DeepEqual(p.Targets, tc.targets) || p.FallbackFull || len(p.FixtureTargets) != 0 {
				t.Fatalf("unexpected plan: modules=%v targets=%v fixtures=%v fallback=%v", p.Modules, p.Targets, p.FixtureTargets, p.FallbackFull)
			}
		})
	}
}
func TestGraphAndEmbeddedInputs(t *testing.T) {
	s := &snapshot{files: map[string][]byte{"go-domain/migration/embed.go": []byte("//go:embed sql/*"), "go-domain/migration/sql/data_test.go": []byte("embedded"), "go-domain/migration/ordinary_test.go": []byte("test")}, imports: map[string][]string{"go-domain/cmd/news-domain": {"go-domain/migration"}}, testImports: map[string][]string{"go-web/test": {"go-domain/migration"}}, packages: map[string]bool{"go-domain/cmd/news-domain": true, "go-domain/migration": true, "go-web/test": true}}
	if !reverse(s, "go-domain/migration")["go-web/test"] {
		t.Fatal("test import consumer was omitted")
	}
	pkgs := closure(s, "go-domain/cmd/news-domain")
	if !included(s, "executor", "go-domain/migration/sql/data_test.go", pkgs) {
		t.Fatal("embedded test-named input omitted")
	}
	_, sets := fingerprints(s)
	if !sets["executor"]["go-domain/migration/sql/data_test.go"] {
		t.Fatal("embedded input missing in fingerprint")
	}
}
func TestInvalidBaseFailsClosed(t *testing.T) {
	root, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = PlanChanges(root, "not-a-commit", "HEAD"); e == nil {
		t.Fatal("invalid base accepted")
	}
}
func TestRecipeFingerprintChanges(t *testing.T) {
	s := &snapshot{files: map[string][]byte{"docker/Dockerfile": []byte("one")}, imports: map[string][]string{}, testImports: map[string][]string{}, packages: map[string]bool{}}
	before, _ := fingerprints(s)
	s.files["docker/Dockerfile"] = []byte("two")
	after, _ := fingerprints(s)
	for _, target := range Targets {
		if before[target] == after[target] {
			t.Errorf("recipe omitted from %s fingerprint", target)
		}
	}
	if before["extractor"] != after["extractor"] {
		t.Error("unrelated recipe changed extractor")
	}
}
func TestMain(m *testing.M) { os.Exit(m.Run()) }

func TestFixtureRepository(t *testing.T) {
	dir := t.TempDir()
	gitRun := func(args ...string) string {
		t.Helper()
		c := exec.Command("git", append([]string{"-C", dir}, args...)...)
		b, e := c.CombinedOutput()
		if e != nil {
			t.Fatalf("git %v: %v %s", args, e, b)
		}
		return strings.TrimSpace(string(b))
	}
	gitRun("init", "-q")
	gitRun("config", "user.email", "test@example.invalid")
	gitRun("config", "user.name", "CI test")
	put := func(name, content string) {
		t.Helper()
		p := filepath.Join(dir, name)
		if e := os.MkdirAll(filepath.Dir(p), 0755); e != nil {
			t.Fatal(e)
		}
		if e := os.WriteFile(p, []byte(content), 0644); e != nil {
			t.Fatal(e)
		}
	}
	for _, f := range []string{"docker/Dockerfile", "docker/Dockerfile.extractor", "docker/Dockerfile.browser"} {
		b, e := os.ReadFile(filepath.Join("../..", f))
		if e != nil {
			t.Fatal(e)
		}
		put(f, string(b))
	}
	put("go-domain/cmd/news-domain/main.go", "package main\nimport _ \"github.com/Dzarlax-AI/evening-news/go-domain/foo\"\nfunc main() {}\n")
	put("go-domain/foo/foo.go", "package foo\n")
	put("go-domain/foo/foo_test.go", "package foo\n")
	put("docs/guide.md", "one")
	gitRun("add", ".")
	gitRun("commit", "-qm", "base")
	base := gitRun("rev-parse", "HEAD")
	check := func(name string, wantMods, wantTargets []string, full bool) {
		t.Helper()
		gitRun("add", "-A")
		gitRun("commit", "-qm", name)
		head := gitRun("rev-parse", "HEAD")
		p, e := PlanChanges(dir, base, head)
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(p.Modules, wantMods) || !reflect.DeepEqual(p.Targets, wantTargets) || p.FallbackFull != full {
			t.Fatalf("%s: modules=%v targets=%v fallback=%v", name, p.Modules, p.Targets, p.FallbackFull)
		}
		base = head
	}
	put("docs/guide.md", "two")
	check("docs", []string{}, []string{}, false)
	put("go-domain/foo/foo_test.go", "package foo\n// changed\n")
	check("test", []string{"go-domain"}, []string{}, false)
	put("tests/fixtures/operational-snapshot-v1/job.json", "{}")
	check("fixture", []string{"go-background", "go-domain"}, []string{}, false)
	gitRun("mv", "go-domain/foo/foo.go", "go-domain/foo/renamed.go")
	check("rename", []string{"go-domain"}, []string{"executor"}, false)
	put("go-editorial/testdata/generate-request.json", "{}")
	check("cross-module fixture", []string{"go-domain", "go-editorial"}, []string{}, false)
	put("go/.dockerignore", "secret")
	gitRun("add", "-A")
	gitRun("commit", "-qm", "extractor ignore")
	head := gitRun("rev-parse", "HEAD")
	p, e := PlanChanges(dir, base, head)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(p.FixtureTargets, []string{"extractor"}) || p.FallbackFull {
		t.Fatalf("extractor ignore plan: %+v", p)
	}
	base = head
	oldDocker, e := os.ReadFile(filepath.Join(dir, "docker/Dockerfile"))
	if e != nil {
		t.Fatal(e)
	}
	newDocker := append(append([]byte{}, oldDocker...), []byte("\n# reviewed update\n")...)
	put("docker/Dockerfile", string(newDocker))
	gitRun("add", "-A")
	gitRun("commit", "-qm", "recipe update")
	recipeHead := gitRun("rev-parse", "HEAD")
	if _, e := PlanChanges(dir, base, recipeHead); e == nil {
		t.Fatal("unreviewed recipe accepted")
	}
	oldHash := sha256.Sum256(oldDocker)
	newHash := sha256.Sum256(newDocker)
	recipes := map[string][]string{}
	for f, hashes := range reviewedRecipes {
		recipes[f] = append([]string{}, hashes...)
	}
	recipes["docker/Dockerfile"] = []string{hex.EncodeToString(oldHash[:]), hex.EncodeToString(newHash[:])}
	recipePlan, e := planChanges(dir, base, recipeHead, recipes)
	if e != nil || !reflect.DeepEqual(recipePlan.Targets, Targets) {
		t.Fatalf("reviewed recipe upgrade: targets=%v error=%v", recipePlan.Targets, e)
	}
	base = recipeHead
	put("docker/Dockerfile", string(oldDocker))
	gitRun("add", "-A")
	gitRun("commit", "-qm", "restore recipe")
	base = gitRun("rev-parse", "HEAD")
	put("scripts/test_ci_core.py", "# tests")
	gitRun("add", "-A")
	gitRun("commit", "-qm", "control tests")
	controlHead := gitRun("rev-parse", "HEAD")
	controlPlan, e := PlanChanges(dir, base, controlHead)
	if e != nil {
		t.Fatal(e)
	}
	if !controlPlan.IntegrationRequired || controlPlan.FallbackFull || len(controlPlan.Targets) != 0 || len(controlPlan.FixtureTargets) != 0 || !reflect.DeepEqual(controlPlan.Modules, []string{"go-tools"}) {
		t.Fatalf("control test plan: %+v", controlPlan)
	}
	base = controlHead
	put("scripts/test_ci_impact.py", "# selector tests")
	gitRun("add", "-A")
	gitRun("commit", "-qm", "selector tests")
	selectorHead := gitRun("rev-parse", "HEAD")
	selectorPlan, e := PlanChanges(dir, base, selectorHead)
	if e != nil {
		t.Fatal(e)
	}
	if !selectorPlan.IntegrationRequired || selectorPlan.FallbackFull || len(selectorPlan.Targets) != 0 || len(selectorPlan.FixtureTargets) != 0 || !reflect.DeepEqual(selectorPlan.Modules, []string{"go-tools"}) {
		t.Fatalf("selector control test plan: %+v", selectorPlan)
	}
	base = selectorHead
	put("scripts/ci-core.py", "# build invocation")
	gitRun("add", "-A")
	gitRun("commit", "-qm", "core build invocation")
	buildHead := gitRun("rev-parse", "HEAD")
	buildPlan, e := PlanChanges(dir, base, buildHead)
	if e != nil {
		t.Fatal(e)
	}
	if !buildPlan.IntegrationRequired || buildPlan.FallbackFull || !reflect.DeepEqual(buildPlan.Targets, Targets) || !reflect.DeepEqual(buildPlan.FixtureTargets, []string{"extractor"}) {
		t.Fatalf("build invocation plan: %+v", buildPlan)
	}
	for _, target := range append(append([]string{}, Targets...), "extractor") {
		if buildPlan.SourceFingerprints[target] == controlPlan.SourceFingerprints[target] {
			t.Errorf("build invocation did not affect %s fingerprint", target)
		}
	}
	base = buildHead
	put(".github/workflows/shared-browser.yml", "# browser build invocation")
	gitRun("add", "-A")
	gitRun("commit", "-qm", "browser build invocation")
	browserHead := gitRun("rev-parse", "HEAD")
	browserPlan, e := PlanChanges(dir, base, browserHead)
	if e != nil {
		t.Fatal(e)
	}
	if !browserPlan.ImageChangedBrowser || !browserPlan.Checks["browser"] || browserPlan.FallbackFull || len(browserPlan.Targets) != 0 {
		t.Fatalf("browser workflow plan: %+v", browserPlan)
	}
	base = browserHead
	put(".github/workflows/extractor-service.yml", "# extractor build invocation")
	gitRun("add", "-A")
	gitRun("commit", "-qm", "extractor build invocation")
	extractorHead := gitRun("rev-parse", "HEAD")
	extractorPlan, e := PlanChanges(dir, base, extractorHead)
	if e != nil {
		t.Fatal(e)
	}
	if extractorPlan.FallbackFull || !reflect.DeepEqual(extractorPlan.FixtureTargets, []string{"extractor"}) || len(extractorPlan.Targets) != 0 || extractorPlan.SourceFingerprints["extractor"] == browserPlan.SourceFingerprints["extractor"] {
		t.Fatalf("extractor workflow plan: %+v", extractorPlan)
	}
	base = extractorHead
	put("go-tools/configcatalog/catalog.json", "[]")
	gitRun("add", "-A")
	gitRun("commit", "-qm", "CI configuration catalog")
	catalogHead := gitRun("rev-parse", "HEAD")
	catalogPlan, e := PlanChanges(dir, base, catalogHead)
	if e != nil {
		t.Fatal(e)
	}
	if !catalogPlan.IntegrationRequired || catalogPlan.FallbackFull || !reflect.DeepEqual(catalogPlan.Modules, []string{"go-tools"}) || len(catalogPlan.Targets) != 0 || len(catalogPlan.FixtureTargets) != 0 {
		t.Fatalf("config catalog plan: %+v", catalogPlan)
	}
	base = catalogHead
	put("tests/fixtures/example/go.mod", "module example.test/fixture")
	gitRun("add", "-A")
	gitRun("commit", "-qm", "fixture module")
	fixtureHead := gitRun("rev-parse", "HEAD")
	if _, e := PlanChanges(dir, base, fixtureHead); e != nil {
		t.Fatalf("fixture go.mod must be allowed: %v", e)
	}
	base = fixtureHead
	put("go-new/go.mod", "module example.test/new")
	gitRun("add", "-A")
	gitRun("commit", "-qm", "unknown module")
	moduleHead := gitRun("rev-parse", "HEAD")
	if _, e := PlanChanges(dir, base, moduleHead); e == nil || !strings.Contains(e.Error(), "unsupported Go module") {
		t.Fatalf("unknown module must fail closed: %v", e)
	}
	gitRun("rm", "-q", "go-new/go.mod")
	gitRun("commit", "-qm", "remove unknown module")
	base = gitRun("rev-parse", "HEAD")
	put("unclassified.new", "unknown")
	check("unknown", Modules, Targets, true)
}

func TestDedicatedWorkflowRoutes(t *testing.T) {
	cases := []struct{ file, check string }{
		{"docs/extraction-golden-cases.json", "prototype"}, {"docs/extraction-review-decisions.json", "prototype"},
		{"go-tools/extractorcompare/compare.go", "extractor"}, {"go-tools/browsercontract/check.go", "browser"},
		{"go-tools/go.mod", "browser"}, {"tests/fixtures/go-ingestion/a.json", "web_api"},
		{"go-worker/worker.go", "ingestion"}, {"tests/fixtures/a.json", "processing"},
		{"scripts/shadow_cdp_relay.py", "browser"}, {"scripts/shadow_cdp_relay.py", "extractor"},
		{"web/static/app.css", "scheduler"},
	}
	for _, tc := range cases {
		if !matchesAny(tc.file, workflowInputs[tc.check]) {
			t.Errorf("%s does not enable %s", tc.file, tc.check)
		}
	}
}
func TestSharedInputOwners(t *testing.T) {
	for _, tc := range []struct{ file, owner string }{
		{"go-editorial/testdata/generate-request.json", "go-domain"}, {"go-editorial/testdata/generate-response.json", "go-domain"},
		{"go-scheduler/migrations/001_initial.sql", "go-domain"}, {"go-tools/managedsetup/grants.sql", "go-domain"},
		{"go/runtimefile/lookup.go", "go-tools"}, {"docs/managed-settings-recovery.md", "go-domain"},
	} {
		found := false
		for pattern, owners := range sharedInputs {
			if matchesAny(tc.file, []string{pattern}) {
				for _, owner := range owners {
					if owner == tc.owner {
						found = true
					}
				}
			}
		}
		if !found {
			t.Errorf("missing %s owner for %s", tc.owner, tc.file)
		}
	}
}
func TestExtractorDockerignoreFingerprint(t *testing.T) {
	s := &snapshot{files: map[string][]byte{"go/.dockerignore": []byte("one")}, imports: map[string][]string{}, testImports: map[string][]string{}, packages: map[string]bool{}}
	before, sets := fingerprints(s)
	if !sets["extractor"]["go/.dockerignore"] {
		t.Fatal("extractor context .dockerignore missing")
	}
	s.files["go/.dockerignore"] = []byte("two")
	after, _ := fingerprints(s)
	if before["extractor"] == after["extractor"] {
		t.Fatal("extractor context change did not change fingerprint")
	}
	for _, target := range Targets {
		if before[target] != after[target] {
			t.Errorf("extractor context changed %s", target)
		}
	}
}
func TestRecipeRegistrySupportsBaselineAcrossUpgrade(t *testing.T) {
	old, newRecipe := []byte("old reviewed Dockerfile"), []byte("new reviewed Dockerfile")
	hash := func(b []byte) string { h := sha256.Sum256(b); return hex.EncodeToString(h[:]) }
	known := []string{hash(old), hash(newRecipe)}
	if !recipeReviewed(old, known) || !recipeReviewed(newRecipe, known) {
		t.Fatal("both baseline and updated recipes must be accepted")
	}
	if recipeReviewed([]byte("unreviewed"), known) || recipeReviewed(nil, known) {
		t.Fatal("unreviewed recipe accepted")
	}
}

func TestBrowserImageInputs(t *testing.T) {
	for _, tc := range []struct {
		file string
		want bool
	}{
		{"go-tools/browsercontract/check.go", false},
		{"deploy/news-extractor/compose.native.yml", false},
		{"deploy/news-extractor/browser/entrypoint.py", true},
		{"scripts/shadow_x11_client.sh", true},
		{"docker/Dockerfile.browser", true},
		{".dockerignore", true},
	} {
		if got := browserImageInput(tc.file); got != tc.want {
			t.Errorf("%s: got %t, want %t", tc.file, got, tc.want)
		}
	}
}
func TestEnvironmentCatalogDetection(t *testing.T) {
	if !hasEnvironmentCandidate([]byte("package p\nvar x = getenv(\"EXAMPLE_KEY\")")) {
		t.Fatal("environment candidate omitted")
	}
	if hasEnvironmentCandidate([]byte("package p\nvar x = \"normal text\"")) {
		t.Fatal("unexpected environment candidate")
	}
}

func TestUnsupportedGitObjectsFailClosed(t *testing.T) {
	dir := t.TempDir()
	run := func(args ...string) string {
		t.Helper()
		c := exec.Command("git", append([]string{"-C", dir}, args...)...)
		b, e := c.CombinedOutput()
		if e != nil {
			t.Fatalf("git %v: %v %s", args, e, b)
		}
		return strings.TrimSpace(string(b))
	}
	run("init", "-q")
	run("config", "user.email", "test@example.invalid")
	run("config", "user.name", "CI test")
	if e := os.WriteFile(filepath.Join(dir, "README.md"), []byte("base"), 0644); e != nil {
		t.Fatal(e)
	}
	run("add", ".")
	run("commit", "-qm", "base")
	base := run("rev-parse", "HEAD")
	for _, p := range []string{"web/static", "go-web/templates"} {
		if e := os.MkdirAll(filepath.Join(dir, p), 0755); e != nil {
			t.Fatal(e)
		}
	}
	link := filepath.Join(dir, "web/static/link.css")
	if e := os.Symlink("first.css", link); e != nil {
		t.Fatal(e)
	}
	run("add", "-A")
	run("commit", "-qm", "add symlink")
	added := run("rev-parse", "HEAD")
	if e := os.Remove(link); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink("second.css", link); e != nil {
		t.Fatal(e)
	}
	run("add", "-A")
	run("commit", "-qm", "retarget symlink")
	retarget := run("rev-parse", "HEAD")
	if e := os.Remove(link); e != nil {
		t.Fatal(e)
	}
	run("add", "-A")
	run("commit", "-qm", "delete symlink")
	deleted := run("rev-parse", "HEAD")
	for _, pair := range [][2]string{{base, added}, {added, retarget}, {retarget, deleted}} {
		if _, e := PlanChanges(dir, pair[0], pair[1]); e == nil || !strings.Contains(e.Error(), "symlinks and submodules") {
			t.Errorf("symlink transition unexpectedly accepted: %v", e)
		}
	}
	goLink := filepath.Join(dir, "go-web/templates/link.go")
	if e := os.Symlink("outside.go", goLink); e != nil {
		t.Fatal(e)
	}
	run("add", "-A")
	run("commit", "-qm", "go symlink")
	goHead := run("rev-parse", "HEAD")
	if _, e := PlanChanges(dir, deleted, goHead); e == nil || !strings.Contains(e.Error(), "symlinks and submodules") {
		t.Errorf("Go symlink unexpectedly accepted: %v", e)
	}
	run("rm", "-q", "go-web/templates/link.go")
	run("commit", "-qm", "remove go symlink")
	clean := run("rev-parse", "HEAD")
	run("update-index", "--add", "--cacheinfo", "160000,"+base+",vendor/submodule")
	run("commit", "-qm", "add gitlink")
	gitlink := run("rev-parse", "HEAD")
	if _, e := PlanChanges(dir, clean, gitlink); e == nil || !strings.Contains(e.Error(), "symlinks and submodules") {
		t.Errorf("gitlink unexpectedly accepted: %v", e)
	}
}
