// Package ciimpact computes a conservative change and image input plan from Git commits.
package ciimpact

import (
	"archive/tar"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io"
	"os/exec"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

var Modules = []string{"go", "go-background", "go-domain", "go-editorial", "go-execution", "go-ingestion", "go-processing", "go-scheduler", "go-tools", "go-web", "go-worker"}
var Targets = []string{"background", "executor", "web"}
var CheckNames = []string{"prototype", "web_api", "scheduler", "ingestion", "processing", "extractor", "browser", "recovery"}

const prefix = "github.com/Dzarlax-AI/evening-news/"

// Exact paths and directory prefixes mirror the dedicated workflow triggers.
var workflowInputs = map[string][]string{
	"prototype":  {"go/", "go-tools/replaycheck/", "go-tools/cmd/compare-extraction-replays/", "docs/extraction-review-decisions.json", "docs/extraction-golden-cases.json", "tests/fixtures/go-parity/", ".github/workflows/go-prototype.yml"},
	"web_api":    {"go-web/", "go-domain/", "go-tools/", "web/", "tests/fixtures/web-contract-v1/", "tests/fixtures/go-web/", "tests/fixtures/go-ingestion/", "tests/fixtures/go-source-manager/", "tests/fixtures/go-scheduler/", ".github/workflows/web-api.yml"},
	"scheduler":  {"go-scheduler/", "go-tools/", "go-worker/", "go-processing/", "web/", "tests/", "docker/", ".github/workflows/go-scheduler.yml"},
	"ingestion":  {"go-ingestion/", "go-domain/", "go-processing/", "go/", "go-worker/", "web/", "docker/Dockerfile", "docker/Dockerfile.extractor", "deploy/news-ingestion/", "tests/fixtures/go-ingestion/", ".github/workflows/go-article-ingestion.yml"},
	"processing": {"go-processing/", "go-domain/", "tests/", "deploy/news-processing/", ".github/workflows/go-article-processing.yml"},
	"extractor":  {"go/", "go-tools/", "go-processing/", "go-web/templates/public/fragments/feed_core.html", "tests/fixtures/", "scripts/shadow_cdp_relay.py", "scripts/shadow_x11_client.sh", "deploy/news-extractor/", "docker/Dockerfile.extractor", "deploy/compose/extractor-test.yml", ".github/workflows/extractor-service.yml"},
	"browser":    {"deploy/news-extractor/browser/", "deploy/news-extractor/compose.shared-browser.yml", "deploy/news-extractor/compose.native.yml", "docker/Dockerfile.browser", "scripts/launch_shadow_cloak.py", "scripts/shadow_cdp_relay.py", "scripts/shadow_x11_client.sh", "go-tools/browsercontract/", "go-tools/go.mod", "go-tools/go.sum", ".github/workflows/shared-browser.yml"},
}

// Direct cross-module file reads; other shared test fixtures fail closed.
var sharedInputs = map[string][]string{
	"go-editorial/testdata/":                              {"go-domain"},
	"go-scheduler/migrations/001_initial.sql":             {"go-domain"},
	"go-tools/managedsetup/grants.sql":                    {"go-domain"},
	"go-web/internal/config/config.go":                    {"go-tools"},
	"go-domain/cmd/news-domain/runtime_config.go":         {"go-tools"},
	"go-background/cmd/news-background/runtime_config.go": {"go-tools"},
	"go/runtimefile/lookup.go":                            {"go-tools"},
	"docs/managed-settings-recovery.md":                   {"go-domain"},
	"docs/extraction-golden-cases.json":                   {"go-tools"},
}
var reviewedRecipes = map[string][]string{
	"docker/Dockerfile":           {"d24c0c0ff5fbb335944742b2f4e855f7bd5185b6957cc3d19831f1f2bffb9d3e"},
	"docker/Dockerfile.extractor": {"04f832b3cde6aefef5f7179330498dcb49975fd6520f74523ff60573efe4b09f"},
	"docker/Dockerfile.browser":   {"37368cf07b34d01be992c25675c0fdde00ee06bfb89b9214c6c2fb9408893066"},
}

func matchesAny(file string, patterns []string) bool {
	for _, p := range patterns {
		if (strings.HasSuffix(p, "/") && strings.HasPrefix(file, p)) || file == p {
			return true
		}
	}
	return false
}
func recipeReviewed(content []byte, known []string) bool {
	if content == nil {
		return false
	}
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	for _, h := range known {
		if h == hash {
			return true
		}
	}
	return false
}

type Plan struct {
	SchemaVersion       int                 `json:"schemaVersion"`
	Base                string              `json:"base"`
	Head                string              `json:"head"`
	Modules             []string            `json:"modules"`
	Targets             []string            `json:"targets"`
	FixtureTargets      []string            `json:"fixture_targets"`
	SourceFingerprints  map[string]string   `json:"source_fingerprints"`
	IntegrationRequired bool                `json:"integration_required"`
	Checks              map[string]bool     `json:"checks"`
	Reasons             map[string][]string `json:"reasons"`
	ImageChangedBrowser bool                `json:"image_changed_browser"`
	FallbackFull        bool                `json:"fallback_full"`
}
type snapshot struct {
	files                map[string][]byte
	imports, testImports map[string][]string
	packages             map[string]bool
}

func git(root string, args ...string) ([]byte, error) {
	c := exec.Command("git", append([]string{"-C", root}, args...)...)
	b, e := c.Output()
	if e != nil {
		return nil, fmt.Errorf("git %s: %w", strings.Join(args, " "), e)
	}
	return b, nil
}
func readSnapshot(root, rev string) (*snapshot, error) {
	// git archive does not preserve submodule contents; reject both gitlinks and
	// symlinks, including deleted ones encountered in the baseline snapshot.
	tree, e := git(root, "ls-tree", "-rz", "--full-tree", "-r", rev)
	if e != nil {
		return nil, e
	}
	for _, entry := range bytes.Split(tree, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		parts := bytes.SplitN(entry, []byte{'\t'}, 2)
		if len(parts) != 2 {
			return nil, fmt.Errorf("malformed Git tree entry in %s", rev)
		}
		name := string(parts[1])
		if path.Base(name) == "go.mod" {
			excluded := false
			for _, component := range strings.Split(name, "/") {
				if component == "vendor" || component == "testdata" || component == "fixtures" {
					excluded = true
					break
				}
			}
			known := false
			for _, m := range Modules {
				if name == m+"/go.mod" {
					known = true
					break
				}
			}
			if !excluded && !known {
				return nil, fmt.Errorf("unsupported Go module %s; update the CI module catalog", name)
			}
		}
		fields := bytes.SplitN(entry, []byte(" "), 2)
		if len(fields) != 2 || (string(fields[0]) != "100644" && string(fields[0]) != "100755") {
			return nil, fmt.Errorf("unsupported Git tree object in %s: symlinks and submodules require catalog review", rev)
		}
	}
	b, e := git(root, "archive", "--format=tar", rev)
	if e != nil {
		return nil, e
	}
	s := &snapshot{map[string][]byte{}, map[string][]string{}, map[string][]string{}, map[string]bool{}}
	tr := tar.NewReader(bytes.NewReader(b))
	for {
		h, e := tr.Next()
		if e == io.EOF {
			break
		}
		if e != nil {
			return nil, e
		}
		if h.Typeflag == tar.TypeDir || h.Typeflag == tar.TypeXGlobalHeader {
			continue
		}
		if h.Typeflag != tar.TypeReg {
			return nil, fmt.Errorf("unsupported archive object in %s", rev)
		}
		v, e := io.ReadAll(tr)
		if e != nil {
			return nil, e
		}
		s.files[h.Name] = v
	}
	for f, b := range s.files {
		if !strings.HasSuffix(f, ".go") || module(f) == "" {
			continue
		}
		pkg := path.Dir(f)
		s.packages[pkg] = true
		ast, e := parser.ParseFile(token.NewFileSet(), f, b, parser.ImportsOnly)
		if e != nil {
			return nil, fmt.Errorf("parse %s: %w", f, e)
		}
		for _, im := range ast.Imports {
			name, e := strconv.Unquote(im.Path.Value)
			if e != nil {
				return nil, e
			}
			if strings.HasPrefix(name, prefix) {
				dep := strings.TrimPrefix(name, prefix)
				if strings.HasSuffix(f, "_test.go") {
					s.testImports[pkg] = append(s.testImports[pkg], dep)
				} else {
					s.imports[pkg] = append(s.imports[pkg], dep)
				}
			}
		}
	}
	for p, deps := range s.imports {
		for _, d := range deps {
			if !s.packages[d] {
				return nil, fmt.Errorf("unresolved local import %s in %s", d, p)
			}
		}
	}
	for p, deps := range s.testImports {
		for _, d := range deps {
			if !s.packages[d] {
				return nil, fmt.Errorf("unresolved local test import %s in %s", d, p)
			}
		}
	}
	return s, nil
}
func module(p string) string {
	for _, m := range Modules {
		if p == m || strings.HasPrefix(p, m+"/") {
			return m
		}
	}
	return ""
}
func closure(s *snapshot, roots ...string) map[string]bool {
	seen := map[string]bool{}
	var walk func(string)
	walk = func(p string) {
		if seen[p] {
			return
		}
		seen[p] = true
		for _, d := range s.imports[p] {
			walk(d)
		}
	}
	for _, p := range roots {
		walk(p)
	}
	return seen
}
func reverse(s *snapshot, changed string) map[string]bool {
	seen := map[string]bool{changed: true}
	for updated := true; updated; {
		updated = false
		for p := range s.packages {
			if seen[p] {
				continue
			}
			for _, d := range append(append([]string{}, s.imports[p]...), s.testImports[p]...) {
				if seen[d] {
					seen[p] = true
					updated = true
					break
				}
			}
		}
	}
	return seen
}
func imagePackages(s *snapshot) map[string]map[string]bool {
	return map[string]map[string]bool{
		"web": closure(s, "go-web/cmd/news-web"), "background": closure(s, "go-background/cmd/news-background"), "executor": closure(s, "go-domain/cmd/news-domain", "go-domain/cmd/news-migrate", "go-domain/cmd/newsctl"), "extractor": closure(s, "go/cmd/extract-service", "go/cmd/extract-egress")}
}
func included(s *snapshot, target, f string, pkgs map[string]bool) bool {
	if f == "scripts/ci-core.py" || f == ".github/workflows/docker-build-push.yml" {
		return true
	}
	if f == ".github/workflows/extractor-service.yml" && target == "extractor" {
		return true
	}
	if (f == ".dockerignore" || f == "docker-bake.hcl") && target != "extractor" {
		return true
	}
	if f == "docker/Dockerfile" && target != "extractor" {
		return true
	}
	if (f == "docker/Dockerfile.extractor" || f == "go/.dockerignore") && target == "extractor" {
		return true
	}
	if target == "web" && (strings.HasPrefix(f, "go-web/templates/") || strings.HasPrefix(f, "go-web/contracts/") || strings.HasPrefix(f, "web/static/") || f == "web/package.json" || f == "web/package-lock.json" || f == "web/tsconfig.json") {
		return true
	}
	// Every module manifest visible to its build stage influences dependency resolution.
	ms := map[string][]string{"web": {"go-web", "go-tools"}, "background": {"go-background", "go-tools", "go-worker", "go-scheduler", "go-ingestion", "go-processing", "go-execution"}, "executor": {"go-domain", "go-editorial", "go", "go-tools"}, "extractor": {"go"}}
	for _, m := range ms[target] {
		if f == m+"/go.mod" || f == m+"/go.sum" {
			return true
		}
	}
	if strings.HasSuffix(f, "_test.go") {
		for p := range pkgs {
			if !strings.HasPrefix(f, p+"/") {
				continue
			}
			for file, b := range s.files {
				if path.Dir(file) == p && strings.HasSuffix(file, ".go") && !strings.HasSuffix(file, "_test.go") && bytes.Contains(b, []byte("//go:embed")) {
					return true
				}
			}
		}
		return false
	}
	if pkgs[path.Dir(f)] {
		return true
	}
	// Conservatively include every descendant of a package declaring go:embed.
	for p := range pkgs {
		if strings.HasPrefix(f, p+"/") {
			for file, b := range s.files {
				if path.Dir(file) == p && strings.HasSuffix(file, ".go") && !strings.HasSuffix(file, "_test.go") && bytes.Contains(b, []byte("//go:embed")) {
					return true
				}
			}
		}
	}
	return false
}
func fingerprints(s *snapshot) (map[string]string, map[string]map[string]bool) {
	out := map[string]string{}
	sets := map[string]map[string]bool{}
	for target, pkgs := range imagePackages(s) {
		files := []string{}
		sets[target] = map[string]bool{}
		for f := range s.files {
			if included(s, target, f, pkgs) {
				files = append(files, f)
				sets[target][f] = true
			}
		}
		sort.Strings(files)
		h := sha256.New()
		for _, f := range files {
			fmt.Fprintf(h, "%d:%s:%d:", len(f), f, len(s.files[f]))
			h.Write(s.files[f])
		}
		out[target] = hex.EncodeToString(h.Sum(nil))
	}
	return out, sets
}
func add(m map[string]bool, x string) { m[x] = true }
func sorted(m map[string]bool) []string {
	v := make([]string, 0, len(m))
	for x := range m {
		v = append(v, x)
	}
	sort.Strings(v)
	return v
}

var environmentKey = regexp.MustCompile(`^[A-Z][A-Z0-9_]{2,}$`)

func hasEnvironmentCandidate(content []byte) bool {
	if content == nil {
		return false
	}
	f, err := parser.ParseFile(token.NewFileSet(), "input.go", content, 0)
	if err != nil {
		return true
	}
	found := false
	ast.Inspect(f, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if ok && lit.Kind == token.STRING {
			value, e := strconv.Unquote(lit.Value)
			if e == nil && environmentKey.MatchString(value) {
				found = true
			}
		}
		return !found
	})
	return found
}
func browserImageInput(f string) bool {
	return matchesAny(f, []string{".dockerignore", "docker/Dockerfile.browser", ".github/workflows/shared-browser.yml", "deploy/news-extractor/browser/", "scripts/launch_shadow_cloak.py", "scripts/shadow_cdp_relay.py", "scripts/shadow_x11_client.sh"})
}
func PlanChanges(root, base, head string) (Plan, error) {
	return planChanges(root, base, head, reviewedRecipes)
}

func planChanges(root, base, head string, recipes map[string][]string) (Plan, error) {
	for _, rev := range []string{base, head} {
		if len(rev) < 7 || len(rev) > 64 {
			return Plan{}, fmt.Errorf("base and head must be hexadecimal commit IDs")
		}
		for _, c := range rev {
			if !strings.ContainsRune("0123456789abcdef", c) {
				return Plan{}, fmt.Errorf("base and head must be hexadecimal commit IDs")
			}
		}
	}
	p := Plan{SchemaVersion: 1, Base: base, Head: head, Modules: []string{}, Targets: []string{}, FixtureTargets: []string{}, SourceFingerprints: map[string]string{}, Checks: map[string]bool{}, Reasons: map[string][]string{}}
	for _, c := range CheckNames {
		p.Checks[c] = false
	}
	shallow, e := git(root, "rev-parse", "--is-shallow-repository")
	if e != nil || strings.TrimSpace(string(shallow)) != "false" {
		return p, fmt.Errorf("complete Git history required")
	}
	for _, r := range []string{base, head} {
		if _, e = git(root, "rev-parse", "--verify", r+"^{commit}"); e != nil {
			return p, e
		}
	}
	baseID, e := git(root, "rev-parse", "--verify", base+"^{commit}")
	if e != nil {
		return p, e
	}
	headID, e := git(root, "rev-parse", "--verify", head+"^{commit}")
	if e != nil {
		return p, e
	}
	p.Base = strings.TrimSpace(string(baseID))
	p.Head = strings.TrimSpace(string(headID))
	if _, e = git(root, "merge-base", "--is-ancestor", base, head); e != nil {
		return p, fmt.Errorf("base must be an ancestor of head: %w", e)
	}
	a, e := readSnapshot(root, base)
	if e != nil {
		return p, e
	}
	b, e := readSnapshot(root, head)
	if e != nil {
		return p, e
	}
	p.SourceFingerprints, _ = fingerprints(b)
	_, as := fingerprints(a)
	_, bs := fingerprints(b)
	// Keep older reviewed hashes so published baselines remain comparable.
	for file, hashes := range recipes {
		for _, s := range []*snapshot{a, b} {
			if !recipeReviewed(s.files[file], hashes) {
				return p, fmt.Errorf("%s is not a reviewed recipe; update the input catalog before planning", file)
			}
		}
	}
	delta, e := git(root, "diff", "--name-only", "--no-renames", "-z", base, head, "--")
	if e != nil {
		return p, e
	}
	mods, targets, fixtures := map[string]bool{}, map[string]bool{}, map[string]bool{}
	reason := func(k, v string) { p.Reasons[k] = append(p.Reasons[k], v) }
	full := func(f string) {
		p.FallbackFull = true
		p.IntegrationRequired = true
		for _, m := range Modules {
			add(mods, m)
		}
		for _, t := range Targets {
			add(targets, t)
		}
		add(fixtures, "extractor")
		for _, c := range CheckNames {
			p.Checks[c] = true
		}
		reason("fallback_full", f)
	}
	for _, f := range strings.Split(string(delta), "\x00") {
		if f == "" {
			continue
		}
		if strings.HasPrefix(f, "docker-compose") && strings.HasSuffix(f, ".yml") {
			p.Checks["processing"] = true
			reason("check:processing", f)
		}
		for check, patterns := range workflowInputs {
			if matchesAny(f, patterns) {
				p.Checks[check] = true
				reason("check:"+check, f)
			}
		}
		for pattern, owners := range sharedInputs {
			if matchesAny(f, []string{pattern}) {
				for _, owner := range owners {
					add(mods, owner)
					reason("module:"+owner, "shared input "+f)
				}
			}
		}
		if browserImageInput(f) {
			p.ImageChangedBrowser = true
			reason("image:browser", f)
		}
		m := module(f)
		if m != "" && strings.HasSuffix(f, ".go") && !strings.HasSuffix(f, "_test.go") && !strings.HasPrefix(f, "go-tools/configcatalog/") && (hasEnvironmentCandidate(a.files[f]) || hasEnvironmentCandidate(b.files[f])) {
			add(mods, "go-tools")
			reason("module:go-tools", "environment catalog scans "+f)
		}
		if m != "" {
			add(mods, m)
			reason("module:"+m, f)
			if !strings.HasSuffix(f, "_test.go") && strings.HasSuffix(f, ".go") {
				for _, s := range []*snapshot{a, b} {
					for pkg := range reverse(s, path.Dir(f)) {
						if x := module(pkg); x != "" {
							add(mods, x)
							reason("module:"+x, "imports "+path.Dir(f))
						}
					}
				}
			}
			if f == m+"/go.mod" || f == m+"/go.sum" {
				for _, s := range []*snapshot{a, b} {
					for pkg := range s.packages {
						if module(pkg) == m {
							for consumer := range reverse(s, pkg) {
								add(mods, module(consumer))
							}
						}
					}
				}
			}
			if f == "go-tools/configcatalog/catalog.json" || strings.HasPrefix(f, "go-tools/ciimpact/") || strings.HasPrefix(f, "go-tools/ciartifact/") || strings.HasPrefix(f, "go-tools/cmd/ci-artifacts/") || strings.HasPrefix(f, "go-tools/cmd/ci-impact/") || strings.HasPrefix(f, "go-tools/corerelease/") || strings.HasPrefix(f, "go-tools/fixturegate/") || strings.HasPrefix(f, "go-tools/cmd/core-image-release/") || strings.HasPrefix(f, "go-tools/cmd/core-image-catalog/") || strings.HasPrefix(f, "go-tools/cmd/check-fixture-receipts/") || strings.HasPrefix(f, "go-tools/cmd/vertical-fixture-gate/") || strings.HasPrefix(f, "go-tools/cmd/managed-adoption-gate/") {
				p.IntegrationRequired = true
			}
			if f != "go/.dockerignore" && f != "go-tools/configcatalog/catalog.json" && !strings.HasSuffix(f, ".go") && f != m+"/go.mod" && f != m+"/go.sum" && !strings.Contains(f, "/testdata/") && !strings.HasSuffix(f, ".md") {
				known := false
				for _, t := range append(append([]string{}, Targets...), "extractor") {
					if as[t][f] || bs[t][f] {
						known = true
					}
				}
				if !known {
					full("unclassified module input: " + f)
				}
			}
		} else if f == "scripts/ci-core.py" || f == "scripts/test_ci_core.py" || f == "scripts/test_ci_impact.py" || f == "scripts/ci-baseline.sh" || f == ".github/workflows/docker-build-push.yml" || f == ".github/workflows/extractor-service.yml" || f == ".github/workflows/shared-browser.yml" {
			add(mods, "go-tools")
			p.IntegrationRequired = true
			reason("module:go-tools", "CI control "+f)
		} else if strings.HasPrefix(f, "tests/fixtures/operational-snapshot-v1/") {
			add(mods, "go-background")
			add(mods, "go-domain")
			reason("fixture", f)
		} else if strings.HasPrefix(f, "docs/") || f == "README.md" || f == "AGENTS.md" || f == "LICENSE" {
			reason("docs", f)
		} else if strings.HasPrefix(f, "web/") {
			add(mods, "go-web")
			p.Checks["web_api"] = true
		} else if strings.HasPrefix(f, ".github/") || strings.HasPrefix(f, "docker/") || f == "docker-bake.hcl" {
			full("CI/build configuration: " + f)
		} else {
			full("unknown path: " + f)
		}
		for _, t := range append(append([]string{}, Targets...), "extractor") {
			if as[t][f] || bs[t][f] {
				if t == "extractor" {
					add(fixtures, t)
				} else {
					add(targets, t)
				}
				reason("target:"+t, f)
			}
		}
	}
	p.Modules = sorted(mods)
	p.Targets = sorted(targets)
	p.FixtureTargets = sorted(fixtures)
	for _, m := range p.Modules {
		switch m {
		case "go":
			p.Checks["prototype"] = true
			p.Checks["extractor"] = true
		case "go-web":
			p.Checks["web_api"] = true
		case "go-scheduler":
			p.Checks["scheduler"] = true
		case "go-ingestion":
			p.Checks["ingestion"] = true
		case "go-processing":
			p.Checks["processing"] = true
		}
	}
	if len(p.Targets) > 0 || len(p.FixtureTargets) > 0 {
		p.IntegrationRequired = true
	}
	for k := range p.Reasons {
		sort.Strings(p.Reasons[k])
		values := p.Reasons[k][:0]
		for _, v := range p.Reasons[k] {
			if len(values) == 0 || values[len(values)-1] != v {
				values = append(values, v)
			}
		}
		p.Reasons[k] = values
	}
	return p, nil
}
