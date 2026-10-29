package configcatalog

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type entry struct {
	Key          string   `json:"key"`
	Owner        string   `json:"owner"`
	Type         string   `json:"type"`
	Default      string   `json:"default"`
	Secret       bool     `json:"secret"`
	Store        string   `json:"store"`
	Reload       string   `json:"reload"`
	Migration    string   `json:"migration"`
	Precedence   string   `json:"precedence"`
	Sources      []string `json:"sources"`
	Class        string   `json:"class"`
	ManagedRoles []string `json:"managed_roles"`
}

var keyPattern = regexp.MustCompile(`^[A-Z][A-Z0-9_]{2,}$`)
var helpers = map[string]bool{"Getenv": true, "LookupEnv": true, "getenv": true, "lookup": true, "env": true, "get": true, "envOr": true, "envInt": true, "envIntFrom": true, "envFrom": true, "intEnv": true, "boolEnv": true, "numberEnv": true, "durationEnv": true, "csvEnv": true, "value": true, "seconds": true, "duration": true, "boolean": true, "Read": true, "durationIntegerSeconds": true, "durationSeconds": true, "integer": true}

// discover parses real code rather than matching a duplicate list. It also
// inspects table-driven config literals and the background role DSN adapter.
func discover(root string) (map[string][]string, error) {
	found := map[string][]string{}
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != root && (strings.HasPrefix(d.Name(), ".") || d.Name() == "archive" || d.Name() == "vendor" || d.Name() == "node_modules") {
				return filepath.SkipDir
			}
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		rel = filepath.ToSlash(rel)
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || !strings.HasPrefix(rel, "go") || strings.HasPrefix(rel, "go-tools/configcatalog/") {
			return nil
		}
		fs := token.NewFileSet()
		file, e := parser.ParseFile(fs, path, nil, 0)
		if e != nil {
			return e
		}
		add := func(lit *ast.BasicLit) {
			if lit.Kind != token.STRING {
				return
			}
			s, e := strconv.Unquote(lit.Value)
			if e != nil || !keyPattern.MatchString(s) {
				return
			}
			loc := rel + ":" + strconv.Itoa(fs.Position(lit.Pos()).Line)
			found[s] = append(found[s], loc)
		}
		ast.Inspect(file, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.CallExpr:
				name := ""
				switch f := x.Fun.(type) {
				case *ast.Ident:
					name = f.Name
				case *ast.SelectorExpr:
					name = f.Sel.Name
				}
				if helpers[name] {
					for _, a := range x.Args {
						if l, ok := a.(*ast.BasicLit); ok {
							add(l)
						} else if id, ok := a.(*ast.Ident); ok && rel == "go-tools/cmd/compare-extractor-service/main.go" && name == "Getenv" && id.Name == "tokenEnv" {
							found["<TOKEN_ENV_FLAG>"] = append(found["<TOKEN_ENV_FLAG>"], rel+":"+strconv.Itoa(fs.Position(id.Pos()).Line))
						} else if expr, ok := a.(*ast.BinaryExpr); ok && (name == "getenv" || name == "Getenv" || name == "get" || name == "env") {
							prefix, suffix := dynamicParts(expr)
							if prefix != "" && suffix != "" {
								found[prefix+"<ALIAS>"+suffix] = append(found[prefix+"<ALIAS>"+suffix], rel+":"+strconv.Itoa(fs.Position(expr.Pos()).Line))
							}
						}
					}
				}
			case *ast.CompositeLit:
				if rel == "go-processing/runner/runner.go" || rel == "go-domain/cmd/news-domain/runtime_config.go" || rel == "go-background/cmd/news-background/runtime_config.go" || rel == "go-web/internal/config/config.go" || rel == "go/runtimefile/lookup.go" || rel == "go-tools/cmd/prepare-runtime-env/main.go" {
					for _, v := range x.Elts {
						if l, ok := v.(*ast.BasicLit); ok {
							add(l)
						}
					}
				}
			case *ast.AssignStmt:
				if rel == "go-background/internal/config/config.go" {
					for _, v := range x.Rhs {
						if l, ok := v.(*ast.BasicLit); ok {
							add(l)
						}
					}
				}
			}
			return true
		})
		return nil
	})
	return found, err
}

// dynamicParts recognizes the scheduler's prefix + alias + suffix construction.
func dynamicParts(expr ast.Expr) (string, string) {
	var parts []string
	var visit func(ast.Expr)
	visit = func(e ast.Expr) {
		if b, ok := e.(*ast.BinaryExpr); ok && b.Op == token.ADD {
			visit(b.X)
			visit(b.Y)
			return
		}
		if l, ok := e.(*ast.BasicLit); ok && l.Kind == token.STRING {
			s, err := strconv.Unquote(l.Value)
			if err == nil {
				parts = append(parts, s)
				return
			}
		}
		parts = append(parts, "<variable>")
	}
	visit(expr)
	if len(parts) == 3 && parts[1] == "<variable>" && keyPattern.MatchString(parts[0]+"X"+parts[2]) {
		return parts[0], parts[2]
	}
	return "", ""
}
func TestCatalogCoversGoEnvironmentReads(t *testing.T) {
	root := filepath.Join("..", "..")
	b, err := os.ReadFile("catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []entry
	if err = json.Unmarshal(b, &entries); err != nil {
		t.Fatal(err)
	}
	catalog := map[string]entry{}
	for _, e := range entries {
		if _, ok := catalog[e.Key]; ok {
			t.Errorf("duplicate %s", e.Key)
		}
		catalog[e.Key] = e
		if e.Owner == "" || e.Type == "" || e.Default == "" || e.Store == "" || e.Reload == "" || e.Migration == "" || e.Precedence == "" || e.Class == "" || len(e.Sources) == 0 {
			t.Errorf("incomplete metadata for %s", e.Key)
		}
	}
	found, err := discover(root)
	if err != nil {
		t.Fatal(err)
	}
	var missing []string
	for key, sites := range found {
		if _, ok := catalog[key]; !ok {
			missing = append(missing, key+" "+strings.Join(sites, ", "))
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("uncatalogued Go environment candidates:\n%s", strings.Join(missing, "\n"))
	}
	// Dynamic names are built by concatenation and cannot be represented by one literal.
	for _, key := range []string{"SCHEDULER_TARGET_<ALIAS>_URL", "SCHEDULER_TARGET_<ALIAS>_TOKEN"} {
		if _, ok := catalog[key]; !ok {
			t.Errorf("missing dynamic key %s", key)
		}
	}
}

func TestCatalogSourceLocations(t *testing.T) {
	b, err := os.ReadFile("catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []entry
	if err = json.Unmarshal(b, &entries); err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		for _, ref := range e.Sources {
			p, n, ok := strings.Cut(ref, ":")
			if !ok {
				t.Errorf("invalid source %s", ref)
				continue
			}
			line, err := strconv.Atoi(n)
			if err != nil || line < 1 {
				t.Errorf("invalid source line %s", ref)
				continue
			}
			data, err := os.ReadFile(filepath.Join("..", "..", p))
			if err != nil {
				t.Errorf("source %s: %v", ref, err)
				continue
			}
			lines := strings.Split(string(data), "\n")
			if line > len(lines) {
				t.Errorf("stale source %s for %s", ref, e.Key)
				continue
			}
			needle := e.Key
			if needle == "<TOKEN_ENV_FLAG>" {
				needle = "os.Getenv(tokenEnv)"
			}
			if strings.Contains(needle, "<ALIAS>") {
				needle = strings.Split(needle, "<ALIAS>")[0]
			}
			if !strings.Contains(lines[line-1], needle) {
				t.Errorf("stale source %s for %s", ref, e.Key)
			}
		}
	}
}

func TestDeploymentInputsAreCatalogued(t *testing.T) {
	b, err := os.ReadFile("catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []entry
	if err = json.Unmarshal(b, &entries); err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	for _, e := range entries {
		keys[e.Key] = true
	}
	interpolation := regexp.MustCompile(`\$\{([A-Z][A-Z0-9_]*)`)
	assignment := regexp.MustCompile(`(?m)^\s*([A-Z][A-Z0-9_]*)=`)
	check := func(path string, pattern *regexp.Regexp) {
		data, e := os.ReadFile(path)
		if e != nil {
			t.Fatal(e)
		}
		for _, m := range pattern.FindAllStringSubmatch(string(data), -1) {
			if !keys[m[1]] {
				t.Errorf("uncatalogued deployment input %s in %s", m[1], path)
			}
		}
	}
	check("../../.env.legacy.example", assignment)
	check("../../docker-compose.example.yml", interpolation)
	checkComposeOutputs(t, "../../docker-compose.example.yml", keys)
	check("../../docker-compose.managed.yml", interpolation)
	checkComposeOutputs(t, "../../docker-compose.managed.yml", keys)
	err = filepath.WalkDir("../../deploy", func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if strings.HasSuffix(path, ".yml") {
			check(path, interpolation)
			checkComposeOutputs(t, path, keys)
		}
		if strings.HasSuffix(path, ".env.example") {
			check(path, assignment)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// The shared browser wrapper reads env directly rather than via interpolation.
	check("../../deploy/news-extractor/browser/entrypoint.py", regexp.MustCompile(`os\.environ\.get\("([A-Z][A-Z0-9_]*)"`))
	minimal, err := os.ReadFile("../../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range assignment.FindAllStringSubmatch(string(minimal), -1) {
		if m[1] != "SITE_BASE_URL" {
			t.Errorf("unexpected minimal environment key %s", m[1])
		}
	}
}

// checkComposeOutputs recognizes mapping and list forms in environment blocks.
// Compose interpolation inputs are checked independently above.
func checkComposeOutputs(t *testing.T, path string, keys map[string]bool) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	environment := regexp.MustCompile(`^\s*environment:\s*$`)
	mapping := regexp.MustCompile(`^\s*(?:-\s+)?([A-Z][A-Z0-9_]+)\s*[:=]`)
	inside := false
	indent := 0
	for _, line := range strings.Split(string(data), "\n") {
		trim := strings.TrimSpace(line)
		current := len(line) - len(strings.TrimLeft(line, " "))
		if environment.MatchString(line) {
			inside = true
			indent = current
			continue
		}
		if inside && trim != "" && !strings.HasPrefix(trim, "#") && current <= indent {
			inside = false
		}
		if inside {
			if m := mapping.FindStringSubmatch(line); m != nil && !keys[m[1]] {
				t.Errorf("uncatalogued Compose environment output %s in %s", m[1], path)
			}
		}
	}
}

func TestFullExampleCoversProductionNames(t *testing.T) {
	b, err := os.ReadFile("catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []entry
	if err = json.Unmarshal(b, &entries); err != nil {
		t.Fatal(err)
	}
	template, err := os.ReadFile("../../.env.full.example")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Store, "test/fixture") || strings.HasPrefix(e.Store, "operator/CI") || e.Class == "compose_output" {
			continue
		}
		if !strings.Contains(string(template), "# "+e.Key+"=") {
			t.Errorf("full example missing commented %s", e.Key)
		}
	}
	for _, line := range strings.Split(string(template), "\n") {
		s := strings.TrimSpace(line)
		if s != "" && !strings.HasPrefix(s, "#") {
			t.Errorf("full example contains an active assignment")
		}
	}
}

func TestDiscoverHelpersAndTableEntries(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "go-processing", "runner")
	if e := os.MkdirAll(dir, 0700); e != nil {
		t.Fatal(e)
	}
	source := `package runner
 func f(getenv func(string) string, alias string) { _ = getenv("DIRECT_KEY"); _ = integer(getenv, "HELPER_KEY", 2, 1, 3); _ = []struct{ key string; n int }{{"TABLE_KEY", 4}}; _ = getenv("SCHEDULER_TARGET_"+alias+"_URL") }
 `
	if e := os.WriteFile(filepath.Join(dir, "runner.go"), []byte(source), 0600); e != nil {
		t.Fatal(e)
	}
	found, e := discover(root)
	if e != nil {
		t.Fatal(e)
	}
	for _, k := range []string{"DIRECT_KEY", "HELPER_KEY", "TABLE_KEY", "SCHEDULER_TARGET_<ALIAS>_URL"} {
		if len(found[k]) == 0 {
			t.Errorf("failed to discover %s", k)
		}
	}
}

func TestManagedRoleAllowlistsAreCatalogued(t *testing.T) {
	b, err := os.ReadFile("catalog.json")
	if err != nil {
		t.Fatal(err)
	}
	var entries []entry
	if err = json.Unmarshal(b, &entries); err != nil {
		t.Fatal(err)
	}
	byKey := map[string]entry{}
	for _, e := range entries {
		byKey[e.Key] = e
	}
	expected := map[string]map[string]bool{}
	for _, v := range []struct{ path, role string }{{"../../go-web/internal/config/config.go", "web"}, {"../../go-domain/cmd/news-domain/runtime_config.go", "domain"}, {"../../go-background/cmd/news-background/runtime_config.go", "background"}} {
		expected[v.role] = map[string]bool{}
		file, e := parser.ParseFile(token.NewFileSet(), v.path, nil, 0)
		if e != nil {
			t.Fatal(e)
		}
		found := false
		ast.Inspect(file, func(n ast.Node) bool {
			a, ok := n.(*ast.AssignStmt)
			if !ok || len(a.Lhs) != 1 || len(a.Rhs) != 1 {
				return true
			}
			id, ok := a.Lhs[0].(*ast.Ident)
			if !ok || id.Name != "allowed" {
				return true
			}
			list, ok := a.Rhs[0].(*ast.CompositeLit)
			if !ok {
				return true
			}
			found = true
			for _, node := range list.Elts {
				lit, ok := node.(*ast.BasicLit)
				if !ok {
					t.Errorf("dynamic manifest allowlist in %s", v.path)
					continue
				}
				key, e := strconv.Unquote(lit.Value)
				if e != nil {
					t.Fatal(e)
				}
				expected[v.role][key] = true
				item, ok := byKey[key]
				if !ok || !contains(item.ManagedRoles, v.role) {
					t.Errorf("catalog missing managed role %s for %s", v.role, key)
				}
			}
			return true
		})
		if !found {
			t.Errorf("managed allowlist not found in %s", v.path)
		}
	}
	// Extractor and egress role lists live in their isolated module.
	file, e := parser.ParseFile(token.NewFileSet(), "../../go/runtimefile/lookup.go", nil, 0)
	if e != nil {
		t.Fatal(e)
	}
	found := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		c, ok := n.(*ast.CaseClause)
		if !ok || len(c.List) != 1 {
			return true
		}
		l, ok := c.List[0].(*ast.BasicLit)
		if !ok {
			return true
		}
		role, e := strconv.Unquote(l.Value)
		if e != nil || (role != "extractor" && role != "egress") {
			return true
		}
		for _, stmt := range c.Body {
			ret, ok := stmt.(*ast.ReturnStmt)
			if !ok || len(ret.Results) < 1 {
				continue
			}
			list, ok := ret.Results[0].(*ast.CompositeLit)
			if !ok {
				continue
			}
			found[role] = true
			expected[role] = map[string]bool{}
			for _, node := range list.Elts {
				lit, ok := node.(*ast.BasicLit)
				if !ok {
					t.Errorf("dynamic %s allowlist", role)
					continue
				}
				key, e := strconv.Unquote(lit.Value)
				if e != nil {
					t.Fatal(e)
				}
				expected[role][key] = true
				if !contains(byKey[key].ManagedRoles, role) {
					t.Errorf("catalog missing managed role %s for %s", role, key)
				}
			}
		}
		return true
	})
	for _, role := range []string{"extractor", "egress"} {
		if !found[role] {
			t.Errorf("managed allowlist not found for %s", role)
		}
	}
	for _, item := range entries {
		for _, role := range item.ManagedRoles {
			if !expected[role][item.Key] {
				t.Errorf("catalog has stale managed role %s for %s", role, item.Key)
			}
		}
	}
}
func contains(values []string, wanted string) bool {
	for _, v := range values {
		if v == wanted {
			return true
		}
	}
	return false
}
