package main

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
)

func TestComposeComparisonAllowsOnlyExpectedAdoptionChanges(t *testing.T) {
	before := fixtureConfig(t)
	after := cloneFixtureConfig(t, before)
	applyExpectedOverlay(after)
	if !sameNonRuntimeCompose(before, after) {
		t.Fatal("expected managed overlay rejected")
	}

	tests := []struct {
		name   string
		change func(map[string]any)
	}{
		{"network definition", func(c map[string]any) { c["networks"].(map[string]any)["fixture"].(map[string]any)["internal"] = false }},
		{"neighbor service", func(c map[string]any) {
			c["services"].(map[string]any)["topology-sentinel"].(map[string]any)["command"] = []any{"exit", "0"}
		}},
		{"published port", func(c map[string]any) {
			c["services"].(map[string]any)["app"].(map[string]any)["ports"] = []any{map[string]any{"published": "8000"}}
		}},
		{"lost legacy mount", func(c map[string]any) {
			c["services"].(map[string]any)["app"].(map[string]any)["volumes"] = []any{managedMount("managed_web")}
		}},
		{"inherited environment secret", func(c map[string]any) {
			c["services"].(map[string]any)["app"].(map[string]any)["environment"] = map[string]any{"NEWS_RUNTIME_FILE": "/run/news/web.json", "ADMIN_PASSWORD": "fixture-secret"}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := cloneFixtureConfig(t, after)
			test.change(changed)
			if sameNonRuntimeCompose(before, changed) {
				t.Fatal("unexpected Compose change accepted")
			}
		})
	}
}

func fixtureConfig(t *testing.T) map[string]any {
	t.Helper()
	raw := []byte(`{"name":"fixture","networks":{"fixture":{"internal":true,"labels":{"sentinel":"keep"}}},"volumes":{"app_cache":{"name":"fixture_app_cache"},"executor_data":{"name":"fixture_executor_data"}},"services":{"app":{"image":"news-web","environment":{"DATABASE_URL":"legacy-db","ADMIN_PASSWORD":"fixture-only"},"env_file":["legacy.env"],"volumes":[{"type":"volume","source":"app_cache","target":"/app/cache"}],"networks":["fixture"],"ports":[],"depends_on":{"executor":{"condition":"service_healthy"}}},"executor":{"image":"news-executor","environment":{"DATABASE_URL":"legacy-db"},"volumes":[{"type":"volume","source":"executor_data","target":"/app/data"}],"networks":["fixture"]},"background":{"image":"news-background","environment":{"WORKER_DATABASE_URL":"legacy-db"},"networks":["fixture"]},"extractor":{"image":"news-extractor","environment":{"EXTRACTOR_SERVICE_TOKEN":"fixture"},"networks":["fixture"]},"migrate":{"image":"news-executor","entrypoint":["news-migrate"]},"topology-sentinel":{"image":"alpine","command":["sleep","infinity"],"labels":{"fixture.sentinel":"preserve"},"networks":["fixture"]}}}`)
	var config map[string]any
	if err := json.Unmarshal(raw, &config); err != nil {
		t.Fatal(err)
	}
	return config
}

func cloneFixtureConfig(t *testing.T, input map[string]any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	var result map[string]any
	if err = json.Unmarshal(raw, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func applyExpectedOverlay(config map[string]any) {
	services := config["services"].(map[string]any)
	files := map[string]string{"app": "web", "executor": "domain", "background": "background", "extractor": "extractor"}
	volumes := map[string]string{"app": "managed_web", "executor": "managed_domain", "background": "managed_background", "extractor": "managed_extractor"}
	for service := range files {
		definition := services[service].(map[string]any)
		definition["environment"] = map[string]any{"NEWS_RUNTIME_FILE": "/run/news/runtime.json"}
		definition["env_file"] = []any{}
		mounts, _ := definition["volumes"].([]any)
		definition["volumes"] = append(mounts, managedMount(volumes[service]))
		config["volumes"].(map[string]any)[volumes[service]] = map[string]any{"name": "fixture_" + volumes[service]}
	}
	services["migrate"].(map[string]any)["entrypoint"] = []any{"/bin/true"}
}

func managedMount(name string) map[string]any {
	return map[string]any{"type": "volume", "source": name, "target": "/run/news", "read_only": true}
}

func TestFixtureComposeIsInternalAndUsesOnlySyntheticCredentials(t *testing.T) {
	compose := composeTemplate("16", "/tmp/news-adoption-fixture")
	for _, required := range []string{
		"image: postgres:16-alpine", "internal: true", "fixture-only-password",
		"fixture-admin-password", "  app:", "aliases: [web]", "topology-sentinel:",
		"ADMIN_USERNAME: fixture-admin", "GEMINI_API_KEY: fixture-only-gemini-key",
		"TRUSTED_PROXY_CIDRS: 172.29.253.0/24", "subnet: 172.29.253.0/24",
		"SITE_BASE_URL: https://web:8000",
	} {
		if !strings.Contains(compose, required) {
			t.Errorf("fixture Compose missing %q", required)
		}
	}
	if strings.Contains(compose, "ports:") || strings.Contains(compose, "SITE_BASE_URL: http://") || strings.Contains(compose, "GEMINI_API_ENDPOINT:") || strings.Contains(compose, "NEWS_FIXTURE_MODE") || strings.Contains(compose, "NEWS_TEST_TELEGRAM_ENDPOINT") {
		t.Fatal("fixture Compose exposes a host port or real provider endpoint")
	}
}

func TestFailureDiagnosticsUseOnlyFiniteCodes(t *testing.T) {
	for input, want := range map[string]string{
		"dependency failed to start: executor": "service_dependency_failed",
		"container is unhealthy":               "service_healthcheck_failed",
		"invalid compose file":                 "compose_configuration_invalid",
		"untrusted-secret-value":               "compose_operation_failed",
	} {
		if got := composeFailureCode(input); got != want {
			t.Errorf("composeFailureCode(%q) = %q, want %q", input, got, want)
		}
	}
	for input, want := range map[string]string{
		"password authentication failed for role": "database_authentication_failed",
		"permission denied for schema news":       "database_permission_denied",
		"invalid runtime configuration":           "service_configuration_invalid",
		"untrusted-secret-value":                  "service_exit_unclassified",
	} {
		if got := startupLogHint(input); got != want {
			t.Errorf("startupLogHint(%q) = %q, want %q", input, got, want)
		}
	}
	for _, input := range []string{"running", "exited", "dead", "secret-value"} {
		got := safeStateToken(input)
		if got == input && input == "secret-value" {
			t.Fatalf("unsafe state token passed through: %q", got)
		}
	}
	if got := finiteHTTPFailure("legacy_article", 404); got != "legacy_article_status_404" {
		t.Fatalf("finite HTTP status classification = %q", got)
	}
	if got := httpReplyHint([]byte(`{"detail":"permission denied for schema news"}`)); got != "http_error_database_permission" {
		t.Fatalf("finite HTTP body classification = %q", got)
	}
	if got := httpReplyHint([]byte(`{"detail":"some-secret-value"}`)); got != "http_error_unclassified" {
		t.Fatalf("untrusted HTTP body was not reduced to a finite code: %q", got)
	}
	if got := adoptionPreflightHint("unsupported runtime key", errors.New("operation failed")); got != "adoption_unsupported_runtime_key" {
		t.Fatalf("adoption failure classification = %q", got)
	}
	if got := adoptionPreflightHint("some-secret-value", errors.New("operation failed")); got != "adoption_preflight_unclassified" {
		t.Fatalf("untrusted adoption output was not reduced to a finite code: %q", got)
	}
	if got := adoptionPreflightHint("managed additive migration unavailable; verify schema ownership", errors.New("operation failed")); got != "adoption_migration_unavailable" {
		t.Fatalf("migration failure classification = %q", got)
	}
	if got := safeNewsctlErrorHint("info\nnewsctl: managed adoption is missing runtime grants; review and apply: GRANT SELECT ON secret_table"); got != "adoption_error_managed_adoption_is_missing_runtime_grants" {
		t.Fatalf("newsctl finite error hint = %q", got)
	}
	if got := safeNewsctlErrorHint("newsctl: invalid configuration https://secret.example/path and 'private' value"); got != "adoption_error_invalid_configuration" {
		t.Fatalf("newsctl diagnostic leaked or retained unsafe values: %q", got)
	}
}

func TestFixtureSubnetCollisionCheck(t *testing.T) {
	for name, test := range map[string]struct {
		raw   string
		clash bool
	}{
		"no overlap":        {`[{"Subnet":"172.30.0.0/16"}]`, false},
		"same subnet":       {`[{"Subnet":"172.29.253.0/24"}]`, true},
		"containing subnet": {`[{"Subnet":"172.29.0.0/16"}]`, true},
		"contained subnet":  {`[{"Subnet":"172.29.253.64/26"}]`, true},
		"invalid inventory": {`not-json`, true},
	} {
		t.Run(name, func(t *testing.T) {
			if got := fixtureSubnetOverlaps([]byte(test.raw), fixtureSubnet); got != test.clash {
				t.Fatalf("fixture subnet overlap=%t, want %t", got, test.clash)
			}
		})
	}
}

func TestMissingGrantAdviceAcceptsOnlyTheScopedExpectedStatements(t *testing.T) {
	const prefix = "newsctl: managed adoption is missing runtime grants; review and apply these scoped grants before retrying: "
	valid := prefix + strings.Join(expectedOperatorGrantStatements(), "; ") + ";"
	if !validMissingGrantAdvice(valid) {
		t.Fatal("exact scoped grant advice rejected")
	}
	for name, advice := range map[string]string{
		"broad grant":   prefix + strings.Join(append(expectedOperatorGrantStatements(), `GRANT ALL ON SCHEMA "news" TO "news_web_runtime"`), "; ") + ";",
		"wrong role":    strings.Replace(valid, `TO "news_web_runtime"`, `TO "news_domain_runtime"`, 1),
		"missing grant": prefix + strings.Join(expectedOperatorGrantStatements()[1:], "; ") + ";",
		"other error":   "newsctl: database unavailable",
	} {
		t.Run(name, func(t *testing.T) {
			if validMissingGrantAdvice(advice) {
				t.Fatal("unexpected grant advice accepted")
			}
		})
	}
}
