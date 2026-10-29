package main

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/credentialstore"
	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func adoptionFixture() managedsetup.RenderedCompose {
	news := "postgres://role:pw@127.0.0.1:5432/news?search_path=public"
	services := map[string]map[string]string{
		"web":        {"DATABASE_URL": news, "ADMIN_USERNAME": "editor", "ADMIN_PASSWORD": "legacy-password", "JWT_SECRET": "legacy-signing", "WEB_BACKEND_URL": "http://executor", "WEB_BACKEND_TOKEN": "web-domain", "SCHEDULED_JOBS_TOKEN": "scheduled", "SCHEDULER_API_URL": "http://background", "SCHEDULER_API_TOKEN": "scheduler"},
		"executor":   {"DATABASE_URL": news, "AI_PROVIDER": "gemini", "GEMINI_API_KEY": "provider-secret", "TELEGRAM_TOKEN": "telegram-secret", "TELEGRAM_CHAT_ID": "-100123", "JOB_EXECUTOR_TOKEN": "job", "ARTICLE_AI_TOKEN": "article-ai", "EXTRACTION_SERVICE_URL": "http://extractor", "EXTRACTION_SERVICE_TOKEN": "extractor", "WEB_BACKEND_TOKEN": "web-domain"},
		"background": {"WORKER_DATABASE_URL": news, "EXECUTION_DATABASE_URL": news, "INGESTION_DATABASE_URL": news, "PROCESSING_DATABASE_URL": news, "SCHEDULER_DATABASE_URL": "postgres://sched:pw@127.0.0.1:5432/scheduler?search_path=scheduler_service", "SCHEDULER_DB_SCHEMA": "scheduler_service", "JOB_EXECUTOR_TOKEN": "job", "ARTICLE_AI_TOKEN": "article-ai", "ARTICLE_INGESTION_TOKEN": "ingestion", "ARTICLE_PROCESSING_TOKEN": "processing", "SCHEDULER_API_TOKEN": "scheduler", "SCHEDULER_TARGET_NEWS_WORKER_TOKEN": "scheduled", "EXTRACTION_SERVICE_TOKEN": "extractor"},
		"extractor":  {"EXTRACTOR_SERVICE_TOKEN": "extractor"},
	}
	return managedsetup.RenderedCompose{Services: services, Digest: strings.Repeat("a", 64)}
}

func TestAdoptionAllowlistMatchesRuntimeConsumers(t *testing.T) {
	root := filepath.Join("..", "..", "..")
	for role, rel := range map[string]string{"web": "go-web/internal/config/config.go", "domain": "go-domain/cmd/news-domain/runtime_config.go", "background": "go-background/cmd/news-background/runtime_config.go"} {
		f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, rel), nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		var actual []string
		ast.Inspect(f, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, lhs := range assign.Lhs {
				id, ok := lhs.(*ast.Ident)
				if !ok || id.Name != "allowed" || i >= len(assign.Rhs) {
					continue
				}
				lit, ok := assign.Rhs[i].(*ast.CompositeLit)
				if !ok {
					continue
				}
				for _, elt := range lit.Elts {
					if b, ok := elt.(*ast.BasicLit); ok {
						v, e := strconv.Unquote(b.Value)
						if e == nil {
							actual = append(actual, v)
						}
					}
				}
			}
			return true
		})
		if !sameSet(actual, adoptionPolicies[role].Allowed) {
			t.Errorf("%s adoption allowlist drifted from runtime reader", role)
		}
	}
	f, err := parser.ParseFile(token.NewFileSet(), filepath.Join(root, "go/runtimefile/lookup.go"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var extractor []string
	ast.Inspect(f, func(n ast.Node) bool {
		c, ok := n.(*ast.CaseClause)
		if !ok || len(c.List) != 1 {
			return true
		}
		lit, ok := c.List[0].(*ast.BasicLit)
		if !ok {
			return true
		}
		name, e := strconv.Unquote(lit.Value)
		if e != nil || name != "extractor" {
			return true
		}
		for _, stmt := range c.Body {
			ret, ok := stmt.(*ast.ReturnStmt)
			if !ok || len(ret.Results) < 1 {
				continue
			}
			values, ok := ret.Results[0].(*ast.CompositeLit)
			if !ok {
				continue
			}
			for _, elt := range values.Elts {
				if b, ok := elt.(*ast.BasicLit); ok {
					v, e := strconv.Unquote(b.Value)
					if e == nil {
						extractor = append(extractor, v)
					}
				}
			}
		}
		return true
	})
	if !sameSet(extractor, adoptionPolicies["extractor"].Allowed) {
		t.Error("extractor adoption allowlist drifted from runtime reader")
	}
}

func sameSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := map[string]bool{}
	for _, v := range a {
		set[v] = true
	}
	for _, v := range b {
		if !set[v] {
			return false
		}
	}
	return true
}

func TestAdoptionImportPostgresFixture(t *testing.T) {
	dsn := os.Getenv("NEWS_INSTALLER_TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("dedicated installer PostgreSQL fixture not configured")
	}
	u, err := url.Parse(dsn)
	if err != nil || u.Hostname() != "127.0.0.1" || u.Path != "/news_fixture" {
		t.Fatal("adoption fixture must be dedicated loopback news_fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	// Recover the one fixture role left by an interrupted test invocation.
	// The name is reserved exclusively by this isolated test fixture.
	roleName := "news_offline_fixture"
	var staleRole bool
	if err = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolname=$1)`, roleName).Scan(&staleRole); err != nil {
		t.Fatal("could not inspect offline-check fixture role")
	}
	if staleRole {
		if _, err = admin.Exec(ctx, "REVOKE USAGE ON SCHEMA public FROM "+pgx.Identifier{roleName}.Sanitize()); err != nil {
			t.Fatal("could not clean interrupted offline-check fixture role")
		}
		if _, err = admin.Exec(ctx, "DROP ROLE "+pgx.Identifier{roleName}.Sanitize()); err != nil {
			t.Fatal("could not clean interrupted offline-check fixture role")
		}
	}
	var busy bool
	if err = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='news_adopt_fixture')`).Scan(&busy); err != nil || busy {
		t.Fatal("adoption fixture schema must not exist")
	}
	schema := "news_adopt_fixture"
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err = admin.Exec(ctx, `CREATE SCHEMA `+ident); err != nil {
		t.Fatal("isolated schema creation failed")
	}
	defer func() {
		if _, e := admin.Exec(context.Background(), `DROP SCHEMA `+ident+` CASCADE`); e != nil {
			t.Error("fixture schema cleanup failed")
		}
	}()
	for _, q := range []string{
		`CREATE TABLE ` + ident + `.installation_state(id smallint primary key,installation_id text,revision bigint,provisioning_complete boolean,maintenance_mode boolean,onboarding_step text,onboarding_completed boolean,updated_at timestamptz default now())`,
		`CREATE TABLE ` + ident + `.managed_admin(id smallint primary key,username text,password_hash text,revision bigint)`,
		`CREATE TABLE ` + ident + `.integration_credentials(provider text primary key,revision bigint,envelope_version integer,key_id text,nonce bytea,ciphertext bytea)`,
		`CREATE TABLE ` + ident + `.settings(key text primary key,value jsonb)`,
	} {
		if _, err = admin.Exec(ctx, q); err != nil {
			t.Fatal("isolated adoption tables failed")
		}
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatal(err)
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if _, err = admin.Exec(ctx, "CREATE ROLE "+pgx.Identifier{roleName}.Sanitize()+" LOGIN PASSWORD 'fixture-only-password'"); err != nil {
		t.Fatal("could not create offline-check role")
	}
	defer func() {
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS news_scheduler_other CASCADE`)
		_, _ = admin.Exec(context.Background(), `DROP SCHEMA IF EXISTS scheduler_service CASCADE`)
		// This fixture grants USAGE on its isolated schema to exercise runtime
		// parity. Revoke that dependency before dropping the fixture role.
		_, _ = admin.Exec(context.Background(), "REVOKE USAGE ON SCHEMA "+ident+" FROM "+pgx.Identifier{roleName}.Sanitize())
		_, _ = admin.Exec(context.Background(), "REVOKE USAGE ON SCHEMA public FROM "+pgx.Identifier{roleName}.Sanitize())
		if _, e := admin.Exec(context.Background(), "DROP ROLE "+pgx.Identifier{roleName}.Sanitize()); e != nil {
			t.Error("offline-check role cleanup failed")
		}
	}()
	runtimeURL, err := url.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	runtimeURL.User = url.UserPassword(roleName, "fixture-only-password")
	query := runtimeURL.Query()
	query.Set("search_path", schema)
	runtimeURL.RawQuery = query.Encode()
	runtimePool, err := pgxpool.New(ctx, runtimeURL.String())
	if err != nil {
		t.Fatal("offline-check runtime connection failed")
	}
	if err = runtimePool.Ping(ctx); err != nil {
		runtimePool.Close()
		t.Fatal("offline-check runtime connection failed")
	}
	var schedulerSchemaExists bool
	if err = admin.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='scheduler_service')`).Scan(&schedulerSchemaExists); err != nil || schedulerSchemaExists {
		t.Fatal("scheduler ownership fixture requires an unused scheduler_service schema")
	}
	if _, err = admin.Exec(ctx, `CREATE SCHEMA scheduler_service AUTHORIZATION `+pgx.Identifier{roleName}.Sanitize()); err != nil {
		t.Fatal("could not create scheduler ownership fixture schema")
	}
	if _, err = runtimePool.Exec(ctx, `CREATE TABLE scheduler_service.toast_fixture(payload text)`); err != nil {
		t.Fatal("could not create scheduler-owned fixture table")
	}
	if _, err = runtimePool.Exec(ctx, `INSERT INTO scheduler_service.toast_fixture VALUES ((SELECT string_agg(md5(random()::text), '') FROM generate_series(1,2000)))`); err != nil {
		t.Fatal("could not create scheduler-owned fixture payload")
	}
	var toastPresent bool
	if err = runtimePool.QueryRow(ctx, `SELECT reltoastrelid<>0 FROM pg_class WHERE oid='scheduler_service.toast_fixture'::regclass`).Scan(&toastPresent); err != nil || !toastPresent {
		t.Fatal("scheduler fixture did not create a TOAST relation")
	}
	if owns, e := adoptionRoleOwnsForbiddenObjects(ctx, runtimePool, true); e != nil || owns {
		t.Fatalf("scheduler-owned TOAST objects were treated as foreign ownership: owns=%t err=%v", owns, e)
	}
	if _, err = admin.Exec(ctx, `CREATE SCHEMA news_scheduler_other AUTHORIZATION `+pgx.Identifier{roleName}.Sanitize()); err != nil {
		t.Fatal("could not create unrelated ownership fixture schema")
	}
	if owns, e := adoptionRoleOwnsForbiddenObjects(ctx, runtimePool, true); e != nil || !owns {
		t.Fatalf("scheduler-owned unrelated schema was not rejected: owns=%t err=%v", owns, e)
	}
	offlineRoles := map[string]map[string]string{
		"web":        {"DATABASE_URL": runtimeURL.String()},
		"domain":     {"DATABASE_URL": runtimeURL.String()},
		"background": {"WORKER_DATABASE_URL": runtimeURL.String(), "EXECUTION_DATABASE_URL": runtimeURL.String(), "INGESTION_DATABASE_URL": runtimeURL.String(), "PROCESSING_DATABASE_URL": runtimeURL.String(), "SCHEDULER_DATABASE_URL": runtimeURL.String()},
	}
	if err = requireAdoptionOffline(ctx, offlineRoles); err == nil {
		runtimePool.Close()
		t.Fatal("active runtime database session accepted")
	}
	runtimePool.Close()
	if err = requireAdoptionOffline(ctx, offlineRoles); err != nil {
		t.Fatalf("stopped runtime role was not accepted: %v", err)
	}
	if _, err = admin.Exec(ctx, "GRANT USAGE ON SCHEMA public TO "+pgx.Identifier{roleName}.Sanitize()); err != nil {
		t.Fatal("could not configure schema parity fixture")
	}
	if _, err = admin.Exec(ctx, `CREATE TABLE `+ident+`.setup_tokens(id smallint primary key,token_hash bytea,expires_at timestamptz,consumed_at timestamptz)`); err != nil {
		t.Fatal("could not create post-migration managed table fixture")
	}
	if _, err = admin.Exec(ctx, "GRANT USAGE ON SCHEMA "+ident+" TO "+pgx.Identifier{roleName}.Sanitize()); err != nil {
		t.Fatal("could not grant fixture schema usage")
	}
	if err = verifyManagedRuntimeGrants(ctx, map[string]map[string]string{
		"web": {"DATABASE_URL": runtimeURL.String()}, "domain": {"DATABASE_URL": runtimeURL.String()},
	}); err == nil || !strings.Contains(err.Error(), "missing runtime grants") || !strings.Contains(err.Error(), `GRANT SELECT ON "news_adopt_fixture"."managed_admin"`) {
		t.Fatalf("missing managed grants did not fail with scoped operator guidance: %v", err)
	}
	splitRoles := map[string]map[string]string{
		"web":        {"DATABASE_URL": runtimeURL.String()},
		"domain":     {"DATABASE_URL": runtimeURL.String()},
		"background": {"WORKER_DATABASE_URL": runtimeURL.String(), "EXECUTION_DATABASE_URL": runtimeURL.String(), "INGESTION_DATABASE_URL": runtimeURL.String(), "PROCESSING_DATABASE_URL": runtimeURL.String(), "SCHEDULER_DATABASE_URL": runtimeURL.String()},
	}
	workerURL, err := url.Parse(runtimeURL.String())
	if err != nil {
		t.Fatal(err)
	}
	workerQuery := workerURL.Query()
	workerQuery.Set("search_path", "public,"+schema)
	workerURL.RawQuery = workerQuery.Encode()
	splitRoles["background"]["WORKER_DATABASE_URL"] = workerURL.String()
	if err = verifyAdoptionSchemaParity(ctx, dsn, splitRoles); err == nil {
		t.Fatal("worker role resolving a different current schema was accepted")
	}
	key := make([]byte, 32)
	if _, err = rand.Read(key); err != nil {
		t.Fatal(err)
	}
	id := strings.Repeat("c", 32)
	keyID := base64.RawURLEncoding.EncodeToString([]byte("fixture-key-id"))
	envelope, err := credentialstore.Seal(key, keyID, id, "gemini", []byte("fixture-provider-secret"))
	if err != nil {
		t.Fatal(err)
	}
	secrets := adoptionSecrets{AdminUsername: "fixture-admin", AdminHash: "$argon2id$fixture", Credentials: map[string]credentialstore.Envelope{"gemini": envelope}}
	if _, err = pool.Exec(ctx, `INSERT INTO settings(key,value) VALUES('ai_settings','{"active_provider":"openai","profiles":{}}'::jsonb),('telegram_chat_id','"-100-existing"'::jsonb),('telegram_service_chat_id','""'::jsonb)`); err != nil {
		t.Fatal(err)
	}
	domain := map[string]string{"AI_PROVIDER": "gemini", "SUMMARIZATION_MODEL": "fixture-model", "TELEGRAM_CHAT_ID": "-100123", "TELEGRAM_SERVICE_CHAT_ID": "-100-service"}
	background := map[string]string{"NEWS_LIMIT_ENABLED": "true", "NEWS_LIMIT_MAX_ARTICLES": "42", "NEWS_LIMIT_PER_SOURCE": "7", "NEWS_LIMIT_DAYS": "3", "NEWS_LIMIT_NEWEST_DATE": "2026-10-29"}
	if err = importAdoption(ctx, pool, id, secrets, domain, background); err != nil {
		t.Fatal(err)
	}
	if err = importAdoption(ctx, pool, id, secrets, domain, background); err != nil {
		t.Fatalf("idempotent import: %v", err)
	}
	var username, hash string
	var revision int64
	if err = pool.QueryRow(ctx, `SELECT username,password_hash,revision FROM managed_admin WHERE id=1`).Scan(&username, &hash, &revision); err != nil || username != "fixture-admin" || hash != secrets.AdminHash || revision != 1 {
		t.Fatal("managed admin import mismatch")
	}
	var got []byte
	var gotID string
	if err = pool.QueryRow(ctx, `SELECT key_id,ciphertext FROM integration_credentials WHERE provider='gemini'`).Scan(&gotID, &got); err != nil || gotID != keyID || len(got) == 0 {
		t.Fatal("credential envelope not imported")
	}
	var aiRaw, chatRaw []byte
	if err = pool.QueryRow(ctx, `SELECT value FROM settings WHERE key='ai_settings'`).Scan(&aiRaw); err != nil {
		t.Fatal("effective AI settings missing")
	}
	if !strings.Contains(string(aiRaw), `"active_provider": "openai"`) {
		t.Fatal("existing AI settings override was not preserved")
	}
	if err = pool.QueryRow(ctx, `SELECT value FROM settings WHERE key='telegram_chat_id'`).Scan(&chatRaw); err != nil || string(chatRaw) != `"-100-existing"` {
		t.Fatal("existing Telegram database override was not preserved")
	}
	if err = pool.QueryRow(ctx, `SELECT value FROM settings WHERE key='telegram_service_chat_id'`).Scan(&chatRaw); err != nil || string(chatRaw) != `"-100-service"` {
		t.Fatal("empty Telegram database override did not retain the environment fallback")
	}
	var maxArticles int
	if err = pool.QueryRow(ctx, `SELECT (value #>> '{}')::integer FROM settings WHERE key='operational_max_articles'`).Scan(&maxArticles); err != nil || maxArticles != 42 {
		t.Fatal("operational collection limit was not imported")
	}
}

func TestAdoptionPlanPreservesRuntimeAndSeparatesSecrets(t *testing.T) {
	p, err := adoptionPlanFromCompose(adoptionFixture(), strings.Repeat("1", 32))
	if err != nil {
		t.Fatal(err)
	}
	if p.roles["web"]["JWT_SECRET"] != "legacy-signing" || p.roles["web"]["ADMIN_USERNAME"] != "editor" {
		t.Fatal("web parity values lost")
	}
	if p.roles["domain"]["GEMINI_API_KEY"] != "" || p.roles["domain"]["TELEGRAM_TOKEN"] != "" || p.roles["web"]["ADMIN_PASSWORD"] != "" {
		t.Fatal("legacy secrets leaked into role manifests")
	}
	if p.legacy["executor"]["GEMINI_API_KEY"] != "provider-secret" {
		t.Fatal("in-process credential import source missing")
	}
}

func TestAdoptionParityNormalizesAsyncPGOnlyForValidation(t *testing.T) {
	c := adoptionFixture()
	for role, key := range map[string]string{"web": "DATABASE_URL", "executor": "DATABASE_URL", "background": "WORKER_DATABASE_URL"} {
		c.Services[role][key] = strings.Replace(c.Services[role][key], "postgres://", "postgresql+asyncpg://", 1)
	}
	for _, key := range []string{"EXECUTION_DATABASE_URL", "INGESTION_DATABASE_URL", "PROCESSING_DATABASE_URL"} {
		c.Services["background"][key] = strings.Replace(c.Services["background"][key], "postgres://", "postgresql+asyncpg://", 1)
	}
	p, err := adoptionPlanFromCompose(c, strings.Repeat("2", 32))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(p.roles["domain"]["DATABASE_URL"], "postgresql+asyncpg://") {
		t.Fatal("runtime DSN was rewritten instead of preserved")
	}
	c.Services["background"]["SCHEDULER_DB_SCHEMA"] = "news"
	if _, err = adoptionPlanFromCompose(c, strings.Repeat("3", 32)); err == nil {
		t.Fatal("scheduler schema mismatch accepted")
	}
}

func TestAdoptionPlanResolvesLegacyDefaults(t *testing.T) {
	c := adoptionFixture()
	delete(c.Services["web"], "JWT_SECRET")
	delete(c.Services["executor"], "AI_PROVIDER")
	p, err := adoptionPlanFromCompose(c, strings.Repeat("1", 32))
	if err != nil {
		t.Fatal(err)
	}
	if p.roles["web"]["JWT_SECRET"] != "legacy-password" {
		t.Fatal("legacy JWT fallback was not preserved")
	}
	if p.roles["web"]["ADMIN_USERNAME"] != "editor" || p.roles["domain"]["AI_PROVIDER"] != "gemini" {
		t.Fatal("effective legacy defaults changed")
	}
}

func TestAdoptionPlanRejectsTokenAndDatabaseDrift(t *testing.T) {
	c := adoptionFixture()
	c.Services["background"]["SCHEDULER_TARGET_NEWS_WORKER_TOKEN"] = "mismatch"
	if _, err := adoptionPlanFromCompose(c, strings.Repeat("1", 32)); err == nil {
		t.Fatal("accepted purpose-token mismatch")
	}
	c = adoptionFixture()
	c.Services["background"]["PROCESSING_DATABASE_URL"] = "postgres://other:pw@other:5432/news?search_path=public"
	if _, err := adoptionPlanFromCompose(c, strings.Repeat("1", 32)); err == nil {
		t.Fatal("accepted split News database")
	}
}

func TestValidateAdoptionWebTransport(t *testing.T) {
	tests := []struct {
		name string
		web  map[string]string
		port bool
		want bool
	}{
		{name: "hosted https with exact proxy", web: map[string]string{"SITE_BASE_URL": "https://news.example.invalid", "TRUSTED_PROXY_CIDRS": "172.20.0.2/32"}},
		{name: "hosted https without proxy", web: map[string]string{"SITE_BASE_URL": "https://news.example.invalid"}, want: true},
		{name: "hosted http", web: map[string]string{"SITE_BASE_URL": "http://news.example.invalid", "TRUSTED_PROXY_CIDRS": "172.20.0.2/32"}, want: true},
		{name: "invalid proxy", web: map[string]string{"SITE_BASE_URL": "https://news.example.invalid", "TRUSTED_PROXY_CIDRS": "0.0.0.0/0"}, want: true},
		{name: "loopback without explicit profile", web: map[string]string{"SITE_BASE_URL": "http://127.0.0.1:8000"}, port: true, want: true},
		{name: "loopback profile without port binding", web: map[string]string{"SITE_BASE_URL": "http://127.0.0.1:8000", "LOCAL_HTTP_SETUP": "true"}, want: true},
		{name: "loopback explicit bound profile", web: map[string]string{"SITE_BASE_URL": "http://127.0.0.1:8000", "LOCAL_HTTP_SETUP": "true"}, port: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := adoptionPlan{roles: map[string]map[string]string{"web": tt.web}, localHTTPPortsOK: tt.port}
			err := validateAdoptionWebTransport(plan)
			if (err != nil) != tt.want {
				t.Fatalf("validateAdoptionWebTransport() error=%v wantErr=%t", err, tt.want)
			}
		})
	}
}

func TestAdoptApplyRejectsHostedMissingProxyBeforeDatabaseOrStaging(t *testing.T) {
	compose := adoptionFixture()
	compose.Services["web"]["SITE_BASE_URL"] = "https://news.example.invalid"
	services := map[string]any{}
	for name, environment := range compose.Services {
		services[name] = map[string]any{"environment": environment}
	}
	raw, err := json.Marshal(map[string]any{"services": services})
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	configPath := filepath.Join(root, "rendered-compose.json")
	if err = os.WriteFile(configPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	setup := filepath.Join(root, "setup")
	err = adoptionCommand(context.Background(), managedsetup.Layout{Setup: setup}, []string{"--config", configPath, "--apply", "--bootstrap-input", filepath.Join(root, "missing-bootstrap.json"), "--compose-output", filepath.Join(root, "override.yml")})
	if err == nil || !strings.Contains(err.Error(), "trusted proxy CIDRs") {
		t.Fatalf("apply did not fail on transport readiness before database work: %v", err)
	}
	if _, statErr := os.Lstat(setup); !os.IsNotExist(statErr) {
		t.Fatal("failed transport preflight created adoption state")
	}
}

func TestNativeAdoptionPreservesCurrentCloakContract(t *testing.T) {
	compose := func(t *testing.T, listen string) managedsetup.RenderedCompose {
		t.Helper()
		fixture := adoptionFixture()
		app := fixture.Services["web"]
		delete(fixture.Services, "web")
		app["BROWSER_WS_ENDPOINT"] = "ws://10.77.0.2:9222"
		app["BROWSER_RESTART_MARKER_PATH"] = "/browser-control/restart.request"
		app["BROWSER_PROXY_ENABLED"] = "true"
		app["BROWSER_PROXY_URL"] = "http://10.77.0.5:8081"
		app["BROWSER_PROXY_TOKEN"] = "fixture-proxy-token-01"
		app["EXTRACTION_SERVICE_URL"] = "http://10.77.0.4:8080"
		app["EXTRACTION_SERVICE_TOKEN"] = "extractor"
		app["EXTRACTION_SERVICE_MODE"] = "go"
		app["EXTRACTION_SERVICE_NATIVE_BROWSER"] = "true"
		app["EXTRACTION_SERVICE_PYTHON_FALLBACK"] = "false"
		app["TELEGRAM_SOURCE_MODE"] = "go"
		app["PAGE_MONITOR_SOURCE_MODE"] = "go"
		extractor := map[string]string{"EXTRACTOR_SERVICE_TOKEN": "extractor", "LISTEN_ADDR": ":8080", "EXTRACTOR_HTTP_ONLY": "false", "EXTRACTOR_BROWSER_BACKEND": "native", "EXTRACTOR_NATIVE_NETWORK_ISOLATED": "true", "CHROME_ENDPOINT": "http://10.77.0.2:9222", "EXTRACTOR_NATIVE_PROXY_URL": "http://10.77.0.5:8081", "EXTRACTOR_NATIVE_PROXY_TOKEN": "fixture-proxy-token-01", "EXTRACTOR_MAX_CONCURRENT": "2", "EXTRACTOR_BROWSER_CONCURRENT": "2"}
		egress := map[string]string{"EGRESS_LISTEN_ADDR": listen, "EGRESS_TOKEN": "fixture-proxy-token-01", "EGRESS_MAX_CONCURRENT": "4"}
		limits := map[string]any{"init": true, "read_only": true, "cap_drop": []string{"ALL"}, "security_opt": []string{"no-new-privileges:true"}, "pids_limit": 128, "mem_limit": "268435456", "cpus": 1, "stop_grace_period": "55s"}
		egressLimits := map[string]any{"init": true, "read_only": true, "cap_drop": []string{"ALL"}, "security_opt": []string{"no-new-privileges:true"}, "pids_limit": 64, "mem_limit": "134217728", "cpus": 0.25, "stop_grace_period": "10s"}
		services := map[string]any{
			"app":        map[string]any{"environment": app},
			"executor":   map[string]any{"environment": fixture.Services["executor"]},
			"background": map[string]any{"environment": fixture.Services["background"]},
			"extractor":  map[string]any{"environment": extractor, "networks": map[string]any{"private-control": map[string]string{"ipv4_address": "10.77.0.4"}, "outbound": map[string]any{}}},
			"egress":     map[string]any{"environment": egress, "networks": map[string]any{"private-control": map[string]string{"ipv4_address": "10.77.0.5"}, "outbound": map[string]any{}}},
			"migrate":    map[string]any{"environment": map[string]string{"DATABASE_URL": fixture.Services["executor"]["DATABASE_URL"]}},
		}
		for key, value := range limits {
			services["extractor"].(map[string]any)[key] = value
		}
		for key, value := range egressLimits {
			services["egress"].(map[string]any)[key] = value
		}
		raw, err := json.Marshal(map[string]any{"services": services, "networks": map[string]any{
			"private-control": map[string]any{"name": "shared-control", "external": true},
			"outbound":        map[string]any{"name": "shared-outbound"},
		}})
		if err != nil {
			t.Fatal(err)
		}
		path := filepath.Join(t.TempDir(), "compose.json")
		if err = os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		compose, err := managedsetup.ReadRenderedCompose(path)
		if err != nil {
			t.Fatal(err)
		}
		return compose
	}
	c := compose(t, "10.77.0.5:8081")
	p, err := adoptionPlanFromCompose(c, strings.Repeat("4", 32))
	if err != nil {
		t.Fatal(err)
	}
	if p.services["web"] != "app" || p.roles["extractor"]["EXTRACTOR_BROWSER_CONCURRENT"] != "2" || p.roles["extractor"]["EXTRACTOR_NATIVE_NETWORK_ISOLATED"] != "true" || p.roles["egress"]["EGRESS_TOKEN"] == "" {
		t.Fatal("native browser runtime settings were not carried into protected roles")
	}
	override, err := managedsetup.AdoptionComposeOverride(p.services)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(override), "  app:") || !strings.Contains(string(override), "  egress:") || !strings.Contains(string(override), "entrypoint: !override") || strings.Contains(string(override), "  chrome:") || strings.Contains(string(override), "  readeck:") {
		t.Fatal("generated override changed or omitted the expected News-only services")
	}
	if _, err = adoptionPlanFromCompose(compose(t, "10.77.0.6:8081"), strings.Repeat("5", 32)); err == nil {
		t.Fatal("native adoption accepted proxy/listener address mismatch")
	}
}
