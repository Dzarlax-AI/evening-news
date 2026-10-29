package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-domain/migration"
	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestReadExternalInputRequiresPrivateStrictBundle(t *testing.T) {
	roles := `"runtime_dsns":{"web":"postgres://web:p@db/news?search_path=news","domain":"postgres://domain:p@db/news?search_path=news","worker":"postgres://worker:p@db/news?search_path=news","execution":"postgres://execution:p@db/news?search_path=news","ingestion":"postgres://ingestion:p@db/news?search_path=news","processing":"postgres://processing:p@db/news?search_path=news","scheduler":"postgres://scheduler:p@db/scheduler?search_path=scheduler_service"}`
	path := filepath.Join(t.TempDir(), "input.json")
	data := `{"version":1,"bootstrap_dsn":"postgres://bootstrap:p@db/news?search_path=news",` + roles + `}`
	if err := os.WriteFile(path, []byte(data), 0600); err != nil {
		t.Fatal(err)
	}
	input, digest, err := readExternalInput(path)
	if err != nil {
		t.Fatal(err)
	}
	if input.Version != 1 || input.RuntimeDSNs["scheduler"] == "" || len(digest) != 64 {
		t.Fatal("valid external input not parsed")
	}
	if err := os.Chmod(path, 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, err = readExternalInput(path); err == nil {
		t.Fatal("group-readable input accepted")
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(strings.Replace(data, `"version":1`, `"version":1,"unexpected":"x"`, 1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, _, err = readExternalInput(path); err == nil {
		t.Fatal("unknown input field accepted")
	}
}

func TestExternalInstallPostgresFixture(t *testing.T) {
	adminDSN := os.Getenv("NEWS_INSTALLER_TEST_DATABASE_URL")
	if adminDSN == "" {
		t.Skip("dedicated installer PostgreSQL fixture not configured")
	}
	adminURL, err := url.Parse(adminDSN)
	if err != nil || adminURL.Hostname() != "127.0.0.1" || adminURL.Path != "/news_fixture" {
		t.Fatal("external fixture requires disposable loopback news_fixture")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	admin, err := pgx.Connect(ctx, adminDSN)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close(ctx)
	random := make([]byte, 5)
	if _, err = rand.Read(random); err != nil {
		t.Fatal(err)
	}
	suffix := hex.EncodeToString(random)
	newsDB := "news_external_" + suffix
	schedulerDB := "sched_external_" + suffix
	roles := map[string]string{"web": "nxw_" + suffix, "domain": "nxd_" + suffix, "worker": "nxwk_" + suffix, "execution": "nxe_" + suffix, "ingestion": "nxi_" + suffix, "processing": "nxp_" + suffix, "scheduler": "nxs_" + suffix, "ingestion_fence": "nxif_" + suffix, "processing_fence": "nxpf_" + suffix}
	const password = "fixture-only-password"
	createdRoles := []string{}
	createdDBs := []string{}
	cleanup := func() {
		for _, name := range createdDBs {
			_, _ = admin.Exec(context.Background(), "DROP DATABASE "+pgx.Identifier{name}.Sanitize()+" WITH (FORCE)")
		}
		for _, name := range createdRoles {
			_, _ = admin.Exec(context.Background(), "DROP ROLE "+pgx.Identifier{name}.Sanitize())
		}
	}
	defer cleanup()
	for key, name := range roles {
		login := key != "ingestion_fence" && key != "processing_fence"
		ddl := "CREATE ROLE " + pgx.Identifier{name}.Sanitize() + " NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS"
		if login {
			ddl += " LOGIN PASSWORD '" + password + "'"
		}
		if _, err = admin.Exec(ctx, ddl); err != nil {
			t.Fatalf("fixture role create failed: %s", key)
		}
		createdRoles = append(createdRoles, name)
	}
	createDB := func(name string) {
		if _, e := admin.Exec(ctx, "CREATE DATABASE "+pgx.Identifier{name}.Sanitize()); e != nil {
			t.Fatalf("fixture database create failed: %s", name)
		}
		createdDBs = append(createdDBs, name)
	}
	createDB(newsDB)
	createDB(schedulerDB)
	if _, err = admin.Exec(ctx, "REVOKE CONNECT ON DATABASE "+pgx.Identifier{newsDB}.Sanitize()+" FROM PUBLIC"); err != nil {
		t.Fatal(err)
	}
	if _, err = admin.Exec(ctx, "REVOKE CONNECT ON DATABASE "+pgx.Identifier{schedulerDB}.Sanitize()+" FROM PUBLIC"); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"web", "domain", "worker", "execution", "ingestion", "processing"} {
		if _, err = admin.Exec(ctx, "GRANT CONNECT ON DATABASE "+pgx.Identifier{newsDB}.Sanitize()+" TO "+pgx.Identifier{roles[role]}.Sanitize()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = admin.Exec(ctx, "GRANT CONNECT ON DATABASE "+pgx.Identifier{schedulerDB}.Sanitize()+" TO "+pgx.Identifier{roles["scheduler"]}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	bootstrapPassword, _ := adminURL.User.Password()
	newsAdminURL := databaseURL(adminURL, newsDB, adminURL.User.Username(), bootstrapPassword, "news")
	newsPool, err := pgxpool.New(ctx, newsAdminURL)
	if err != nil {
		t.Fatal(err)
	}
	defer newsPool.Close()
	if _, err = newsPool.Exec(ctx, `CREATE SCHEMA news`); err != nil {
		t.Fatal(err)
	}
	migrated, err := migration.Run(ctx, newsPool, true, false)
	if err != nil || len(migrated.Errors) != 0 {
		t.Fatal("fixture News migration failed")
	}
	grantSQL := managedsetup.GrantPolicy
	for old, key := range map[string]string{"news_web_runtime": "web", "news_domain_runtime": "domain", "news_worker_runtime": "worker", "news_execution_runtime": "execution", "news_ingestion_runtime": "ingestion", "news_processing_runtime": "processing", "news_ingestion_fence_owner": "ingestion_fence", "news_processing_fence_owner": "processing_fence"} {
		grantSQL = strings.ReplaceAll(grantSQL, old, roles[key])
	}
	if _, err = newsPool.Exec(ctx, grantSQL); err != nil {
		t.Fatal("fixture role grants failed")
	}
	schedulerAdminURL := databaseURL(adminURL, schedulerDB, adminURL.User.Username(), bootstrapPassword, "scheduler_service")
	schedulerAdmin, err := pgxpool.New(ctx, schedulerAdminURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = schedulerAdmin.Exec(ctx, `CREATE SCHEMA scheduler_service`); err != nil {
		schedulerAdmin.Close()
		t.Fatal(err)
	}
	if _, err = schedulerAdmin.Exec(ctx, "GRANT USAGE ON SCHEMA scheduler_service TO "+pgx.Identifier{roles["scheduler"]}.Sanitize()); err != nil {
		schedulerAdmin.Close()
		t.Fatal(err)
	}
	schedulerAdmin.Close()
	input := externalFixtureInput(adminURL, newsDB, schedulerDB, roles, password)
	if err = validateExternalBundle(ctx, input); err == nil {
		t.Fatal("scheduler role without CREATE on its dedicated schema was accepted")
	}
	var identityRows int
	if err = newsPool.QueryRow(ctx, `SELECT count(*) FROM news.installation_state`).Scan(&identityRows); err != nil || identityRows != 0 {
		t.Fatal("scheduler privilege failure mutated installer identity")
	}
	schedulerAdmin, err = pgxpool.New(ctx, schedulerAdminURL)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = schedulerAdmin.Exec(ctx, "GRANT CREATE ON SCHEMA scheduler_service TO "+pgx.Identifier{roles["scheduler"]}.Sanitize()); err != nil {
		schedulerAdmin.Close()
		t.Fatal(err)
	}
	schedulerAdmin.Close()
	if err = validateExternalBundle(ctx, input); err != nil {
		t.Fatalf("positive external privilege preflight failed: %v", err)
	}
	if _, e := newsPool.Exec(ctx, "GRANT CONNECT ON DATABASE "+pgx.Identifier{newsDB}.Sanitize()+" TO "+pgx.Identifier{roles["scheduler"]}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	if _, e := newsPool.Exec(ctx, "GRANT USAGE ON SCHEMA news TO "+pgx.Identifier{roles["scheduler"]}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	if e := validateExternalBundle(ctx, input); e == nil {
		t.Fatal("scheduler News schema access accepted")
	}
	if _, e := newsPool.Exec(ctx, "REVOKE USAGE ON SCHEMA news FROM "+pgx.Identifier{roles["scheduler"]}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	if _, e := newsPool.Exec(ctx, "REVOKE CONNECT ON DATABASE "+pgx.Identifier{newsDB}.Sanitize()+" FROM "+pgx.Identifier{roles["scheduler"]}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	if _, e := newsPool.Exec(ctx, "GRANT SELECT ON news.managed_admin TO "+pgx.Identifier{roles["domain"]}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	if e := preflightExternalRuntimePrivileges(ctx, input); e == nil {
		t.Fatal("domain access to managed_admin accepted")
	}
	if _, e := newsPool.Exec(ctx, "REVOKE SELECT ON news.managed_admin FROM "+pgx.Identifier{roles["domain"]}.Sanitize()); e != nil {
		t.Fatal(e)
	}
	if _, err = newsPool.Exec(ctx, "REVOKE USAGE ON SEQUENCE news.articles_id_seq FROM "+pgx.Identifier{roles["ingestion"]}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if err = preflightExternalRuntimePrivileges(ctx, input); err == nil {
		t.Fatal("missing ingestion sequence grant accepted")
	}
	if _, err = newsPool.Exec(ctx, "GRANT USAGE ON SEQUENCE news.articles_id_seq TO "+pgx.Identifier{roles["ingestion"]}.Sanitize()); err != nil {
		t.Fatal(err)
	}
	if err = validateExternalBundle(ctx, input); err != nil {
		t.Fatalf("restored external privilege preflight failed: %v", err)
	}
	root := t.TempDir()
	layout := managedsetup.Layout{Setup: filepath.Join(root, "setup"), Web: filepath.Join(root, "web"), Domain: filepath.Join(root, "domain"), Background: filepath.Join(root, "background"), Extractor: filepath.Join(root, "extractor"), WebUID: -1, DomainUID: -1, BackgroundUID: -1, ExtractorUID: -1}
	for _, dir := range []string{layout.Setup, layout.Web, layout.Domain, layout.Background, layout.Extractor} {
		if err = os.Mkdir(dir, 0700); err != nil {
			t.Fatal(err)
		}
	}
	if err = installExternal(ctx, layout, "http://localhost:8000", managedsetup.Options{}, input); err != nil {
		t.Fatalf("positive external install failed: %v", err)
	}
	state, err := managedsetup.ReadState(layout.Setup)
	if err != nil {
		t.Fatal(err)
	}
	if err = managedsetup.VerifyExternalState(layout, state); err != nil {
		t.Fatal(err)
	}
	var id string
	if err = newsPool.QueryRow(ctx, `SELECT installation_id FROM news.installation_state WHERE id=1`).Scan(&id); err != nil || id != state.InstallationID {
		t.Fatal("external DB identity did not match committed state")
	}
}

func databaseURL(base *url.URL, database, user, password, schema string) string {
	u := *base
	u.Path = "/" + database
	u.User = url.UserPassword(user, password)
	q := u.Query()
	q.Set("sslmode", "disable")
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

func externalFixtureInput(base *url.URL, newsDB, schedulerDB string, roles map[string]string, password string) externalInput {
	makeURL := func(role, db, schema string) string { return databaseURL(base, db, roles[role], password, schema) }
	bootstrapPassword, _ := base.User.Password()
	return externalInput{Version: 1, BootstrapDSN: databaseURL(base, newsDB, base.User.Username(), bootstrapPassword, "news"), RuntimeDSNs: map[string]string{
		"web": makeURL("web", newsDB, "news"), "domain": makeURL("domain", newsDB, "news"), "worker": makeURL("worker", newsDB, "news"), "execution": makeURL("execution", newsDB, "news"), "ingestion": makeURL("ingestion", newsDB, "news"), "processing": makeURL("processing", newsDB, "news"), "scheduler": makeURL("scheduler", schedulerDB, "scheduler_service"),
	}}
}

func TestValidateExternalBundleEnforcesDatabaseBoundary(t *testing.T) {
	input := externalInput{BootstrapDSN: "postgres://bootstrap:p@db/news?search_path=news", RuntimeDSNs: map[string]string{
		"web": "postgres://web:p@db/news?search_path=news", "domain": "postgres://domain:p@db/news?search_path=news",
		"worker": "postgres://worker:p@db/news?search_path=news", "execution": "postgres://execution:p@db/news?search_path=news",
		"ingestion": "postgres://ingestion:p@db/news?search_path=news", "processing": "postgres://processing:p@db/news?search_path=news",
		"scheduler": "postgres://scheduler:p@db/scheduler?search_path=scheduler_service",
	}}
	if err := validateExternalBundleTargets(input); err != nil {
		t.Fatal(err)
	}
	input.RuntimeDSNs["worker"] = "postgres://worker:p@other/news?search_path=news"
	if err := validateExternalBundleTargets(input); err == nil {
		t.Fatal("split News database accepted")
	}
}

func TestParseExternalGrantPolicyCoversRuntimeMatrix(t *testing.T) {
	grants, err := parseExternalGrantPolicy(managedsetup.GrantPolicy)
	if err != nil {
		t.Fatal(err)
	}
	if len(grants) != 6 {
		t.Fatalf("expected six runtime grant maps, got %d", len(grants))
	}
	find := func(role, kind, object, privilege string, columns ...string) bool {
		for _, grant := range grants[role] {
			if grant.kind == kind && grant.object == object && grant.privilege == privilege && strings.Join(grant.columns, ",") == strings.Join(columns, ",") {
				return true
			}
		}
		return false
	}
	if !find("ingestion", "SEQUENCE", "articles_id_seq", "USAGE") {
		t.Fatal("ingestion articles sequence USAGE grant missing from parsed policy")
	}
	if !find("domain", "FUNCTION", "fence_article_processing_attempt(\n    UUID, UUID, INTEGER, TEXT, UUID, INTEGER, INTEGER, UUID, TIMESTAMP\n)", "EXECUTE") {
		t.Fatal("domain processing fence function grant missing from parsed policy")
	}
	if !find("execution", "TABLE", "articles", "UPDATE", "processing_state", "processing_attempts", "next_attempt_at", "last_processing_error", "processed", "summary_processed", "category_processed", "ad_processed") {
		t.Fatal("execution column-limited article update was not parsed")
	}
	for _, grant := range grants["ingestion"] {
		if grant.kind == "TABLE" && grant.object == "article_ingestion_runs" && grant.privilege == "INSERT" {
			t.Fatal("NOLOGIN fence-owner INSERT was incorrectly assigned to ingestion")
		}
	}
}
