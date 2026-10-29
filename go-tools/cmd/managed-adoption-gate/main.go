// managed-adoption-gate runs a bounded, isolated end-to-end legacy adoption
// exercise against the candidate News images.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-tools/managedsetup"
)

const (
	fixtureSubnet   = "172.29.253.0/24"
	fixturePassword = "fixture-only-password"
	fixtureAdmin    = "fixture-admin-password"
	maxRunTime      = 12 * time.Minute
	commandTimeout  = 90 * time.Second
)

type gate struct {
	project       string
	dir           string
	compose       string
	phase         string
	serviceStates []string
	startupHint   string
	ctx           context.Context
}

func main() {
	os.Exit(runMain())
}

func runMain() int {
	outputDir := flag.String("output-dir", "", "private directory for a sanitized acceptance receipt")
	major := flag.String("postgres-major", "17", "isolated PostgreSQL major version (16 or 17)")
	flag.Parse()
	if flag.NArg() != 0 || *outputDir == "" || (*major != "16" && *major != "17") {
		fmt.Fprintln(os.Stderr, "managed adoption gate failed: invalid arguments")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), maxRunTime)
	defer cancel()
	if err := os.MkdirAll(*outputDir, 0700); err != nil {
		fmt.Fprintln(os.Stderr, "managed adoption gate failed: output directory unavailable")
		return 1
	}
	g, err := newGate(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "managed adoption gate failed: private fixture unavailable")
		return 1
	}
	cleaned := false
	defer func() {
		if !cleaned {
			_ = g.cleanup()
		}
	}()
	g.phase = "prepare"
	if err = g.prepare(*major); err == nil {
		err = g.accept()
	}
	if err != nil {
		_ = writeFailureReceipt(*outputDir, *major, g.phase, safeReason(err), g.serviceStates, g.startupHint)
		fmt.Fprintf(os.Stderr, "managed adoption gate failed: %s\n", safeReason(err))
		return 1
	}
	g.phase = "cleanup"
	if err = g.cleanup(); err != nil {
		_ = writeFailureReceipt(*outputDir, *major, g.phase, "isolated fixture cleanup failed", nil, "")
		fmt.Fprintln(os.Stderr, "managed adoption gate failed: isolated fixture cleanup failed")
		return 1
	}
	cleaned = true
	receipt := map[string]any{
		"version": 1, "postgres_major": *major, "adoption": "passed",
		"legacy_services_started": true, "managed_services_restarted": true,
		"web_auth_preserved": true, "fixture_article_preserved": true,
		"legacy_receipt_preserved": true, "network_topology_retained": true,
		"external_source_ai_telegram_calls": 0,
	}
	raw, _ := json.MarshalIndent(receipt, "", "  ")
	if err = os.WriteFile(filepath.Join(*outputDir, "managed-adoption.json"), append(raw, '\n'), 0600); err != nil {
		fmt.Fprintln(os.Stderr, "managed adoption gate failed: acceptance receipt unavailable")
		return 1
	}
	fmt.Printf("managed adoption acceptance passed (PostgreSQL %s)\n", *major)
	return 0
}

func writeFailureReceipt(outputDir, major, phase, reason string, serviceStates []string, startupHint string) error {
	receipt := map[string]any{"version": "1", "postgres_major": major, "status": "failed", "phase": phase, "reason": reason}
	if len(serviceStates) > 0 {
		receipt["service_states"] = serviceStates
	}
	if startupHint != "" {
		receipt["startup_hint"] = startupHint
	}
	raw, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return errors.New("failure receipt encoding failed")
	}
	if err = os.WriteFile(filepath.Join(outputDir, "managed-adoption-failure.json"), append(raw, '\n'), 0600); err != nil {
		return errors.New("failure receipt unavailable")
	}
	return nil
}

func newGate(ctx context.Context) (*gate, error) {
	var id [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		return nil, errors.New("random fixture identity unavailable")
	}
	dir, err := os.MkdirTemp("", "news-managed-adoption-")
	if err != nil {
		return nil, errors.New("private fixture directory unavailable")
	}
	if err = os.Chmod(dir, 0700); err != nil {
		_ = os.RemoveAll(dir)
		return nil, errors.New("private fixture directory unavailable")
	}
	return &gate{project: "newsadopt" + hex.EncodeToString(id[:]), dir: dir, compose: filepath.Join(dir, "compose.yml"), ctx: ctx}, nil
}

func (g *gate) cleanup() error {
	// Compose's random project name scopes removal to resources created here.
	ctx, cancel := context.WithTimeout(context.Background(), commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "--project-name", g.project, "--file", g.compose, "down", "--volumes", "--remove-orphans")
	composeErr := cmd.Run()
	removeErr := os.RemoveAll(g.dir)
	if composeErr != nil || removeErr != nil {
		return errors.New("isolated fixture cleanup failed")
	}
	return nil
}

func (g *gate) composeRun(input []byte, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(g.ctx, commandTimeout)
	defer cancel()
	base := []string{"compose", "--project-name", g.project, "--file", g.compose}
	cmd := exec.CommandContext(ctx, "docker", append(base, args...)...)
	if input != nil {
		cmd.Stdin = bytes.NewReader(input)
	}
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return "", context.DeadlineExceeded
		}
		return "", errors.New(composeFailureCode(stdout.String() + "\n" + stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

func (g *gate) composeRunDiagnostic(input []byte, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(g.ctx, commandTimeout)
	defer cancel()
	base := []string{"compose", "--project-name", g.project, "--file", g.compose}
	cmd := exec.CommandContext(ctx, "docker", append(base, args...)...)
	if input != nil {
		cmd.Stdin = bytes.NewReader(input)
	}
	output, err := cmd.CombinedOutput()
	if len(output) > 64*1024 {
		output = output[:64*1024]
	}
	return string(output), err
}

func (g *gate) prepare(major string) error {
	if err := g.ensureFixtureSubnetAvailable(); err != nil {
		return err
	}
	var imagePlatform string
	for _, image := range []string{"evening-news-web:consolidation-fixture", "evening-news-executor:consolidation-fixture", "evening-news-background:consolidation-fixture", "evening-news-collector:consolidation-fixture"} {
		output, err := exec.CommandContext(g.ctx, "docker", "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", image).Output()
		if err != nil {
			return errors.New("candidate fixture image unavailable")
		}
		current := strings.TrimSpace(string(output))
		if imagePlatform != "" && current != imagePlatform {
			return errors.New("candidate fixture image platforms differ")
		}
		imagePlatform = current
	}
	arch := strings.TrimPrefix(imagePlatform, "linux/")
	if arch != "amd64" && arch != "arm64" {
		return errors.New("candidate fixture image platform is unsupported")
	}
	if _, err := exec.LookPath("go"); err != nil {
		return errors.New("Go toolchain unavailable for isolated HTTP probe")
	}
	probePath := filepath.Join(g.dir, "fixture-http")
	moduleDir, err := goToolsModuleDir()
	if err != nil {
		return err
	}
	build := exec.CommandContext(g.ctx, "go", "build", "-o", probePath, "./cmd/fixture-http")
	build.Dir = moduleDir
	build.Env = replaceEnv(os.Environ(), map[string]string{"GOOS": "linux", "GOARCH": arch, "CGO_ENABLED": "0"})
	if err := build.Run(); err != nil {
		return errors.New("fixture HTTP probe build failed")
	}
	_ = os.Chmod(probePath, 0555)
	if err := os.WriteFile(g.compose, []byte(composeTemplate(major, g.dir)), 0600); err != nil {
		return errors.New("private Compose configuration unavailable")
	}
	return nil
}

func (g *gate) ensureFixtureSubnetAvailable() error {
	ctx, cancel := context.WithTimeout(g.ctx, 10*time.Second)
	defer cancel()
	idsOutput, err := exec.CommandContext(ctx, "docker", "network", "ls", "--quiet").Output()
	if err != nil || len(idsOutput) > 64*1024 {
		return errors.New("fixture Docker network inventory unavailable")
	}
	ids := strings.Fields(string(idsOutput))
	if len(ids) > 512 {
		return errors.New("fixture Docker network inventory exceeded its bound")
	}
	for _, id := range ids {
		if len(id) < 12 || len(id) > 64 {
			return errors.New("fixture Docker network inventory invalid")
		}
		for _, r := range id {
			if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
				return errors.New("fixture Docker network inventory invalid")
			}
		}
	}
	if len(ids) == 0 {
		return nil
	}
	args := append([]string{"network", "inspect", "--format", "{{json .IPAM.Config}}"}, ids...)
	raw, err := exec.CommandContext(ctx, "docker", args...).Output()
	if err != nil || len(raw) > 1<<20 {
		return errors.New("fixture Docker network inventory unavailable")
	}
	if fixtureSubnetOverlaps(raw, fixtureSubnet) {
		return errors.New("fixture subnet overlaps an existing Docker network")
	}
	return nil
}

func fixtureSubnetOverlaps(raw []byte, candidate string) bool {
	_, target, err := net.ParseCIDR(candidate)
	if err != nil {
		return true
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	for _, line := range lines {
		if strings.TrimSpace(line) == "" {
			continue
		}
		var configs []struct {
			Subnet string `json:"Subnet"`
		}
		if json.Unmarshal([]byte(line), &configs) != nil {
			return true
		}
		for _, config := range configs {
			_, existing, parseErr := net.ParseCIDR(config.Subnet)
			if parseErr != nil || existing.Contains(target.IP) || target.Contains(existing.IP) {
				return true
			}
		}
	}
	return false
}

func goToolsModuleDir() (string, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", errors.New("Go tools module directory unavailable")
	}
	for current := cwd; ; current = filepath.Dir(current) {
		if _, err := os.Stat(filepath.Join(current, "go-tools", "go.mod")); err == nil {
			return filepath.Join(current, "go-tools"), nil
		}
		if _, err := os.Stat(filepath.Join(current, "go.mod")); err == nil {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
	}
	return "", errors.New("Go tools module directory unavailable")
}

func replaceEnv(env []string, replacements map[string]string) []string {
	out := make([]string, 0, len(env)+len(replacements))
	for _, value := range env {
		key, _, ok := strings.Cut(value, "=")
		if !ok {
			continue
		}
		if _, replace := replacements[key]; !replace {
			out = append(out, value)
		}
	}
	for key, value := range replacements {
		out = append(out, key+"="+value)
	}
	return out
}

func (g *gate) accept() error {
	g.phase = "postgres_start"
	if _, err := g.composeRun(nil, "up", "-d", "--pull", "never", "postgres"); err != nil {
		return err
	}
	if err := g.waitDatabase(); err != nil {
		return err
	}
	g.phase = "legacy_database_init"
	if err := g.initializeLegacyDatabase(); err != nil {
		return err
	}
	if _, err := g.composeRun(nil, "run", "--rm", "--no-deps", "--entrypoint", "/usr/local/bin/news-migrate", "migrate"); err != nil {
		return errors.New("synthetic legacy schema migration failed")
	}
	g.phase = "legacy_role_grants"
	if err := g.applyRuntimeGrants(); err != nil {
		return err
	}
	if err := g.seedLegacyData(); err != nil {
		return err
	}
	if err := g.checkDatabaseSize(); err != nil {
		return err
	}
	g.phase = "legacy_services_start"
	if _, err := g.composeRun(nil, "up", "-d", "--pull", "never", "extractor", "executor", "background", "app"); err != nil {
		g.serviceStates = g.collectServiceStates()
		g.startupHint = g.collectStartupHint()
		return err
	}
	if err := g.waitLegacyServices(); err != nil {
		return err
	}
	g.phase = "legacy_http_acceptance"
	if err := g.checkLegacyArticleAndAuth(); err != nil {
		return err
	}
	// Stop every database consumer before adoption checks sessions and runs DDL.
	if _, err := g.composeRun(nil, "stop", "app", "background", "executor", "extractor"); err != nil {
		return err
	}
	g.phase = "pre016_legacy_fixture"
	if err := g.simulatePre016(); err != nil {
		return err
	}
	g.phase = "protected_adoption_config"
	if err := g.writeProtectedInputs(); err != nil {
		return err
	}
	if err := g.writeRenderedConfig(); err != nil {
		return err
	}
	g.phase = "adoption_missing_grants_check"
	output, firstErr := g.composeRunDiagnostic(nil, "run", "--rm", "--no-deps", "--entrypoint", "/usr/local/bin/newsctl", "adopter", "adopt", "--config", "/gate/rendered-compose.json", "--apply", "--bootstrap-input", "/gate/bootstrap.json", "--compose-output", "/gate/managed-compose-override.yml")
	if firstErr == nil || !validMissingGrantAdvice(output) {
		g.startupHint = adoptionPreflightHint(output, firstErr)
		if g.startupHint == "adoption_scheduler_role_privileged" {
			g.startupHint = g.schedulerToastOwnershipHint()
		}
		return errors.New("pre-016 adoption did not fail closed on missing managed runtime grants")
	}
	if err := g.assertNotActivated(); err != nil {
		return err
	}
	g.phase = "operator_runtime_grants"
	if err := g.applyAdoptionRuntimeGrants(); err != nil {
		return err
	}
	g.phase = "adoption_retry"
	if _, err := g.composeRun(nil, "run", "--rm", "--no-deps", "--entrypoint", "/usr/local/bin/newsctl", "adopter", "adopt", "--config", "/gate/rendered-compose.json", "--apply", "--bootstrap-input", "/gate/bootstrap.json", "--compose-output", "/gate/managed-compose-override.yml"); err != nil {
		return errors.New("newsctl adoption retry failed")
	}
	if err := g.setOverrideOwner(); err != nil {
		return err
	}
	g.phase = "managed_topology_validation"
	if err := g.checkOverrideTopology(); err != nil {
		return err
	}
	g.phase = "managed_services_start"
	if _, err := g.composeRun(nil, "--file", filepath.Join(g.dir, "managed-compose-override.yml"), "up", "-d", "--pull", "never", "extractor", "executor", "background", "app"); err != nil {
		return errors.New("managed service startup failed")
	}
	if err := g.waitLegacyServices(); err != nil {
		return errors.New("managed services did not become ready")
	}
	g.phase = "managed_http_and_data_acceptance"
	if err := g.checkManagedArticleAndAuth(); err != nil {
		return err
	}
	return nil
}

func validMissingGrantAdvice(output string) bool {
	const marker = "review and apply these scoped grants before retrying:"
	index := strings.Index(output, marker)
	if index < 0 || !strings.Contains(output, "managed adoption is missing runtime grants") {
		return false
	}
	payload := strings.TrimSpace(output[index+len(marker):])
	if newline := strings.IndexAny(payload, "\r\n"); newline >= 0 {
		payload = strings.TrimSpace(payload[:newline])
	}
	actual := map[string]bool{}
	for _, statement := range strings.Split(payload, ";") {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		if !strings.HasPrefix(statement, "GRANT ") || actual[statement] {
			return false
		}
		actual[statement] = true
	}
	want := expectedOperatorGrantStatements()
	if len(actual) != len(want) {
		return false
	}
	for _, statement := range want {
		if !actual[statement] {
			return false
		}
	}
	return true
}

func expectedOperatorGrantStatements() []string {
	return []string{
		`GRANT SELECT ON "news"."installation_state" TO "news_web_runtime"`,
		`GRANT SELECT ON "news"."managed_admin" TO "news_web_runtime"`,
		`GRANT INSERT ON "news"."managed_admin" TO "news_web_runtime"`,
		`GRANT UPDATE ON "news"."managed_admin" TO "news_web_runtime"`,
		`GRANT DELETE ON "news"."managed_admin" TO "news_web_runtime"`,
		`GRANT SELECT ON "news"."setup_tokens" TO "news_web_runtime"`,
		`GRANT INSERT ON "news"."setup_tokens" TO "news_web_runtime"`,
		`GRANT UPDATE ON "news"."setup_tokens" TO "news_web_runtime"`,
		`GRANT DELETE ON "news"."setup_tokens" TO "news_web_runtime"`,
		`GRANT UPDATE ("onboarding_step") ON "news"."installation_state" TO "news_web_runtime"`,
		`GRANT UPDATE ("onboarding_completed") ON "news"."installation_state" TO "news_web_runtime"`,
		`GRANT UPDATE ("revision") ON "news"."installation_state" TO "news_web_runtime"`,
		`GRANT UPDATE ("updated_at") ON "news"."installation_state" TO "news_web_runtime"`,
		`GRANT SELECT ON "news"."installation_state" TO "news_domain_runtime"`,
		`GRANT SELECT ON "news"."integration_credentials" TO "news_domain_runtime"`,
		`GRANT INSERT ON "news"."integration_credentials" TO "news_domain_runtime"`,
		`GRANT UPDATE ON "news"."integration_credentials" TO "news_domain_runtime"`,
	}
}

func (g *gate) waitDatabase() error {
	for i := 0; i < 60; i++ {
		if _, err := g.composeRun(nil, "exec", "-T", "postgres", "pg_isready", "-h", "127.0.0.1", "-U", "postgres", "-d", "postgres"); err == nil {
			return nil
		}
		if !pause(g.ctx, time.Second) {
			break
		}
	}
	return errors.New("fixture PostgreSQL readiness failed")
}

func (g *gate) initializeLegacyDatabase() error {
	const setup = `CREATE ROLE news_web_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'fixture-only-password';
CREATE ROLE news_domain_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'fixture-only-password';
CREATE ROLE news_worker_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'fixture-only-password';
CREATE ROLE news_execution_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'fixture-only-password';
CREATE ROLE news_ingestion_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'fixture-only-password';
CREATE ROLE news_processing_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'fixture-only-password';
CREATE ROLE news_ingestion_fence_owner NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE news_processing_fence_owner NOLOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;
CREATE ROLE scheduler_runtime LOGIN NOINHERIT NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS PASSWORD 'fixture-only-password';
REVOKE ALL ON DATABASE newsdb FROM PUBLIC;
GRANT CONNECT ON DATABASE newsdb TO news_web_runtime,news_domain_runtime,news_worker_runtime,news_execution_runtime,news_ingestion_runtime,news_processing_runtime,scheduler_runtime;
CREATE SCHEMA news;
CREATE SCHEMA scheduler_service AUTHORIZATION scheduler_runtime;`
	steps := [][]string{
		{"exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "postgres", "-c", "CREATE DATABASE newsdb"},
	}
	for _, args := range steps {
		if _, err := g.composeRun(nil, args...); err != nil {
			return errors.New("synthetic legacy database initialization failed")
		}
	}
	if _, err := g.composeRun([]byte(setup), "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb"); err != nil {
		return errors.New("synthetic runtime role setup failed")
	}
	return nil
}

func (g *gate) applyRuntimeGrants() error {
	// Use the shipped least-privilege policy rather than test-only broad grants.
	if _, err := g.composeRun([]byte(managedsetup.GrantPolicy), "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb"); err != nil {
		return errors.New("synthetic least-privilege grants failed")
	}
	return nil
}

func (g *gate) simulatePre016() error {
	const sql = `DROP TABLE news.integration_credentials;
DROP TABLE news.setup_tokens;
DROP TABLE news.managed_admin;
DROP TABLE news.installation_state;`
	if _, err := g.composeRun([]byte(sql), "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb"); err != nil {
		return errors.New("isolated pre-016 legacy fixture setup failed")
	}
	return nil
}

func (g *gate) assertNotActivated() error {
	const check = `test ! -e /state/setup/adoption-committed.json && test ! -e /state/web/runtime.json && test ! -e /state/domain/runtime.json && test ! -e /state/background/runtime.json && test ! -e /state/extractor/runtime.json`
	if _, err := g.composeRun(nil, "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "adopter", "-ec", check); err != nil {
		return errors.New("failed grant preflight activated managed runtime state")
	}
	const query = `SELECT (SELECT count(*) FROM news.installation_state) || ':' || (SELECT count(*) FROM news.managed_admin) || ':' || (SELECT count(*) FROM news.setup_tokens) || ':' || (SELECT count(*) FROM news.integration_credentials)`
	out, err := g.composeRun(nil, "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb", "-qAtc", query)
	if err != nil || out != "0:0:0:0" {
		return errors.New("failed grant preflight changed managed database state")
	}
	return nil
}

func (g *gate) applyAdoptionRuntimeGrants() error {
	const sql = `GRANT SELECT ON news.installation_state TO news_web_runtime;
GRANT SELECT, INSERT, UPDATE, DELETE ON news.managed_admin, news.setup_tokens TO news_web_runtime;
GRANT UPDATE (onboarding_step, onboarding_completed, revision, updated_at) ON news.installation_state TO news_web_runtime;
GRANT SELECT ON news.installation_state TO news_domain_runtime;
GRANT SELECT, INSERT, UPDATE ON news.integration_credentials TO news_domain_runtime;`
	if _, err := g.composeRun([]byte(sql), "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb"); err != nil {
		return errors.New("operator fixture grants failed")
	}
	return nil
}

func (g *gate) seedLegacyData() error {
	const sql = `INSERT INTO news.categories(name,display_name,description,color,is_local,created_at) VALUES ('fixture','Fixture category','Synthetic adoption fixture','#123456',false,timezone('UTC',now()));
INSERT INTO news.category_mapping(ai_category,fixed_category,confidence_threshold,description,created_by,usage_count,is_active,created_at,updated_at) VALUES ('fixture topic','fixture',0.75,'synthetic fixture','gate',3,true,timezone('UTC',now()),timezone('UTC',now()));
INSERT INTO news.sources(name,source_type,url,enabled,config,fetch_interval,created_at,updated_at) VALUES ('adoption fixture source','rss','https://fixture.invalid/feed.xml',false,'{}',3600,timezone('UTC',now()),timezone('UTC',now()));
INSERT INTO news.articles(source_id,title,url,content,summary,published_at,fetched_at,processed,summary_processed,category_processed,ad_processed,is_advertisement,ad_confidence) VALUES (currval('news.sources_id_seq'),'Synthetic adoption fixture article','https://fixture.invalid/article/1','fixture-only body','fixture-only summary',timezone('UTC',now()),timezone('UTC',now()),true,true,true,true,false,0.0);
INSERT INTO news.background_jobs(id,kind,payload,idempotency_key,state,attempt_generation,created_at,finished_at,result) VALUES ('00000000-0000-4000-8000-000000000001','news_processing','{"fixture":true}','adoption-fixture:completed','succeeded',1,timezone('UTC',now()),timezone('UTC',now()),'{}');
INSERT INTO news.article_ingestion_runs(id,job_id,attempt_generation,envelope_sha256,owner_id,worker_generation,backend_pid,executor_instance_id,deadline_at,state,finished_at,result) VALUES ('00000000-0000-4000-8000-000000000002','00000000-0000-4000-8000-000000000001',1,repeat('a',64),'00000000-0000-4000-8000-000000000003',1,1,'00000000-0000-4000-8000-000000000004',timezone('UTC',now()) + interval '1 minute','succeeded',timezone('UTC',now()),'{"fixture":true}');
INSERT INTO news.article_ingestion_source_runs(run_id,source_id,source_type,config_sha256,state,candidates,saved,updated,skipped,fetched_at) VALUES ('00000000-0000-4000-8000-000000000002',currval('news.sources_id_seq'),'rss',repeat('b',64),'succeeded',1,1,0,0,timezone('UTC',now()));
INSERT INTO news.settings(key,value,description,updated_at) VALUES ('managed-adoption-fixture','"preserve-me"','synthetic fixture receipt',timezone('UTC',now()));
INSERT INTO news.schedule_settings(task_name,enabled,schedule_type,hour,minute,timezone,task_config,created_at,updated_at) VALUES ('adoption-fixture',false,'interval',3,15,'UTC','{}',timezone('UTC',now()),timezone('UTC',now()));`
	if _, err := g.composeRun([]byte(sql), "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb"); err != nil {
		return errors.New("synthetic legacy fixture seed failed")
	}
	return nil
}

func (g *gate) checkDatabaseSize() error {
	out, err := g.composeRun(nil, "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb", "-qAtc", "SELECT pg_database_size('newsdb')")
	if err != nil {
		return errors.New("fixture database size check failed")
	}
	var size int64
	if _, err = fmt.Sscan(out, &size); err != nil || size < 0 || size > 512<<20 {
		return errors.New("fixture database exceeded its 512 MiB budget")
	}
	return nil
}

func (g *gate) waitLegacyServices() error {
	for i := 0; i < 90; i++ {
		if g.probe("web", "GET", "/health", "", nil, nil).Status == http.StatusOK &&
			g.probe("domain", "GET", "/internal/v1/status", "Bearer fixture-only-executor-token", nil, nil).Status == http.StatusOK &&
			g.probe("scheduler", "GET", "/healthz", "", nil, nil).Status == http.StatusOK &&
			g.extractorReady() {
			return nil
		}
		if !pause(g.ctx, time.Second) {
			break
		}
	}
	return errors.New("fixture service readiness failed")
}

func (g *gate) extractorReady() bool {
	_, err := g.composeRun(nil, "exec", "-T", "extractor", "/extract-service", "-healthcheck")
	return err == nil
}

type probeReply struct {
	Status int    `json:"status"`
	Body   []byte `json:"body"`
}

func (g *gate) probe(target, method, path, auth string, body []byte, headers map[string]string) probeReply {
	input, _ := json.Marshal(struct {
		Target, Method, Path, Authorization string
		Body                                []byte
		Headers                             map[string]string
	}{target, method, path, auth, body, headers})
	out, err := g.composeRun(input, "run", "--rm", "--no-deps", "-T", "--entrypoint", "/fixture/fixture-http", "probe")
	if err != nil {
		return probeReply{}
	}
	var reply probeReply
	_ = json.Unmarshal([]byte(out), &reply)
	return reply
}

func (g *gate) checkLegacyArticleAndAuth() error {
	auth := "Basic Zml4dHVyZS1hZG1pbjpmaXh0dXJlLWFkbWluLXBhc3N3b3Jk"
	login := g.probe("web", "POST", "/auth/login", auth, nil, map[string]string{"X-Forwarded-Proto": "https"})
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if login.Status != http.StatusOK || json.Unmarshal(login.Body, &token) != nil || token.AccessToken == "" {
		g.startupHint = httpReplyHint(login.Body)
		return errors.New(finiteHTTPFailure("legacy_login", login.Status))
	}
	articleID, err := g.fixtureArticleID()
	if err != nil {
		return errors.New("legacy fixture article identifier unavailable")
	}
	article := g.probe("web", "GET", "/api/v1/articles/"+articleID, "Bearer "+token.AccessToken, nil, nil)
	if article.Status != http.StatusOK {
		g.startupHint = httpReplyHint(article.Body)
		return errors.New(finiteHTTPFailure("legacy_article", article.Status))
	}
	if !bytes.Contains(article.Body, []byte("Synthetic adoption fixture article")) {
		return errors.New("legacy_article_payload_mismatch")
	}
	return nil
}

func (g *gate) fixtureArticleID() (string, error) {
	const query = "SELECT id FROM news.articles WHERE url='https://fixture.invalid/article/1' AND title='Synthetic adoption fixture article'"
	out, err := g.composeRun(nil, "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb", "-qAtc", query)
	if err != nil || out == "" || strings.Contains(out, "\n") {
		return "", errors.New("fixture article identifier unavailable")
	}
	for _, r := range out {
		if r < '0' || r > '9' {
			return "", errors.New("fixture article identifier invalid")
		}
	}
	return out, nil
}

func (g *gate) writeProtectedInputs() error {
	bootstrap, err := json.Marshal(map[string]any{"version": 1, "bootstrap_dsn": "postgres://postgres:" + fixturePassword + "@postgres:5432/newsdb?search_path=news&sslmode=disable"})
	if err != nil {
		return errors.New("bootstrap fixture input unavailable")
	}
	if err = os.WriteFile(filepath.Join(g.dir, "bootstrap.json"), bootstrap, 0600); err != nil {
		return errors.New("bootstrap fixture input unavailable")
	}
	return nil
}

func (g *gate) writeRenderedConfig() error {
	ctx, cancel := context.WithTimeout(g.ctx, commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "--project-name", g.project, "--file", g.compose, "config", "--format", "json")
	var output bytes.Buffer
	cmd.Stdout = &output
	if err := cmd.Run(); err != nil || output.Len() == 0 || output.Len() > 1<<20 {
		return errors.New("rendered legacy Compose configuration unavailable")
	}
	return os.WriteFile(filepath.Join(g.dir, "rendered-compose.json"), output.Bytes(), 0600)
}

func (g *gate) checkOverrideTopology() error {
	path := filepath.Join(g.dir, "managed-compose-override.yml")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() == 0 || info.Size() > 64*1024 {
		return errors.New("managed Compose override unavailable")
	}
	// The candidate override must only add role-file mounts and unset legacy
	// runtime variables; source topology remains the source Compose's authority.
	original, err := g.renderConfig()
	if err != nil {
		return errors.New("legacy Compose topology render failed")
	}
	combined, err := g.renderConfig(path)
	if err != nil {
		return errors.New("managed Compose topology render failed")
	}
	var before, after map[string]any
	if json.Unmarshal(original, &before) != nil || json.Unmarshal(combined, &after) != nil || !sameNonRuntimeCompose(before, after) || !isolatedCompose(before) {
		return errors.New("managed override changed legacy network topology")
	}
	if _, err = g.composeRun(nil, "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "adopter", "-c", "test -s /state/web/runtime.json && test -s /state/domain/runtime.json && test -s /state/background/runtime.json && test -s /state/extractor/runtime.json"); err != nil {
		return errors.New("managed runtime files unavailable")
	}
	return nil
}

func (g *gate) setOverrideOwner() error {
	// The adopter runs as root to write its private 0600 output into a bind
	// mount. Change ownership of only that gate-owned file so a non-root CI host
	// user can pass it back to Compose. Keep its restrictive mode unchanged.
	command := fmt.Sprintf("chown %d:%d /gate/managed-compose-override.yml", os.Getuid(), os.Getgid())
	if _, err := g.composeRun(nil, "run", "--rm", "--no-deps", "--entrypoint", "/bin/sh", "adopter", "-ec", command); err != nil {
		return errors.New("managed Compose override ownership could not be assigned to fixture user")
	}
	path := filepath.Join(g.dir, "managed-compose-override.yml")
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0077 != 0 {
		return errors.New("managed Compose override permissions are unsafe")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || int(stat.Uid) != os.Getuid() || int(stat.Gid) != os.Getgid() {
		return errors.New("managed Compose override is not readable by fixture user")
	}
	return nil
}

func isolatedCompose(config map[string]any) bool {
	networks, ok := config["networks"].(map[string]any)
	if !ok {
		return false
	}
	fixture, ok := networks["fixture"].(map[string]any)
	if !ok || fixture["internal"] != true {
		return false
	}
	services, ok := config["services"].(map[string]any)
	if !ok {
		return false
	}
	for _, raw := range services {
		service, ok := raw.(map[string]any)
		if !ok {
			return false
		}
		if ports, exists := service["ports"]; exists && ports != nil {
			bindings, ok := ports.([]any)
			if !ok || len(bindings) != 0 {
				return false
			}
		}
	}
	return services["app"] != nil && services["executor"] != nil && services["background"] != nil && services["extractor"] != nil
}

func sameNonRuntimeCompose(before, after map[string]any) bool {
	clone := func(input map[string]any) map[string]any {
		raw, err := json.Marshal(input)
		if err != nil {
			return nil
		}
		var out map[string]any
		if json.Unmarshal(raw, &out) != nil {
			return nil
		}
		_, ok := out["services"].(map[string]any)
		if !ok {
			return nil
		}
		return out
	}
	a, b := clone(before), clone(after)
	if a == nil || b == nil {
		return false
	}
	beforeServices, beforeOK := a["services"].(map[string]any)
	afterServices, afterOK := b["services"].(map[string]any)
	if !beforeOK || !afterOK || len(beforeServices) != len(afterServices) {
		return false
	}
	roleServices := map[string]string{"app": "web", "executor": "domain", "background": "background", "extractor": "extractor"}
	roleVolumes := map[string]string{"app": "managed_web", "executor": "managed_domain", "background": "managed_background", "extractor": "managed_extractor"}
	for service, role := range roleServices {
		old, oldOK := beforeServices[service].(map[string]any)
		current, currentOK := afterServices[service].(map[string]any)
		if !oldOK || !currentOK || !validManagedEnvironment(current["environment"], role) || !emptyEnvFile(current["env_file"]) {
			return false
		}
		oldMounts, oldValid := composeMounts(old["volumes"])
		newMounts, newValid := composeMounts(current["volumes"])
		if !oldValid || !newValid || containsMountTarget(oldMounts, "/run/news") {
			return false
		}
		newMounts, added := removeRoleMount(newMounts, roleVolumes[service])
		if added != 1 || !sameMounts(oldMounts, newMounts) {
			return false
		}
		copyOrDelete(current, old, "volumes")
		copyOrDelete(current, old, "environment")
		copyOrDelete(current, old, "env_file")
	}
	oldMigrate, oldMigrateOK := beforeServices["migrate"].(map[string]any)
	newMigrate, newMigrateOK := afterServices["migrate"].(map[string]any)
	if !oldMigrateOK || !newMigrateOK || !sameJSONValue(newMigrate["entrypoint"], []any{"/bin/true"}) {
		return false
	}
	newMigrate["entrypoint"] = oldMigrate["entrypoint"]
	// Adoption may introduce only the managed role volumes referenced by its
	// four migrated services; keep every pre-existing volume definition intact.
	oldVolumes, oldVolumesOK := a["volumes"].(map[string]any)
	newVolumes, newVolumesOK := b["volumes"].(map[string]any)
	if oldVolumesOK && !newVolumesOK {
		return false
	}
	if newVolumesOK {
		for name := range newVolumes {
			if _, existed := oldVolumes[name]; !existed {
				if !map[string]bool{"managed_web": true, "managed_domain": true, "managed_background": true, "managed_extractor": true}[name] {
					return false
				}
				delete(newVolumes, name)
			}
		}
		if !oldVolumesOK {
			if len(newVolumes) != 0 {
				return false
			}
			delete(b, "volumes")
		}
	}
	ra, errA := json.Marshal(a)
	rb, errB := json.Marshal(b)
	return errA == nil && errB == nil && bytes.Equal(ra, rb)
}

func sameMounts(left, right []any) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !sameJSONValue(left[i], right[i]) {
			return false
		}
	}
	return true
}

func copyOrDelete(target, source map[string]any, key string) {
	if value, exists := source[key]; exists {
		target[key] = value
	} else {
		delete(target, key)
	}
}

func validManagedEnvironment(raw any, role string) bool {
	values, ok := raw.(map[string]any)
	if !ok || len(values) != 1 {
		return false
	}
	files := map[string]string{"web": "/run/news/runtime.json", "domain": "/run/news/runtime.json", "background": "/run/news/runtime.json", "extractor": "/run/news/runtime.json"}
	return values["NEWS_RUNTIME_FILE"] == files[role]
}

func emptyEnvFile(raw any) bool {
	if raw == nil {
		return true
	}
	values, ok := raw.([]any)
	return ok && len(values) == 0
}

func composeMounts(raw any) ([]any, bool) {
	if raw == nil {
		return nil, true
	}
	values, ok := raw.([]any)
	return values, ok
}

func containsMountTarget(mounts []any, target string) bool {
	for _, raw := range mounts {
		if mount, ok := raw.(map[string]any); ok && mount["target"] == target {
			return true
		}
	}
	return false
}

func removeRoleMount(mounts []any, source string) ([]any, int) {
	remaining := make([]any, 0, len(mounts))
	removed := 0
	for _, raw := range mounts {
		mount, ok := raw.(map[string]any)
		if ok && mount["target"] == "/run/news" && mount["source"] == source && mount["read_only"] == true {
			removed++
			continue
		}
		remaining = append(remaining, raw)
	}
	return remaining, removed
}

func sameJSONValue(left, right any) bool {
	a, errA := json.Marshal(left)
	b, errB := json.Marshal(right)
	return errA == nil && errB == nil && bytes.Equal(a, b)
}

func (g *gate) renderConfig(extra ...string) ([]byte, error) {
	args := []string{"compose", "--project-name", g.project, "--file", g.compose}
	for _, path := range extra {
		args = append(args, "--file", path)
	}
	args = append(args, "config", "--format", "json")
	ctx, cancel := context.WithTimeout(g.ctx, commandTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", args...)
	var output bytes.Buffer
	cmd.Stdout = &output
	if err := cmd.Run(); err != nil || output.Len() == 0 || output.Len() > 1<<20 {
		return nil, errors.New("Compose render failed")
	}
	return output.Bytes(), nil
}

func (g *gate) checkManagedArticleAndAuth() error {
	login := g.probe("web", "POST", "/auth/login", "Basic Zml4dHVyZS1hZG1pbjpmaXh0dXJlLWFkbWluLXBhc3N3b3Jk", nil, map[string]string{"X-Forwarded-Proto": "https"})
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if login.Status != http.StatusOK || json.Unmarshal(login.Body, &token) != nil || token.AccessToken == "" {
		g.startupHint = httpReplyHint(login.Body)
		return errors.New(finiteHTTPFailure("managed_login", login.Status))
	}
	articleID, err := g.fixtureArticleID()
	if err != nil {
		return errors.New("managed fixture article identifier unavailable")
	}
	article := g.probe("web", "GET", "/api/v1/articles/"+articleID, "Bearer "+token.AccessToken, nil, map[string]string{"X-Forwarded-Proto": "https"})
	if article.Status != http.StatusOK {
		g.startupHint = httpReplyHint(article.Body)
		return errors.New(finiteHTTPFailure("managed_article", article.Status))
	}
	if !bytes.Contains(article.Body, []byte("Synthetic adoption fixture article")) {
		return errors.New("managed_article_payload_mismatch")
	}
	// Read the synthetic migration receipt and preserved schedule row through
	// PostgreSQL after managed startup. No source or job is active in the fixture.
	out, err := g.composeRun(nil, "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb", "-qAtc", "SELECT (SELECT value::text FROM news.settings WHERE key='managed-adoption-fixture') || ':' || (SELECT count(*)::text FROM news.schedule_settings WHERE task_name='adoption-fixture' AND enabled=false) || ':' || (SELECT count(*)::text FROM news.sources WHERE enabled) || ':' || (SELECT count(*)::text FROM news.background_jobs WHERE state IN ('queued','running')) || ':' || (SELECT count(*)::text FROM news.article_ingestion_source_runs WHERE run_id='00000000-0000-4000-8000-000000000002' AND state='succeeded' AND saved=1)")
	if err != nil || out != `"preserve-me":1:0:0:1` {
		return errors.New("legacy fixture receipt was not preserved")
	}
	if err = g.checkDatabaseSize(); err != nil {
		return err
	}
	return nil
}

func finiteHTTPFailure(prefix string, status int) string {
	if status < 100 || status > 599 {
		return prefix + "_status_unknown"
	}
	return fmt.Sprintf("%s_status_%d", prefix, status)
}

func httpReplyHint(body []byte) string {
	if len(body) > 64*1024 {
		return "http_error_body_unavailable"
	}
	text := strings.ToLower(string(body))
	switch {
	case strings.Contains(text, "https required"):
		return "http_error_https_required"
	case strings.Contains(text, "csrf check failed"):
		return "http_error_csrf_rejected"
	case strings.Contains(text, "invalid credentials"), strings.Contains(text, "not authenticated"):
		return "http_error_auth_rejected"
	case strings.Contains(text, "permission denied"), strings.Contains(text, "insufficient privilege"):
		return "http_error_database_permission"
	case strings.Contains(text, "database"), strings.Contains(text, "postgres"), strings.Contains(text, "query failed"):
		return "http_error_database"
	case strings.Contains(text, "backend unavailable"), strings.Contains(text, "connection refused"):
		return "http_error_service_dependency"
	case strings.Contains(text, "not found"):
		return "http_error_not_found"
	default:
		return "http_error_unclassified"
	}
}

func adoptionPreflightHint(output string, err error) string {
	if err == nil {
		return "adoption_unexpectedly_succeeded"
	}
	text := strings.ToLower(output)
	switch {
	case strings.Contains(text, "managed adoption is missing runtime grants") && strings.Contains(text, "review and apply these scoped grants before retrying"):
		return "adoption_scoped_grant_advice_invalid"
	case strings.Contains(text, "unsupported runtime key"):
		return "adoption_unsupported_runtime_key"
	case strings.Contains(text, "administrator credential unavailable"):
		return "adoption_admin_credential_unavailable"
	case strings.Contains(text, "runtime role is privileged"):
		for _, role := range []string{"web", "domain", "worker", "execution", "ingestion", "processing", "scheduler"} {
			if strings.Contains(text, role+" runtime role is privileged") {
				return "adoption_" + role + "_role_privileged"
			}
		}
		return "adoption_runtime_role_privileged"
	case strings.Contains(text, "runtime grants could not be verified"):
		return "adoption_grant_verification_unavailable"
	case strings.Contains(text, "managed additive migration unavailable"):
		return "adoption_migration_unavailable"
	case strings.Contains(text, "runtime services stopped"):
		return "adoption_services_not_stopped"
	case strings.Contains(text, "runtime roles resolve to different schemas"):
		return "adoption_schema_parity_mismatch"
	case strings.Contains(text, "news database schema preflight unavailable"):
		return "adoption_schema_preflight_unavailable"
	case strings.Contains(text, "bootstrap database unavailable"):
		return "adoption_bootstrap_connection_unavailable"
	case strings.Contains(text, "bootstrap connection must target the same news database"):
		return "adoption_bootstrap_database_mismatch"
	case strings.Contains(text, "bootstrap identity must be separate"):
		return "adoption_bootstrap_identity_mismatch"
	case strings.Contains(text, "runtime database role preflight failed"):
		return "adoption_runtime_role_connection_failed"
	case strings.Contains(text, "runtime role configuration invalid"):
		return "adoption_runtime_role_configuration_invalid"
	case strings.Contains(text, "role privilege preflight unavailable"):
		return "adoption_role_privilege_check_unavailable"
	case strings.Contains(text, "managed runtime grants"):
		return "adoption_managed_grant_preflight"
	default:
		if hint := safeNewsctlErrorHint(output); hint != "" {
			return hint
		}
		return "adoption_preflight_unclassified"
	}
}

func safeNewsctlErrorHint(output string) string {
	var message string
	for _, line := range strings.Split(output, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "newsctl: ") {
			message = strings.TrimSpace(strings.TrimPrefix(line, "newsctl: "))
		}
	}
	if message == "" {
		return ""
	}
	message = strings.TrimSpace(message)
	for _, marker := range []string{"http://", "https://"} {
		if index := strings.Index(strings.ToLower(message), marker); index >= 0 {
			message = message[:index]
		}
	}
	if index := strings.IndexAny(message, `"'`); index >= 0 {
		message = message[:index]
	}
	if index := strings.IndexAny(message, ";:"); index >= 0 {
		message = message[:index]
	}
	message = strings.ToLower(message)
	var slug strings.Builder
	slug.WriteString("adoption_error_")
	previousUnderscore := false
	for _, r := range message {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			slug.WriteRune(r)
			previousUnderscore = false
		} else if r == ' ' || r == '_' {
			if !previousUnderscore {
				slug.WriteByte('_')
				previousUnderscore = true
			}
		}
		if slug.Len() >= 100 {
			break
		}
	}
	result := strings.Trim(slug.String(), "_")
	if result == "adoption_error" {
		return ""
	}
	return result
}

func (g *gate) schedulerToastOwnershipHint() string {
	const query = `SELECT count(*) FROM pg_class parent JOIN pg_namespace pn ON pn.oid=parent.relnamespace JOIN pg_class toast ON toast.oid=parent.reltoastrelid JOIN pg_namespace tn ON tn.oid=toast.relnamespace WHERE pn.nspname='scheduler_service' AND tn.nspname LIKE 'pg_toast%' AND parent.relowner='scheduler_runtime'::regrole AND toast.relowner=parent.relowner`
	out, err := g.composeRun(nil, "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "postgres", "-d", "newsdb", "-qAtc", query)
	if err != nil {
		return "adoption_scheduler_toast_check_unavailable"
	}
	var count int64
	if _, err = fmt.Sscan(out, &count); err != nil || count < 0 {
		return "adoption_scheduler_toast_check_unavailable"
	}
	if count > 0 {
		return "adoption_scheduler_toast_owner_present"
	}
	return "adoption_scheduler_toast_owner_absent"
}

func pause(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func safeReason(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "bounded acceptance time exceeded"
	}
	return err.Error()
}

func composeFailureCode(output string) string {
	text := strings.ToLower(output)
	switch {
	case strings.Contains(text, "unhealthy"), strings.Contains(text, "healthcheck"):
		return "service_healthcheck_failed"
	case strings.Contains(text, "dependency failed"), strings.Contains(text, "dependency is not healthy"):
		return "service_dependency_failed"
	case strings.Contains(text, "no such image"), strings.Contains(text, "pull access denied"), strings.Contains(text, "image not found"):
		return "candidate_image_unavailable"
	case strings.Contains(text, "invalid compose"), strings.Contains(text, "unsupported config"), strings.Contains(text, "yaml"):
		return "compose_configuration_invalid"
	default:
		return "compose_operation_failed"
	}
}

// collectServiceStates records only finite service names, process state,
// health, and exit code. It deliberately does not read logs or environment.
func (g *gate) collectServiceStates() []string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "--project-name", g.project, "--file", g.compose, "ps", "--all", "--format", "json")
	raw, err := cmd.Output()
	if err != nil || len(raw) > 32*1024 {
		return []string{"compose_state_unavailable"}
	}
	// Compose versions return either a JSON array or newline-delimited objects.
	var rows []map[string]any
	if json.Unmarshal(raw, &rows) != nil {
		for _, line := range strings.Split(string(raw), "\n") {
			var row map[string]any
			if json.Unmarshal([]byte(line), &row) == nil {
				rows = append(rows, row)
			}
		}
	}
	allowed := map[string]bool{"app": true, "executor": true, "background": true, "extractor": true}
	var states []string
	for _, row := range rows {
		service := jsonStringField(row, "Service", "service")
		if !allowed[service] {
			continue
		}
		state := safeStateToken(jsonStringField(row, "State", "state"))
		status := strings.ToLower(jsonStringField(row, "Status", "status"))
		health := "none"
		if strings.Contains(status, "(healthy)") {
			health = "healthy"
		} else if strings.Contains(status, "(unhealthy)") {
			health = "unhealthy"
		} else if strings.Contains(status, "(health: starting)") {
			health = "starting"
		}
		exit := jsonStringField(row, "ExitCode", "exit_code")
		if exit == "" {
			exit = "unknown"
		}
		states = append(states, service+":"+state+":"+health+":"+safeExitCode(exit))
	}
	if len(states) == 0 {
		return []string{"compose_state_unavailable"}
	}
	return states
}

// collectStartupHint scans a small bounded log sample in memory and returns
// only one fixed label. Logs and their potentially sensitive contents are
// never written to artifacts or stderr.
func (g *gate) collectStartupHint() string {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "--project-name", g.project, "--file", g.compose, "logs", "--no-color", "--no-log-prefix", "--tail", "100", "app", "executor", "background", "extractor")
	raw, err := cmd.Output()
	if err != nil || len(raw) > 128*1024 {
		return "log_hint_unavailable"
	}
	return startupLogHint(string(raw))
}

func startupLogHint(output string) string {
	text := strings.ToLower(output)
	switch {
	case strings.Contains(text, "password authentication failed"):
		return "database_authentication_failed"
	case strings.Contains(text, "permission denied for schema"), strings.Contains(text, "permission denied for table"):
		return "database_permission_denied"
	case strings.Contains(text, "database unavailable"), strings.Contains(text, "connection refused"), strings.Contains(text, "connection timeout"):
		return "database_connection_failed"
	case strings.Contains(text, "invalid runtime configuration"), strings.Contains(text, "missing configuration"), strings.Contains(text, "required configuration"):
		return "service_configuration_invalid"
	case strings.Contains(text, "no such host"), strings.Contains(text, "lookup postgres"):
		return "database_name_resolution_failed"
	case strings.Contains(text, "panic:"):
		return "service_panic"
	default:
		return "service_exit_unclassified"
	}
}

func jsonStringField(row map[string]any, keys ...string) string {
	for _, key := range keys {
		if value, ok := row[key]; ok {
			return fmt.Sprint(value)
		}
	}
	return ""
}

func safeStateToken(value string) string {
	switch strings.ToLower(value) {
	case "running", "created", "restarting", "exited", "paused", "dead", "removing":
		return strings.ToLower(value)
	default:
		return "unknown"
	}
}

func safeExitCode(value string) string {
	if value == "unknown" || value == "" {
		return "unknown"
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return "unknown"
		}
	}
	return value
}

func composeTemplate(major, dir string) string {
	// g.dir is generated by os.MkdirTemp and contains no YAML metacharacters.
	base := "postgres://%s:" + fixturePassword + "@postgres:5432/newsdb?search_path=news&sslmode=disable"
	webDB := fmt.Sprintf(base, "news_web_runtime")
	domainDB := fmt.Sprintf(base, "news_domain_runtime")
	workerDB := fmt.Sprintf(base, "news_worker_runtime")
	executionDB := fmt.Sprintf(base, "news_execution_runtime")
	ingestionDB := fmt.Sprintf(base, "news_ingestion_runtime")
	processingDB := fmt.Sprintf(base, "news_processing_runtime")
	schedulerDB := "postgres://scheduler_runtime:" + fixturePassword + "@postgres:5432/newsdb?search_path=scheduler_service&sslmode=disable"
	adminPassword := fixtureAdmin
	fixtureDir := filepath.ToSlash(dir)
	return fmt.Sprintf(`services:
  postgres:
    image: postgres:%s-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: %s
      POSTGRES_DB: postgres
    networks: [fixture]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d postgres"]
      interval: 2s
      timeout: 2s
      retries: 30
  migrate:
    image: evening-news-executor:consolidation-fixture
    entrypoint: ["/usr/local/bin/news-migrate"]
    environment:
      DATABASE_URL: 'postgres://postgres:%s@postgres:5432/newsdb?search_path=news&sslmode=disable'
      ALLOW_CREATE_ALL: "true"
    depends_on:
      postgres: {condition: service_healthy}
    networks: [fixture]
    restart: "no"
  app:
    image: evening-news-web:consolidation-fixture
    environment:
      DATABASE_URL: '%s'
      ADMIN_USERNAME: fixture-admin
      ADMIN_PASSWORD: '%s'
      JWT_SECRET: fixture-only-jwt-signing-value
      WEB_BACKEND_URL: http://executor:8100
      WEB_BACKEND_TOKEN: fixture-only-web-backend-token
      SCHEDULED_JOBS_TOKEN: fixture-only-scheduled-jobs-token
      SCHEDULER_API_URL: http://background:8080
      SCHEDULER_API_TOKEN: fixture-only-scheduler-token
      SCHEDULER_API_ALLOW_HTTP: "true"
      SCHEDULE_DISPATCH_MODE: service
      BACKGROUND_EXECUTION_MODE: worker
      SITE_BASE_URL: https://web:8000
      WEB_LISTEN_ADDRESS: ':8000'
      TRUSTED_HOSTS: web,localhost,127.0.0.1
      TRUSTED_PROXY_CIDRS: %s
      DEVELOPMENT: "true"
    depends_on:
      migrate: {condition: service_completed_successfully}
      executor: {condition: service_healthy}
      background: {condition: service_healthy}
      extractor: {condition: service_healthy}
    networks:
      fixture:
        aliases: [web]
  executor:
    image: evening-news-executor:consolidation-fixture
    environment:
      DATABASE_URL: '%s'
      AI_PROVIDER: gemini
      GEMINI_API_KEY: fixture-only-gemini-key
      JOB_EXECUTOR_TOKEN: fixture-only-executor-token
      ARTICLE_AI_TOKEN: fixture-only-article-ai-token
      WEB_BACKEND_TOKEN: fixture-only-web-backend-token
      EXTRACTION_SERVICE_URL: http://extractor:8080
      EXTRACTION_SERVICE_TOKEN: fixture-only-collector-token
      EXECUTION_SERVICE_URL: http://background:8101
      BACKGROUND_EXECUTION_MODE: worker
      SITE_BASE_URL: https://web:8000
      TELEGRAM_CHAT_ID: fixture-only-chat
    depends_on:
      migrate: {condition: service_completed_successfully}
      extractor: {condition: service_healthy}
    networks: [fixture]
  background:
    image: evening-news-background:consolidation-fixture
    environment:
      WORKER_DATABASE_URL: '%s'
      EXECUTION_DATABASE_URL: '%s'
      INGESTION_DATABASE_URL: '%s'
      PROCESSING_DATABASE_URL: '%s'
      SCHEDULER_DATABASE_URL: '%s'
      SCHEDULER_DB_SCHEMA: scheduler_service
      JOB_EXECUTOR_TOKEN: fixture-only-executor-token
      ARTICLE_AI_TOKEN: fixture-only-article-ai-token
      ARTICLE_INGESTION_TOKEN: fixture-only-ingestion-token
      ARTICLE_PROCESSING_TOKEN: fixture-only-processing-token
      SCHEDULER_API_TOKEN: fixture-only-scheduler-token
      SCHEDULER_TARGET_NEWS_WORKER_TOKEN: fixture-only-scheduled-jobs-token
      EXTRACTION_SERVICE_TOKEN: fixture-only-collector-token
      EXTRACTION_SERVICE_URL: http://extractor:8080
      ARTICLE_AI_URL: http://executor:8100
      ARTICLE_INGESTION_LISTEN: ':8093'
      ARTICLE_PROCESSING_LISTEN: ':8094'
      JOB_EXECUTOR_URL: http://background:8101
      JOB_EXECUTOR_LISTEN: ':8101'
      DOMAIN_SERVICE_URL: http://executor:8100
      SCHEDULER_LISTEN_ADDR: ':8080'
      SCHEDULER_TARGET_ALIASES: news-worker
      SCHEDULER_TARGET_NEWS_WORKER_URL: http://web:8000/api/v1/internal/scheduled-jobs
      SCHEDULER_ALLOW_HTTP_TARGETS: "true"
      SCHEDULER_START_PAUSED: "true"
      MAX_WORKERS: "1"
      PROCESSING_BATCH_SIZE: "1"
      WORKER_POLL_INTERVAL_SECONDS: "2"
      SCHEDULER_POLL_INTERVAL: 2s
      EXTRACTION_SERVICE_NATIVE_BROWSER: "false"
    depends_on:
      migrate: {condition: service_completed_successfully}
      executor: {condition: service_healthy}
      extractor: {condition: service_healthy}
    networks: [fixture]
  extractor:
    image: evening-news-collector:consolidation-fixture
    environment:
      LISTEN_ADDR: ':8080'
      EXTRACTOR_HTTP_ONLY: "true"
      EXTRACTOR_MAX_CONCURRENT: "1"
      EXTRACTOR_SERVICE_TOKEN: fixture-only-collector-token
    networks: [fixture]
  topology-sentinel:
    image: alpine:3.21
    command: ["sleep", "infinity"]
    labels:
      fixture.sentinel: preserve
    networks: [fixture]
  adopter:
    image: evening-news-executor:consolidation-fixture
    user: "0:0"
    entrypoint: ["/usr/local/bin/newsctl"]
    volumes:
      - managed_setup:/state/setup
      - managed_web:/state/web
      - managed_domain:/state/domain
      - managed_background:/state/background
      - managed_extractor:/state/extractor
      - %s:/gate
    networks: [fixture]
  probe:
    image: alpine:3.21
    entrypoint: ["/fixture/fixture-http"]
    volumes: ["%s:/fixture:ro"]
    networks: [fixture]
networks:
  fixture:
    internal: true
    ipam:
      config:
        - subnet: %s
volumes:
  managed_setup: {}
  managed_web: {}
  managed_domain: {}
  managed_background: {}
  managed_extractor: {}
`, major, fixturePassword, fixturePassword, webDB, adminPassword, fixtureSubnet, domainDB, workerDB, executionDB, ingestionDB, processingDB, schedulerDB, fixtureDir, fixtureDir, fixtureSubnet)
}
