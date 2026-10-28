// vertical-fixture-gate exercises the initial image pipeline only. It never
// writes a release acceptance receipt: recovery and fault phases are separate.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/fixturegate"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type gate struct{ project, dir, compose string }

func (g *gate) docker(input []byte, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", append([]string{"compose", "--project-name", g.project, "--file", g.compose}, args...)...)
	cmd.Stdin = bytes.NewReader(input)
	out, e := cmd.Output()
	if e != nil {
		return "", fmt.Errorf("fixture docker %s failed", args[0])
	}
	return strings.TrimSpace(string(out)), nil
}
func (g *gate) run() (resultErr error) {
	refs := map[string]string{"web": "evening-news-web:consolidation-fixture", "background": "evening-news-background:consolidation-fixture", "executor": "evening-news-executor:consolidation-fixture"}
	inspect := func(args ...string) (string, error) {
		out, err := exec.Command("docker", args...).Output()
		return string(out), err
	}
	initial, err := fixturegate.CaptureImages(refs, inspect)
	if err != nil {
		return err
	}
	defer func() {
		if _, err := g.docker(nil, "down", "--volumes", "--remove-orphans"); err != nil {
			resultErr = errors.New("fixture cleanup failed")
		}
	}()
	if _, e := g.docker(nil, "up", "-d", "--pull", "never", "postgres", "origin", "fake", "collector"); e != nil {
		return e
	}
	for i := 0; i < 60; i++ {
		if _, e := g.docker(nil, "exec", "-T", "postgres", "pg_isready", "-U", "gate_user", "-d", "gate_db"); e == nil {
			break
		}
		if i == 59 {
			return errors.New("postgres not ready")
		}
		time.Sleep(time.Second)
	}
	if _, e := g.docker(nil, "run", "--rm", "--no-deps", "--entrypoint", "/usr/local/bin/news-migrate", "executor"); e != nil {
		return errors.New("migration failed")
	}
	for _, role := range []struct {
		path, prompt, replacement string
		callback                  bool
	}{{"deploy/news-ingestion/provision-runtime-role.sql", `\password news_ingestion_runtime`, `ALTER ROLE news_ingestion_runtime PASSWORD 'fixture-only-password';`, false}, {"deploy/news-processing/provision-runtime-role.sql", `\password news_processing_runtime`, `ALTER ROLE news_processing_runtime PASSWORD 'fixture-only-password';`, true}} {
		raw, e := os.ReadFile(role.path)
		if e != nil {
			return e
		}
		if strings.Count(string(raw), role.prompt) != 1 {
			return errors.New("role template changed")
		}
		sql := strings.Replace(string(raw), role.prompt, role.replacement, 1)
		args := []string{"exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-v", "news_schema=public"}
		if role.callback {
			args = append(args, "-v", "news_callback_role=gate_user")
		}
		args = append(args, "-U", "gate_user", "-d", "gate_db")
		if _, e = g.docker([]byte(sql), args...); e != nil {
			return errors.New("role provisioning failed")
		}
	}
	if _, e := g.docker([]byte(roleSQL), "exec", "-T", "postgres", "psql", "-v", "ON_ERROR_STOP=1", "-U", "gate_user", "-d", "gate_db"); e != nil {
		return errors.New("background role provisioning failed")
	}
	if _, e := g.docker(nil, "exec", "-T", "postgres", "createdb", "-U", "gate_user", "-O", "fixture_scheduler_runtime", "gate_scheduler"); e != nil {
		return errors.New("scheduler database creation failed")
	}
	if _, e := g.docker(nil, "run", "--rm", "--no-deps", "seed"); e != nil {
		return errors.New("seed failed")
	}
	if _, e := g.docker(nil, "up", "-d", "--pull", "never", "executor", "background", "web"); e != nil {
		return e
	}
	port, e := g.docker(nil, "port", "web", "8000")
	if e != nil {
		return e
	}
	idx := strings.LastIndex(port, ":")
	if idx < 0 {
		return errors.New("web port unavailable")
	}
	base := "http://127.0.0.1:" + port[idx+1:]
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	var job string
	for i := 0; i < 120; i++ {
		status, _, e := request(client, "GET", base+"/health", "", nil, nil)
		if e == nil && status == 200 {
			break
		}
		if i == 119 {
			return errors.New("web not ready")
		}
		time.Sleep(time.Second)
	}
	auth := "Basic Zml4dHVyZS1hZG1pbjpmaXh0dXJlLWFkbWluLXBhc3N3b3Jk"
	for i := 0; i < 120; i++ {
		status, body, e := request(client, "POST", base+"/api/v1/process/run", auth, []byte(`{}`), map[string]string{"Idempotency-Key": "vertical-fixture-job"})
		if e == nil && status == 202 {
			var v struct {
				Job string `json:"job_id"`
			}
			if json.Unmarshal(body, &v) != nil || v.Job == "" {
				return errors.New("invalid accepted job")
			}
			job = v.Job
			break
		}
		if i == 119 {
			return errors.New("job not accepted")
		}
		time.Sleep(time.Second)
	}
	for i := 0; i < 180; i++ {
		status, body, e := request(client, "GET", base+"/api/v1/jobs/"+job, auth, nil, nil)
		if e == nil && status == 200 {
			var v struct {
				Status string `json:"status"`
			}
			if json.Unmarshal(body, &v) == nil {
				if v.Status == "succeeded" {
					break
				}
				if v.Status == "failed" || v.Status == "interrupted" || v.Status == "delivery_unknown" {
					return errors.New("job did not succeed")
				}
			}
		}
		if i == 179 {
			return errors.New("job timed out")
		}
		time.Sleep(time.Second)
	}
	receipt, e := g.docker(nil, "run", "--rm", "--no-deps", "receipt", job)
	if e != nil {
		return errors.New("receipt probe failed")
	}
	var summary struct {
		Valid     bool `json:"valid"`
		Processed int  `json:"processed"`
	}
	if json.Unmarshal([]byte(receipt), &summary) != nil || !summary.Valid || summary.Processed < 1 {
		return errors.New("source receipts incomplete")
	}
	if err := fixturegate.VerifyImages(refs, initial, inspect); err != nil {
		return err
	}
	fmt.Println("Vertical fixture job and eight source receipts passed; this is not release acceptance.")
	return errors.New("remaining fault and lifecycle acceptance phases not implemented")
}
func request(c *http.Client, method, url, auth string, body []byte, headers map[string]string) (int, []byte, error) {
	r, e := http.NewRequest(method, url, bytes.NewReader(body))
	if e != nil {
		return 0, nil, e
	}
	if auth != "" {
		r.Header.Set("Authorization", auth)
	}
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	resp, e := c.Do(r)
	if e != nil {
		return 0, nil, e
	}
	defer resp.Body.Close()
	out, e := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	return resp.StatusCode, out, e
}
func main() { os.Exit(realMain()) }
func realMain() int {
	var nonce [8]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		return 1
	}
	dir, e := os.MkdirTemp("", "news-vertical-gate-")
	if e != nil {
		return 1
	}
	defer os.RemoveAll(dir)
	g := &gate{project: "news-fixture-" + hex.EncodeToString(nonce[:]), dir: dir, compose: filepath.Join(dir, "compose.yml")}
	if e = prepare(g); e == nil {
		e = g.run()
	}
	if e != nil {
		fmt.Fprintln(os.Stderr, "vertical fixture gate failed:", e)
		return 1
	}
	return 0
}

const roleSQL = `CREATE ROLE fixture_worker_runtime LOGIN PASSWORD 'fixture-only-password';
CREATE ROLE fixture_execution_runtime LOGIN PASSWORD 'fixture-only-password';
CREATE ROLE fixture_scheduler_runtime LOGIN PASSWORD 'fixture-only-password';
GRANT CONNECT ON DATABASE gate_db TO fixture_worker_runtime, fixture_execution_runtime;
GRANT USAGE ON SCHEMA public TO fixture_worker_runtime, fixture_execution_runtime;
GRANT SELECT,INSERT,UPDATE,DELETE ON TABLE public.background_jobs, public.background_worker_status, public.background_job_executions TO fixture_worker_runtime;
GRANT USAGE,SELECT,UPDATE ON SEQUENCE public.background_jobs_queue_position_seq TO fixture_worker_runtime;
GRANT SELECT,UPDATE ON TABLE public.background_jobs,public.background_worker_status TO fixture_execution_runtime;
GRANT SELECT,INSERT,UPDATE ON TABLE public.background_job_executions TO fixture_execution_runtime;
GRANT SELECT,UPDATE ON TABLE public.schedule_settings,public.articles TO fixture_execution_runtime;`

func prepare(g *gate) error {
	for _, name := range []string{"fixture-origin", "fixture-services", "seed-fixture-sources", "check-fixture-receipts"} {
		cmd := exec.Command("go", "build", "-o", filepath.Join(g.dir, name), "./cmd/"+name)
		cmd.Dir = "go-tools"
		cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64", "CGO_ENABLED=0")
		if e := cmd.Run(); e != nil {
			return errors.New("fixture tool build failed")
		}
	}
	cmd := exec.Command("openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-keyout", filepath.Join(g.dir, "fixture-ca.key"), "-out", filepath.Join(g.dir, "fixture-ca.crt"), "-subj", "/CN=go-web-fixture.test", "-addext", "subjectAltName=DNS:t.me,DNS:rss.fixture.test,DNS:pm.fixture.test,IP:11.203.254.243", "-addext", "basicConstraints=critical,CA:TRUE", "-addext", "keyUsage=critical,digitalSignature,keyCertSign,keyEncipherment", "-addext", "extendedKeyUsage=serverAuth")
	if e := cmd.Run(); e != nil {
		return errors.New("fixture certificate generation failed")
	}
	if e := os.Chmod(g.dir, 0755); e != nil {
		return e
	}
	if e := os.Chmod(filepath.Join(g.dir, "fixture-ca.key"), 0444); e != nil {
		return e
	}
	if e := os.Chmod(filepath.Join(g.dir, "fixture-ca.crt"), 0444); e != nil {
		return e
	}
	return os.WriteFile(g.compose, []byte(strings.ReplaceAll(composeTemplate, "FIXTURE_DIR", g.dir)), 0600)
}

const composeTemplate = `services:
  postgres:
    image: postgres:17-alpine
    environment: {POSTGRES_USER: gate_user, POSTGRES_PASSWORD: fixture-only-password, POSTGRES_DB: gate_db}
    networks: [fixture]
  origin:
    image: alpine:3.21
    entrypoint: ["/fixture/fixture-origin", "--cert", "/fixture/fixture-ca.crt", "--key", "/fixture/fixture-ca.key"]
    volumes: ["FIXTURE_DIR:/fixture:ro"]
    networks:
      fixture: {ipv4_address: 11.203.254.243}
  fake:
    image: alpine:3.21
    entrypoint: ["/fixture/fixture-services"]
    volumes: ["FIXTURE_DIR:/fixture:ro"]
    networks: {fixture: {aliases: [fixture-services]}}
  collector:
    image: evening-news-collector:consolidation-fixture
    environment:
      LISTEN_ADDR: ':8080'
      EXTRACTOR_HTTP_ONLY: 'true'
      EXTRACTOR_MAX_CONCURRENT: '1'
      EXTRACTOR_SERVICE_TOKEN: fixture-only-collector-token
      SSL_CERT_FILE: /fixture/fixture-ca.crt
    volumes: ["FIXTURE_DIR:/fixture:ro"]
    extra_hosts: ["rss.fixture.test:11.203.254.243", "pm.fixture.test:11.203.254.243", "t.me:11.203.254.243"]
    networks: [fixture]
  executor:
    image: evening-news-executor:consolidation-fixture
    environment:
      DATABASE_URL: 'postgres://gate_user:fixture-only-password@postgres:5432/gate_db?sslmode=disable'
      GEMINI_API_KEY: fixture-only-gemini-key
      GEMINI_API_ENDPOINT: http://fixture-services:8090/v1/models
      SUMMARIZATION_MODEL: gemini-2.0-flash
      CATEGORIZATION_MODEL: gemini-2.0-flash
      DIGEST_MODEL: gemini-2.0-flash
      TELEGRAM_TOKEN: fixture-only-token
      TELEGRAM_CHAT_ID: fixture-chat-id
      TELEGRAM_SERVICE_CHAT_ID: fixture-service-chat-id
      TELEGRAM_RICH_MESSAGES_ENABLED: 'false'
      NEWS_FIXTURE_MODE: '1'
      NEWS_TEST_TELEGRAM_ENDPOINT: http://fixture-services:8090/telegram
      SITE_BASE_URL: https://fixture.invalid
      BACKGROUND_EXECUTION_MODE: worker
      JOB_EXECUTOR_TOKEN: fixture-only-executor-token
      ARTICLE_AI_TOKEN: fixture-only-article-ai-token
      WEB_BACKEND_TOKEN: fixture-only-web-backend-token
      EXECUTION_SERVICE_URL: http://background:8101
      EXTRACTION_SERVICE_URL: http://collector:8080
      EXTRACTION_SERVICE_TOKEN: fixture-only-collector-token
    networks: [fixture]
  seed:
    image: alpine:3.21
    entrypoint: ["/fixture/seed-fixture-sources"]
    environment: {FIXTURE_DATABASE_URL: 'postgres://gate_user:fixture-only-password@postgres:5432/gate_db?sslmode=disable', NEWS_FIXTURE_CONFIRM: disposable-fixture-only}
    volumes: ["FIXTURE_DIR:/fixture:ro"]
    networks: [fixture]
  receipt:
    image: alpine:3.21
    entrypoint: ["/fixture/check-fixture-receipts"]
    environment: {FIXTURE_DATABASE_URL: 'postgres://gate_user:fixture-only-password@postgres:5432/gate_db?sslmode=disable', NEWS_FIXTURE_CONFIRM: disposable-fixture-only}
    volumes: ["FIXTURE_DIR:/fixture:ro"]
    networks: [fixture]
  background:
    image: evening-news-background:consolidation-fixture
    environment:
      WORKER_DATABASE_URL: 'postgres://fixture_worker_runtime:fixture-only-password@postgres:5432/gate_db?sslmode=disable'
      EXECUTION_DATABASE_URL: 'postgres://fixture_execution_runtime:fixture-only-password@postgres:5432/gate_db?sslmode=disable'
      EXECUTION_MAX_CONNECTIONS: '2'
      DOMAIN_SERVICE_URL: http://executor:8100
      JOB_EXECUTOR_LISTEN: ':8101'
      SCHEDULER_DATABASE_URL: 'postgres://fixture_scheduler_runtime:fixture-only-password@postgres:5432/gate_scheduler?sslmode=disable'
      INGESTION_DATABASE_URL: 'postgres://news_ingestion_runtime:fixture-only-password@postgres:5432/gate_db?search_path=public&sslmode=disable'
      PROCESSING_DATABASE_URL: 'postgres://news_processing_runtime:fixture-only-password@postgres:5432/gate_db?search_path=public&sslmode=disable'
      SCHEDULER_DB_SCHEMA: scheduler_fixture
      SCHEDULER_API_TOKEN: fixture-scheduler-only
      SCHEDULER_TARGET_ALIASES: news-worker
      SCHEDULER_TARGET_NEWS_WORKER_URL: http://web:8000/api/v1/internal/scheduled-jobs
      SCHEDULER_TARGET_NEWS_WORKER_TOKEN: fixture-only-scheduled-jobs-token
      SCHEDULER_ALLOW_HTTP_TARGETS: 'true'
      SCHEDULER_POLL_INTERVAL: 250ms
      SCHEDULER_DB_MAX_CONNS: '3'
      SCHEDULER_LISTEN_ADDR: ':8080'
      ARTICLE_INGESTION_TOKEN: fixture-only-ingestion-token
      ARTICLE_INGESTION_LISTEN: ':8093'
      ARTICLE_INGESTION_DB_MAX_CONNS: '3'
      EXTRACTION_SERVICE_URL: http://collector:8080
      EXTRACTION_SERVICE_TOKEN: fixture-only-collector-token
      EXTRACTION_SERVICE_NATIVE_BROWSER: 'false'
      JOB_EXECUTOR_URL: http://background:8101
      JOB_EXECUTOR_TOKEN: fixture-only-executor-token
      SELECTOR_SERVICE_URL: http://executor:8100
      ARTICLE_PROCESSING_LISTEN: ':8094'
      ARTICLE_PROCESSING_TOKEN: fixture-only-article-processing-token
      ARTICLE_PROCESSING_DB_MAX_CONNS: '3'
      ARTICLE_AI_URL: http://executor:8100
      ARTICLE_AI_TOKEN: fixture-only-article-ai-token
      MAX_WORKERS: '1'
      PROCESSING_BATCH_SIZE: '2'
      BACKGROUND_HEALTH_LISTEN: ':8095'
      BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS: '25'
      BACKGROUND_WORKER_BACKOFF_MIN: 100ms
      BACKGROUND_WORKER_BACKOFF_MAX: 1s
      WORKER_POLL_INTERVAL_SECONDS: '1'
      WORKER_SHUTDOWN_DRAIN_SECONDS: '8'
      WORKER_MAX_JOB_ATTEMPTS: '3'
      WORKER_JOB_RETENTION_DAYS: '7'
      SSL_CERT_FILE: /fixture/fixture-ca.crt
      EXTRACTION_SERVICE_MAX_CONCURRENT: '1'
    volumes: ["FIXTURE_DIR:/fixture:ro"]
    networks: [fixture]
  web:
    image: evening-news-web:consolidation-fixture
    environment:
      DATABASE_URL: 'postgres://gate_user:fixture-only-password@postgres:5432/gate_db?sslmode=disable'
      ADMIN_USERNAME: fixture-admin
      ADMIN_PASSWORD: fixture-admin-password
      JWT_SECRET: fixture-only-jwt-signing-value
      WEB_BACKEND_URL: http://executor:8100
      WEB_BACKEND_TOKEN: fixture-only-web-backend-token
      SCHEDULED_JOBS_TOKEN: fixture-only-scheduled-jobs-token
      SCHEDULED_JOBS_RECEIPT_LIMIT: '100000'
      SCHEDULER_API_URL: http://background:8080
      SCHEDULER_API_TOKEN: fixture-scheduler-only
      SCHEDULER_API_ALLOW_HTTP: 'true'
      SCHEDULE_DISPATCH_MODE: service
      BACKGROUND_EXECUTION_MODE: worker
      SITE_BASE_URL: http://127.0.0.1
      WEB_LISTEN_ADDRESS: ':8000'
      ALLOWED_ORIGINS: http://127.0.0.1
      TRUSTED_HOSTS: 127.0.0.1,localhost,web
      TRUST_FORWARD_AUTH: 'false'
      DEVELOPMENT: 'true'
    ports: ["127.0.0.1::8000"]
    networks: [fixture]
networks:
  fixture:
    internal: true
    ipam:
      config: [{subnet: 11.203.254.240/28}]
`
