// vertical-fixture-gate exercises the initial image pipeline only. It never
// writes a release acceptance receipt: recovery and fault phases are separate.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-tools/fixturegate"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var jobIDPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

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
func (g *gate) diagnose() {
	for _, role := range []string{"postgres", "origin", "fake", "collector", "executor", "background", "web"} {
		id, e := g.docker(nil, "ps", "-aq", role)
		if e != nil || id == "" {
			fmt.Fprintf(os.Stderr, "fixture role=%s state=absent\n", role)
			continue
		}
		raw, e := exec.Command("docker", "inspect", "--format", `{{.State.Status}} {{.State.ExitCode}} {{.State.OOMKilled}}`, id).Output()
		if e != nil {
			fmt.Fprintf(os.Stderr, "fixture role=%s state=unknown\n", role)
			continue
		}
		fields := strings.Fields(string(raw))
		if len(fields) != 3 || !map[string]bool{"created": true, "running": true, "exited": true, "restarting": true, "dead": true, "paused": true}[fields[0]] || !regexp.MustCompile(`^[0-9]{1,3}$`).MatchString(fields[1]) || (fields[2] != "true" && fields[2] != "false") {
			fmt.Fprintf(os.Stderr, "fixture role=%s state=unknown\n", role)
			continue
		}
		logs, _ := exec.Command("docker", "logs", "--tail", "80", id).CombinedOutput()
		code := "none"
		for _, candidate := range []struct{ needle, code string }{{"DATABASE_URL is required", "database_config"}, {"ADMIN_PASSWORD is required", "admin_config"}, {"WEB_BACKEND_URL and WEB_BACKEND_TOKEN", "backend_config"}, {"database unavailable", "database_unavailable"}, {"invalid runtime configuration", "runtime_config"}, {"invalid extractor configuration", "collector_config"}, {"invalid extractor token", "collector_token"}, {"address already in use", "address_in_use"}, {"permission denied", "permission_denied"}, {"no such file or directory", "missing_file"}, {"connection refused", "connection_refused"}, {"schema", "schema_error"}} {
			if strings.Contains(strings.ToLower(string(logs)), strings.ToLower(candidate.needle)) {
				code = candidate.code
				break
			}
		}
		fmt.Fprintf(os.Stderr, "fixture role=%s state=%s exit=%s oom=%s diagnostic=%s\n", role, fields[0], fields[1], fields[2], code)
	}
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
		if resultErr != nil {
			g.diagnose()
		}
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
	if _, e := g.docker(nil, "up", "-d", "--pull", "never", "background"); e != nil {
		return e
	}
	for i := 0; i < 90; i++ {
		if _, e := g.docker(nil, "exec", "-T", "background", "wget", "-q", "-O", "/dev/null", "http://127.0.0.1:8095/healthz"); e == nil {
			break
		}
		if i == 89 {
			return errors.New("background liveness unavailable without domain")
		}
		time.Sleep(time.Second)
	}
	if _, e := g.docker(nil, "exec", "-T", "background", "/news-background", "healthcheck"); e == nil {
		return errors.New("background ready without domain")
	}
	container, e := g.docker(nil, "ps", "-q", "background")
	if e != nil || container == "" {
		return errors.New("background container missing")
	}
	inspectPID := func() (string, error) {
		out, e := exec.Command("docker", "inspect", "--format", "{{.State.Pid}}", container).Output()
		return strings.TrimSpace(string(out)), e
	}
	beforePID, e := inspectPID()
	if e != nil || beforePID == "0" || beforePID == "" {
		return errors.New("background PID missing")
	}
	if _, e := g.docker(nil, "up", "-d", "--pull", "never", "executor"); e != nil {
		return e
	}
	for i := 0; i < 90; i++ {
		if _, e := g.docker(nil, "exec", "-T", "background", "/news-background", "healthcheck"); e == nil {
			break
		}
		if i == 89 {
			return errors.New("background readiness unavailable with domain")
		}
		time.Sleep(time.Second)
	}
	afterPID, e := inspectPID()
	if e != nil || afterPID != beforePID {
		return errors.New("background PID changed during domain recovery")
	}
	if _, e := g.docker(nil, "up", "-d", "--pull", "never", "web"); e != nil {
		return e
	}
	internal, e := exec.Command("docker", "network", "inspect", "--format", "{{.Internal}}", g.project+"_fixture").Output()
	if e != nil || strings.TrimSpace(string(internal)) != "true" {
		return errors.New("fixture network not internal")
	}
	for _, role := range []string{"postgres", "origin", "fake", "collector", "executor", "background", "web"} {
		id, e := g.docker(nil, "ps", "-q", role)
		if e != nil || id == "" {
			return errors.New("fixture container missing")
		}
		raw, e := exec.Command("docker", "inspect", "--format", "{{json .HostConfig.PortBindings}}", id).Output()
		if e != nil {
			return errors.New("fixture port inspection failed")
		}
		var bindings map[string]any
		if json.Unmarshal(raw, &bindings) != nil || len(bindings) != 0 {
			return errors.New("fixture host port exposed")
		}
	}
	base := "http://web:8000"

	var job string
	for i := 0; i < 120; i++ {
		status, _, e := g.request("GET", base+"/health", "", nil, nil)
		if e == nil && status == 200 {
			break
		}
		if i == 119 {
			return errors.New("web not ready")
		}
		time.Sleep(time.Second)
	}
	// Pause, import twice and resume an actual scheduler-owned future instant.
	scheduler := func(method, path string, payload any, authenticated bool) (int, []byte, error) {
		var raw []byte
		if payload != nil {
			raw, _ = json.Marshal(payload)
		}
		token := ""
		if authenticated {
			token = "Bearer fixture-scheduler-only"
		}
		return g.request(method, "http://background:8080"+path, token, raw, nil)
	}
	status, _, e := scheduler("GET", "/v1/status", nil, false)
	if e != nil || status != 401 {
		return errors.New("scheduler authentication boundary failed")
	}
	status, raw, e := scheduler("GET", "/v1/status", nil, true)
	var initialControl struct {
		Revision int `json:"control_revision"`
	}
	if e != nil || status != 200 || json.Unmarshal(raw, &initialControl) != nil {
		return errors.New("scheduler status unavailable")
	}
	status, raw, e = scheduler("PUT", "/v1/control", map[string]any{"expected_revision": initialControl.Revision, "paused": true}, true)
	var paused struct {
		Revision int  `json:"revision"`
		Paused   bool `json:"paused"`
	}
	if e != nil || status != 200 || json.Unmarshal(raw, &paused) != nil || !paused.Paused || paused.Revision != initialControl.Revision+1 {
		return errors.New("scheduler pause failed")
	}
	due := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Microsecond)
	schedule := map[string]any{"schedule_key": "fixture:consolidated-due", "enabled": true, "calendar": map[string]any{"type": "interval", "timezone": "UTC", "interval_minutes": 1440}, "target_alias": "news-worker", "operation": "news_processing", "payload": map[string]any{}, "next_due_at": due.Format(time.RFC3339Nano)}
	imp := map[string]any{"expected_control_revision": paused.Revision, "items": []any{schedule}}
	status, _, e = scheduler("POST", "/v1/import", imp, true)
	if e != nil || status != 200 {
		return errors.New("scheduler import failed")
	}
	status, raw, e = scheduler("POST", "/v1/import", imp, true)
	var replay struct {
		Unchanged []any `json:"unchanged"`
	}
	if e != nil || status != 200 || json.Unmarshal(raw, &replay) != nil || len(replay.Unchanged) != 1 {
		return errors.New("scheduler import replay changed")
	}
	status, raw, e = scheduler("GET", "/v1/schedules/fixture:consolidated-due", nil, true)
	var fetched struct {
		NextDueAt time.Time `json:"next_due_at"`
	}
	if e != nil || status != 200 || json.Unmarshal(raw, &fetched) != nil || !fetched.NextDueAt.Equal(due) {
		return errors.New("scheduler due instant changed")
	}
	status, raw, e = scheduler("PUT", "/v1/control", map[string]any{"expected_revision": paused.Revision, "paused": false}, true)
	var resumed struct {
		Revision int  `json:"revision"`
		Paused   bool `json:"paused"`
	}
	if e != nil || status != 200 || json.Unmarshal(raw, &resumed) != nil || resumed.Paused || resumed.Revision != paused.Revision+1 {
		return errors.New("scheduler resume failed")
	}
	status, raw, e = scheduler("PUT", "/v1/control", map[string]any{"expected_revision": resumed.Revision, "paused": true}, true)
	if e != nil || status != 200 || json.Unmarshal(raw, &paused) != nil || !paused.Paused {
		return errors.New("scheduler repause failed")
	}
	auth := "Basic Zml4dHVyZS1hZG1pbjpmaXh0dXJlLWFkbWluLXBhc3N3b3Jk"
	for i := 0; i < 120; i++ {
		status, body, e := g.request("POST", base+"/api/v1/process/run", auth, []byte(`{}`), map[string]string{"Idempotency-Key": "vertical-fixture-job"})
		if e == nil && status == 202 {
			var v struct {
				Job string `json:"job_id"`
			}
			if json.Unmarshal(body, &v) != nil || !jobIDPattern.MatchString(v.Job) {
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
		status, body, e := g.request("GET", base+"/api/v1/jobs/"+job, auth, nil, nil)
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
	stopStart := time.Now()
	if _, e = g.docker(nil, "stop", "--timeout", "30", "background"); e != nil || time.Since(stopStart) >= 30*time.Second {
		return errors.New("idle background shutdown failed")
	}
	exitCode, e := exec.Command("docker", "inspect", "--format", "{{.State.ExitCode}}", container).Output()
	if e != nil || strings.TrimSpace(string(exitCode)) != "0" {
		return errors.New("idle background shutdown exit failed")
	}
	if _, e = g.docker(nil, "start", "background"); e != nil {
		return errors.New("background restart failed")
	}
	for i := 0; i < 90; i++ {
		if _, e = g.docker(nil, "exec", "-T", "background", "/news-background", "healthcheck"); e == nil {
			break
		}
		if i == 89 {
			return errors.New("background readiness after restart failed")
		}
		time.Sleep(time.Second)
	}
	currentContainer, e := g.docker(nil, "ps", "-q", "background")
	if e != nil || currentContainer != container {
		return errors.New("background container changed during restart")
	}
	// Hold a fresh article during a real AI callback, then stop and restart the
	// same background container. The fake counts acceptance before blocking.
	counters := func() (int, error) {
		out, e := g.docker(nil, "exec", "-T", "fake", "wget", "-q", "-O", "-", "http://127.0.0.1:8090/__gate/counters")
		if e != nil {
			return 0, e
		}
		var v struct {
			Providers struct {
				Gemini int `json:"gemini"`
			} `json:"providers"`
		}
		if json.Unmarshal([]byte(out), &v) != nil {
			return 0, errors.New("fixture counters invalid")
		}
		return v.Providers.Gemini, nil
	}
	control := func(mode string) error {
		_, e := g.docker(nil, "exec", "-T", "fake", "wget", "-q", "-O", "/dev/null", "--header=Content-Type: application/json", "--post-data={\"gemini_mode\":\""+mode+"\"}", "http://127.0.0.1:8090/__gate/control")
		return e
	}
	before, e := counters()
	if e != nil {
		return errors.New("fixture counters unavailable")
	}
	if e = control("block"); e != nil {
		return errors.New("fixture AI hold unavailable")
	}
	defer control("release")
	marker := g.project[len(g.project)-12:]
	content := strings.Repeat("This is a synthetic fixture article created only for the disposable active shutdown test. Its unique body ensures the article has not passed through prior processing. ", 10) + marker
	digest := sha256.Sum256([]byte(content))
	sql := fmt.Sprintf(`INSERT INTO articles (source_id,title,url,content,fetched_at,processed,summary_processed,category_processed,ad_processed,processing_state,processing_attempts,hash_content,is_advertisement,ad_confidence,media_files) SELECT id,'Synthetic active SIGTERM fixture %s','https://rss.fixture.test/active-sigterm/%s','%s',clock_timestamp() AT TIME ZONE 'UTC',false,false,false,false,'pending',0,'%x',false,0,'[]'::jsonb FROM sources WHERE name='Go Web Gate RSS 1' RETURNING id`, marker, marker, content, digest)
	articleID, e := g.docker(nil, "exec", "-T", "postgres", "psql", "-U", "gate_user", "-d", "gate_db", "-qAtc", sql)
	if e != nil || articleID == "" {
		return errors.New("active shutdown article seed failed")
	}
	status, body, e := g.request("POST", base+"/api/v1/articles/"+articleID+"/reprocess", auth, []byte(`{"force":false}`), map[string]string{"Idempotency-Key": "active-term-fixture-" + marker})
	if e != nil || status != 202 {
		return errors.New("active shutdown job not accepted")
	}
	var active struct {
		Job string `json:"job_id"`
	}
	if json.Unmarshal(body, &active) != nil || !jobIDPattern.MatchString(active.Job) {
		return errors.New("active shutdown job invalid")
	}
	for i := 0; i < 90; i++ {
		n, e := counters()
		if e == nil && n > before {
			break
		}
		if i == 89 {
			return errors.New("active shutdown job did not reach fake AI")
		}
		time.Sleep(time.Second)
	}
	stopStart = time.Now()
	if _, e = g.docker(nil, "stop", "--timeout", "30", "background"); e != nil || time.Since(stopStart) >= 30*time.Second {
		return errors.New("active background shutdown failed")
	}
	exitCode, e = exec.Command("docker", "inspect", "--format", "{{.State.ExitCode}}", container).Output()
	if e != nil || strings.TrimSpace(string(exitCode)) != "0" {
		return errors.New("active background shutdown exit failed")
	}
	if e = control("release"); e != nil {
		return errors.New("fixture AI release failed")
	}
	if _, e = g.docker(nil, "start", "background"); e != nil {
		return errors.New("active background restart failed")
	}
	for i := 0; i < 180; i++ {
		status, body, e := g.request("GET", base+"/api/v1/jobs/"+active.Job, auth, nil, nil)
		if e == nil && status == 200 {
			var v struct {
				Status, JobID string `json:"-"`
			}
			var wire map[string]any
			if json.Unmarshal(body, &wire) == nil {
				v.Status, _ = wire["status"].(string)
				v.JobID, _ = wire["job_id"].(string)
				if v.Status == "succeeded" && v.JobID == active.Job {
					break
				}
				if v.Status == "failed" || v.Status == "delivery_unknown" {
					return errors.New("active shutdown job recovery failed")
				}
			}
		}
		if i == 179 {
			return errors.New("active shutdown job recovery timed out")
		}
		time.Sleep(time.Second)
	}
	currentContainer, e = g.docker(nil, "ps", "-q", "background")
	if e != nil || currentContainer != container {
		return errors.New("active shutdown changed background container")
	}
	// Verify each background component uses its own scoped DB login and the
	// worker has no article-table privileges.
	roleRows, e := g.docker(nil, "exec", "-T", "postgres", "psql", "-U", "gate_user", "-d", "gate_db", "-qAtc", `SELECT usename,datname,count(*) FROM pg_stat_activity WHERE usename IN ('fixture_worker_runtime','fixture_execution_runtime','fixture_scheduler_runtime','news_ingestion_runtime','news_processing_runtime') GROUP BY usename,datname ORDER BY usename,datname`)
	if e != nil {
		return errors.New("runtime database roles unavailable")
	}
	expectedDB := map[string]string{"fixture_worker_runtime": "gate_db", "fixture_execution_runtime": "gate_db", "fixture_scheduler_runtime": "gate_scheduler", "news_ingestion_runtime": "gate_db", "news_processing_runtime": "gate_db"}
	caps := map[string]int{"fixture_worker_runtime": 4, "fixture_execution_runtime": 3, "fixture_scheduler_runtime": 3, "news_ingestion_runtime": 3, "news_processing_runtime": 3}
	seen := map[string]bool{}
	for _, line := range strings.Split(roleRows, "\n") {
		fields := strings.Split(line, "|")
		if len(fields) != 3 {
			return errors.New("runtime database roles invalid")
		}
		count, e := strconv.Atoi(fields[2])
		if e != nil || expectedDB[fields[0]] != fields[1] || count < 1 || count > caps[fields[0]] || seen[fields[0]] {
			return errors.New("runtime database roles invalid")
		}
		seen[fields[0]] = true
	}
	if len(seen) != 5 {
		return errors.New("runtime database roles missing")
	}
	privilege, e := g.docker(nil, "exec", "-T", "postgres", "psql", "-U", "gate_user", "-d", "gate_db", "-qAtc", `SELECT (has_table_privilege('fixture_worker_runtime','public.articles','SELECT') OR has_table_privilege('fixture_worker_runtime','public.articles','INSERT') OR has_table_privilege('fixture_worker_runtime','public.articles','UPDATE') OR has_table_privilege('fixture_worker_runtime','public.articles','DELETE'))::text`)
	if e != nil || privilege != "false" {
		return errors.New("worker article privilege exceeded")
	}
	// Generate a fresh immutable edition via the real web/job/domain path.
	pg := func(sql string) (string, error) {
		return g.docker(nil, "exec", "-T", "postgres", "psql", "-U", "gate_user", "-d", "gate_db", "-qAtc", sql)
	}
	enqueue := func(path, key string) (string, error) {
		status, body, e := g.request("POST", base+path, auth, []byte(`{}`), map[string]string{"Idempotency-Key": key})
		if e != nil || status != 202 {
			return "", errors.New("fixture job not accepted")
		}
		var v struct {
			Job string `json:"job_id"`
		}
		if json.Unmarshal(body, &v) != nil || !jobIDPattern.MatchString(v.Job) {
			return "", errors.New("fixture job identity invalid")
		}
		return v.Job, nil
	}
	waitJob := func(id, want string) error {
		for i := 0; i < 180; i++ {
			status, body, e := g.request("GET", base+"/api/v1/jobs/"+id, auth, nil, nil)
			if e == nil && status == 200 {
				var v struct {
					Status string `json:"status"`
				}
				if json.Unmarshal(body, &v) == nil {
					if v.Status == want {
						return nil
					}
					if v.Status == "failed" || v.Status == "delivery_unknown" || v.Status == "interrupted" {
						return errors.New("fixture job terminal state mismatch")
					}
				}
			}
			time.Sleep(time.Second)
		}
		return errors.New("fixture job timed out")
	}
	defaultCounters := func() (int, int, error) {
		out, e := g.docker(nil, "exec", "-T", "fake", "wget", "-q", "-O", "-", "http://127.0.0.1:8090/__gate/counters")
		if e != nil {
			return 0, 0, e
		}
		var v struct {
			Providers struct {
				Telegram  int `json:"telegram"`
				Editorial int `json:"editorial_gemini"`
			} `json:"providers"`
		}
		if json.Unmarshal([]byte(out), &v) != nil {
			return 0, 0, errors.New("fixture counters invalid")
		}
		return v.Providers.Telegram, v.Providers.Editorial, nil
	}
	beforeSend, beforeEditorial, e := defaultCounters()
	if e != nil {
		return errors.New("fixture counters unavailable")
	}
	occurrence := "00000000-0000-4000-8000-000000000071"
	envelope, _ := json.Marshal(map[string]any{"version": 1, "occurrence_id": occurrence, "schedule_key": "news:news_digest", "schedule_revision": 1, "scheduled_for": time.Now().UTC().Format(time.RFC3339), "operation": "news_digest", "payload": map[string]any{}})
	status, body, e = g.request("POST", base+"/api/v1/internal/scheduled-jobs", "Bearer fixture-only-scheduled-jobs-token", envelope, map[string]string{"Idempotency-Key": occurrence})
	if e != nil || status != 202 {
		return errors.New("default digest job not accepted")
	}
	var scheduled struct {
		Job string `json:"job_id"`
	}
	if json.Unmarshal(body, &scheduled) != nil || !jobIDPattern.MatchString(scheduled.Job) {
		return errors.New("default digest job invalid")
	}
	if e = waitJob(scheduled.Job, "succeeded"); e != nil {
		return errors.New("default digest job failed")
	}
	defaultProof, e := pg("SELECT COALESCE(j.result->>'parts_sent','') || '|' || count(e.id)::text || '|' || COALESCE(max(jsonb_array_length(e.snapshot->'articles')),0)::text FROM background_jobs j LEFT JOIN editorial_digest_editions e ON e.job_id=j.id AND e.locale='ru' WHERE j.id='" + scheduled.Job + "'::uuid GROUP BY j.id")
	if e != nil {
		return errors.New("default digest receipt unavailable")
	}
	fields := strings.Split(defaultProof, "|")
	if len(fields) != 3 {
		return errors.New("default digest receipt invalid")
	}
	parts, e1 := strconv.Atoi(fields[0])
	editions, e2 := strconv.Atoi(fields[1])
	articles, e3 := strconv.Atoi(fields[2])
	afterSend, afterEditorial, e := defaultCounters()
	if e != nil || e1 != nil || e2 != nil || e3 != nil || parts < 1 || editions != 1 || articles < 1 || afterSend <= beforeSend || afterEditorial <= beforeEditorial {
		return errors.New("default digest did not generate and deliver")
	}
	previous, e := pg(`SELECT COALESCE((SELECT id::text FROM editorial_digest_editions WHERE edition_date=(clock_timestamp() AT TIME ZONE 'UTC')::date AND locale='ru' ORDER BY created_at DESC,id DESC LIMIT 1),'')`)
	if e != nil {
		return errors.New("edition query failed")
	}
	generation, e := enqueue("/api/v1/summaries/generate", "vertical-fresh-edition-"+marker)
	if e != nil {
		return e
	}
	if e = waitJob(generation, "succeeded"); e != nil {
		return errors.New("edition generation failed")
	}
	fresh, e := pg("SELECT e.id::text FROM editorial_digest_editions e JOIN background_job_executions x ON x.job_id=e.job_id AND x.attempt_generation=e.attempt_generation WHERE e.job_id='" + generation + "'::uuid AND e.locale='ru' AND x.state='succeeded'")
	if e != nil || !jobIDPattern.MatchString(fresh) || fresh == previous {
		return errors.New("fresh edition receipt missing")
	}
	latest, e := pg(`SELECT COALESCE((SELECT id::text FROM editorial_digest_editions WHERE edition_date=(clock_timestamp() AT TIME ZONE 'UTC')::date AND locale='ru' ORDER BY created_at DESC,id DESC LIMIT 1),'')`)
	if e != nil || latest != fresh {
		return errors.New("fresh edition is not latest")
	}
	// Provider accepts one message and withholds its response. Force loss of
	// the worker lease while it is in flight, then require unknown without replay.
	telegramCount := func() (int, error) {
		out, e := g.docker(nil, "exec", "-T", "fake", "wget", "-q", "-O", "-", "http://127.0.0.1:8090/__gate/counters")
		if e != nil {
			return 0, e
		}
		var v struct {
			Providers struct {
				Telegram int `json:"telegram"`
			} `json:"providers"`
		}
		if json.Unmarshal([]byte(out), &v) != nil {
			return 0, errors.New("invalid fixture counters")
		}
		return v.Providers.Telegram, nil
	}
	telegramControl := func(mode string) error {
		_, e := g.docker(nil, "exec", "-T", "fake", "wget", "-q", "-O", "/dev/null", "--header=Content-Type: application/json", "--post-data={\"telegram_mode\":\""+mode+"\"}", "http://127.0.0.1:8090/__gate/control")
		return e
	}
	if e = telegramControl("accept_then_hold"); e != nil {
		return errors.New("telegram hold failed")
	}
	defer telegramControl("release")
	beforeTelegram, e := telegramCount()
	if e != nil {
		return errors.New("telegram counter unavailable")
	}
	delivery, e := enqueue("/api/v1/telegram/send-digest", "vertical-unknown-delivery-"+marker)
	if e != nil {
		return e
	}
	for i := 0; i < 90; i++ {
		n, e := telegramCount()
		if e == nil && n == beforeTelegram+1 {
			break
		}
		if e == nil && n > beforeTelegram+1 {
			return errors.New("duplicate telegram before lease loss")
		}
		if i == 89 {
			return errors.New("fake telegram did not accept")
		}
		time.Sleep(time.Second)
	}
	leasePID, e := inspectPID()
	if e != nil || leasePID == "" || leasePID == "0" {
		return errors.New("worker process unavailable")
	}
	leaseBefore, e := pg("SELECT generation::text || '|' || lock_backend_pid::text FROM background_worker_status WHERE id=1")
	if e != nil || leaseBefore == "" {
		return errors.New("worker lease unavailable")
	}
	lockName := g.project + "-worker-lock"
	lockID, e := g.docker(nil, "run", "-d", "--name", lockName, "--no-deps", "lock")
	if e != nil || lockID == "" {
		return errors.New("worker lock helper failed")
	}
	defer func() { _ = exec.Command("docker", "rm", "-f", lockID).Run() }()
	for i := 0; i < 30; i++ {
		out, _ := exec.Command("docker", "logs", lockID).Output()
		if strings.Contains(string(out), "fixture_lock_acquired") {
			break
		}
		if i == 29 {
			return errors.New("worker lock not acquired")
		}
		time.Sleep(time.Second)
	}
	readinessLost := false
	for i := 0; i < 22; i++ {
		if _, e = g.docker(nil, "exec", "-T", "background", "/news-background", "healthcheck"); e != nil {
			readinessLost = true
			break
		}
		time.Sleep(time.Second)
	}
	if !readinessLost {
		return errors.New("lease loss did not drop readiness")
	}
	lockExit, e := exec.Command("docker", "wait", lockID).Output()
	if e != nil || strings.TrimSpace(string(lockExit)) != "0" {
		return errors.New("worker lock helper failed")
	}
	for i := 0; i < 90; i++ {
		if _, e = g.docker(nil, "exec", "-T", "background", "/news-background", "healthcheck"); e == nil {
			break
		}
		if i == 89 {
			return errors.New("worker did not recover")
		}
		time.Sleep(time.Second)
	}
	leaseAfter, e := pg("SELECT generation::text || '|' || lock_backend_pid::text FROM background_worker_status WHERE id=1")
	if e != nil {
		return errors.New("worker lease not replaced")
	}
	beforeFields, afterFields := strings.Split(leaseBefore, "|"), strings.Split(leaseAfter, "|")
	if len(beforeFields) != 2 || len(afterFields) != 2 {
		return errors.New("worker lease snapshot invalid")
	}
	beforeGeneration, e1 := strconv.Atoi(beforeFields[0])
	afterGeneration, e2 := strconv.Atoi(afterFields[0])
	oldPID, e3 := strconv.Atoi(beforeFields[1])
	newPID, e4 := strconv.Atoi(afterFields[1])
	if e1 != nil || e2 != nil || e3 != nil || e4 != nil || oldPID <= 0 || newPID <= 0 || afterGeneration <= beforeGeneration || newPID == oldPID {
		return errors.New("worker lease not replaced")
	}
	oldSessions, e := pg(fmt.Sprintf("SELECT count(*) FROM pg_stat_activity WHERE pid=%d", oldPID))
	if e != nil || oldSessions != "0" {
		return errors.New("old worker session not released")
	}
	pidAfter, e := inspectPID()
	if e != nil || pidAfter != leasePID {
		return errors.New("worker recovery changed process")
	}
	if e = telegramControl("release"); e != nil {
		return errors.New("telegram release failed")
	}
	if e = waitJob(delivery, "delivery_unknown"); e != nil {
		return errors.New("ambiguous delivery not marked unknown")
	}
	time.Sleep(2 * time.Second)
	afterTelegram, e := telegramCount()
	if e != nil || afterTelegram != beforeTelegram+1 {
		return errors.New("ambiguous delivery replayed")
	}
	// A 429 must be persisted as a definite rejection, with no immediate
	// extra send by the same attempt.
	rateEdition, e := enqueue("/api/v1/summaries/generate", "vertical-rate-edition-"+marker)
	if e != nil {
		return e
	}
	if e = waitJob(rateEdition, "succeeded"); e != nil {
		return errors.New("rate limit edition failed")
	}
	if e = telegramControl("rate_limit"); e != nil {
		return errors.New("rate limit fixture control failed")
	}
	rateBefore, e := telegramCount()
	if e != nil {
		return errors.New("rate limit counter unavailable")
	}
	rateJob, e := enqueue("/api/v1/telegram/send-digest", "vertical-rate-delivery-"+marker)
	if e != nil {
		return e
	}
	if e = waitJob(rateJob, "failed"); e != nil {
		return errors.New("rate limit delivery did not fail")
	}
	rateAfter, e := telegramCount()
	if e != nil || rateAfter != rateBefore+1 {
		return errors.New("rate limited delivery replayed")
	}
	rateProof, e := pg("SELECT count(*) FROM digest_delivery_parts WHERE job_id='" + rateJob + "'::uuid AND state='rejected' AND status_code=429 AND retry_after_seconds>0")
	if e != nil || rateProof != "1" {
		return errors.New("rate limit receipt missing")
	}
	if e = telegramControl("normal"); e != nil {
		return errors.New("rate limit fixture release failed")
	}
	// Back up the synthetic database via the real job and strictly restore its
	// SQL into a separate disposable database of the same server major.
	backupJob, e := enqueue("/api/v1/backup", "vertical-backup-"+marker)
	if e != nil {
		return e
	}
	if e = waitJob(backupJob, "succeeded"); e != nil {
		return errors.New("fixture backup job failed")
	}
	archive, e := g.docker(nil, "exec", "-T", "executor", "find", "/app/backups", "-maxdepth", "1", "-type", "f", "-name", "*"+strings.ReplaceAll(backupJob, "-", "")+"_a*.tar.gz")
	if e != nil || strings.Contains(archive, "\n") || !strings.HasPrefix(archive, "/app/backups/news_aggregator_backup_") {
		return errors.New("fixture backup archive missing")
	}
	members, e := g.docker(nil, "exec", "-T", "executor", "tar", "-tzf", archive)
	if e != nil {
		return errors.New("fixture backup archive invalid")
	}
	sqlMember := ""
	for _, member := range strings.Split(members, "\n") {
		if strings.HasSuffix(member, "/database.sql") {
			if sqlMember != "" || strings.HasPrefix(member, "/") || strings.Contains(member, "..") {
				return errors.New("fixture backup SQL member invalid")
			}
			sqlMember = member
		}
	}
	if sqlMember == "" {
		return errors.New("fixture backup SQL missing")
	}
	if _, e = g.docker(nil, "exec", "-T", "postgres", "createdb", "-U", "gate_user", "gate_restore"); e != nil {
		return errors.New("fixture restore database failed")
	}
	composeArgs := []string{"compose", "--project-name", g.project, "--file", g.compose}
	restoreCtx, restoreCancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer restoreCancel()
	extract := exec.CommandContext(restoreCtx, "docker", append(append([]string{}, composeArgs...), "exec", "-T", "executor", "tar", "-xOzf", archive, sqlMember)...)
	restore := exec.CommandContext(restoreCtx, "docker", append(append([]string{}, composeArgs...), "exec", "-T", "postgres", "psql", "-X", "-v", "ON_ERROR_STOP=1", "-U", "gate_user", "-d", "gate_restore")...)
	pipe, e := extract.StdoutPipe()
	if e != nil {
		return errors.New("fixture restore stream failed")
	}
	restore.Stdin = pipe
	if e = restore.Start(); e != nil {
		return errors.New("fixture restore process failed")
	}
	if e = extract.Start(); e != nil {
		_ = restore.Process.Kill()
		_ = restore.Wait()
		return errors.New("fixture extraction process failed")
	}
	extractErr := extract.Wait()
	restoreErr := restore.Wait()
	if extractErr != nil || restoreErr != nil {
		return errors.New("fixture strict restore failed")
	}
	restored, e := g.docker(nil, "exec", "-T", "postgres", "psql", "-U", "gate_user", "-d", "gate_restore", "-qAtc", `SELECT (SELECT count(*) FROM sources WHERE enabled)::text || '|' || (SELECT count(*) FROM articles WHERE processed)::text`)
	if e != nil {
		return errors.New("fixture restored database query failed")
	}
	restoredCounts := strings.Split(restored, "|")
	if len(restoredCounts) != 2 || restoredCounts[0] != "8" {
		return errors.New("fixture restored source count invalid")
	}
	restoredProcessed, e := strconv.Atoi(restoredCounts[1])
	if e != nil || restoredProcessed < 1 {
		return errors.New("fixture restored article count invalid")
	}
	status, _, e = g.request("POST", base+"/api/v1/backup/restore?filename=fixture.tar.gz", auth, []byte(`{}`), nil)
	if e != nil || status != 409 {
		return errors.New("worker restore boundary changed")
	}
	if err := fixturegate.VerifyImages(refs, initial, inspect); err != nil {
		return err
	}
	fmt.Println("Vertical fixture job and eight source receipts passed; this is not release acceptance.")
	return errors.New("remaining fault and lifecycle acceptance phases not implemented")
}
func (g *gate) request(method, target, auth string, body []byte, headers map[string]string) (int, []byte, error) {
	base, role := "http://web:8000", "web"
	if strings.HasPrefix(target, "http://background:8080/") {
		base, role = "http://background:8080", "scheduler"
	}
	if !strings.HasPrefix(target, base+"/") {
		return 0, nil, errors.New("invalid fixture probe target")
	}
	input, e := json.Marshal(struct {
		Target, Method, Path, Authorization string
		Body                                []byte
		Headers                             map[string]string
	}{role, method, strings.TrimPrefix(target, base), auth, body, headers})
	if e != nil {
		return 0, nil, e
	}
	output, e := g.docker(input, "exec", "-T", "fake", "/fixture/fixture-http")
	if e != nil {
		return 0, nil, e
	}
	var result struct {
		Status int    `json:"status"`
		Body   []byte `json:"body"`
	}
	if json.Unmarshal([]byte(output), &result) != nil {
		return 0, nil, errors.New("invalid fixture probe response")
	}
	return result.Status, result.Body, nil
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
	for _, name := range []string{"fixture-origin", "fixture-services", "seed-fixture-sources", "check-fixture-receipts", "fixture-worker-lock", "fixture-http"} {
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
	major := os.Getenv("NEWS_FIXTURE_PG_MAJOR")
	if major == "" {
		major = "17"
	}
	if major != "16" && major != "17" {
		return errors.New("fixture postgres version invalid")
	}
	config := strings.ReplaceAll(composeTemplate, "FIXTURE_DIR", g.dir)
	config = strings.ReplaceAll(config, "FIXTURE_PG_IMAGE", "postgres:"+major+"-alpine")
	return os.WriteFile(g.compose, []byte(config), 0600)
}

const composeTemplate = `services:
  postgres:
    image: FIXTURE_PG_IMAGE
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
  lock:
    image: alpine:3.21
    entrypoint: ["/fixture/fixture-worker-lock"]
    environment: {FIXTURE_DATABASE_URL: 'postgres://fixture_worker_runtime:fixture-only-password@postgres:5432/gate_db?sslmode=disable', NEWS_FIXTURE_CONFIRM: disposable-fixture-only}
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
    networks: [fixture]
networks:
  fixture:
    internal: true
    ipam:
      config: [{subnet: 11.203.254.240/28}]
`
