// Package browsercontract verifies the unchanged browser runtime in its image.
package browsercontract

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// This adapter only dispatches JSON inputs to the installed runtime functions;
// assertions and expected results belong to the Go tests below.
const policyAdapter = `import json,sys
sys.path.insert(0,'/opt/news-browser')
import policy,entrypoint
for item in json.load(sys.stdin):
 try:
  name=item['name']; args=item.get('args',[])
  fn={'controllers':policy.parse_controllers,'render':policy.render_policy,'fingerprint':policy.stable_fingerprint,'hooks':policy.readback_has_drop_hooks,'comment':policy.readback_comment_matches,'private':policy.private_ipv4,'identity':entrypoint._verified_supervisor_pid}[name]
  print(json.dumps({'value':fn(*args)}))
 except Exception as e:
  print(json.dumps({'error':type(e).__name__}))
`

type request struct {
	Name string `json:"name"`
	Args []any  `json:"args"`
}
type answer struct {
	Value json.RawMessage `json:"value"`
	Error string          `json:"error"`
}

func image(t *testing.T) string {
	t.Helper()
	name := os.Getenv("NEWS_BROWSER_IMAGE")
	if name == "" {
		if os.Getenv("NEWS_BROWSER_CONTRACT_REQUIRED") == "1" {
			t.Fatal("browser image required")
		}
		t.Skip("built browser image required for black-box contract")
	}
	return name
}

type fixtureRef struct{ name, cidfile string }

func fixture(t *testing.T) fixtureRef {
	t.Helper()
	var nonce [8]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		t.Fatal("fixture identity unavailable")
	}
	return fixtureRef{"news-browser-contract-" + hex.EncodeToString(nonce[:]), filepath.Join(t.TempDir(), "container.id")}
}

var containerID = regexp.MustCompile(`^[0-9a-f]{64}$`)

type dockerRunner func(...string) (string, error)

func cleanupOwned(f fixtureRef, run dockerRunner) error {
	raw, err := os.ReadFile(f.cidfile)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	id := strings.TrimSpace(string(raw))
	if !containerID.MatchString(id) {
		return errors.New("invalid_fixture_identity")
	}
	metadata, err := run("inspect", "--format", `{{.Name}} {{index .Config.Labels "com.dzarlax.browser-contract"}}`, id)
	if err != nil {
		remaining, listErr := run("ps", "-aq", "--filter", "id="+id)
		if listErr != nil || strings.TrimSpace(remaining) != "" {
			return errors.New("fixture_cleanup_failed")
		}
		return nil
	}
	if strings.TrimSpace(metadata) != "/"+f.name+" owned" {
		return errors.New("fixture_ownership_mismatch")
	}
	if _, err = run("rm", "-f", id); err != nil {
		return errors.New("fixture_cleanup_failed")
	}
	return nil
}
func cleanupFixture(t *testing.T, f fixtureRef) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	run := func(args ...string) (string, error) {
		out, err := exec.CommandContext(ctx, "docker", args...).Output()
		return string(out), err
	}
	if cleanupOwned(f, run) != nil {
		t.Error("browser fixture cleanup failed")
	}
}
func containerArgs(f fixtureRef, network string) []string {
	return []string{"run", "--name", f.name, "--cidfile", f.cidfile, "--label", "com.dzarlax.browser-contract=owned", "--pull=never", "--rm", "--no-healthcheck", "--network", network, "--memory", "128m", "--cpus", "0.5", "--pids-limit", "64", "--cap-drop", "ALL", "--security-opt", "no-new-privileges", "--read-only", "--env", "PYTHONDONTWRITEBYTECODE=1", "--entrypoint", "python"}
}
func invoke(t *testing.T, requests []request) []answer {
	t.Helper()
	data, err := json.Marshal(requests)
	if err != nil {
		t.Fatal(err)
	}
	imageName := image(t)
	f := fixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	defer cleanupFixture(t, f)
	args := containerArgs(f, "none")
	// Keep stdin open for the JSON request sent to the fixed adapter.
	args = append([]string{"run", "-i"}, args[1:]...)
	args = append(args, imageName, "-c", policyAdapter)
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = bytes.NewReader(data)
	out, err := cmd.Output()
	if err != nil {
		t.Fatal("browser contract adapter failed")
	}
	lines := bytes.Split(bytes.TrimSpace(out), []byte("\n"))
	if len(lines) != len(requests) {
		t.Fatal("invalid adapter response count")
	}
	results := make([]answer, len(lines))
	for i, line := range lines {
		if json.Unmarshal(line, &results[i]) != nil {
			t.Fatal("invalid adapter response")
		}
	}
	return results
}
func TestPolicyAgainstBuiltRuntime(t *testing.T) {
	controllers := map[string]string{"news_app": "10.55.0.3", "worker": "10.55.0.5", "reader": "10.55.0.7"}
	valid, _ := json.Marshal(controllers)
	invalid := []string{"[]", `{"news_app":"10.55.0.3"}`, `{"news_app":"10.55.0.3","worker":"8.8.8.8"}`, `{"news_app":"10.55.0.3","worker":"10.55.0.3"}`, "invalid"}
	requests := []request{{"controllers", []any{string(valid)}}}
	for _, v := range invalid {
		requests = append(requests, request{"controllers", []any{v}})
	}
	requests = append(requests, request{"render", []any{"10.55.0.2", controllers, "10.55.0.4"}}, request{"render", []any{"10.55.0.2", controllers, "10.55.0.3"}})
	got := invoke(t, requests)
	var parsed map[string]string
	if got[0].Error != "" || json.Unmarshal(got[0].Value, &parsed) != nil || len(parsed) != len(controllers) {
		t.Fatal("valid controllers rejected")
	}
	for i := 1; i <= len(invalid); i++ {
		if got[i].Error != "ValueError" {
			t.Fatal("invalid controllers accepted")
		}
	}
	var rules string
	if json.Unmarshal(got[len(got)-2].Value, &rules) != nil {
		t.Fatal("invalid policy result")
	}
	for _, part := range []string{"hook input priority -150; policy drop", "hook output priority -150; policy drop", "hook forward priority -150; policy drop", "meta nfproto ipv6 counter drop", "ct state invalid counter drop", "ip daddr 10.55.0.4 tcp dport 8081", "ip saddr { 10.55.0.3, 10.55.0.7, 10.55.0.5 } tcp dport 9222", "meta skuid 65534", "dport 5100"} {
		if !strings.Contains(rules, part) {
			t.Fatalf("missing policy behavior %q", part)
		}
	}
	if strings.Contains(rules, "0.0.0.0/0") || got[len(got)-1].Error != "ValueError" {
		t.Fatal("unsafe peer accepted")
	}
}
func TestReadbackAndSupervisorIdentityAgainstBuiltRuntime(t *testing.T) {
	comment := "policy-sha256:" + strings.Repeat("a", 64)
	base := map[string]any{"nftables": []any{map[string]any{"table": map[string]any{"name": "news_native_guard", "comment": comment, "handle": 1}}, map[string]any{"chain": map[string]any{"hook": "input", "policy": "drop"}}, map[string]any{"chain": map[string]any{"hook": "output", "policy": "drop"}}, map[string]any{"chain": map[string]any{"hook": "forward", "policy": "drop"}}, map[string]any{"rule": map[string]any{"counter": map[string]int{"packets": 1, "bytes": 2}}}}}
	altered := map[string]any{}
	raw, _ := json.Marshal(base)
	_ = json.Unmarshal(raw, &altered)
	rows := altered["nftables"].([]any)
	rows[4].(map[string]any)["rule"].(map[string]any)["counter"] = map[string]int{"packets": 50, "bytes": 90}
	badHooks := map[string]any{}
	_ = json.Unmarshal(raw, &badHooks)
	badHooks["nftables"].([]any)[1].(map[string]any)["chain"].(map[string]any)["policy"] = "accept"
	text := `table inet news_native_guard { comment "` + comment + `" }`
	got := invoke(t, []request{{"hooks", []any{base}}, {"comment", []any{base, text, comment}}, {"fingerprint", []any{base}}, {"fingerprint", []any{altered}}, {"identity", []any{map[string]any{"supervisor_pid": 1}}}, {"hooks", []any{badHooks}}, {"comment", []any{base, text, "policy-sha256:" + strings.Repeat("b", 64)}}, {"fingerprint", []any{badHooks}}})
	if string(got[0].Value) != "true" || string(got[1].Value) != "true" || !bytes.Equal(got[2].Value, got[3].Value) || got[4].Error != "StartupError" || string(got[5].Value) != "false" || string(got[6].Value) != "false" || bytes.Equal(got[2].Value, got[7].Value) {
		t.Fatal("readback or supervisor contract failed")
	}
}

func TestCleanupRequiresCreatedIdentityAndOwnership(t *testing.T) {
	f := fixture(t)
	calls := 0
	run := func(args ...string) (string, error) { calls++; return "", nil }
	if err := cleanupOwned(f, run); err != nil || calls != 0 {
		t.Fatal("missing CID attempted deletion")
	}
	id := strings.Repeat("a", 64)
	if err := os.WriteFile(f.cidfile, []byte(id), 0600); err != nil {
		t.Fatal(err)
	}
	run = func(args ...string) (string, error) {
		calls++
		if args[0] == "inspect" {
			return "/foreign owned", nil
		}
		t.Fatal("foreign fixture removal attempted")
		return "", nil
	}
	if err := cleanupOwned(f, run); err == nil {
		t.Fatal("foreign fixture accepted")
	}
	run = func(args ...string) (string, error) {
		if args[0] == "inspect" {
			return "/" + f.name + " foreign", nil
		}
		t.Fatal("unowned fixture removal attempted")
		return "", nil
	}
	if err := cleanupOwned(f, run); err == nil {
		t.Fatal("wrong ownership label accepted")
	}
	calls = 0
	run = func(args ...string) (string, error) {
		calls++
		if args[0] == "inspect" {
			return "/" + f.name + " owned", nil
		}
		if args[0] == "rm" && args[len(args)-1] == id {
			return "", nil
		}
		t.Fatal("unexpected cleanup command")
		return "", nil
	}
	if err := cleanupOwned(f, run); err != nil || calls != 2 {
		t.Fatal("owned fixture not removed")
	}
}
