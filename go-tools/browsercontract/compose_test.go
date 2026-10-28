package browsercontract

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func object(v any) map[string]any { x, _ := v.(map[string]any); return x }
func list(v any) []any            { x, _ := v.([]any); return x }
func render(t *testing.T, base, overlay string) map[string]any {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		if os.Getenv("NEWS_BROWSER_CONTRACT_REQUIRED") == "1" {
			t.Fatal("Docker Compose required")
		}
		t.Skip("Docker Compose unavailable")
	}
	dir := t.TempDir()
	basePath := filepath.Join(dir, "base.yml")
	empty := filepath.Join(dir, "empty.env")
	if os.WriteFile(basePath, []byte(base), 0600) != nil || os.WriteFile(empty, nil, 0600) != nil {
		t.Fatal("fixture creation failed")
	}
	root, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("docker", "compose", "--profile", "legacy-browser-rollback", "--env-file", empty, "-f", basePath, "-f", filepath.Join(root, overlay), "config", "--format", "json")
	cmd.Dir = root
	cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "HOME=" + os.Getenv("HOME"), "NEWS_BROWSER_IMAGE=example.invalid/browser@sha256:" + strings.Repeat("a", 64), "NEWS_APP_IMAGE=example.invalid/app@sha256:" + strings.Repeat("b", 64), "NEWS_EXTRACTOR_IMAGE=example.invalid/extractor@sha256:" + strings.Repeat("c", 64), "NEWS_BROWSER_IP=10.77.0.2", "NEWS_APP_IP=10.77.0.3", "NEWS_WORKER_IP=10.77.0.4", "NEWS_PROXY_IP=10.77.0.5", `NEWS_BROWSER_CONTROLLER_IPS={"news_app":"10.77.0.3","worker":"10.77.0.4"}`, "NEWS_CONTROL_NETWORK=news-browser-control", "NEWS_CONTROL_SUBNET=10.77.0.0/24", "NEWS_OUTBOUND_NETWORK=news-extractor-outbound", "NEWS_OUTBOUND_SUBNET=10.78.0.0/24", "NEWS_BROWSER_CONTROL_VOLUME=infra_browser-control", "NEWS_PROXY_TOKEN=fixture-proxy-token-123", "NEWS_EXTRACTOR_TOKEN=fixture-worker-token-456"}
	raw, err := cmd.Output()
	if err != nil {
		t.Fatal("synthetic Compose render failed")
	}
	var config map[string]any
	if json.Unmarshal(raw, &config) != nil {
		t.Fatal("invalid Compose output")
	}
	return config
}
func TestSharedBrowserComposeContract(t *testing.T) {
	config := render(t, `services:
  chrome:
    image: old/chrome:latest
    command: [--remote-debugging-port=9222]
    ports: ["127.0.0.1:9222:9222"]
    networks: [infra]
    profiles: [legacy-browser-rollback]
    restart: 'no'
    volumes: [browser-control:/browser-control]
    healthcheck: {test: [CMD, old-healthcheck]}
volumes: {browser-control: {external: true}}
networks: {infra: {external: true}}
`, "deploy/news-extractor/compose.shared-browser.yml")
	browser := object(object(config["services"])["chrome"])
	networks := object(browser["networks"])
	if browser["image"] != "example.invalid/browser@sha256:"+strings.Repeat("a", 64) || len(networks) != 1 || object(networks["private-control"])["ipv4_address"] != "10.77.0.2" || len(list(browser["ports"])) != 0 || len(list(browser["profiles"])) != 0 || browser["read_only"] != true || browser["restart"] != "unless-stopped" || browser["stop_grace_period"] != "40s" {
		t.Fatal("shared browser isolation changed")
	}
	if !reflect.DeepEqual(list(browser["entrypoint"]), []any{"python", "/opt/news-browser/entrypoint.py"}) || len(list(browser["command"])) != 0 || !reflect.DeepEqual(list(object(browser["healthcheck"])["test"]), []any{"CMD", "python", "/opt/news-browser/entrypoint.py", "--healthcheck"}) {
		t.Fatal("browser entrypoint or health contract changed")
	}
	caps := map[string]bool{}
	for _, v := range list(browser["cap_add"]) {
		s, _ := v.(string)
		caps[s] = true
	}
	if !reflect.DeepEqual(caps, map[string]bool{"NET_ADMIN": true, "SETPCAP": true, "SETUID": true, "SETGID": true, "KILL": true}) {
		t.Fatal("browser capabilities changed")
	}
	volume := object(object(config["volumes"])["browser-control"])
	if volume["name"] != "infra_browser-control" || volume["external"] != true {
		t.Fatal("shared volume changed")
	}
}
func TestNativeComposeContract(t *testing.T) {
	config := render(t, `services:
  app:
    image: old/app:latest
    environment: {KEEP_EXISTING_SETTING: yes}
    networks: [infra, traefik]
networks: {infra: {external: true}, traefik: {external: true}}
`, "deploy/news-extractor/compose.native.yml")
	services := object(config["services"])
	app := object(services["app"])
	worker := object(services["extractor"])
	egress := object(services["egress"])
	env := object(app["environment"])
	if env["KEEP_EXISTING_SETTING"] != "yes" || env["EXTRACTION_SERVICE_MODE"] != "go" || env["EXTRACTION_SERVICE_NATIVE_BROWSER"] != "true" || env["EXTRACTION_SERVICE_PYTHON_FALLBACK"] != "false" {
		t.Fatal("native routing changed")
	}
	for _, s := range []map[string]any{worker, egress} {
		if s["read_only"] != true || len(list(s["ports"])) != 0 || len(object(s["networks"])) != 2 {
			t.Fatal("native service isolation changed")
		}
	}
	if object(object(worker["networks"])["private-control"])["ipv4_address"] != "10.77.0.4" || object(object(egress["networks"])["private-control"])["ipv4_address"] != "10.77.0.5" || object(object(app["networks"])["private-control"])["ipv4_address"] != "10.77.0.3" || !reflect.DeepEqual(list(egress["entrypoint"]), []any{"/extract-egress"}) || object(egress["environment"])["EGRESS_MAX_CONCURRENT"] != "4" || worker["stop_grace_period"] != "55s" {
		t.Fatal("native topology changed")
	}
}
