package cloakfixture

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const expectedBrowserSHA = "715722e8605ae3ce81523c1218aba1ec89425786ab33ceaf99f8a6cb5e70e6e8"
const fixtureAdmissionIP = "11.203.254.242"

func workerAnchorExtra() []string {
	// The worker's container-network namespace inherits this anchor's hosts
	// entry. Admission accepts the fixture IP as public, while the anchor's nft
	// policy still permits traffic only to the browser and isolated egress.
	return []string{"--add-host", "native-fixture.test:" + fixtureAdmissionIP}
}

type GateConfig struct{ Worker, Egress, Fixture, Relay, X11, Output, StageParent string }
type browserMetadata struct {
	Path string   `json:"path"`
	Args []string `json:"args"`
	SHA  string   `json:"sha256"`
}

func checkedFile(path string, executable bool) (string, error) {
	p, e := filepath.Abs(path)
	if e != nil {
		return "", errors.New("input_invalid")
	}
	i, e := os.Lstat(p)
	if e != nil || !i.Mode().IsRegular() || (executable && i.Mode()&0111 == 0) {
		return "", errors.New("input_invalid")
	}
	return p, nil
}
func hostBudget() error {
	b, e := os.ReadFile("/proc/meminfo")
	if e != nil {
		return errors.New("host_budget_unavailable")
	}
	for _, line := range strings.Split(string(b), "\n") {
		var k string
		var kb int64
		if _, e := fmt.Sscanf(line, "%s %d kB", &k, &kb); e == nil && k == "MemAvailable:" {
			if kb < 1536*1024 {
				return errors.New("host_budget_insufficient")
			}
			return nil
		}
	}
	return errors.New("host_budget_unavailable")
}
func certFiles(dir string) (string, string, string, error) {
	key, e := rsa.GenerateKey(rand.Reader, 2048)
	if e != nil {
		return "", "", "", errors.New("certificate_failed")
	}
	serial, e := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if e != nil {
		return "", "", "", errors.New("certificate_failed")
	}
	now := time.Now()
	tmpl := x509.Certificate{SerialNumber: serial, Subject: pkix.Name{CommonName: "native-fixture.test"}, DNSNames: []string{"native-fixture.test"}, IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(48 * time.Hour), KeyUsage: x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, BasicConstraintsValid: true}
	der, e := x509.CreateCertificate(rand.Reader, &tmpl, &tmpl, &key.PublicKey, key)
	if e != nil {
		return "", "", "", errors.New("certificate_failed")
	}
	cert := filepath.Join(dir, "cert.pem")
	priv := filepath.Join(dir, "key.pem")
	if os.WriteFile(cert, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0644) != nil || os.WriteFile(priv, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}), 0600) != nil {
		return "", "", "", errors.New("certificate_failed")
	}
	spki, e := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if e != nil {
		return "", "", "", errors.New("certificate_failed")
	}
	sum := sha256.Sum256(spki)
	return cert, priv, base64.StdEncoding.EncodeToString(sum[:]), nil
}

const discoverCode = `import runpy,hashlib,json
m=runpy.run_path('/usr/local/bin/cloakserve',run_name='fixture_discovery')
p=str(m['ensure_binary']())
a=list(m['BASE_CHROME_ARGS'])+list(m['build_args'](stealth_args=True,extra_args=['--fingerprint=57292'],timezone=None,locale=None,headless=True))
h=hashlib.sha256()
with open(p,'rb') as f:
 while True:
  b=f.read(1048576)
  if not b: break
  h.update(b)
print(json.dumps({'path':p,'args':a,'sha256':h.hexdigest()},separators=(',',':')))`

func (g *dockerGate) discover(ctx context.Context) (browserMetadata, error) {
	var v browserMetadata
	name := g.prefix + "-discovery"
	g.containers = append(g.containers, name)
	out, e := g.docker.Run(ctx, "run", "--rm", "--name", name, "--label", ownerClass, "--label", g.label(), "--pull=never", "--network", "none", "--platform=linux/amd64", "--memory", "256m", "--memory-swap", "256m", "--cpus", "0.5", "--pids-limit", "64", "--cap-drop=ALL", "--security-opt=no-new-privileges:true", "-e", "CLOAKBROWSER_AUTO_UPDATE=false", "--entrypoint", "python", cloakImage, "-c", discoverCode)
	if e == nil {
		g.containers = g.containers[:len(g.containers)-1]
	}
	if e != nil || len(out) > 16384 || json.Unmarshal([]byte(out), &v) != nil || v.SHA != expectedBrowserSHA || !filepath.IsAbs(v.Path) || len(v.Args) == 0 || len(v.Args) > 256 {
		return v, errors.New("browser_metadata_invalid")
	}
	for _, arg := range v.Args {
		if len(arg) > 4096 || strings.ContainsAny(arg, "\x00\r\n") || strings.HasPrefix(arg, "--remote-debugging-") || strings.HasPrefix(arg, "--user-data-dir") || strings.HasPrefix(arg, "--ignore-certificate-errors") {
			return v, errors.New("browser_metadata_invalid")
		}
	}
	return v, nil
}
func (g *dockerGate) wait(ctx context.Context, fn func() bool, timeout time.Duration) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(250 * time.Millisecond)
	defer tick.Stop()
	for {
		if fn() {
			return nil
		}
		if e := g.alive(ctx); e != nil {
			return e
		}
		select {
		case <-ctx.Done():
			return errors.New("gate_deadline_exceeded")
		case <-deadline.C:
			return errors.New("fixture_readiness_timeout")
		case <-tick.C:
		}
	}
}
func (g *dockerGate) send(ctx context.Context, dir, action string) (json.RawMessage, error) {
	var id [16]byte
	if _, e := rand.Read(id[:]); e != nil {
		return nil, errors.New("gate_entropy_failed")
	}
	name := hex.EncodeToString(id[:]) + ".json"
	if e := atomicReceipt(filepath.Join(dir, "commands"), name, map[string]string{"action": action}); e != nil {
		return nil, e
	}
	var result json.RawMessage
	var response struct {
		ID     string          `json:"command_id"`
		Ok     bool            `json:"ok"`
		Result json.RawMessage `json:"result"`
	}
	path := filepath.Join(dir, "results", name)
	readReceipt := func() bool {
		i, err := os.Lstat(path)
		if err != nil || !i.Mode().IsRegular() || i.Size() > 4096 || i.Mode().Perm()&0077 != 0 {
			return false
		}
		b, err := os.ReadFile(path)
		if err != nil || json.Unmarshal(b, &response) != nil {
			return false
		}
		_ = os.Remove(path)
		result = response.Result
		return true
	}
	e := g.wait(ctx, readReceipt, 45*time.Second)
	if e != nil {
		// The observer can exit immediately after publishing its terminal receipt.
		if !readReceipt() {
			return nil, e
		}
	}
	if response.ID != strings.TrimSuffix(name, ".json") || !response.Ok || len(result) == 0 {
		return nil, errors.New("client_gate_failed")
	}
	return result, nil
}

func (g *dockerGate) stopOwnedContainer(ctx context.Context, name string) error {
	if err := g.verify(ctx, name, false); err != nil {
		return err
	}
	if _, err := g.docker.Run(ctx, "rm", "-f", name); err != nil {
		return err
	}
	for i := range g.containers {
		if g.containers[i] == name {
			g.containers = append(g.containers[:i], g.containers[i+1:]...)
			return nil
		}
	}
	return errors.New("owned_container_tracking_invalid")
}

// RunGate executes the disposable synthetic gate, with private finite evidence.
func RunGate(ctx context.Context, cfg GateConfig) (err error) {
	if err = hostBudget(); err != nil {
		return err
	}
	for _, p := range []*string{&cfg.Worker, &cfg.Egress, &cfg.Fixture} {
		*p, err = checkedFile(*p, true)
		if err != nil {
			return err
		}
	}
	if cfg.Worker == cfg.Egress || cfg.Worker == cfg.Fixture || cfg.Egress == cfg.Fixture {
		return errors.New("candidate_binaries_must_be_distinct")
	}
	for _, p := range []*string{&cfg.Relay, &cfg.X11} {
		*p, err = checkedFile(*p, false)
		if err != nil {
			return err
		}
	}
	parent, e := filepath.Abs(cfg.StageParent)
	if e != nil {
		return errors.New("stage_parent_invalid")
	}
	info, e := os.Lstat(parent)
	if e != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("stage_parent_invalid")
	}
	if _, e = os.Lstat(cfg.Output); e == nil || !os.IsNotExist(e) {
		return errors.New("output_must_be_new")
	}
	stage, e := os.MkdirTemp(parent, "native-cloak-concurrency-")
	if e != nil {
		return errors.New("stage_creation_failed")
	}
	defer os.RemoveAll(stage)
	ctx, cancel := context.WithTimeout(ctx, 240*time.Second)
	defer cancel()
	g, e := newDockerGate(systemDocker{})
	if e != nil {
		return e
	}
	phase := "preflight"
	report := map[string]any{"schema_version": 1, "state": "failed"}
	defer func() {
		cleanCtx, stop := context.WithTimeout(context.Background(), 45*time.Second)
		defer stop()
		if e := g.cleanup(cleanCtx); e != nil {
			if err == nil {
				err = e
			}
			report["cleanup_failed"] = true
		}
		if err != nil {
			report["failure_code"] = "gate_failed"
			report["phase"] = phase
		} else {
			report["state"] = "passed"
		}
		if e := privateJSON(cfg.Output, report); e != nil {
			err = e
		}
	}()
	// Validate immutable source image and launcher metadata without network access.
	meta, e := g.discover(ctx)
	if e != nil {
		return e
	}
	for _, image := range []string{cloakImage, alpineImage} {
		out, e := g.docker.Run(ctx, "image", "inspect", "--format", "{{.Os}}/{{.Architecture}}", image)
		if e != nil || out != "linux/amd64" {
			return errors.New("image_platform_invalid")
		}
	}
	var n [1]byte
	if _, e = rand.Read(n[:]); e != nil {
		return errors.New("gate_entropy_failed")
	}
	third := int(n[0])%200 + 20
	subnet := fmt.Sprintf("172.29.%d.0/24", third)
	ip := func(n int) string { return fmt.Sprintf("172.29.%d.%d", third, n) }
	phase = "network"
	if e = g.network(ctx, "control", subnet); e != nil {
		return e
	}
	if e = g.network(ctx, "fixture", "11.203.254.240/28"); e != nil {
		return e
	}
	phase = "isolation"
	anchors := []struct{ role, network, address string }{
		{"browser", "control", ip(2)}, {"worker", "control", ip(3)},
		{"observer", "control", ip(4)}, {"gateway", "control", ip(5)},
		{"origin", "fixture", "11.203.254.242"},
	}
	for _, anchor := range anchors {
		extra := []string(nil)
		if anchor.role == "worker" {
			extra = workerAnchorExtra()
		}
		if e = g.container(ctx, anchor.role+"-anchor", alpineImage, anchor.network, anchor.address, "32m", "0.125", "32", []string{cfg.Fixture + ":/fixture:ro"}, nil, extra, []string{"sleep", "300"}); e != nil {
			return e
		}
	}
	if _, e = g.docker.Run(ctx, "network", "connect", "--ip", "11.203.254.243", g.prefix+"-fixture", g.prefix+"-gateway-anchor"); e != nil {
		return e
	}
	ips := map[string]string{"browser": ip(2), "worker": ip(3), "observer": ip(4), "gateway": ip(5), "origin": "11.203.254.242"}
	for _, anchor := range anchors {
		policy, err := nftPolicy(anchor.role, ips)
		if err != nil {
			return err
		}
		if _, err = g.namespace(ctx, g.prefix+"-"+anchor.role+"-anchor", policy, "nft", "-f", "-"); err != nil {
			return err
		}
		gateway := ip(1)
		if anchor.role == "origin" {
			gateway = "11.203.254.241"
		}
		targets := []string{gateway + ":80", "10.254.254.254:80"}
		if anchor.role == "browser" {
			targets = append(targets, "127.0.0.1:5100")
		}
		if err = g.probeDrops(ctx, anchor.role+"-anchor", targets); err != nil {
			return err
		}
	}
	for _, dir := range []string{"fixture", "fixture/release", "commands", "results", "runtime"} {
		if e = os.Mkdir(filepath.Join(stage, dir), 0700); e != nil {
			return errors.New("stage_creation_failed")
		}
	}
	cert, key, spki, e := certFiles(filepath.Join(stage, "fixture"))
	if e != nil {
		return e
	}
	var tok [32]byte
	if _, e = rand.Read(tok[:]); e != nil {
		return errors.New("gate_entropy_failed")
	}
	token := hex.EncodeToString(tok[:])
	tokenPath := filepath.Join(stage, "private-token")
	if e = os.WriteFile(tokenPath, []byte(token+"\n"), 0600); e != nil {
		return errors.New("stage_creation_failed")
	}
	managedEgressPath := filepath.Join(stage, "runtime", "egress.json")
	managedExtractorPath := filepath.Join(stage, "runtime", "extractor.json")
	egressValues := map[string]string{
		"EGRESS_LISTEN_ADDR": ip(5) + ":8081", "EGRESS_TOKEN": token, "EGRESS_MAX_CONCURRENT": "4",
		"EGRESS_MAX_LIFETIME_SECONDS": "35", "EGRESS_TEST_MODE": "true",
		"EGRESS_TEST_ALLOWLIST": `{"native-fixture.test":"11.203.254.242"}`,
	}
	extractorValues := map[string]string{
		"LISTEN_ADDR": "0.0.0.0:8080", "EXTRACTOR_HTTP_ONLY": "false", "EXTRACTOR_MAX_CONCURRENT": "5",
		"EXTRACTOR_BROWSER_CONCURRENT": "2", "EXTRACTOR_BROWSER_BACKEND": "native",
		"EXTRACTOR_NATIVE_NETWORK_ISOLATED": "true", "EXTRACTOR_NATIVE_READY_SELECTOR": `[data-ready="true"]`,
		"EXTRACTOR_NATIVE_DIAGNOSTICS": "synthetic-soak", "EXTRACTOR_SERVICE_TOKEN": token,
		"CHROME_ENDPOINT": "http://" + ip(2) + ":9222", "EXTRACTOR_NATIVE_PROXY_URL": "http://" + ip(5) + ":8081",
		"EXTRACTOR_NATIVE_PROXY_TOKEN": token,
	}
	if e = writeRuntimeManifest(managedEgressPath, "egress", egressValues); e != nil {
		return e
	}
	if e = writeRuntimeManifest(managedExtractorPath, "extractor", extractorValues); e != nil {
		return e
	}
	phase = "browser"
	args := append([]string{}, meta.Args...)
	args = append(args, "--ignore-certificate-errors-spki-list="+spki, "--remote-debugging-port=5100", "--remote-debugging-address=127.0.0.1", "--user-data-dir=/tmp/native-shadow-browser")
	cmd := append([]string{meta.Path}, args...)
	mounts := []string{cfg.Relay + ":/fixture-relay.py:ro", cfg.X11 + ":/shadow-bin/xdotool:ro", cfg.X11 + ":/shadow-bin/openbox:ro"}
	extra := []string{"--entrypoint", "/entrypoint.sh", "--shm-size", "1g", "--tmpfs", "/run:rw,noexec,nosuid,size=32m", "--tmpfs", "/root/.config:rw,nosuid,nodev,size=16m", "--tmpfs", "/root/.cache:rw,nosuid,nodev,size=16m"}
	env := []string{"CLOAKBROWSER_AUTO_UPDATE=false", "DISPLAY=unix/:99", "PATH=/shadow-bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"}
	if e = g.container(ctx, "cloak", cloakImage, "@browser-anchor", "", "768m", "0.5", "256", mounts, env, extra, cmd); e != nil {
		return e
	}
	if _, e = g.docker.Run(ctx, "exec", "-d", "--user", "65534:65534", g.prefix+"-cloak", "python", "/fixture-relay.py", "--controller-ip", ip(3), "--controller-ip", ip(4)); e != nil {
		return errors.New("relay_start_failed")
	}
	phase = "egress"
	startEgress := func(managed bool) error {
		env := []string{"EGRESS_LISTEN_ADDR=" + egressValues["EGRESS_LISTEN_ADDR"], "EGRESS_TOKEN=" + token, "EGRESS_MAX_CONCURRENT=4", "EGRESS_MAX_LIFETIME_SECONDS=35", "EGRESS_TEST_MODE=true", "EGRESS_TEST_ALLOWLIST=" + egressValues["EGRESS_TEST_ALLOWLIST"]}
		mounts := []string{cfg.Egress + ":/extract-egress:ro"}
		if managed {
			mounts = append(mounts, managedEgressPath+":/run/news/runtime.json:ro")
			env = []string{"NEWS_RUNTIME_FILE=/run/news/runtime.json", "EGRESS_LISTEN_ADDR=127.0.0.1:1", "EGRESS_TOKEN=ambient-conflict-token", "EGRESS_MAX_CONCURRENT=1", "EGRESS_MAX_LIFETIME_SECONDS=1", "EGRESS_TEST_MODE=false", `EGRESS_TEST_ALLOWLIST={}`}
		}
		return g.container(ctx, "egress", alpineImage, "@gateway-anchor", "", "128m", "0.25", "64", mounts, env, nil, []string{"/extract-egress"})
	}
	if e = startEgress(false); e != nil {
		return e
	}
	phase = "origin"
	mounts = []string{cfg.Fixture + ":/fixture:ro", cert + ":/cert.pem:ro", key + ":/key.pem:ro", filepath.Join(stage, "fixture") + ":/state:rw"}
	if e = g.container(ctx, "origin", alpineImage, "@origin-anchor", "", "64m", "0.25", "64", mounts, nil, nil, []string{"/fixture", "origin", "--cert", "/cert.pem", "--key", "/key.pem", "--state-dir", "/state"}); e != nil {
		return e
	}
	if e = g.wait(ctx, func() bool {
		_, x := g.docker.Run(ctx, "exec", g.prefix+"-origin", "/fixture", "probe", "--cert", "/cert.pem")
		return x == nil
	}, 25*time.Second); e != nil {
		return e
	}
	if e = g.wait(ctx, func() bool {
		_, x := g.docker.Run(ctx, "exec", g.prefix+"-egress", "/extract-egress", "--healthcheck")
		return x == nil
	}, 25*time.Second); e != nil {
		return e
	}
	phase = "observer"
	mounts = []string{cfg.Fixture + ":/fixture:ro", filepath.Join(stage, "fixture") + ":/state:rw", filepath.Join(stage, "commands") + ":/commands:rw", filepath.Join(stage, "results") + ":/results:rw", tokenPath + ":/token:ro"}
	cmd = []string{"/fixture", "client", "--state-dir", "/state", "--worker", "http://" + ip(3) + ":8080", "--browser", ip(2) + ":9222", "--proxy", "http://" + ip(5) + ":8081", "--token-file", "/token", "--command-dir", "/commands", "--result-dir", "/results"}
	if e = g.container(ctx, "observer", alpineImage, "@observer-anchor", "", "128m", "0.5", "64", mounts, nil, nil, cmd); e != nil {
		return e
	}
	if e = g.wait(ctx, func() bool {
		i, x := os.Lstat(filepath.Join(stage, "results", "peer-open.json"))
		return x == nil && i.Mode().IsRegular()
	}, 30*time.Second); e != nil {
		return e
	}
	startWorker := func(limit int, managed bool) error {
		values := extractorValues
		if limit == 1 {
			values = make(map[string]string, len(extractorValues))
			for k, v := range extractorValues {
				values[k] = v
			}
			values["EXTRACTOR_BROWSER_CONCURRENT"] = "1"
		}
		env := []string{"LISTEN_ADDR=" + values["LISTEN_ADDR"], "EXTRACTOR_HTTP_ONLY=" + values["EXTRACTOR_HTTP_ONLY"], "EXTRACTOR_MAX_CONCURRENT=" + values["EXTRACTOR_MAX_CONCURRENT"], "EXTRACTOR_BROWSER_CONCURRENT=" + values["EXTRACTOR_BROWSER_CONCURRENT"], "EXTRACTOR_BROWSER_BACKEND=" + values["EXTRACTOR_BROWSER_BACKEND"], "EXTRACTOR_NATIVE_NETWORK_ISOLATED=" + values["EXTRACTOR_NATIVE_NETWORK_ISOLATED"], "EXTRACTOR_NATIVE_READY_SELECTOR=" + values["EXTRACTOR_NATIVE_READY_SELECTOR"], "EXTRACTOR_NATIVE_DIAGNOSTICS=" + values["EXTRACTOR_NATIVE_DIAGNOSTICS"], "EXTRACTOR_SERVICE_TOKEN=" + token, "CHROME_ENDPOINT=" + values["CHROME_ENDPOINT"], "EXTRACTOR_NATIVE_PROXY_URL=" + values["EXTRACTOR_NATIVE_PROXY_URL"], "EXTRACTOR_NATIVE_PROXY_TOKEN=" + token}
		mounts := []string{cfg.Worker + ":/extract-service:ro"}
		if managed {
			mounts = append(mounts, managedExtractorPath+":/run/news/runtime.json:ro")
			env = []string{"NEWS_RUNTIME_FILE=/run/news/runtime.json", "LISTEN_ADDR=127.0.0.1:1", "EXTRACTOR_HTTP_ONLY=true", "EXTRACTOR_MAX_CONCURRENT=1", "EXTRACTOR_BROWSER_CONCURRENT=1", "EXTRACTOR_BROWSER_BACKEND=invalid", "EXTRACTOR_NATIVE_NETWORK_ISOLATED=false", "EXTRACTOR_NATIVE_READY_SELECTOR=invalid", "EXTRACTOR_NATIVE_DIAGNOSTICS=ambient-conflict", "EXTRACTOR_SERVICE_TOKEN=ambient-conflict-token", "CHROME_ENDPOINT=http://127.0.0.1:1", "EXTRACTOR_NATIVE_PROXY_URL=http://127.0.0.1:1", "EXTRACTOR_NATIVE_PROXY_TOKEN=ambient-conflict-token"}
		}
		if e := g.container(ctx, "worker", alpineImage, "@worker-anchor", "", "256m", "1", "128", mounts, env, nil, []string{"/extract-service"}); e != nil {
			return e
		}
		return g.wait(ctx, func() bool {
			_, x := g.docker.Run(ctx, "exec", g.prefix+"-worker", "/extract-service", "--healthcheck")
			return x == nil
		}, 30*time.Second)
	}
	phase = "profile_one"
	if e = startWorker(1, false); e != nil {
		return e
	}
	one, e := g.send(ctx, stage, "profile_one")
	if e != nil {
		return e
	}
	if e = g.alive(ctx); e != nil {
		return e
	}
	if e = g.verify(ctx, g.prefix+"-worker", false); e != nil {
		return e
	}
	if _, e = g.docker.Run(ctx, "rm", "-f", g.prefix+"-worker"); e != nil {
		return e
	}
	g.containers = g.containers[:len(g.containers)-1]
	if e = g.stopOwnedContainer(ctx, g.prefix+"-egress"); e != nil {
		return e
	}
	if e = startEgress(true); e != nil {
		return e
	}
	if e = g.wait(ctx, func() bool {
		_, x := g.docker.Run(ctx, "exec", g.prefix+"-egress", "/extract-egress", "--healthcheck")
		return x == nil
	}, 25*time.Second); e != nil {
		return e
	}
	phase = "profile_two"
	if e = startWorker(2, true); e != nil {
		return e
	}
	two, e := g.send(ctx, stage, "profile_two")
	if e != nil {
		return e
	}
	phase = "managed_supplied_content"
	suppliedContent, e := g.send(ctx, stage, "supplied_content")
	if e != nil {
		return e
	}
	phase = "managed_page_monitor"
	pageMonitor, e := g.send(ctx, stage, "page_monitor")
	if e != nil {
		return e
	}
	phase = "cancellation"
	cancellation, e := g.send(ctx, stage, "cancellation")
	if e != nil {
		return e
	}
	phase = "upstream"
	upstream, e := g.send(ctx, stage, "upstream_error")
	if e != nil {
		return e
	}
	if _, e = g.send(ctx, stage, "probe"); e != nil {
		return e
	}
	if e = g.alive(ctx); e != nil {
		return e
	}
	if _, e = g.send(ctx, stage, "close"); e != nil {
		return e
	}
	report["profiles"] = map[string]json.RawMessage{"one": one, "two": two}
	report["managed_runtime_file"] = map[string]any{"extractor": "passed", "egress": "passed", "conflicting_environment_values": "ignored"}
	report["managed_probes"] = map[string]json.RawMessage{"supplied_content": suppliedContent, "page_monitor": pageMonitor}
	report["cancellation"] = cancellation
	report["upstream_error"] = upstream
	report["browser_generation_reused_for_both_profiles"] = true
	report["network_scope"] = "isolated_internal_docker_networks_with_no_published_ports"
	report["acceptance_limit"] = "synthetic CDP compatibility; not production isolation proof"
	return nil
}
