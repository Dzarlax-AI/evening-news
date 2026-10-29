package managedsetup

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func testLayout(t *testing.T) Layout {
	t.Helper()
	root := t.TempDir()
	pg := filepath.Join(root, "pg")
	if err := os.Mkdir(pg, 0700); err != nil {
		t.Fatal(err)
	}
	return Layout{Setup: filepath.Join(root, "setup"), Web: filepath.Join(root, "web"), Domain: filepath.Join(root, "domain"), Background: filepath.Join(root, "background"), Extractor: filepath.Join(root, "extractor"), Postgres: filepath.Join(root, "pg"), PostgresSecrets: filepath.Join(root, "postgres-secret"), WebUID: -1, DomainUID: -1, BackgroundUID: -1, ExtractorUID: -1}
}
func TestPrepareIdempotentAndStateLossFailsClosed(t *testing.T) {
	l := testLayout(t)
	created, err := Prepare(l, "http://localhost:8000")
	if err != nil || !created {
		t.Fatalf("prepare %v", err)
	}
	before, err := os.ReadFile(filepath.Join(l.Domain, "runtime.json"))
	if err != nil {
		t.Fatal(err)
	}
	created, err = Prepare(l, "http://localhost:8000")
	if err != nil || created {
		t.Fatalf("repeat %v", err)
	}
	after, _ := os.ReadFile(filepath.Join(l.Domain, "runtime.json"))
	if string(before) != string(after) {
		t.Fatal("rotated state")
	}
	if err = os.Remove(filepath.Join(l.Domain, "runtime.json")); err != nil {
		t.Fatal(err)
	}
	if _, err = Prepare(l, "http://localhost:8000"); err == nil {
		t.Fatal("missing file silently replaced")
	}
}
func TestPrepareRejectsExistingDatabaseAndPartialState(t *testing.T) {
	l := testLayout(t)
	if err := os.MkdirAll(l.Postgres, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(l.Postgres, "PG_VERSION"), []byte("17"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(l, "http://localhost:8000"); err == nil {
		t.Fatal("existing database accepted")
	}
	l = testLayout(t)
	if err := os.MkdirAll(l.Web, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(l.Web, "runtime.json"), []byte("partial"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(l, "http://localhost:8000"); err == nil {
		t.Fatal("partial state accepted")
	}
}

func TestPublicProxyOptionsAreExplicitAndPersistent(t *testing.T) {
	l := testLayout(t)
	opts := Options{TrustedProxyCIDRs: []string{"172.20.0.2/32"}}
	if _, err := PrepareWithOptions(l, "https://fixture.invalid", opts); err != nil {
		t.Fatal(err)
	}
	state, err := ReadState(l.Setup)
	if err != nil {
		t.Fatal(err)
	}
	if !sameOptions(state.Options, opts) {
		t.Fatal("proxy options lost")
	}
	raw, err := os.ReadFile(filepath.Join(l.Web, "runtime.json"))
	if err != nil {
		t.Fatal(err)
	}
	var m struct {
		Values map[string]string `json:"values"`
	}
	if json.Unmarshal(raw, &m) != nil || m.Values["TRUSTED_PROXY_CIDRS"] != "172.20.0.2/32" || m.Values["TRUST_FORWARD_AUTH"] != "" {
		t.Fatal("proxy trust was not scoped independently of forwarded auth")
	}
	if _, err := Prepare(l, "https://fixture.invalid"); err != nil {
		t.Fatal("repeat changed state", err)
	}
	if _, err := PrepareWithOptions(l, "https://fixture.invalid", Options{TrustedProxyCIDRs: []string{"172.20.0.3/32"}}); err == nil {
		t.Fatal("existing proxy trust silently changed")
	}
	for _, v := range []string{"0.0.0.0/0", "::/0", "bad"} {
		if (Options{TrustedProxyCIDRs: []string{v}}).Validate() == nil {
			t.Fatal("invalid proxy accepted")
		}
	}
}

func TestPrepareNativeBrowserPersistsEgressAndRefusesTestOptionsBeforeWrites(t *testing.T) {
	options := NativeBrowserOptions{
		ExtractorValues: map[string]string{
			"LISTEN_ADDR": ":8080", "EXTRACTOR_HTTP_ONLY": "false", "EXTRACTOR_BROWSER_BACKEND": "native",
			"EXTRACTOR_NATIVE_NETWORK_ISOLATED": "true", "CHROME_ENDPOINT": "http://10.77.0.2:9222",
			"EXTRACTOR_NATIVE_PROXY_URL": "http://10.77.0.5:8081", "EXTRACTOR_NATIVE_PROXY_TOKEN": "fixture-proxy-token-01",
			"EXTRACTOR_MAX_CONCURRENT": "2", "EXTRACTOR_BROWSER_CONCURRENT": "2",
		},
		EgressValues:    map[string]string{"EGRESS_LISTEN_ADDR": "10.77.0.5:8081", "EGRESS_TOKEN": "fixture-proxy-token-01", "EGRESS_MAX_CONCURRENT": "4"},
		ExtractorLimits: ContainerLimits{Init: true, ReadOnly: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges:true"}, PidsLimit: 128, MemLimit: "256m", CPUs: 1, StopGracePeriod: "55s"},
		EgressLimits:    ContainerLimits{Init: true, ReadOnly: true, CapDrop: []string{"ALL"}, SecurityOpt: []string{"no-new-privileges:true"}, PidsLimit: 64, MemLimit: "128m", CPUs: 0.25, StopGracePeriod: "10s"},
		ControlNetwork:  "control", ControlName: "fixture-control", OutboundNetwork: "outbound", OutboundName: "fixture-outbound",
		ExtractorIP: "10.77.0.4", EgressIP: "10.77.0.5",
	}
	l := testLayout(t)
	l.Egress = filepath.Join(filepath.Dir(l.Setup), "egress")
	l.EgressUID = -1
	if _, err := PrepareWithOptions(l, "http://localhost:8000", Options{NativeBrowser: &options}); err != nil {
		t.Fatal(err)
	}
	state, err := ReadState(l.Setup)
	if err != nil || !sameOptions(state.Options, Options{NativeBrowser: &options}) {
		t.Fatal("native browser state not persisted", err)
	}
	var background struct {
		Values map[string]string `json:"values"`
	}
	for role, target := range map[string]string{"background": filepath.Join(l.Background, "runtime.json"), "egress": filepath.Join(l.Egress, "runtime.json")} {
		raw, err := os.ReadFile(target)
		if err != nil || json.Unmarshal(raw, &background) != nil {
			t.Fatalf("%s runtime state unavailable: %v", role, err)
		}
		if role == "background" && background.Values["EXTRACTION_SERVICE_NATIVE_BROWSER"] != "true" {
			t.Fatal("native browser flag missing from background")
		}
		if role == "egress" && background.Values["EGRESS_TOKEN"] != "fixture-proxy-token-01" {
			t.Fatal("egress runtime manifest missing")
		}
	}
	bad := options
	bad.EgressValues = map[string]string{}
	for key, value := range options.EgressValues {
		bad.EgressValues[key] = value
	}
	bad.EgressValues["EGRESS_TEST_MODE"] = "true"
	reject := testLayout(t)
	if _, err = PrepareWithOptions(reject, "http://localhost:8000", Options{NativeBrowser: &bad}); err == nil {
		t.Fatal("test-only egress settings accepted")
	}
	if _, err = os.Stat(filepath.Join(reject.Setup, "installation.json")); !os.IsNotExist(err) {
		t.Fatal("invalid native options wrote installer state")
	}
	copyValues := func(values map[string]string) map[string]string {
		copied := make(map[string]string, len(values))
		for key, value := range values {
			copied[key] = value
		}
		return copied
	}
	for name, mutate := range map[string]func(*NativeBrowserOptions){
		"short-token": func(o *NativeBrowserOptions) {
			o.EgressValues["EGRESS_TOKEN"] = "short-token"
			o.ExtractorValues["EXTRACTOR_NATIVE_PROXY_TOKEN"] = "short-token"
		},
		"long-token": func(o *NativeBrowserOptions) {
			token := strings.Repeat("x", 257)
			o.EgressValues["EGRESS_TOKEN"] = token
			o.ExtractorValues["EXTRACTOR_NATIVE_PROXY_TOKEN"] = token
		},
		"proxy-path":      func(o *NativeBrowserOptions) { o.ExtractorValues["EXTRACTOR_NATIVE_PROXY_URL"] += "path" },
		"proxy-query":     func(o *NativeBrowserOptions) { o.ExtractorValues["EXTRACTOR_NATIVE_PROXY_URL"] += "?mode=bad" },
		"proxy-fragment":  func(o *NativeBrowserOptions) { o.ExtractorValues["EXTRACTOR_NATIVE_PROXY_URL"] += "#bad" },
		"proxy-opaque":    func(o *NativeBrowserOptions) { o.ExtractorValues["EXTRACTOR_NATIVE_PROXY_URL"] = "http:10.77.0.5:8081" },
		"chrome-query":    func(o *NativeBrowserOptions) { o.ExtractorValues["CHROME_ENDPOINT"] += "?mode=bad" },
		"chrome-fragment": func(o *NativeBrowserOptions) { o.ExtractorValues["CHROME_ENDPOINT"] += "#bad" },
		"chrome-opaque":   func(o *NativeBrowserOptions) { o.ExtractorValues["CHROME_ENDPOINT"] = "http:10.77.0.2:9222" },
	} {
		t.Run(name, func(t *testing.T) {
			invalid := options
			invalid.ExtractorValues = copyValues(options.ExtractorValues)
			invalid.EgressValues = copyValues(options.EgressValues)
			mutate(&invalid)
			layout := testLayout(t)
			if _, err := PrepareWithOptions(layout, "http://localhost:8000", Options{NativeBrowser: &invalid}); err == nil {
				t.Fatal("invalid native configuration accepted")
			}
			if _, err := os.Stat(filepath.Join(layout.Setup, "installation.json")); !os.IsNotExist(err) {
				t.Fatal("rejected native configuration wrote installer state")
			}
		})
	}
}
