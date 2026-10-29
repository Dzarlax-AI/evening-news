package managedsetup

import (
	"encoding/json"
	"os"
	"path/filepath"
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
