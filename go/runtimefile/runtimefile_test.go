package runtimefile

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestManagedSelectionNeverFallsBack(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "extractor.json")
	manifest := Manifest{Version: 1, Service: "extractor", InstallationID: "fixture-install", Values: map[string]string{"EXTRACTOR_SERVICE_TOKEN": "file-token", "EXTRACTOR_HTTP_ONLY": "true"}}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{"NEWS_RUNTIME_FILE": path, "EXTRACTOR_SERVICE_TOKEN": "ambient-token", "CHROME_ENDPOINT": "http://ambient-browser:9222"}
	lookup := func(k string) string { return env[k] }
	get, err := Lookup(lookup, "extractor")
	if err != nil {
		t.Fatal(err)
	}
	if get("EXTRACTOR_SERVICE_TOKEN") != "file-token" || get("CHROME_ENDPOINT") != "" {
		t.Fatal("managed values mixed with environment")
	}
	if _, err := Lookup(lookup, "egress"); err == nil {
		t.Fatal("wrong role accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if get, err := Lookup(lookup, "extractor"); err == nil || get != nil {
		t.Fatal("missing file fell back to environment")
	}
	delete(env, "NEWS_RUNTIME_FILE")
	get, err = Lookup(lookup, "extractor")
	if err != nil || get("EXTRACTOR_SERVICE_TOKEN") != "ambient-token" {
		t.Fatal("legacy configuration changed")
	}
}

func TestRejectAmbiguousOrCrossServiceManifests(t *testing.T) {
	allowed, required := Keys("egress")
	for _, raw := range []string{
		`{"version":1,"service":"egress","installation_id":"fixture","values":{"EGRESS_TOKEN":"secret-fixture","EGRESS_TOKEN":"second"}}`,
		`{"version":1,"service":"egress","installation_id":"fixture","values":{"EGRESS_TOKEN":"secret-fixture","DATABASE_URL":"not-allowed"}}`,
		`{"version":1,"service":"egress","installation_id":"fixture","values":{"EGRESS_TOKEN":"secret-fixture"},"unknown":true}`,
		`{"version":2,"service":"egress","installation_id":"fixture","values":{"EGRESS_TOKEN":"secret-fixture"}}`,
		`{"version":1,"service":"egress","installation_id":"fixture","values":{}}`,
		`{"version":1,"service":"egress","installation_id":"fixture","values":{"EGRESS_TOKEN":"secret-fixture"}} {}`,
	} {
		if _, err := Decode([]byte(raw), "egress", allowed, required); err == nil || strings.Contains(err.Error(), "secret-fixture") {
			t.Fatal("ambiguous input accepted or secret leaked")
		}
	}
}
