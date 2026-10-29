package cloakfixture

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteRuntimeManifestPrivateAndRoleScoped(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.json")
	values := map[string]string{"EXTRACTOR_SERVICE_TOKEN": "fixture-token", "EXTRACTOR_BROWSER_CONCURRENT": "2"}
	if err := writeRuntimeManifest(path, "extractor", values); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatalf("manifest mode=%v err=%v", info.Mode().Perm(), err)
	}
	var got runtimeManifest
	raw, err := os.ReadFile(path)
	if err != nil || json.Unmarshal(raw, &got) != nil {
		t.Fatal("manifest JSON unavailable")
	}
	if got.Version != 1 || got.Service != "extractor" || got.InstallationID == "" || got.Values["EXTRACTOR_SERVICE_TOKEN"] != "fixture-token" || got.Values["EXTRACTOR_BROWSER_CONCURRENT"] != "2" {
		t.Fatal("manifest values did not round trip")
	}
	if err := writeRuntimeManifest(path, "egress", map[string]string{"EGRESS_TOKEN": "other"}); err == nil {
		t.Fatal("manifest writer overwrote an existing file")
	}
}
