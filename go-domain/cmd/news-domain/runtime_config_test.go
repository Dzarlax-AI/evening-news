package main

import (
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
)

func TestManagedSourceFailsClosedAndDoesNotInheritEnv(t *testing.T) {
	t.Setenv("DATABASE_URL", "legacy-secret")
	path := filepath.Join(t.TempDir(), "state.json")
	t.Setenv("NEWS_RUNTIME_FILE", path)
	if _, err := loadSource(); err == nil {
		t.Fatal("missing file accepted")
	}
	values := map[string]string{"DATABASE_URL": "managed-db", "JOB_EXECUTOR_TOKEN": "1234567890123456", "ARTICLE_AI_TOKEN": "1234567890123456", "EXTRACTION_SERVICE_URL": "http://extractor", "EXTRACTION_SERVICE_TOKEN": "1234567890123456", "WEB_BACKEND_TOKEN": "1234567890123456", "NEWS_CREDENTIAL_KEY": base64.RawStdEncoding.EncodeToString(make([]byte, 32)), "NEWS_CREDENTIAL_KEY_ID": "fixture"}
	data, err := json.Marshal(runtimeconfig.Manifest{Version: 1, Service: "domain", InstallationID: "fixture", Values: values})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, data, 0600); err != nil {
		t.Fatal(err)
	}
	source, err := loadSource()
	if err != nil {
		t.Fatal(err)
	}
	if !source.managed || source.get("DATABASE_URL") != "managed-db" || source.get("GEMINI_API_KEY") != "" {
		t.Fatal("unexpected managed lookup")
	}
	values["GEMINI_API_KEY"] = "disallowed"
	data, _ = json.Marshal(runtimeconfig.Manifest{Version: 1, Service: "domain", InstallationID: "fixture", Values: values})
	os.WriteFile(path, data, 0600)
	if _, err = loadSource(); err == nil {
		t.Fatal("provider key accepted in role file")
	}
}
