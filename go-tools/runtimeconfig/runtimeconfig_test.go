package runtimeconfig

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDecodeStrict(t *testing.T) {
	valid := `{"version":1,"service":"web","installation_id":"example","values":{"DATABASE_URL":"secret"}}`
	allow := []string{"DATABASE_URL"}
	tests := []struct {
		name, raw, service string
		ok                 bool
	}{
		{"valid", valid, "web", true},
		{"wrong service", valid, "domain", false},
		{"wrong version", strings.Replace(valid, `"version":1`, `"version":2`, 1), "web", false},
		{"duplicate key", strings.Replace(valid, `"DATABASE_URL":"secret"`, `"DATABASE_URL":"secret","DATABASE_URL":"other"`, 1), "web", false},
		{"unknown key", strings.Replace(valid, `"DATABASE_URL"`, `"PASSWORD"`, 1), "web", false},
		{"unknown field", strings.Replace(valid, `"version":1`, `"unknown":1,"version":1`, 1), "web", false},
		{"null values", strings.Replace(valid, `{"DATABASE_URL":"secret"}`, `null`, 1), "web", false},
		{"wrong type", strings.Replace(valid, `"secret"`, `12`, 1), "web", false},
		{"trailing", valid + `{}`, "web", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, err := Decode([]byte(tt.raw), tt.service, allow, allow)
			if (err == nil) != tt.ok {
				t.Fatalf("unexpected error: %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "secret") {
				t.Fatal("secret leaked")
			}
			if tt.ok && m.Values["DATABASE_URL"] != "secret" {
				t.Fatal("value changed")
			}
		})
	}
}
func TestLoadFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.json")
	if _, err := Load(path, "web", nil, nil); err == nil {
		t.Fatal("missing file accepted")
	}
	if err := os.WriteFile(path, []byte(strings.Repeat("x", maxSize+1)), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path, "web", nil, nil); err == nil {
		t.Fatal("oversized file accepted")
	}
}
