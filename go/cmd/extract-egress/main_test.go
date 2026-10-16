package main

import (
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go/egress"
)

func TestConfigurationDefaultsArePrivateAndAuthenticated(t *testing.T) {
	addr, cfg, err := loadConfig(func(key string) string {
		if key == "EGRESS_TOKEN" {
			return "isolated-test-token"
		}
		return ""
	})
	if err != nil || addr != "127.0.0.1:8081" || cfg.TestMode || len(cfg.TestAllowlist) != 0 {
		t.Fatal(addr, cfg.TestMode, err)
	}
	if _, err := egress.New(cfg); err != nil {
		t.Fatal(err)
	}
	cfg.Token = ""
	if _, err := egress.New(cfg); err == nil {
		t.Fatal("missing token accepted")
	}
}

func TestConfigurationRejectsPublicBindsAndUngatedFixtures(t *testing.T) {
	for _, values := range []map[string]string{
		{"EGRESS_LISTEN_ADDR": "0.0.0.0:8081"},
		{"EGRESS_LISTEN_ADDR": "8.8.8.8:8081"},
		{"EGRESS_LISTEN_ADDR": "localhost:8081"},
		{"EGRESS_LISTEN_ADDR": "[fe80::1%lo]:8081"},
		{"EGRESS_ALLOW_WILDCARD_BIND": "yes"},
		{"EGRESS_TEST_ALLOWLIST": `{"fixture.example":"127.0.0.1"}`},
		{"EGRESS_TEST_MODE": "true", "EGRESS_TEST_ALLOWLIST": `{"fixture.example":"invalid-secret-value"}`},
		{"EGRESS_MAX_LIFETIME_SECONDS": "120"},
		{"EGRESS_MAX_TUNNEL_BYTES": "0"},
	} {
		_, _, err := loadConfig(func(key string) string { return values[key] })
		if err == nil {
			t.Fatal("invalid config accepted", values)
		}
		if strings.Contains(err.Error(), "invalid-secret-value") {
			t.Fatal("configuration value leaked")
		}
	}
}

func TestExplicitIsolatedStandConfiguration(t *testing.T) {
	values := map[string]string{"EGRESS_LISTEN_ADDR": "0.0.0.0:8081", "EGRESS_ALLOW_WILDCARD_BIND": "true", "EGRESS_TOKEN": "isolated-test-token", "EGRESS_TEST_MODE": "true", "EGRESS_TEST_ALLOWLIST": `{"fixture.example":"172.30.0.2"}`}
	addr, cfg, err := loadConfig(func(key string) string { return values[key] })
	if err != nil || addr != "0.0.0.0:8081" || !cfg.TestMode {
		t.Fatal(addr, err)
	}
	if _, err := egress.New(cfg); err != nil {
		t.Fatal(err)
	}
}
