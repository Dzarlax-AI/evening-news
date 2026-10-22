package main

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/Dzarlax-AI/evening-news/go/transport"
)

func TestNativeBackendRequiresExplicitIsolationAndSelection(t *testing.T) {
	keys := []string{"EXTRACTOR_BROWSER_BACKEND", "EXTRACTOR_NATIVE_PROXY_URL", "EXTRACTOR_NATIVE_PROXY_TOKEN", "EXTRACTOR_NATIVE_NETWORK_ISOLATED", "EXTRACTOR_NATIVE_DIAGNOSTICS"}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	if worker, err := configuredNativeBrowser(true, "", 1); err != nil || worker != nil {
		t.Fatal("HTTP default changed")
	}
	t.Setenv("EXTRACTOR_NATIVE_DIAGNOSTICS", "synthetic-soak")
	if _, err := configuredNativeBrowser(true, "", 1); err == nil {
		t.Fatal("native diagnostics accepted in HTTP-only mode")
	}
	t.Setenv("EXTRACTOR_NATIVE_DIAGNOSTICS", "")
	if _, err := configuredNativeBrowser(false, "http://chrome:9222", 1); err == nil {
		t.Fatal("implicit experimental backend accepted")
	}
	t.Setenv("EXTRACTOR_BROWSER_BACKEND", "native")
	t.Setenv("EXTRACTOR_NATIVE_PROXY_URL", "http://gateway:8081")
	t.Setenv("EXTRACTOR_NATIVE_PROXY_TOKEN", "synthetic-test-token")
	if _, err := configuredNativeBrowser(false, "http://chrome:9222", 1); err == nil {
		t.Fatal("missing isolation attestation accepted")
	}
	t.Setenv("EXTRACTOR_NATIVE_NETWORK_ISOLATED", "true")
	worker, err := configuredNativeBrowser(false, "http://chrome:9222", 1)
	if err != nil || worker == nil || worker.Ready() {
		t.Fatalf("unprobed browser became ready: %v", err)
	}
	if _, err := configuredNativeBrowser(true, "", 1); err == nil {
		t.Fatal("native config allowed in HTTP-only mode")
	}
	if _, err := configuredNativeBrowser(false, "http://chrome:9222", 2); err == nil {
		t.Fatal("unsupported concurrency accepted")
	}
}

func TestNativeDiagnosticLoggerIsOptInAndAllowlisted(t *testing.T) {
	var output bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&output)
	t.Cleanup(func() { log.SetOutput(previous) })
	if callback := nativeDiagnosticLogger(false); callback != nil {
		callback("discovery", "operation_failed")
	}
	if output.Len() != 0 {
		t.Fatal("diagnostics logged while disabled")
	}
	callback := nativeDiagnosticLogger(true)
	callback("events", "protocol_error_-32603")
	for _, reason := range []string{"client_disconnected", "malformed_detach", "owned_detach", "target_crashed"} {
		callback("events", reason)
		if !strings.Contains(output.String(), "stage=events reason="+reason) {
			t.Fatalf("finite diagnostic was discarded: %s", reason)
		}
	}
	callback("private https://secret.example", "secret-token")
	line := output.String()
	if !strings.Contains(line, "native_browser_diagnostic stage=events reason=protocol_error_-32603") ||
		strings.Contains(line, "secret.example") || strings.Contains(line, "secret-token") {
		t.Fatalf("unexpected diagnostic log: %q", line)
	}
}

func TestNativeDiagnosticModesAreNarrowlyScoped(t *testing.T) {
	for _, tc := range []struct {
		mode, selector string
		want           bool
	}{
		{"synthetic-soak", `[data-ready="true"]`, true},
		{"synthetic-soak", "", false},
		{"live-quality", "", true},
		{"live-quality", `[data-ready="true"]`, false},
		{"live-quality-extra", "", false},
		{"", "", false},
	} {
		if got := nativeDiagnosticsEnabled(tc.mode, tc.selector); got != tc.want {
			t.Errorf("nativeDiagnosticsEnabled(%q, %q)=%t, want %t", tc.mode, tc.selector, got, tc.want)
		}
	}
}

func TestNativeReadinessCallbacksDoNotBootstrapOrRepeatDiscovery(t *testing.T) {
	native, err := transport.NewNativeBrowser(transport.NativeBrowserConfig{
		Endpoint: "http://127.0.0.1:1", ProxyURL: "http://gateway:8081",
		ProxyToken: "synthetic-test-token", NetworkIsolationVerified: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	ready, available := nativeBrowserReadiness(native)
	if ready == nil || available == nil {
		t.Fatal("native readiness callbacks were not configured")
	}
	if ready() || available() || native.Ready() {
		t.Fatal("readiness callback bootstrapped an unprobed native browser")
	}
}
