package main

import "testing"

func TestNativeBackendRequiresExplicitIsolationAndSelection(t *testing.T) {
	keys := []string{"EXTRACTOR_BROWSER_BACKEND", "EXTRACTOR_NATIVE_PROXY_URL", "EXTRACTOR_NATIVE_PROXY_TOKEN", "EXTRACTOR_NATIVE_NETWORK_ISOLATED"}
	for _, key := range keys {
		t.Setenv(key, "")
	}
	if worker, err := configuredNativeBrowser(true, "", 1); err != nil || worker != nil {
		t.Fatal("HTTP default changed")
	}
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
