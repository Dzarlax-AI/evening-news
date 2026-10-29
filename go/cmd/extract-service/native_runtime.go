package main

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/transport"
)

func configuredNativeBrowser(httpOnly bool, endpoint string, workers int) (*transport.NativeBrowser, error) {
	return configuredNativeBrowserFrom(os.Getenv, httpOnly, endpoint, workers)
}

func configuredNativeBrowserFrom(get func(string) string, httpOnly bool, endpoint string, workers int) (*transport.NativeBrowser, error) {
	backend := get("EXTRACTOR_BROWSER_BACKEND")
	proxy := get("EXTRACTOR_NATIVE_PROXY_URL")
	token := get("EXTRACTOR_NATIVE_PROXY_TOKEN")
	isolation := get("EXTRACTOR_NATIVE_NETWORK_ISOLATED")
	selector := get("EXTRACTOR_NATIVE_READY_SELECTOR")
	diagnostic := nativeDiagnosticLogger(nativeDiagnosticsEnabled(
		get("EXTRACTOR_NATIVE_DIAGNOSTICS"), selector))
	if httpOnly {
		if backend != "" || proxy != "" || token != "" || isolation != "" || selector != "" ||
			get("EXTRACTOR_NATIVE_DIAGNOSTICS") != "" {
			return nil, errors.New("native configuration forbidden in HTTP-only mode")
		}
		return nil, nil
	}
	if backend == "fulfill-experimental" && proxy == "" && token == "" && isolation == "" {
		return nil, nil
	}
	if backend != "native" {
		return nil, errors.New("experimental browser backend must be selected explicitly")
	}
	if len(selector) > 2048 {
		return nil, errors.New("native readiness selector exceeds limit")
	}
	if workers < 1 || workers > 2 {
		return nil, errors.New("native backend supports browser concurrency one or two")
	}
	return transport.NewNativeBrowser(transport.NativeBrowserConfig{Endpoint: endpoint, ProxyURL: proxy, ProxyToken: token,
		MaxConcurrent: workers, MaxWaiters: 8, NetworkIsolationVerified: isolation == "true", Diagnostic: diagnostic})
}

func nativeDiagnosticsEnabled(mode, selector string) bool {
	return (mode == "synthetic-soak" && selector == `[data-ready="true"]`) ||
		(mode == "live-quality" && selector == "")
}

func nativeBrowserReadiness(native *transport.NativeBrowser) (ready, available func() bool) {
	if native == nil {
		return nil, nil
	}
	ready = func() bool {
		probe, done := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer done()
		return native.CheckReady(probe)
	}
	return ready, native.Ready
}

func nativeDiagnosticLogger(enabled bool) func(stage, reason string) {
	if !enabled {
		return nil
	}
	stages := map[string]bool{"readiness": true, "discovery": true, "websocket": true, "events": true,
		"create_context": true, "create_target": true, "attach_target": true,
		"configure": true, "navigate": true, "dom": true, "cleanup": true}
	reasons := map[string]bool{"context_timeout": true, "context_cancelled": true,
		"event_overflow": true, "ownership_loss": true, "unsupported": true,
		"client_disconnected": true, "malformed_detach": true, "owned_detach": true, "target_crashed": true,
		"protocol_error": true, "transport_error": true, "operation_failed": true}
	return func(stage, reason string) {
		validReason := reasons[reason]
		if !validReason && len(reason) <= 32 && len(reason) > len("protocol_error_") &&
			len(reason) <= len("protocol_error_")+7 && reason[:len("protocol_error_")] == "protocol_error_" {
			code, err := strconv.ParseInt(reason[len("protocol_error_"):], 10, 32)
			validReason = err == nil && code >= -999999 && code <= 999999
		}
		if stages[stage] && validReason {
			log.Printf("native_browser_diagnostic stage=%s reason=%s", stage, reason)
		}
	}
}
