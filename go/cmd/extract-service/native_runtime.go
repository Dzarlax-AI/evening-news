package main

import (
	"errors"
	"os"

	"github.com/Dzarlax-AI/evening-news/go/transport"
)

func configuredNativeBrowser(httpOnly bool, endpoint string, workers int) (*transport.NativeBrowser, error) {
	backend := os.Getenv("EXTRACTOR_BROWSER_BACKEND")
	proxy := os.Getenv("EXTRACTOR_NATIVE_PROXY_URL")
	token := os.Getenv("EXTRACTOR_NATIVE_PROXY_TOKEN")
	isolation := os.Getenv("EXTRACTOR_NATIVE_NETWORK_ISOLATED")
	selector := os.Getenv("EXTRACTOR_NATIVE_READY_SELECTOR")
	if httpOnly {
		if backend != "" || proxy != "" || token != "" || isolation != "" || selector != "" {
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
	if workers != 1 {
		return nil, errors.New("native backend currently requires browser concurrency one")
	}
	return transport.NewNativeBrowser(transport.NativeBrowserConfig{Endpoint: endpoint, ProxyURL: proxy, ProxyToken: token, NetworkIsolationVerified: isolation == "true"})
}
