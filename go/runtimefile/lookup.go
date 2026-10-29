package runtimefile

import (
	"errors"
	"os"
)

// FromEnvironment distinguishes omission from an explicitly empty selector.
func FromEnvironment(service string) (func(string) string, error) {
	if path, present := os.LookupEnv("NEWS_RUNTIME_FILE"); present && path == "" {
		return nil, errors.New("runtime manifest path is empty")
	}
	return Lookup(os.Getenv, service)
}

// Keys is deliberately service-scoped: reusable services cannot read News
// credentials, database connections, or another service's private values.
func Keys(service string) (allowed, required []string) {
	switch service {
	case "extractor":
		return []string{"LISTEN_ADDR", "EXTRACTOR_SERVICE_TOKEN", "EXTRACTOR_MAX_CONCURRENT", "EXTRACTOR_BROWSER_CONCURRENT", "CHROME_ENDPOINT", "EXTRACTOR_HTTP_ONLY", "EXTRACTOR_BROWSER_BACKEND", "EXTRACTOR_NATIVE_PROXY_URL", "EXTRACTOR_NATIVE_PROXY_TOKEN", "EXTRACTOR_NATIVE_NETWORK_ISOLATED", "EXTRACTOR_NATIVE_READY_SELECTOR", "EXTRACTOR_NATIVE_DIAGNOSTICS"}, []string{"EXTRACTOR_SERVICE_TOKEN"}
	case "egress":
		return []string{"EGRESS_LISTEN_ADDR", "EGRESS_TOKEN", "EGRESS_ALLOW_WILDCARD_BIND", "EGRESS_MAX_CONCURRENT", "EGRESS_MAX_TUNNEL_BYTES", "EGRESS_MAX_LIFETIME_SECONDS", "EGRESS_TEST_MODE", "EGRESS_TEST_ALLOWLIST"}, []string{"EGRESS_TOKEN"}
	default:
		return nil, nil
	}
}

// Lookup selects exactly one configuration source. Once a file is selected,
// neither missing values nor file failures fall back to ambient environment.
func Lookup(getenv func(string) string, service string) (func(string) string, error) {
	path := getenv("NEWS_RUNTIME_FILE")
	if path == "" {
		return getenv, nil
	}
	allowed, required := Keys(service)
	m, err := Load(path, service, allowed, required)
	if err != nil {
		return nil, err
	}
	return func(key string) string { return m.Values[key] }, nil
}
