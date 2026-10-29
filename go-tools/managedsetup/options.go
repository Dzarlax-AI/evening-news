package managedsetup

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/netip"
	"net/url"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
)

// Options configures only explicitly trusted reverse proxies. It never enables
// forwarded administrator identity or changes listener exposure.
type Options struct {
	TrustedProxyCIDRs []string              `json:"trusted_proxy_cidrs,omitempty"`
	NativeBrowser     *NativeBrowserOptions `json:"native_browser,omitempty"`
}

// NativeBrowserOptions contains only values imported from a protected rendered
// Compose file. It never invents browser endpoints, proxy tokens, or networks.
type NativeBrowserOptions struct {
	ExtractorValues map[string]string `json:"extractor_values"`
	EgressValues    map[string]string `json:"egress_values"`
	ExtractorLimits ContainerLimits   `json:"extractor_limits"`
	EgressLimits    ContainerLimits   `json:"egress_limits"`
	ControlNetwork  string            `json:"control_network"`
	ControlName     string            `json:"control_name"`
	OutboundNetwork string            `json:"outbound_network"`
	OutboundName    string            `json:"outbound_name"`
	ExtractorIP     string            `json:"extractor_ip"`
	EgressIP        string            `json:"egress_ip"`
}

// ContainerLimits carries only the reviewed non-secret hardening contract for
// the extractor and egress containers. It deliberately excludes arbitrary
// Compose service fields.
type ContainerLimits struct {
	Init            bool     `json:"init"`
	ReadOnly        bool     `json:"read_only"`
	CapDrop         []string `json:"cap_drop"`
	SecurityOpt     []string `json:"security_opt"`
	PidsLimit       int      `json:"pids_limit"`
	MemLimit        string   `json:"mem_limit"`
	CPUs            float64  `json:"cpus"`
	StopGracePeriod string   `json:"stop_grace_period"`
}

func (c ContainerLimits) Validate() error {
	if !c.Init || !c.ReadOnly || !reflect.DeepEqual(c.CapDrop, []string{"ALL"}) || !reflect.DeepEqual(c.SecurityOpt, []string{"no-new-privileges:true"}) {
		return errors.New("native browser container hardening settings are incomplete")
	}
	if c.PidsLimit < 1 || c.PidsLimit > 4096 || !regexp.MustCompile(`^[1-9][0-9]*(b|k|kb|m|mb|g|gb)?$`).MatchString(strings.ToLower(c.MemLimit)) || c.CPUs <= 0 || c.CPUs > 64 || math.IsNaN(c.CPUs) || math.IsInf(c.CPUs, 0) {
		return errors.New("native browser container resource limits are invalid")
	}
	grace, err := time.ParseDuration(c.StopGracePeriod)
	if err != nil || grace <= 0 || grace > 5*time.Minute {
		return errors.New("native browser stop grace period is invalid")
	}
	return nil
}

func (o Options) Validate() error {
	if len(o.TrustedProxyCIDRs) > 32 {
		return errors.New("too many trusted proxies")
	}
	seen := map[string]bool{}
	for _, s := range o.TrustedProxyCIDRs {
		p, e := netip.ParsePrefix(s)
		if e != nil || p.Bits() == 0 || p.Addr().IsUnspecified() || seen[s] {
			return errors.New("invalid trusted proxy CIDR")
		}
		seen[s] = true
	}
	if o.NativeBrowser != nil {
		if err := o.NativeBrowser.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (o NativeBrowserOptions) Validate() error {
	if len(o.ExtractorValues) == 0 || len(o.EgressValues) == 0 || o.ControlNetwork == "" || o.ControlName == "" || o.OutboundNetwork == "" || o.OutboundName == "" {
		return errors.New("native browser configuration incomplete")
	}
	if o.ExtractorLimits.Validate() != nil || o.EgressLimits.Validate() != nil {
		return errors.New("native browser container hardening or resource limits invalid")
	}
	extractorKeys := map[string]bool{"LISTEN_ADDR": true, "EXTRACTOR_MAX_CONCURRENT": true, "EXTRACTOR_BROWSER_CONCURRENT": true, "CHROME_ENDPOINT": true, "EXTRACTOR_HTTP_ONLY": true, "EXTRACTOR_BROWSER_BACKEND": true, "EXTRACTOR_NATIVE_PROXY_URL": true, "EXTRACTOR_NATIVE_PROXY_TOKEN": true, "EXTRACTOR_NATIVE_NETWORK_ISOLATED": true, "EXTRACTOR_NATIVE_READY_SELECTOR": true, "EXTRACTOR_NATIVE_DIAGNOSTICS": true}
	egressKeys := map[string]bool{"EGRESS_LISTEN_ADDR": true, "EGRESS_TOKEN": true, "EGRESS_ALLOW_WILDCARD_BIND": true, "EGRESS_MAX_CONCURRENT": true, "EGRESS_MAX_TUNNEL_BYTES": true, "EGRESS_MAX_LIFETIME_SECONDS": true, "EGRESS_TEST_MODE": true, "EGRESS_TEST_ALLOWLIST": true}
	for key := range o.ExtractorValues {
		if !extractorKeys[key] {
			return errors.New("native browser extractor configuration contains an unsupported key")
		}
	}
	for key := range o.EgressValues {
		if !egressKeys[key] {
			return errors.New("native browser egress configuration contains an unsupported key")
		}
	}
	for _, key := range []string{"LISTEN_ADDR", "EXTRACTOR_HTTP_ONLY", "EXTRACTOR_BROWSER_BACKEND", "EXTRACTOR_NATIVE_NETWORK_ISOLATED", "CHROME_ENDPOINT", "EXTRACTOR_NATIVE_PROXY_URL", "EXTRACTOR_NATIVE_PROXY_TOKEN", "EXTRACTOR_MAX_CONCURRENT", "EXTRACTOR_BROWSER_CONCURRENT"} {
		if strings.TrimSpace(o.ExtractorValues[key]) == "" {
			return fmt.Errorf("native browser extractor setting %s unavailable", key)
		}
	}
	if o.ExtractorValues["EXTRACTOR_HTTP_ONLY"] != "false" || o.ExtractorValues["EXTRACTOR_BROWSER_BACKEND"] != "native" || o.ExtractorValues["EXTRACTOR_NATIVE_NETWORK_ISOLATED"] != "true" {
		return errors.New("native browser requires native backend, HTTP fallback disabled, and isolated network mode")
	}
	if o.ExtractorValues["EXTRACTOR_NATIVE_PROXY_TOKEN"] != o.EgressValues["EGRESS_TOKEN"] || len(o.EgressValues["EGRESS_TOKEN"]) < 16 || len(o.EgressValues["EGRESS_TOKEN"]) > 256 {
		return errors.New("native browser proxy credentials do not match")
	}
	proxyURL, err := url.Parse(o.ExtractorValues["EXTRACTOR_NATIVE_PROXY_URL"])
	if err != nil || proxyURL.Scheme != "http" || proxyURL.User != nil || proxyURL.Hostname() == "" || (proxyURL.Path != "" && proxyURL.Path != "/") || proxyURL.RawQuery != "" || proxyURL.Fragment != "" || proxyURL.Opaque != "" {
		return errors.New("native browser proxy endpoint invalid")
	}
	listenHost, listenPort, err := net.SplitHostPort(o.EgressValues["EGRESS_LISTEN_ADDR"])
	if err != nil || listenHost != o.EgressIP || proxyURL.Hostname() != listenHost || proxyURL.Port() != listenPort {
		return errors.New("native browser proxy endpoint does not match the egress listener")
	}
	chromeURL, err := url.Parse(o.ExtractorValues["CHROME_ENDPOINT"])
	if err != nil || (chromeURL.Scheme != "http" && chromeURL.Scheme != "https") || chromeURL.User != nil || chromeURL.Hostname() == "" || chromeURL.RawQuery != "" || chromeURL.Fragment != "" || chromeURL.Opaque != "" {
		return errors.New("native browser endpoint invalid")
	}
	browserIP, err := netip.ParseAddr(chromeURL.Hostname())
	if err != nil || !browserIP.IsPrivate() || browserIP.Is4In6() {
		return errors.New("native browser endpoint must use a private control address")
	}
	if o.EgressValues["EGRESS_TEST_MODE"] == "true" || strings.TrimSpace(o.EgressValues["EGRESS_TEST_ALLOWLIST"]) != "" || o.EgressValues["EGRESS_ALLOW_WILDCARD_BIND"] == "true" {
		return errors.New("native browser test-only or wildcard egress options are forbidden")
	}
	for key, bounds := range map[string][2]int{"EXTRACTOR_MAX_CONCURRENT": {1, 100}, "EXTRACTOR_BROWSER_CONCURRENT": {1, 2}, "EGRESS_MAX_CONCURRENT": {1, 128}} {
		value := o.ExtractorValues[key]
		if strings.HasPrefix(key, "EGRESS_") {
			value = o.EgressValues[key]
		}
		n, err := strconv.Atoi(value)
		if err != nil || n < bounds[0] || n > bounds[1] {
			return fmt.Errorf("native browser concurrency setting %s invalid", key)
		}
	}
	extractorIP, err := netip.ParseAddr(o.ExtractorIP)
	if err != nil || !extractorIP.IsPrivate() || extractorIP.Is4In6() {
		return errors.New("native browser extractor address invalid")
	}
	egressIP, err := netip.ParseAddr(o.EgressIP)
	if err != nil || !egressIP.IsPrivate() || egressIP.Is4In6() || extractorIP == egressIP {
		return errors.New("native browser egress address invalid")
	}
	if _, _, err = net.SplitHostPort(o.EgressValues["EGRESS_LISTEN_ADDR"]); err != nil {
		return errors.New("native browser egress listener invalid")
	}
	if !composeName(o.ControlNetwork) || !composeName(o.OutboundNetwork) || !composeName(o.ControlName) || !composeName(o.OutboundName) {
		return errors.New("native browser network name invalid")
	}
	if o.ControlNetwork == o.OutboundNetwork || o.ControlName == o.OutboundName || o.ControlNetwork == "default" || o.OutboundNetwork == "default" || o.ControlNetwork == "extraction-call" || o.OutboundNetwork == "extraction-call" {
		return errors.New("native browser network names conflict with managed networking")
	}
	return nil
}

func composeName(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for _, r := range value {
		if !(r == '-' || r == '_' || r == '.' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9') {
			return false
		}
	}
	return true
}
func sameOptions(a, b Options) bool { return reflect.DeepEqual(a, b) }
func ReadOptions(path string) (Options, error) {
	var options Options
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 || info.Size() > 65536 {
		return options, errors.New("options input must be a private regular file")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return options, errors.New("options input unavailable")
	}
	if runtimeconfig.DecodeStrict(raw, &options) != nil {
		return Options{}, errors.New("invalid installer options")
	}
	if err = options.Validate(); err != nil {
		return Options{}, fmt.Errorf("invalid installer options: %w", err)
	}
	return options, nil
}
