package managedsetup

import (
	"errors"
	"net/netip"
	"net/url"
	"strings"
)

// ValidateWebTransport rejects managed web profiles that would start without
// a secure login path. localHTTPProfile is true only when the caller will emit
// the explicit loopback-only LOCAL_HTTP_SETUP runtime setting.
func ValidateWebTransport(site string, options Options, localHTTPProfile bool) error {
	if err := options.Validate(); err != nil {
		return errors.New("trusted proxy configuration is invalid")
	}
	u, err := url.Parse(strings.TrimSpace(site))
	if err != nil || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("SITE_BASE_URL must be an HTTP or HTTPS origin")
	}
	loopback := isLoopbackHost(u.Hostname())
	if u.Scheme == "http" && loopback {
		if !localHTTPProfile {
			return errors.New("loopback HTTP requires the explicit local HTTP profile")
		}
		return nil
	}
	if localHTTPProfile {
		return errors.New("the local HTTP profile requires a supported loopback HTTP SITE_BASE_URL")
	}
	if u.Scheme != "https" {
		return errors.New("SITE_BASE_URL must use HTTPS unless the explicit loopback HTTP profile is selected")
	}
	if len(options.TrustedProxyCIDRs) == 0 {
		return errors.New("hosted HTTPS setup requires explicit trusted proxy CIDRs in the protected options file")
	}
	for _, raw := range options.TrustedProxyCIDRs {
		prefix, err := netip.ParsePrefix(raw)
		if err != nil || prefix.Bits() == 0 || prefix.Addr().IsUnspecified() {
			return errors.New("trusted proxy CIDRs must be valid and must not include all addresses")
		}
	}
	return nil
}

func IsLoopbackHTTPOrigin(site string) bool {
	u, err := url.Parse(strings.TrimSpace(site))
	return err == nil && u.Scheme == "http" && u.Hostname() != "" && isLoopbackHost(u.Hostname())
}

func isLoopbackHost(raw string) bool {
	if raw == "localhost" {
		return true
	}
	ip, err := netip.ParseAddr(raw)
	return err == nil && (ip == netip.MustParseAddr("127.0.0.1") || ip == netip.IPv6Loopback())
}
