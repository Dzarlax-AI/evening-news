// Package transport owns the HTTP deployment boundary, not application rules.
package transport

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type Policy struct {
	TrustedHosts   []string
	AllowedOrigins []string
	TrustedProxies []*net.IPNet
	PublicOrigin   string
	Development    bool
	LocalHTTPSetup bool
	RequestTimeout time.Duration
}

type secureRequestKey struct{}

// SecureRequest is set by the transport boundary for TLS, an explicitly trusted
// TLS proxy, or an explicitly enabled loopback-published local setup profile.
func SecureRequest(r *http.Request) bool {
	v, _ := r.Context().Value(secureRequestKey{}).(bool)
	return v
}

// Wrap never obtains identity or the public scheme from an untrusted header.
// Identity assertion headers are meaningful only behind an explicitly trusted
// peer; ordinary Basic and JWT authentication do not require a proxy.
func (p Policy) Wrap(next http.Handler) (http.Handler, error) {
	var public *url.URL
	if p.PublicOrigin != "" {
		var err error
		public, err = url.Parse(p.PublicOrigin)
		if err != nil || public.Host == "" || public.User != nil || public.RawQuery != "" || public.Fragment != "" || (public.Scheme != "http" && public.Scheme != "https") || (public.Path != "" && public.Path != "/") {
			return nil, errors.New("invalid public site origin")
		}
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !p.Development && !hostAllowed(r.Host, p.TrustedHosts) {
			http.Error(w, "Invalid host header", http.StatusBadRequest)
			return
		}
		r = r.Clone(r.Context())
		r.Header = r.Header.Clone()
		if !trustedPeer(r.RemoteAddr, p.TrustedProxies) {
			r.Header.Del("X-authentik-username")
		}
		// The explicit local installation profile publishes only on host loopback.
		// It is opt-in and cannot be used with a public site origin or peer.
		localHTTP := false
		if p.LocalHTTPSetup && public != nil && public.Scheme == "http" && loopbackHost(public.Hostname()) && hostAllowed(r.Host, []string{"localhost", "127.0.0.1", "::1"}) {
			peer, _, err := net.SplitHostPort(r.RemoteAddr)
			if err == nil {
				ip := net.ParseIP(peer)
				localHTTP = ip != nil && (ip.IsLoopback() || ip.IsPrivate())
			}
		}
		secure := localHTTP || r.TLS != nil || (trustedPeer(r.RemoteAddr, p.TrustedProxies) && strings.EqualFold(strings.TrimSpace(r.Header.Get("X-Forwarded-Proto")), "https"))
		if p.Development {
			host, _, err := net.SplitHostPort(r.RemoteAddr)
			if err != nil {
				host = r.RemoteAddr
			}
			ip := net.ParseIP(host)
			secure = secure || (ip != nil && ip.IsLoopback())
		}
		r = r.WithContext(context.WithValue(r.Context(), secureRequestKey{}, secure))
		r.URL.Scheme = "http"
		if r.TLS != nil {
			r.URL.Scheme = "https"
		}
		if public != nil && !p.Development && strings.EqualFold(public.Host, r.Host) {
			r.URL.Scheme = public.Scheme
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		origin := r.Header.Get("Origin")
		if origin != "" {
			w.Header().Add("Vary", "Origin")
			if contains(p.AllowedOrigins, origin) {
				w.Header().Set("Access-Control-Allow-Origin", origin)
				w.Header().Set("Access-Control-Allow-Credentials", "true")
			}
		}
		if r.Method == http.MethodOptions && origin != "" && r.Header.Get("Access-Control-Request-Method") != "" {
			method := r.Header.Get("Access-Control-Request-Method")
			if !contains(p.AllowedOrigins, origin) || !contains([]string{"GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"}, method) {
				http.Error(w, "Disallowed CORS request", http.StatusBadRequest)
				return
			}
			w.Header().Set("Access-Control-Allow-Methods", "DELETE, GET, HEAD, OPTIONS, PATCH, POST, PUT")
			w.Header().Set("Access-Control-Allow-Headers", r.Header.Get("Access-Control-Request-Headers"))
			w.Header().Set("Access-Control-Max-Age", "600")
			w.WriteHeader(http.StatusOK)
			return
		}
		if p.RequestTimeout > 0 {
			ctx, cancel := context.WithTimeout(r.Context(), p.RequestTimeout)
			defer cancel()
			r = r.WithContext(ctx)
		}
		next.ServeHTTP(w, r)
	}), nil
}

func contains(items []string, value string) bool {
	for _, item := range items {
		if item == value {
			return true
		}
	}
	return false
}

func hostAllowed(host string, patterns []string) bool {
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.ToLower(host)
	for _, pattern := range patterns {
		pattern = strings.ToLower(pattern)
		if pattern == "*" || pattern == host {
			return true
		}
		if strings.HasPrefix(pattern, "*.") && strings.HasSuffix(host, pattern[1:]) {
			return true
		}
	}
	return false
}

func trustedPeer(remote string, ranges []*net.IPNet) bool {
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	for _, block := range ranges {
		if block.Contains(ip) {
			return true
		}
	}
	return false
}

func loopbackHost(host string) bool {
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}
