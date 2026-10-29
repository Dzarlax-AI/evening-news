package transport

import (
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSecureRequestRequiresTLSOrTrustedProxy(t *testing.T) {
	_, proxy, _ := net.ParseCIDR("10.0.0.0/8")
	tests := []struct {
		name, remote, forwarded string
		tls, dev, want          bool
	}{
		{"forged host and forwarded proto", "203.0.113.1:1234", "https", false, false, false},
		{"direct http", "203.0.113.1:1234", "", false, false, false},
		{"direct tls", "203.0.113.1:1234", "", true, false, true},
		{"trusted proxy", "10.0.0.1:1234", "https", false, false, true},
		{"proxy plaintext", "10.0.0.1:1234", "http", false, false, false},
		{"dev loopback", "127.0.0.1:1234", "", false, true, true},
		{"dev remote", "203.0.113.1:1234", "", false, true, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Policy{TrustedHosts: []string{"news.test"}, TrustedProxies: []*net.IPNet{proxy}, PublicOrigin: "https://news.test", Development: tt.dev}
			h, err := p.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if SecureRequest(r) != tt.want {
					t.Errorf("secure = %v", SecureRequest(r))
				}
			}))
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "http://news.test/api/v1/setup/claim", nil)
			r.RemoteAddr = tt.remote
			r.Header.Set("X-Forwarded-Proto", tt.forwarded)
			if tt.tls {
				r.TLS = &tls.ConnectionState{}
			}
			h.ServeHTTP(httptest.NewRecorder(), r)
		})
	}
}
