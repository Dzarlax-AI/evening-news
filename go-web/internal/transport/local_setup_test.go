package transport

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestLocalSetupExceptionIsExplicitAndLoopbackScoped(t *testing.T) {
	for _, tc := range []struct {
		name, site, host, peer string
		enabled, want          bool
	}{
		{"docker loopback publication", "http://localhost:8000", "localhost:8000", "192.168.65.1:4123", true, true},
		{"disabled local exception", "http://localhost:8000", "localhost:8000", "192.168.65.1:4123", false, false},
		{"public site cannot opt in", "http://news.test", "news.test", "192.168.65.1:4123", true, false},
		{"public peer denied", "http://localhost:8000", "localhost:8000", "203.0.113.1:4123", true, false},
		{"public host denied", "http://localhost:8000", "news.test", "192.168.65.1:4123", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			wrapped, err := (Policy{TrustedHosts: []string{"localhost", "news.test"}, PublicOrigin: tc.site, LocalHTTPSetup: tc.enabled}).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if SecureRequest(r) != tc.want {
					t.Errorf("setup allowed=%v want=%v", SecureRequest(r), tc.want)
				}
			}))
			if err != nil {
				t.Fatal(err)
			}
			r := httptest.NewRequest("POST", "http://"+tc.host+"/api/v1/setup/claim", nil)
			r.RemoteAddr = tc.peer
			wrapped.ServeHTTP(httptest.NewRecorder(), r)
		})
	}
}
