package transport

import (
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHostAndIdentityBoundary(t *testing.T) {
	_, block, _ := net.ParseCIDR("192.0.2.0/24")
	var identity, scheme string
	var deadline bool
	h, err := (Policy{TrustedHosts: []string{"news.example.test", "*.fixture.test"}, TrustedProxies: []*net.IPNet{block}, PublicOrigin: "https://news.example.test", RequestTimeout: time.Second}).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity = r.Header.Get("X-authentik-username")
		scheme = r.URL.Scheme
		_, deadline = r.Context().Deadline()
		w.WriteHeader(204)
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		host, peer, wantIdentity string
		status                   int
	}{
		{"news.example.test", "198.51.100.2:1234", "", 204},
		{"news.example.test", "192.0.2.1:1234", "admin", 204},
		{"evil.test", "192.0.2.1:1234", "", 400},
		{"evilfixture.test", "192.0.2.1:1234", "", 400},
		{"a.fixture.test", "192.0.2.1:1234", "admin", 204},
	} {
		t.Run(tc.host+tc.peer, func(t *testing.T) {
			identity, scheme, deadline = "", "", false
			r := httptest.NewRequest("GET", "http://"+tc.host+"/", nil)
			r.RemoteAddr = tc.peer
			r.Header.Set("X-authentik-username", "admin")
			r.Header.Set("X-Forwarded-Proto", "https")
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			if w.Code != tc.status || identity != tc.wantIdentity {
				t.Fatalf("status=%d identity=%q", w.Code, identity)
			}
			if w.Code == 204 && !deadline {
				t.Fatal("request has no deadline")
			}
			if tc.host == "news.example.test" && w.Code == 204 && scheme != "https" {
				t.Fatal("configured public origin was lost")
			}
		})
	}
}

func TestCORSPreflightAndUntrustedOrigin(t *testing.T) {
	h, _ := (Policy{TrustedHosts: []string{"localhost"}, AllowedOrigins: []string{"https://ui.example.test"}}).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	for _, tc := range []struct {
		origin string
		status int
	}{{"https://ui.example.test", 200}, {"https://evil.example.test", 400}} {
		r := httptest.NewRequest("OPTIONS", "http://localhost/", nil)
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("Access-Control-Request-Method", "POST")
		r.Header.Set("Access-Control-Request-Headers", "content-type,idempotency-key")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatalf("status=%d", w.Code)
		}
		if tc.status == 200 && (w.Header().Get("Access-Control-Allow-Origin") != tc.origin || w.Header().Get("Access-Control-Allow-Credentials") != "true") {
			t.Fatal("missing credentialed CORS response")
		}
		if tc.status != 200 && w.Header().Get("Access-Control-Allow-Origin") != "" {
			t.Fatal("untrusted origin was reflected")
		}
	}
}

func TestInvalidPublicOriginRejected(t *testing.T) {
	for _, origin := range []string{"file:///etc/passwd", "https://u:p@example.test", "https://example.test/path", "https://example.test?secret=x"} {
		if _, err := (Policy{PublicOrigin: origin}).Wrap(http.NotFoundHandler()); err == nil {
			t.Fatalf("accepted origin %q", origin)
		}
	}
}
