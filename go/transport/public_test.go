package transport

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/extractor"
)

type fixedResolver struct{ ips []netip.Addr }

func (r fixedResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return r.ips, nil
}

func TestPublicPolicyDeniesSpecialAddresses(t *testing.T) {
	for _, raw := range []string{"http://127.0.0.1/", "http://[::1]/", "http://[::ffff:127.0.0.1]/", "http://10.0.0.1/", "http://100.64.0.1/", "http://169.254.169.254/latest", "http://192.168.1.1/", "http://[64:ff9b::7f00:1]/", "http://[2002:7f00:1::]/", "http://[2001::1]/", "http://[fe80::1%25eth0]/", "http://localhost./", "http://a.localhost/", "http://name.local/", "http://user:secret@example.com/", "file:///private", "https://example.com:9222/"} {
		if err := ValidatePublicURL(raw); !errors.Is(err, ErrUnsafePublicURL) {
			t.Errorf("accepted %s: %v", raw, err)
		}
	}
	if publicIP(netip.MustParseAddr("fec0::1")) {
		t.Fatal("deprecated IPv6 site-local accepted")
	}
	for _, raw := range []string{"https://example.com/", "http://8.8.8.8/", "https://[2606:4700:4700::1111]/"} {
		if err := ValidatePublicURL(raw); err != nil {
			t.Errorf("public rejected: %s: %v", raw, err)
		}
	}
}

func TestPublicDecodeEnforcesOutputBeforeAllocation(t *testing.T) {
	var wire bytes.Buffer
	z := gzip.NewWriter(&wire)
	z.Write([]byte(strings.Repeat("large output ", 10000)))
	z.Close()
	if _, err := publicDecode(wire.Bytes(), "gzip", 128); !errors.Is(err, extractor.ErrOutputLimit) {
		t.Fatalf("decompression output not bounded: %v", err)
	}
	if _, err := publicDecode(append(wire.Bytes(), 1, 2, 3), "gzip", 200000); err == nil {
		t.Fatal("trailing compressed bytes accepted")
	}
}

func TestPublicDNSRejectsMixedAndReboundAnswers(t *testing.T) {
	public := netip.MustParseAddr("8.8.8.8")
	private := netip.MustParseAddr("127.0.0.1")
	for _, ips := range [][]netip.Addr{{public, private}, {private}} {
		if _, err := resolvePublic(context.Background(), fixedResolver{ips}, "news.example"); !errors.Is(err, ErrUnsafePublicURL) {
			t.Errorf("unsafe resolution allowed: %v %v", ips, err)
		}
	}
	if ips, err := resolvePublic(context.Background(), fixedResolver{[]netip.Addr{public}}, "news.example"); err != nil || ips[0] != public {
		t.Fatalf("public resolution failed: %v %v", ips, err)
	}
	if _, err := publicDial(context.Background(), "tcp", "127.0.0.1:80"); !errors.Is(err, ErrUnsafePublicURL) {
		t.Fatalf("actual socket policy bypassed: %v", err)
	}
}

func TestPublicNeverUsesInjectedClientOrPrivateServer(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Error("private server was reached") }))
	defer server.Close()
	if _, err := FetchPublic(context.Background(), server.URL, Options{}); !errors.Is(err, ErrUnsafePublicURL) {
		t.Fatal(err)
	}
	if _, err := FetchPublic(context.Background(), "https://example.com", Options{Client: server.Client()}); err == nil {
		t.Fatal("injected transport accepted")
	}
}

func TestConfiguredBrowserDiscoveryPinsAuthorityAndDeniesRedirect(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"webSocketDebuggerUrl":"ws://attacker.example:1/devtools/browser/owned"}`)
	}))
	defer server.Close()
	got, err := configuredBrowserEndpoint(context.Background(), server.URL)
	if err != nil || got != "ws"+strings.TrimPrefix(server.URL, "http")+"/devtools/browser/owned" {
		t.Fatalf("discovered authority escaped: %q %v", got, err)
	}
	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, server.URL, http.StatusFound) }))
	defer redirect.Close()
	if _, err := configuredBrowserEndpoint(context.Background(), redirect.URL); err == nil {
		t.Fatal("discovery redirect accepted")
	}
}

func TestConfiguredBrowserDNSHostUsesPinnedNumericDiscovery(t *testing.T) {
	var observedHost string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		observedHost = r.Host
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil || net.ParseIP(host) == nil {
			http.Error(w, "Host header must contain an IP address", http.StatusInternalServerError)
			return
		}
		fmt.Fprint(w, `{"webSocketDebuggerUrl":"ws://untrusted.example:9999/devtools/browser/owned"}`)
	}))
	defer server.Close()
	_, port, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	configured := "http://chrome-fixture.invalid:" + port
	got, err := configuredBrowserEndpointWithResolver(context.Background(), configured, fixedResolver{[]netip.Addr{netip.MustParseAddr("127.0.0.1")}})
	if err != nil {
		t.Fatal(err)
	}
	if observedHost != "127.0.0.1:"+port {
		t.Fatalf("Chrome received a DNS-name Host: %q", observedHost)
	}
	if got != "ws://chrome-fixture.invalid:"+port+"/devtools/browser/owned" {
		t.Fatalf("configured websocket authority escaped: %q", got)
	}
}

func TestRetryAfterDeltaAndHTTPDateAreBounded(t *testing.T) {
	now := time.Date(2026, 10, 16, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		raw   string
		want  int
		valid bool
	}{{"30", 30, true}, {"0", 0, true}, {"999999", 86400, true}, {now.Add(90 * time.Second).Format(http.TimeFormat), 90, true}, {now.Add(-time.Minute).Format(http.TimeFormat), 0, true}, {"-1", 0, false}, {"not a date", 0, false}, {"1.5", 0, false}, {"", 0, false}} {
		got := parseRetryAfter(tc.raw, now)
		if (got != nil) != tc.valid || (got != nil && *got != tc.want) {
			t.Errorf("Retry-After %q: %v want %d valid=%v", tc.raw, got, tc.want, tc.valid)
		}
	}
}

func TestPublicTimeoutBoundsKeepDefaultAndRSSPathsSeparate(t *testing.T) {
	if err := validatePublicTimeout(30*time.Second, 30*time.Second); err != nil {
		t.Fatalf("default public maximum should remain 30s: %v", err)
	}
	if err := validatePublicTimeout(31*time.Second, 30*time.Second); err == nil {
		t.Fatal("default public path accepted a timeout above 30s")
	}
	if err := validatePublicTimeout(60*time.Second, 60*time.Second); err != nil {
		t.Fatalf("RSS path should accept its 60s bound: %v", err)
	}
	if err := validatePublicTimeout(61*time.Second, 60*time.Second); err == nil {
		t.Fatal("RSS path accepted a timeout above 60s")
	}
}

func TestFetchPublicRSSStillRejectsPrivateDestinations(t *testing.T) {
	_, err := FetchPublicRSS(context.Background(), "http://192.168.1.2/feed.xml", Options{Timeout: 60 * time.Second})
	if !errors.Is(err, ErrUnsafePublicURL) {
		t.Fatalf("RSS public fetch error = %v, want private URL rejection", err)
	}
	_, err = FetchPublic(context.Background(), "http://192.168.1.2/feed.xml", Options{Timeout: 31 * time.Second})
	if !errors.Is(err, ErrUnsafePublicURL) {
		t.Fatalf("default public fetch error = %v, want private URL rejection before any connection", err)
	}
}

func TestChallengeDetectionUsesInterstitialMarkup(t *testing.T) {
	if !IsChallengePage([]byte(`<html><title>Just a moment...</title><form id="challenge-form"></form></html>`)) {
		t.Fatal("known challenge not recognized")
	}
	if IsChallengePage([]byte(`<article><h1>How Cloudflare challenges work</h1><p>The CAPTCHA study described challenges.</p></article>`)) {
		t.Fatal("ordinary article misclassified")
	}
}

func TestBrowserDelegationDNSAdmissionRejectsPrivateAndUnresolved(t *testing.T) {
	for _, ips := range [][]netip.Addr{{netip.MustParseAddr("127.0.0.1")}, {netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("10.1.2.3")}} {
		if err := admitPublicURLWithResolver(context.Background(), "https://private-dns.example/report", fixedResolver{ips}); !errors.Is(err, ErrUnsafePublicURL) {
			t.Fatalf("unadmitted DNS delegated: %v %v", ips, err)
		}
	}
	if err := admitPublicURLWithResolver(context.Background(), "https://public-dns.example/report", fixedResolver{[]netip.Addr{netip.MustParseAddr("8.8.8.8")}}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := admitPublicURLWithResolver(ctx, "https://public-dns.example/report", fixedResolver{nil}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled admission: %v", err)
	}
}

type failingResolver struct{ err error }

func (r failingResolver) LookupNetIP(context.Context, string, string) ([]netip.Addr, error) {
	return nil, r.err
}

func TestBrowserAdmissionDNSFailureIsRetryableUnavailable(t *testing.T) {
	for _, resolver := range []publicResolver{fixedResolver{nil}, failingResolver{&net.DNSError{Err: "temporary", IsTemporary: true}}, failingResolver{context.DeadlineExceeded}} {
		err := admitPublicURLWithResolver(context.Background(), "https://public-dns.example/report", resolver)
		if !errors.Is(err, ErrPublicAdmissionUnavailable) || errors.Is(err, ErrUnsafePublicURL) {
			t.Fatalf("DNS outage became permanent policy rejection: %v", err)
		}
	}
}
