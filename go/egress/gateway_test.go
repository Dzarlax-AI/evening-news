package egress

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const testToken = "isolated-egress-test-token"

type resolverFunc func(context.Context, string, string) ([]netip.Addr, error)

func (f resolverFunc) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return f(ctx, network, host)
}

type dialerFunc func(context.Context, string, string) (net.Conn, error)

func (f dialerFunc) DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	return f(ctx, network, address)
}

func newGateway(t *testing.T, cfg Config) (*Gateway, *httptest.Server) {
	t.Helper()
	if cfg.Token == "" {
		cfg.Token = testToken
	}
	g, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(g)
	t.Cleanup(func() {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = g.Drain(ctx)
		server.Close()
	})
	return g, server
}

func proxyAuth() string {
	return "Basic " + base64.StdEncoding.EncodeToString([]byte("extractor:"+testToken))
}

func TestExpiredDialCannotReportSuccessfulCONNECT(t *testing.T) {
	_, server := newGateway(t, Config{MaxLifetime: 5 * time.Millisecond, Dialer: dialerFunc(func(ctx context.Context, _, _ string) (net.Conn, error) {
		<-ctx.Done()
		left, right := net.Pipe()
		right.Close()
		return left, nil
	})})
	c, _, status := connect(t, server.URL, "8.8.8.8:443", proxyAuth())
	defer c.Close()
	if status != http.StatusServiceUnavailable {
		t.Fatalf("expired tunnel reported %d", status)
	}
}

func connect(t *testing.T, proxy, target, auth string) (net.Conn, *bufio.Reader, int) {
	t.Helper()
	c, err := net.DialTimeout("tcp", strings.TrimPrefix(proxy, "http://"), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.SetDeadline(time.Now().Add(3 * time.Second))
	_, err = fmt.Fprintf(c, "CONNECT %s HTTP/1.1\r\nHost: %s\r\nProxy-Authorization: %s\r\n\r\n", target, target, auth)
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	b := bufio.NewReader(c)
	r, err := http.ReadResponse(b, &http.Request{Method: http.MethodConnect})
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	return c, b, r.StatusCode
}

func waitIdle(t *testing.T, g *Gateway) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for g.Snapshot().Active != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if g.Snapshot().Active != 0 {
		t.Fatal("tunnel quota not released")
	}
}

func TestPublicAdmissionRejectsSpecialMixedAndPrivateAddresses(t *testing.T) {
	for _, address := range []string{"0.0.0.0", "127.0.0.1", "10.0.0.1", "169.254.169.254", "100.100.100.200", "192.168.1.1", "192.0.2.1", "224.0.0.1", "::1", "::ffff:127.0.0.1", "64:ff9b::a00:1", "2002:a00:1::", "2001:db8::1", "fc00::1", "fe80::1", "fec0::1", "ff02::1"} {
		t.Run(address, func(t *testing.T) {
			g, err := New(Config{Token: testToken, Resolver: resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
				return []netip.Addr{netip.MustParseAddr(address)}, nil
			})})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := g.admit(context.Background(), "publisher.example:443"); !errors.Is(err, ErrDenied) {
				t.Fatalf("admitted %s: %v", address, err)
			}
			if _, err := g.admit(context.Background(), net.JoinHostPort(address, "443")); !errors.Is(err, ErrDenied) {
				t.Fatalf("admitted literal %s: %v", address, err)
			}
		})
	}
	g, _ := New(Config{Token: testToken, Resolver: resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
		return []netip.Addr{netip.MustParseAddr("8.8.8.8"), netip.MustParseAddr("10.0.0.1")}, nil
	})})
	if _, err := g.admit(context.Background(), "mixed.example:443"); !errors.Is(err, ErrDenied) {
		t.Fatal("mixed DNS admitted", err)
	}
}

func TestAuthorityAndAuthenticationFailBeforeDial(t *testing.T) {
	var calls atomic.Int32
	g, server := newGateway(t, Config{Dialer: dialerFunc(func(context.Context, string, string) (net.Conn, error) {
		calls.Add(1)
		return nil, errors.New("unexpected dial")
	})})
	for _, target := range []string{"example.com:80", "example.com:8443", "localhost:443", "metadata.local:443", "127.0.0.1:443", "[fe80::1%lo]:443", "user@example.com:443"} {
		c, _, status := connect(t, server.URL, target, proxyAuth())
		c.Close()
		if status != 403 && status != 400 {
			t.Errorf("target %s status %d", target, status)
		}
	}
	for _, auth := range []string{"", "Basic " + base64.StdEncoding.EncodeToString([]byte("extractor:wrong-token")), "Bearer " + testToken} {
		c, _, status := connect(t, server.URL, "example.com:443", auth)
		c.Close()
		if status != 407 {
			t.Errorf("invalid auth status %d", status)
		}
	}
	if calls.Load() != 0 || g.Snapshot().Tunnels != 0 {
		t.Fatal("unadmitted traffic reached dial")
	}
}

func TestDNSPinningAndRebindingRecheckedForEveryCONNECT(t *testing.T) {
	var lookups atomic.Int32
	dialed := make(chan string, 1)
	g, server := newGateway(t, Config{
		Resolver: resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
			if lookups.Add(1) == 1 {
				return []netip.Addr{netip.MustParseAddr("8.8.8.8")}, nil
			}
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		}),
		Dialer: dialerFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
			dialed <- address
			client, peer := net.Pipe()
			go func() { io.Copy(io.Discard, peer); peer.Close() }()
			return client, nil
		}),
	})
	c, _, status := connect(t, server.URL, "rebound.example:443", proxyAuth())
	if status != 200 {
		t.Fatal(status)
	}
	if address := <-dialed; address != "8.8.8.8:443" {
		t.Fatal("hostname redialed instead of pinned IP", address)
	}
	c.Close()
	waitIdle(t, g)
	c, _, status = connect(t, server.URL, "rebound.example:443", proxyAuth())
	c.Close()
	if status != 403 || lookups.Load() != 2 {
		t.Fatal("DNS rebinding bypassed", status, lookups.Load())
	}
}

func TestDNSFailureIsUnavailableAndNeverDials(t *testing.T) {
	for _, resolve := range []resolverFunc{
		func(context.Context, string, string) ([]netip.Addr, error) {
			return nil, errors.New("resolver failure")
		},
		func(ctx context.Context, _, _ string) ([]netip.Addr, error) { <-ctx.Done(); return nil, ctx.Err() },
	} {
		_, server := newGateway(t, Config{DNSTimeout: 20 * time.Millisecond, Resolver: resolve,
			Dialer: dialerFunc(func(context.Context, string, string) (net.Conn, error) {
				t.Error("unresolved dial")
				return nil, errors.New("denied")
			})})
		c, _, status := connect(t, server.URL, "unresolved.example:443", proxyAuth())
		c.Close()
		if status != 503 {
			t.Fatal("resolver failure became permanent denial", status)
		}
	}
}

type bufferedConn struct {
	net.Conn
	reader *bufio.Reader
}

func (c bufferedConn) Read(p []byte) (int, error) { return c.reader.Read(p) }

func TestOpaqueTLSReachesOriginWithOriginalCertificateAndCookies(t *testing.T) {
	requests := make(chan *http.Request, 2)
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests <- r
		w.Header().Set("Set-Cookie", "fixture=native; Path=/; Secure")
		fmt.Fprint(w, `<article>Native HTTPS fixture</article><script>fetch('/data')</script>`)
	}))
	defer origin.Close()
	var dialAddress string
	g, server := newGateway(t, Config{TestMode: true, TestAllowlist: map[string]netip.Addr{"fixture.example.com": netip.MustParseAddr("127.0.0.1")},
		Resolver: resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
			t.Error("fixture performed DNS")
			return nil, errors.New("unexpected DNS")
		}),
		Dialer: dialerFunc(func(ctx context.Context, network, address string) (net.Conn, error) {
			dialAddress = address
			return (&net.Dialer{}).DialContext(ctx, network, origin.Listener.Addr().String())
		}),
	})
	c, b, status := connect(t, server.URL, "fixture.example.com:443", proxyAuth())
	if status != 200 {
		t.Fatal(status)
	}
	pool := x509.NewCertPool()
	pool.AddCert(origin.Certificate())
	secure := tls.Client(bufferedConn{c, b}, &tls.Config{RootCAs: pool, ServerName: "example.com", MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}})
	if err := secure.Handshake(); err != nil {
		t.Fatal(err)
	}
	if !secure.ConnectionState().PeerCertificates[0].Equal(origin.Certificate()) {
		t.Fatal("gateway replaced origin certificate")
	}
	reader := bufio.NewReader(secure)
	for i := 0; i < 2; i++ {
		cookie := ""
		if i == 1 {
			cookie = "Cookie: fixture=native\r\n"
		}
		fmt.Fprintf(secure, "GET /data HTTP/1.1\r\nHost: fixture.example.com\r\n%s\r\n", cookie)
		response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodGet})
		if err != nil {
			t.Fatal(err)
		}
		body, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || !strings.Contains(string(body), "fetch('/data')") {
			t.Fatal("opaque origin body changed", err)
		}
		request := <-requests
		if request.TLS == nil || request.TLS.ServerName != "example.com" || request.Header.Get("Proxy-Authorization") != "" {
			t.Fatal("TLS identity or proxy auth leaked")
		}
		if i == 1 && request.Header.Get("Cookie") != "fixture=native" {
			t.Fatal("cookie lost inside tunnel")
		}
	}
	secure.Close()
	waitIdle(t, g)
	if dialAddress != "127.0.0.1:443" || g.Snapshot().Tunnels != 1 {
		t.Fatal("fixture pin/tunnel mismatch", dialAddress, g.Snapshot())
	}
}

func pipeDialer(onPeer func(net.Conn)) Dialer {
	return dialerFunc(func(context.Context, string, string) (net.Conn, error) {
		client, peer := net.Pipe()
		go func() { defer peer.Close(); onPeer(peer) }()
		return client, nil
	})
}

func TestWireBudgetClosesTunnelAndReleasesSlot(t *testing.T) {
	received := make(chan []byte, 1)
	g, server := newGateway(t, Config{MaxTunnelBytes: 64, Dialer: pipeDialer(func(peer net.Conn) { data, _ := io.ReadAll(peer); received <- data })})
	c, _, status := connect(t, server.URL, "8.8.8.8:443", proxyAuth())
	if status != 200 {
		t.Fatal(status)
	}
	c.Write([]byte(strings.Repeat("x", 1024)))
	if data := <-received; len(data) != 64 {
		t.Fatal("wire budget not enforced", len(data))
	}
	c.Close()
	waitIdle(t, g)
	if g.Snapshot().Bytes != 64 {
		t.Fatal(g.Snapshot())
	}
}

func TestLifetimeCancellationBusyAndDrainCloseOwnedTunnels(t *testing.T) {
	for _, mode := range []string{"lifetime", "client_disconnect", "drain"} {
		t.Run(mode, func(t *testing.T) {
			closed := make(chan struct{})
			lifetime := time.Second
			if mode == "lifetime" {
				lifetime = 50 * time.Millisecond
			}
			g, server := newGateway(t, Config{MaxConcurrent: 1, MaxLifetime: lifetime,
				Dialer: pipeDialer(func(peer net.Conn) { io.Copy(io.Discard, peer); close(closed) })})
			c, b, status := connect(t, server.URL, "8.8.8.8:443", proxyAuth())
			if status != 200 {
				t.Fatal(status)
			}
			other, _, busy := connect(t, server.URL, "8.8.4.4:443", proxyAuth())
			other.Close()
			if busy != 503 {
				t.Fatal("unbounded tunnel admission", busy)
			}
			switch mode {
			case "client_disconnect":
				c.Close()
			case "drain":
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
				if err := g.Drain(ctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatal(err)
				}
				cancel()
			default:
				if _, err := b.ReadByte(); err == nil {
					t.Fatal("idle tunnel did not expire")
				}
			}
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("upstream survived cancellation")
			}
			c.Close()
			waitIdle(t, g)
			if mode == "drain" {
				other, _, status := connect(t, server.URL, "8.8.8.8:443", proxyAuth())
				other.Close()
				if status != 503 {
					t.Fatal("draining gateway accepted work")
				}
			}
		})
	}
}

func TestFixtureAllowlistIsOffByDefaultAndCannotAuthorizeAnotherHost(t *testing.T) {
	fixture := map[string]netip.Addr{"fixture.example": netip.MustParseAddr("127.0.0.1")}
	if _, err := New(Config{Token: testToken, TestAllowlist: fixture}); err == nil {
		t.Fatal("fixture policy enabled without test mode")
	}
	g, err := New(Config{Token: testToken, TestMode: true, TestAllowlist: fixture,
		Resolver: resolverFunc(func(context.Context, string, string) ([]netip.Addr, error) {
			return []netip.Addr{netip.MustParseAddr("127.0.0.1")}, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	fixture["other.example"] = netip.MustParseAddr("127.0.0.1")
	if ip, err := g.admit(context.Background(), "fixture.example:443"); err != nil || ip.String() != "127.0.0.1" {
		t.Fatal(ip, err)
	}
	for _, authority := range []string{"other.example:443", "fixture.example:444", "127.0.0.1:443"} {
		if _, err := g.admit(context.Background(), authority); !errors.Is(err, ErrDenied) {
			t.Fatal("fixture allowance escaped", authority, err)
		}
	}
}

func TestDrainCancelsPendingDNSAndStopsNewAdmissions(t *testing.T) {
	started := make(chan struct{})
	resolverClosed := make(chan struct{})
	g, server := newGateway(t, Config{Resolver: resolverFunc(func(ctx context.Context, _, _ string) ([]netip.Addr, error) {
		close(started)
		<-ctx.Done()
		close(resolverClosed)
		return nil, ctx.Err()
	}), Dialer: dialerFunc(func(context.Context, string, string) (net.Conn, error) {
		t.Error("draining unresolved request dialed")
		return nil, errors.New("unexpected dial")
	})})
	c, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fmt.Fprintf(c, "CONNECT pending.example:443 HTTP/1.1\r\nHost: pending.example:443\r\nProxy-Authorization: %s\r\n\r\n", proxyAuth())
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("DNS did not start")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := g.Drain(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	select {
	case <-resolverClosed:
	case <-time.After(time.Second):
		t.Fatal("pending DNS survived drain")
	}
	waitIdle(t, g)
}

func TestBufferedCONNECTBytesAreForwardedAndBudgetIsShared(t *testing.T) {
	received := make(chan int, 1)
	g, server := newGateway(t, Config{MaxTunnelBytes: 96, Dialer: pipeDialer(func(peer net.Conn) {
		initial := make([]byte, 32)
		if _, err := io.ReadFull(peer, initial); err != nil {
			received <- -1
			return
		}
		peer.Write([]byte(strings.Repeat("s", 32)))
		rest, _ := io.ReadAll(peer)
		received <- len(initial) + len(rest)
	})})
	c, err := net.Dial("tcp", strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(2 * time.Second))
	// The initial tunnel bytes may already be buffered by the HTTP parser.
	fmt.Fprintf(c, "CONNECT 8.8.8.8:443 HTTP/1.1\r\nHost: 8.8.8.8:443\r\nProxy-Authorization: %s\r\n\r\n%s", proxyAuth(), strings.Repeat("c", 32))
	reader := bufio.NewReader(c)
	response, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil || response.StatusCode != 200 {
		t.Fatal("CONNECT failed", err)
	}
	if _, err := io.ReadFull(reader, make([]byte, 32)); err != nil {
		t.Fatal("buffered tunnel bytes lost", err)
	}
	c.Write([]byte(strings.Repeat("c", 512)))
	if n := <-received; n != 64 {
		t.Fatal("bidirectional budget not shared", n)
	}
	waitIdle(t, g)
	if g.Snapshot().Bytes != 96 {
		t.Fatal("wire accounting", g.Snapshot())
	}
}
