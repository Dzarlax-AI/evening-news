// Package egress provides the native browser's bounded HTTPS CONNECT gateway.
// It pins an admitted public IP, then copies opaque TLS bytes without terminating
// TLS. Network isolation must prevent the browser from bypassing this gateway.
package egress

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/transport"
)

var (
	ErrDenied               = errors.New("egress_destination_denied")
	ErrAdmissionUnavailable = errors.New("egress_admission_unavailable")
)

// Resolver and Dialer are trusted process dependencies, never request options.
type Resolver interface {
	LookupNetIP(context.Context, string, string) ([]netip.Addr, error)
}
type Dialer interface {
	DialContext(context.Context, string, string) (net.Conn, error)
}

type Config struct {
	Token          string
	MaxConcurrent  int
	MaxTunnelBytes int64 // Combined bytes in both directions, including TLS overhead.
	MaxLifetime    time.Duration
	DNSTimeout     time.Duration
	DialTimeout    time.Duration
	Resolver       Resolver
	Dialer         Dialer
	// TestAllowlist bypasses DNS only for these exact host/IP pairs. Both fields
	// must be explicitly set by the isolated stand; they have no wire equivalent.
	TestMode      bool
	TestAllowlist map[string]netip.Addr
}

type Stats struct {
	Active  int    `json:"active"`
	Tunnels uint64 `json:"tunnels"`
	Denied  uint64 `json:"denied"`
	Bytes   uint64 `json:"bytes"`
}

type Gateway struct {
	cfg      Config
	authHash [32]byte
	mu       sync.Mutex
	draining bool
	active   map[*operation]struct{}
	wg       sync.WaitGroup
	tunnels  atomic.Uint64
	denied   atomic.Uint64
	bytes    atomic.Uint64
}

type operation struct {
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	connections []net.Conn
}

func (op *operation) add(conn net.Conn) bool {
	op.mu.Lock()
	defer op.mu.Unlock()
	if op.ctx.Err() != nil {
		conn.Close()
		return false
	}
	op.connections = append(op.connections, conn)
	return true
}

func (op *operation) close() {
	op.mu.Lock()
	defer op.mu.Unlock()
	for _, conn := range op.connections {
		conn.Close()
	}
}

func New(cfg Config) (*Gateway, error) {
	if len(cfg.Token) < 16 || len(cfg.Token) > 256 {
		return nil, errors.New("egress token must contain 16 to 256 bytes")
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = 32
	}
	if cfg.MaxTunnelBytes == 0 {
		cfg.MaxTunnelBytes = 32 << 20
	}
	if cfg.MaxLifetime == 0 {
		cfg.MaxLifetime = 35 * time.Second
	}
	if cfg.DNSTimeout == 0 {
		cfg.DNSTimeout = 3 * time.Second
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 3 * time.Second
	}
	if cfg.MaxConcurrent < 1 || cfg.MaxConcurrent > 128 || cfg.MaxTunnelBytes < 1 || cfg.MaxTunnelBytes > 128<<20 || cfg.MaxLifetime < time.Millisecond || cfg.MaxLifetime > time.Minute || cfg.DNSTimeout < time.Millisecond || cfg.DNSTimeout > 5*time.Second || cfg.DialTimeout < time.Millisecond || cfg.DialTimeout > 5*time.Second {
		return nil, errors.New("invalid egress limits")
	}
	if len(cfg.TestAllowlist) != 0 && !cfg.TestMode {
		return nil, errors.New("egress fixture destinations require test mode")
	}
	fixtures := make(map[string]netip.Addr, len(cfg.TestAllowlist))
	for host, ip := range cfg.TestAllowlist {
		canonical, err := destination(net.JoinHostPort(host, "443"))
		if err != nil || canonical != host || !ip.IsValid() || ip.IsUnspecified() || ip.IsMulticast() || ip.Zone() != "" {
			return nil, errors.New("invalid egress fixture destination")
		}
		if _, err := netip.ParseAddr(host); err == nil {
			return nil, errors.New("fixture hostname must be a DNS name")
		}
		fixtures[host] = ip.Unmap()
	}
	cfg.TestAllowlist = fixtures
	if cfg.Resolver == nil {
		cfg.Resolver = net.DefaultResolver
	}
	if cfg.Dialer == nil {
		cfg.Dialer = &net.Dialer{Timeout: cfg.DialTimeout, KeepAlive: -1}
	}
	hash := sha256.Sum256([]byte("extractor:" + cfg.Token))
	cfg.Token = ""
	return &Gateway{cfg: cfg, authHash: hash, active: make(map[*operation]struct{})}, nil
}

// destination accepts only an authority with an explicit HTTPS port.
func destination(authority string) (string, error) {
	if len(authority) > 260 || strings.ContainsAny(authority, "/\\?#@%\r\n\t ") {
		return "", ErrDenied
	}
	host, port, err := net.SplitHostPort(authority)
	if err != nil || port != "443" || host == "" {
		return "", ErrDenied
	}
	host = strings.ToLower(strings.TrimSuffix(host, "."))
	if ip, err := netip.ParseAddr(host); err == nil {
		return ip.String(), nil
	}
	if len(host) > 253 {
		return "", ErrDenied
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return "", ErrDenied
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-') {
				return "", ErrDenied
			}
		}
	}
	return host, nil
}

func publicIP(ip netip.Addr) bool {
	return ip.IsValid() && transport.ValidatePublicURL("https://"+net.JoinHostPort(ip.String(), "443")) == nil
}

func (g *Gateway) admit(ctx context.Context, authority string) (netip.Addr, error) {
	host, err := destination(authority)
	if err != nil {
		return netip.Addr{}, err
	}
	if ip, ok := g.cfg.TestAllowlist[host]; ok {
		return ip, nil
	}
	if transport.ValidatePublicURL("https://"+net.JoinHostPort(host, "443")) != nil {
		return netip.Addr{}, ErrDenied
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		if !publicIP(ip) {
			return netip.Addr{}, ErrDenied
		}
		return ip.Unmap(), nil
	}
	ctx, cancel := context.WithTimeout(ctx, g.cfg.DNSTimeout)
	defer cancel()
	ips, err := g.cfg.Resolver.LookupNetIP(ctx, "ip", host)
	if err != nil || len(ips) == 0 {
		return netip.Addr{}, ErrAdmissionUnavailable
	}
	for _, ip := range ips {
		if !publicIP(ip) {
			return netip.Addr{}, ErrDenied
		}
	}
	return ips[0].Unmap(), nil
}

func (g *Gateway) authorized(r *http.Request) bool {
	values := r.Header.Values("Proxy-Authorization")
	if len(values) != 1 || len(values[0]) > 512 {
		return false
	}
	scheme, encoded, ok := strings.Cut(values[0], " ")
	if !ok || !strings.EqualFold(scheme, "Basic") {
		return false
	}
	decoded, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return false
	}
	hash := sha256.Sum256(decoded)
	return subtle.ConstantTimeCompare(hash[:], g.authHash[:]) == 1
}

func (g *Gateway) begin(ctx context.Context) (*operation, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.draining || len(g.active) >= g.cfg.MaxConcurrent {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, g.cfg.MaxLifetime)
	op := &operation{ctx: ctx, cancel: cancel}
	g.active[op] = struct{}{}
	g.wg.Add(1)
	return op, true
}

func (g *Gateway) end(op *operation) {
	op.cancel()
	op.close()
	g.mu.Lock()
	delete(g.active, op)
	g.mu.Unlock()
	g.wg.Done()
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet && r.URL.Path == "/health/ready" {
		g.mu.Lock()
		draining := g.draining
		g.mu.Unlock()
		if draining {
			http.Error(w, "draining", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	if !g.authorized(r) {
		w.Header().Set("Proxy-Authenticate", `Basic realm="extractor-egress"`)
		http.Error(w, "proxy_authentication_required", http.StatusProxyAuthRequired)
		return
	}
	if r.Method != http.MethodConnect || r.ProtoMajor != 1 || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.URL.RawQuery != "" || r.URL.Path != "" || r.Host != r.RequestURI {
		http.Error(w, "connect_required", http.StatusBadRequest)
		return
	}
	op, ok := g.begin(r.Context())
	if !ok {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "egress_busy", http.StatusServiceUnavailable)
		return
	}
	defer g.end(op)
	stopClose := context.AfterFunc(op.ctx, op.close)
	defer stopClose()
	ip, err := g.admit(op.ctx, r.RequestURI)
	if err != nil {
		g.denied.Add(1)
		status := http.StatusForbidden
		if errors.Is(err, ErrAdmissionUnavailable) {
			status = http.StatusServiceUnavailable
		}
		http.Error(w, "egress_admission_failed", status)
		return
	}
	dialCtx, cancelDial := context.WithTimeout(op.ctx, g.cfg.DialTimeout)
	upstream, err := g.cfg.Dialer.DialContext(dialCtx, "tcp", net.JoinHostPort(ip.String(), "443"))
	cancelDial()
	if err != nil {
		http.Error(w, "egress_connect_failed", http.StatusBadGateway)
		return
	}
	if !op.add(upstream) {
		http.Error(w, "egress_unavailable", http.StatusServiceUnavailable)
		return
	}
	hijacker, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "connect_unavailable", http.StatusInternalServerError)
		return
	}
	client, buffered, err := hijacker.Hijack()
	if err != nil {
		return
	}
	if !op.add(client) {
		return
	}
	deadline, _ := op.ctx.Deadline()
	client.SetDeadline(deadline)
	upstream.SetDeadline(deadline)
	if _, err := buffered.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
		return
	}
	if err := buffered.Flush(); err != nil {
		return
	}
	g.tunnels.Add(1)
	var remaining atomic.Int64
	remaining.Store(g.cfg.MaxTunnelBytes)
	done := make(chan struct{}, 2)
	go func() { g.copyTunnel(upstream, buffered, &remaining); done <- struct{}{} }()
	go func() { g.copyTunnel(client, upstream, &remaining); done <- struct{}{} }()
	<-done
	op.cancel()
	op.close()
	<-done
}

// copyTunnel shares one strict wire-byte budget across both directions.
func (g *Gateway) copyTunnel(dst io.Writer, src io.Reader, remaining *atomic.Int64) {
	buffer := make([]byte, 16*1024)
	for {
		n, err := src.Read(buffer)
		if n > 0 {
			allowed := int64(n)
			for {
				left := remaining.Load()
				if left <= 0 {
					return
				}
				if allowed > left {
					allowed = left
				}
				if remaining.CompareAndSwap(left, left-allowed) {
					break
				}
			}
			written, writeErr := dst.Write(buffer[:allowed])
			g.bytes.Add(uint64(written))
			if writeErr != nil || int64(written) != allowed || allowed < int64(n) || remaining.Load() == 0 {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (g *Gateway) Snapshot() Stats {
	g.mu.Lock()
	active := len(g.active)
	g.mu.Unlock()
	return Stats{Active: active, Tunnels: g.tunnels.Load(), Denied: g.denied.Load(), Bytes: g.bytes.Load()}
}

// Drain stops new tunnels, waits for bounded completion, then cancels any
// remaining operations. net/http.Shutdown does not close hijacked connections.
func (g *Gateway) Drain(ctx context.Context) error {
	g.mu.Lock()
	g.draining = true
	g.mu.Unlock()
	done := make(chan struct{})
	go func() { g.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		g.mu.Lock()
		for op := range g.active {
			op.cancel()
			op.close()
		}
		g.mu.Unlock()
		return ctx.Err()
	}
}
