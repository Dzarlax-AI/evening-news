// extract-egress runs the private CONNECT gateway for an isolated native browser.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go/egress"
	"golang.org/x/net/netutil"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func boolEnv(get func(string) string, key string) (bool, error) {
	switch get(key) {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, errors.New("invalid " + key)
	}
}

func numberEnv(get func(string) string, key string, fallback, minimum, maximum int64) (int64, error) {
	value := get(key)
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n < minimum || n > maximum {
		return 0, errors.New("invalid " + key)
	}
	return n, nil
}

func loadConfig(get func(string) string) (string, egress.Config, error) {
	var cfg egress.Config
	addr := get("EGRESS_LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8081"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "", cfg, errors.New("invalid EGRESS_LISTEN_ADDR")
	}
	ip, err := netip.ParseAddr(host)
	n, portErr := strconv.Atoi(port)
	if err != nil || portErr != nil || n < 1 || n > 65535 || ip.Zone() != "" {
		return "", cfg, errors.New("EGRESS_LISTEN_ADDR must use an explicit private IP and port")
	}
	wildcard, err := boolEnv(get, "EGRESS_ALLOW_WILDCARD_BIND")
	if err != nil {
		return "", cfg, err
	}
	if !(ip.IsLoopback() || ip.IsPrivate() || wildcard && ip.IsUnspecified()) {
		return "", cfg, errors.New("egress public or wildcard bind denied")
	}
	cfg.Token = get("EGRESS_TOKEN")
	concurrent, err := numberEnv(get, "EGRESS_MAX_CONCURRENT", 32, 1, 128)
	if err != nil {
		return "", cfg, err
	}
	cfg.MaxConcurrent = int(concurrent)
	cfg.MaxTunnelBytes, err = numberEnv(get, "EGRESS_MAX_TUNNEL_BYTES", 32<<20, 1, 128<<20)
	if err != nil {
		return "", cfg, err
	}
	lifetime, err := numberEnv(get, "EGRESS_MAX_LIFETIME_SECONDS", 35, 1, 60)
	if err != nil {
		return "", cfg, err
	}
	cfg.MaxLifetime = time.Duration(lifetime) * time.Second
	cfg.TestMode, err = boolEnv(get, "EGRESS_TEST_MODE")
	if err != nil {
		return "", cfg, err
	}
	if raw := get("EGRESS_TEST_ALLOWLIST"); raw != "" {
		if !cfg.TestMode || len(raw) > 4096 {
			return "", cfg, errors.New("invalid test-only egress allowlist")
		}
		var hosts map[string]string
		if json.Unmarshal([]byte(raw), &hosts) != nil || len(hosts) == 0 || len(hosts) > 16 {
			return "", cfg, errors.New("invalid test-only egress allowlist")
		}
		cfg.TestAllowlist = make(map[string]netip.Addr, len(hosts))
		for host, value := range hosts {
			ip, err := netip.ParseAddr(value)
			if err != nil {
				return "", cfg, errors.New("invalid test-only egress IP")
			}
			cfg.TestAllowlist[host] = ip
		}
	}
	return addr, cfg, nil
}

func run() error {
	healthcheck := flag.Bool("healthcheck", false, "check local readiness and exit")
	flag.Parse()
	addr, cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return err
	}
	if *healthcheck {
		return health(addr)
	}
	gateway, err := egress.New(cfg)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return errors.New("egress listener unavailable")
	}
	// Bound even unauthenticated clients and incomplete request headers. Tunnel
	// admission has its own stricter cap; the HTTP server bounds pre-CONNECT time.
	listener = netutil.LimitListener(listener, cfg.MaxConcurrent*2+16)
	server := &http.Server{Handler: gateway, ReadHeaderTimeout: 3 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second,
		MaxHeaderBytes: 8192, ErrorLog: log.New(os.Stderr, "egress_http ", 0)}
	finished := make(chan error, 1)
	go func() { finished <- server.Serve(listener) }()
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	log.Print("egress ready")
	var serveErr error
	select {
	case serveErr = <-finished:
	case <-signals.Done():
	}
	drain, cancelDrain := context.WithTimeout(context.Background(), 5*time.Second)
	_ = gateway.Drain(drain)
	cancelDrain()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancelShutdown()
	if server.Shutdown(shutdown) != nil {
		_ = server.Close()
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		return errors.New("egress HTTP server stopped")
	}
	return nil
}

func health(addr string) error {
	host, port, _ := net.SplitHostPort(addr)
	ip, _ := netip.ParseAddr(host)
	if ip.IsUnspecified() {
		host = "127.0.0.1"
		if ip.Is6() {
			host = "::1"
		}
	}
	client := http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil}}
	defer client.CloseIdleConnections()
	response, err := client.Get("http://" + net.JoinHostPort(host, port) + "/health/ready")
	if err != nil {
		return errors.New("egress health unavailable")
	}
	response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return errors.New("egress not ready")
	}
	return nil
}
