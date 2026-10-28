// Command cloak-fixture runs isolated native browser acceptance fixtures.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-tools/cloakfixture"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "fixture_mode_required")
		os.Exit(2)
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "fixture_failed")
		os.Exit(1)
	}
}
func run(mode string, args []string) error {
	if mode == "netprobe" {
		f := flag.NewFlagSet("netprobe", flag.ContinueOnError)
		target := f.String("target", "", "synthetic blocked destination")
		if err := f.Parse(args); err != nil {
			return err
		}
		host, port, err := net.SplitHostPort(*target)
		if err != nil || net.ParseIP(host) == nil || (port != "80" && !(host == "127.0.0.1" && port == "5100")) || f.NArg() != 0 {
			return fmt.Errorf("fixture_arguments_invalid")
		}
		c, err := net.DialTimeout("tcp", *target, 400*time.Millisecond)
		if err == nil {
			c.Close()
			return fmt.Errorf("namespace_bypass_gate_failed")
		}
		if errors.Is(err, syscall.ENETUNREACH) {
			fmt.Println("network_unreachable")
		} else {
			fmt.Println("blocked")
		}
		return nil
	}
	if mode == "gate" {
		f := flag.NewFlagSet("gate", flag.ContinueOnError)
		worker := f.String("worker-binary", "", "native worker executable")
		egress := f.String("egress-binary", "", "egress executable")
		fixture := f.String("fixture-binary", "", "Linux fixture executable")
		relay := f.String("relay-script", "", "protected relay script")
		x11 := f.String("x11-script", "", "protected X11 shim")
		output := f.String("output", "", "new private receipt")
		stage := f.String("stage-parent", os.TempDir(), "private stage parent")
		if err := f.Parse(args); err != nil {
			return err
		}
		if *worker == "" || *egress == "" || *fixture == "" || *relay == "" || *x11 == "" || *output == "" || f.NArg() != 0 {
			return fmt.Errorf("fixture_arguments_invalid")
		}
		ctx, stop := gateSignalContext()
		defer stop()
		return cloakfixture.RunGate(ctx, cloakfixture.GateConfig{Worker: *worker, Egress: *egress, Fixture: *fixture, Relay: *relay, X11: *x11, Output: *output, StageParent: *stage})
	}
	if mode == "probe" {
		f := flag.NewFlagSet("probe", flag.ContinueOnError)
		cert := f.String("cert", "", "fixture CA")
		if err := f.Parse(args); err != nil || *cert == "" || f.NArg() != 0 {
			return fmt.Errorf("fixture_arguments_invalid")
		}
		b, err := os.ReadFile(*cert)
		if err != nil || len(b) > 65536 {
			return fmt.Errorf("fixture_probe_failed")
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(b) {
			return fmt.Errorf("fixture_probe_failed")
		}
		client := http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{Proxy: nil, TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}}, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		response, err := client.Get("https://127.0.0.1/healthz")
		if err != nil {
			return fmt.Errorf("fixture_probe_failed")
		}
		defer response.Body.Close()
		if response.StatusCode != 200 {
			return fmt.Errorf("fixture_probe_failed")
		}
		return nil
	}
	if mode == "client" {
		f := flag.NewFlagSet("client", flag.ContinueOnError)
		worker := f.String("worker", "", "worker HTTP address")
		browser := f.String("browser", "", "browser CDP address")
		proxy := f.String("proxy", "", "browser proxy address")
		tokenFile := f.String("token-file", "", "private token file")
		state := f.String("state-dir", "", "fixture state directory")
		commands := f.String("command-dir", "", "private command directory")
		results := f.String("result-dir", "", "private result directory")
		if err := f.Parse(args); err != nil {
			return err
		}
		if *worker == "" || *browser == "" || *proxy == "" || *tokenFile == "" || *state == "" || *commands == "" || *results == "" || f.NArg() != 0 {
			return fmt.Errorf("fixture_arguments_invalid")
		}
		token, err := os.ReadFile(*tokenFile)
		if err != nil || len(token) > 4096 {
			return fmt.Errorf("fixture_token_invalid")
		}
		client, err := cloakfixture.NewWorkerClient(*worker, strings.TrimSpace(string(token)))
		if err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 240*time.Second)
		defer cancel()
		var peer *cloakfixture.CDPPeer
		for deadline := time.Now().Add(25 * time.Second); time.Now().Before(deadline); {
			peer, err = cloakfixture.NewPeer(ctx, *browser, *proxy)
			if err == nil {
				break
			}
			time.Sleep(250 * time.Millisecond)
		}
		if err != nil {
			return err
		}
		return cloakfixture.RunObserver(ctx, &cloakfixture.Profiles{Worker: client, StateDir: *state, Peer: peer}, peer, *commands, *results)
	}
	if mode != "origin" {
		return fmt.Errorf("fixture_mode_invalid")
	}
	f := flag.NewFlagSet("origin", flag.ContinueOnError)
	f.SetOutput(os.Stderr)
	cert := f.String("cert", "", "TLS certificate")
	key := f.String("key", "", "TLS key")
	state := f.String("state-dir", "", "fixture state directory")
	addr := f.String("listen", ":443", "fixture listen address")
	if err := f.Parse(args); err != nil {
		return err
	}
	if *cert == "" || *key == "" || *state == "" || f.NArg() != 0 {
		return fmt.Errorf("fixture_arguments_invalid")
	}
	o, err := cloakfixture.NewOrigin(*state)
	if err != nil {
		return err
	}
	server := &http.Server{Addr: *addr, Handler: o, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 5 * time.Second, MaxHeaderBytes: 16384}
	return server.ListenAndServeTLS(*cert, *key)
}

func gateSignalContext() (context.Context, context.CancelFunc) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}
