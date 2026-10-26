package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-background/internal/config"
	background "github.com/Dzarlax-AI/evening-news/go-background/internal/runtime"
)

var buildID = "development"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) > 1 {
		return errors.New("usage: news-background [healthcheck]")
	}
	if len(args) == 1 {
		if args[0] != "healthcheck" {
			return errors.New("usage: news-background [healthcheck]")
		}
		addr := strings.TrimSpace(os.Getenv("BACKGROUND_HEALTH_LISTEN"))
		if addr == "" {
			addr = ":8095"
		}
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := Healthcheck(ctx, addr); err != nil {
			return errors.New("background readiness check failed")
		}
		return nil
	}
	cfg, err := config.Load(os.Getenv, buildID)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := background.Run(ctx, cfg); err != nil {
		return err
	}
	return nil
}

// Healthcheck probes only the local runtime endpoint; it does not load component
// credentials or database settings.
func Healthcheck(ctx context.Context, addr string) error {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return errors.New("invalid health address")
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		host = "127.0.0.1"
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, port)+"/readyz", nil)
	if err != nil {
		return errors.New("health endpoint unavailable")
	}
	client := http.Client{Timeout: 1500 * time.Millisecond}
	resp, err := client.Do(req)
	if err != nil {
		return errors.New("health endpoint unavailable")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("background is not ready")
	}
	return nil
}
