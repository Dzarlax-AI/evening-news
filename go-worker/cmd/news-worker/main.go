package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/Dzarlax-AI/evening-news/go-worker/runner"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	health := flag.Bool("healthcheck", false, "check worker database and lease health")
	flag.Parse()
	cfg, err := runner.ConfigFromEnv()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err != nil {
		logger.Error("worker configuration is invalid", "error", err)
		os.Exit(1)
	}
	if *health {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runner.Healthcheck(ctx, cfg); err != nil {
			logger.Error("worker is unhealthy", "error", err)
			os.Exit(1)
		}
		return
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runner.Run(ctx, cfg, nil); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker stopped", "error", err)
		if errors.Is(err, runner.ErrLeaseHeld) {
			os.Exit(2)
		}
		os.Exit(1)
	}
	fmt.Fprintln(os.Stdout, "worker stopped")
}
