package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-worker/internal/config"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/newsadapter"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/store"
	"github.com/Dzarlax-AI/evening-news/go-worker/internal/worker"
)

func main() {
	healthcheck := flag.Bool("healthcheck", false, "check worker database and lease health")
	flag.Parse()
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg, err := config.Load()
	if err != nil {
		logger.Error("worker configuration is invalid", "error", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	db, err := store.Open(ctx, cfg.DatabaseURL, 4)
	cancel()
	if err != nil {
		logger.Error("worker database is unavailable", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.CheckSchema(ctx); err != nil {
		logger.Error("worker schema is unavailable", "error", err)
		os.Exit(1)
	}
	if *healthcheck {
		if err := db.CheckWorkerHealth(ctx); err != nil {
			logger.Error("worker is unhealthy", "error", err)
			os.Exit(1)
		}
		return
	}

	workerCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	instance, err := worker.New(cfg, db, newsadapter.Adapter{}, logger)
	if err != nil {
		logger.Error("worker startup failed", "error", err)
		os.Exit(1)
	}
	if err := instance.Run(workerCtx); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("worker stopped", "error", err)
		if errors.Is(err, store.ErrLeaseHeld) {
			os.Exit(2)
		}
		os.Exit(1)
	}
	fmt.Fprintln(os.Stdout, "worker stopped")
}
