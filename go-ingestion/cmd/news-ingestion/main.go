package main

import (
	"context"
	"github.com/Dzarlax-AI/evening-news/go-ingestion/runner"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := runner.ConfigFromEnv()
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runner.Run(ctx, cfg, nil); err != nil {
		log.Print("article ingestion listener failed")
		os.Exit(1)
	}
}
