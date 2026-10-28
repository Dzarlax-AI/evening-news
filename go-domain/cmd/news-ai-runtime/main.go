package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/Dzarlax-AI/evening-news/go-domain/airuntime"
)

func main() {
	config, err := airuntime.ConfigFromEnv()
	if err != nil {
		log.Fatal("ai-runtime configuration invalid")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	db, err := airuntime.OpenDatabase(ctx, config)
	if err != nil {
		log.Fatal("ai-runtime startup dependency unavailable")
	}
	server := airuntime.NewServer(config, db)
	if err = airuntime.ServeUnix(ctx, server); err != nil {
		log.Fatal("ai-runtime stopped with error")
	}
}
