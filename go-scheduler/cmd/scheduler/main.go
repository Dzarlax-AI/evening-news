package main

import (
	"context"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/runner"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var buildID = "development"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if runner.Healthcheck(ctx, os.Getenv("LISTEN_ADDR")) != nil {
			log.Print("scheduler healthcheck failed")
			os.Exit(1)
		}
		return
	}
	if len(os.Args) > 2 || (len(os.Args) == 2 && os.Args[1] != "serve") {
		log.Print("unknown command")
		os.Exit(2)
	}
	cfg, err := runner.ConfigFromEnv()
	if err != nil {
		log.Print("scheduler initialization failed phase=config")
		os.Exit(1)
	}
	if cfg.BuildID == "" || cfg.BuildID == "development" {
		cfg.BuildID = buildID
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runner.Run(ctx, cfg, nil); err != nil {
		log.Print("scheduler stopped with an error")
		os.Exit(1)
	}
}
