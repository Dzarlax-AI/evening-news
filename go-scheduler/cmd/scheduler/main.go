package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/api"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/config"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/dispatch"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/store"
)

var buildID = "development"

func main() {
	if len(os.Args) == 2 && os.Args[1] == "healthcheck" {
		if err := healthcheck(); err != nil {
			log.Print("scheduler healthcheck failed")
			os.Exit(1)
		}
		return
	}
	if len(os.Args) == 2 && os.Args[1] == "serve" {
		if err := run(); err != nil {
			logRunFailure(err)
			os.Exit(1)
		}
		return
	}
	if len(os.Args) != 1 {
		log.Print("unknown command")
		os.Exit(2)
	}
	if err := run(); err != nil {
		logRunFailure(err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return &store.InitError{Phase: "config"}
	}
	cfg.BuildID = buildID
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	dbCtx, dbCancel := context.WithTimeout(ctx, 15*time.Second)
	db, err := store.Open(dbCtx, cfg.DatabaseURL, cfg.Schema, cfg.DBMaxConns, cfg.StartPaused)
	dbCancel()
	if err != nil {
		return err
	}
	defer db.Close()
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	apiServer := &api.API{Store: db, Token: cfg.APIToken, Targets: cfg.Targets, PollEvery: cfg.PollEvery, BuildID: cfg.BuildID}
	dispatcher := dispatch.New(db, cfg.Targets, cfg.HTTPTimeout, cfg.MaxInflight)
	httpServer := &http.Server{
		Addr: cfg.ListenAddr, Handler: apiServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 30 * time.Second,
		IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10,
	}
	serveErr := make(chan error, 1)
	go func() {
		err := httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()
	var loops sync.WaitGroup
	loops.Add(2)
	go func() { defer loops.Done(); runScheduleLoop(workerCtx, db, apiServer, cfg.PollEvery, cfg.MaxOutbox) }()
	go func() { defer loops.Done(); runDispatchLoop(workerCtx, dispatcher, cfg.PollEvery) }()
	select {
	case <-ctx.Done():
		cancelWorkers()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := httpServer.Shutdown(shutdownCtx)
		waitForLoops(&loops, 7*time.Second)
		return err
	case <-serveErr:
		cancelWorkers()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		waitForLoops(&loops, 7*time.Second)
		return errors.New("scheduler HTTP server failed")
	}
}

func logRunFailure(err error) {
	var initErr *store.InitError
	if errors.As(err, &initErr) {
		log.Print(initErr.Error())
		return
	}
	log.Print("scheduler stopped with an error")
}

func runScheduleLoop(ctx context.Context, db *store.Store, apiServer *api.API, interval time.Duration, maxOutbox int) {
	tick := func() {
		now := time.Now().UTC().Truncate(time.Microsecond)
		tickCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, err := db.GenerateDue(tickCtx, now, 100, maxOutbox); err != nil {
			log.Print("scheduler database cycle unavailable")
			return
		}
		apiServer.LastTickNS.Store(time.Now().UnixNano())
	}
	tick()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tick()
		}
	}
}

func waitForLoops(loops *sync.WaitGroup, timeout time.Duration) {
	done := make(chan struct{})
	go func() { loops.Wait(); close(done) }()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		log.Print("scheduler worker shutdown exceeded its bound")
	}
}

func runDispatchLoop(ctx context.Context, dispatcher *dispatch.Dispatcher, interval time.Duration) {
	cycle := func() {
		if err := dispatcher.Cycle(ctx, time.Now().UTC().Truncate(time.Microsecond)); err != nil {
			log.Print("scheduler dispatch cycle unavailable")
		}
	}
	cycle()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cycle()
		}
	}
}

func healthcheck() error {
	addr := strings.TrimSpace(os.Getenv("LISTEN_ADDR"))
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid listen address")
	}
	if host == "" {
		host = "127.0.0.1"
	}
	client := http.Client{Timeout: 1500 * time.Millisecond}
	resp, err := client.Get("http://" + net.JoinHostPort(host, port) + "/readyz")
	if err != nil {
		return errors.New("scheduler is not ready")
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return errors.New("scheduler is not ready")
	}
	return nil
}
