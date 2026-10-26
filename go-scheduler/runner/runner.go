package runner

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
	_ "time/tzdata"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/api"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/config"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/dispatch"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/store"
)

var ErrLeaseLost = store.ErrLeaseLost

type InitError = store.InitError

type Target struct {
	URL   string
	Token string
}

var ErrInvalidConfig = errors.New("component configuration is invalid")

type Config struct {
	DatabaseURL string
	Schema      string
	APIToken    string
	ListenAddr  string
	Targets     map[string]Target
	DBMaxConns  int32
	MaxInflight int
	MaxOutbox   int
	PollEvery   time.Duration
	HTTPTimeout time.Duration
	StartPaused bool
	BuildID     string
}

func LoadConfig(getenv func(string) string) (Config, error) {
	c, e := config.LoadFrom(getenv)
	if e != nil {
		return Config{}, fmt.Errorf("%w: %s", ErrInvalidConfig, e)
	}
	out := Config{
		DatabaseURL: c.DatabaseURL,
		Schema:      c.Schema,
		APIToken:    c.APIToken,
		ListenAddr:  c.ListenAddr,
		DBMaxConns:  c.DBMaxConns,
		MaxInflight: c.MaxInflight,
		MaxOutbox:   c.MaxOutbox,
		PollEvery:   c.PollEvery,
		HTTPTimeout: c.HTTPTimeout,
		StartPaused: c.StartPaused,
		BuildID:     c.BuildID,
		Targets:     map[string]Target{},
	}
	for k, v := range c.Targets {
		out.Targets[k] = Target{URL: v.URL, Token: v.Token}
	}
	return out, nil
}
func ConfigFromEnv() (Config, error) { return LoadConfig(os.Getenv) }
func Healthcheck(ctx context.Context, addr string) error {
	if addr == "" {
		addr = ":8080"
	}
	host, port, e := net.SplitHostPort(addr)
	if e != nil {
		return errors.New("invalid listen address")
	}
	if host == "" {
		host = "127.0.0.1"
	}
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+net.JoinHostPort(host, port)+"/readyz", nil)
	if e != nil {
		return errors.New("scheduler is not ready")
	}
	client := http.Client{Timeout: 1500 * time.Millisecond}
	r, e := client.Do(req)
	if e != nil {
		return errors.New("scheduler is not ready")
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		return errors.New("scheduler is not ready")
	}
	return nil
}
func Run(ctx context.Context, c Config, onReady func()) error {
	if ctx == nil {
		return errors.New("scheduler context is required")
	}
	if c.DBMaxConns < 1 || c.PollEvery <= 0 || c.HTTPTimeout <= 0 || c.MaxInflight < 1 || c.MaxOutbox < 1 || c.ListenAddr == "" || c.Schema == "" || c.APIToken == "" || len(c.Targets) == 0 {
		return ErrInvalidConfig
	}
	if ctx.Err() != nil {
		return nil
	}
	if strings.TrimSpace(c.BuildID) == "" {
		c.BuildID = "development"
	}
	targets := map[string]model.Target{}
	for k, v := range c.Targets {
		targets[k] = model.Target{URL: v.URL, Token: v.Token}
	}
	dbctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	db, e := store.Open(dbctx, c.DatabaseURL, c.Schema, c.DBMaxConns, c.StartPaused)
	cancel()
	if e != nil {
		if ctx.Err() != nil && errors.Is(e, context.Canceled) {
			return nil
		}
		return e
	}
	defer db.Close()
	apiServer := &api.API{
		Store:     db,
		Token:     c.APIToken,
		Targets:   targets,
		PollEvery: c.PollEvery,
		BuildID:   c.BuildID,
	}
	d := dispatch.New(db, targets, c.HTTPTimeout, c.MaxInflight)
	srv := &http.Server{
		Addr:              c.ListenAddr,
		Handler:           apiServer.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
	ln, e := net.Listen("tcp", c.ListenAddr)
	if e != nil {
		return e
	}
	if ctx.Err() != nil {
		_ = ln.Close()
		return nil
	}
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()
	var loops sync.WaitGroup
	loops.Add(2)
	go func() { defer loops.Done(); scheduleLoop(workerCtx, db, apiServer, c.PollEvery, c.MaxOutbox) }()
	go func() { defer loops.Done(); dispatchLoop(workerCtx, d, c.PollEvery) }()
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	if onReady != nil {
		onReady()
	}
	select {
	case <-ctx.Done():
		cancelWorkers()
		sctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		e = srv.Shutdown(sctx)
		stop()
		wait(&loops, 7*time.Second)
		if e != nil {
			return e
		}
		return nil
	case e = <-done:
		cancelWorkers()
		sctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
		_ = srv.Shutdown(sctx)
		stop()
		wait(&loops, 7*time.Second)
		if errors.Is(e, http.ErrServerClosed) {
			return nil
		}
		return errors.New("scheduler HTTP server failed")
	}
}
func scheduleLoop(ctx context.Context, db *store.Store, a *api.API, interval time.Duration, max int) {
	tick := func() {
		now := time.Now().UTC().Truncate(time.Microsecond)
		c, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if _, e := db.GenerateDue(c, now, 100, max); e != nil {
			log.Print("scheduler database cycle unavailable")
			return
		}
		a.LastTickNS.Store(time.Now().UnixNano())
	}
	tick()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			tick()
		}
	}
}
func dispatchLoop(ctx context.Context, d *dispatch.Dispatcher, interval time.Duration) {
	cycle := func() {
		if e := d.Cycle(ctx, time.Now().UTC().Truncate(time.Microsecond)); e != nil {
			log.Print("scheduler dispatch cycle unavailable")
		}
	}
	cycle()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			cycle()
		}
	}
}
func wait(w *sync.WaitGroup, bound time.Duration) {
	done := make(chan struct{})
	go func() { w.Wait(); close(done) }()
	t := time.NewTimer(bound)
	defer t.Stop()
	select {
	case <-done:
	case <-t.C:
		log.Print("scheduler worker shutdown exceeded its bound")
	}
}
