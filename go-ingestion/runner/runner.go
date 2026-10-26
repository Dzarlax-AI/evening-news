package runner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-ingestion/internal/ingest"
	"github.com/Dzarlax-AI/evening-news/go-ingestion/internal/store"
)

var ErrStaleFence = store.ErrStaleFence
var ErrRunConflict = store.ErrRunConflict

var ErrInvalidConfig = errors.New("component configuration is invalid")

type Config struct {
	DatabaseURL    string
	ListenAddr     string
	Token          string
	ExtractorURL   string
	ExtractorToken string
	SelectorURL    string
	SelectorToken  string
	NativeBrowser  bool
	DBMaxConns     int32
}

func LoadConfig(getenv func(string) string) (Config, error) {
	get := func(k, d string) string {
		if v := strings.TrimSpace(getenv(k)); v != "" {
			return v
		}
		return d
	}
	c := Config{
		DatabaseURL:    strings.TrimSpace(getenv("DATABASE_URL")),
		ListenAddr:     get("ARTICLE_INGESTION_LISTEN", ":8093"),
		Token:          strings.TrimSpace(getenv("ARTICLE_INGESTION_TOKEN")),
		ExtractorURL:   strings.TrimSpace(getenv("EXTRACTION_SERVICE_URL")),
		ExtractorToken: strings.TrimSpace(getenv("EXTRACTION_SERVICE_TOKEN")),
		SelectorURL:    get("JOB_EXECUTOR_URL", "http://127.0.0.1:8100"),
		SelectorToken:  strings.TrimSpace(getenv("JOB_EXECUTOR_TOKEN")),
		DBMaxConns:     5,
	}
	if v := strings.TrimSpace(getenv("ARTICLE_INGESTION_DB_MAX_CONNS")); v != "" {
		n, e := strconv.Atoi(v)
		if e != nil || n < 1 || n > 5 {
			return c, fmt.Errorf("%w: ARTICLE_INGESTION_DB_MAX_CONNS must be from 1 through 5", ErrInvalidConfig)
		}
		c.DBMaxConns = int32(n)
	}
	v := strings.TrimSpace(getenv("EXTRACTION_SERVICE_NATIVE_BROWSER"))
	if v != "" {
		b, e := strconv.ParseBool(v)
		if e != nil {
			return c, fmt.Errorf("%w: EXTRACTION_SERVICE_NATIVE_BROWSER must be true or false", ErrInvalidConfig)
		}
		c.NativeBrowser = b
	}
	if c.DatabaseURL == "" || len(c.Token) < 16 || c.ExtractorURL == "" || len(c.ExtractorToken) < 16 {
		return c, fmt.Errorf("%w: required article ingestion configuration is missing", ErrInvalidConfig)
	}
	return c, nil
}
func ConfigFromEnv() (Config, error) { return LoadConfig(os.Getenv) }
func Healthcheck(ctx context.Context, c Config) error {
	db, e := store.Open(ctx, c.DatabaseURL, c.DBMaxConns)
	if e != nil {
		return e
	}
	defer db.Close()
	return db.Pool.Ping(ctx)
}
func Run(ctx context.Context, c Config, onReady func()) error {
	return RunWithHealth(ctx, c, onReady, nil)
}

// RunWithHealth reports database availability from the listener's existing
// pool. Health checks are bounded and do not allocate a second pool.
func RunWithHealth(ctx context.Context, c Config, onReady func(), onDatabaseHealth func(time.Time)) error {
	if ctx == nil {
		return errors.New("ingestion context is required")
	}
	if c.DBMaxConns < 1 || c.ListenAddr == "" || c.Token == "" || c.ExtractorURL == "" || c.ExtractorToken == "" {
		return ErrInvalidConfig
	}
	if ctx.Err() != nil {
		return nil
	}
	db, e := store.Open(ctx, c.DatabaseURL, c.DBMaxConns)
	if e != nil {
		if ctx.Err() != nil && errors.Is(e, context.Canceled) {
			return nil
		}
		return e
	}
	h := ingest.NewHandlerWithContext(ctx, db, ingest.Config{
		Token:                  c.Token,
		ExtractorURL:           c.ExtractorURL,
		ExtractorToken:         c.ExtractorToken,
		ExtractorNativeBrowser: c.NativeBrowser,
		SelectorURL:            c.SelectorURL,
		SelectorToken:          c.SelectorToken,
	})
	srv := &http.Server{
		Handler:           h.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	ln, e := net.Listen("tcp", c.ListenAddr)
	if e != nil {
		db.Close()
		return e
	}
	if ctx.Err() != nil {
		_ = ln.Close()
		db.Close()
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	if onReady != nil {
		onReady()
	}
	var healthDone chan struct{}
	var cancelHealth context.CancelFunc
	if onDatabaseHealth != nil {
		onDatabaseHealth(time.Now().UTC()) // store.Open performed a pool Ping before serving.
		healthCtx, cancel := context.WithCancel(ctx)
		cancelHealth = cancel
		healthDone = make(chan struct{})
		go func() {
			defer close(healthDone)
			watchPoolHealth(healthCtx, 5*time.Second, 2*time.Second, db.Pool.Ping, onDatabaseHealth)
		}()
	}

	var serveErr error
	var shutdownErr error
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		shutdownErr = srv.Shutdown(shutdownCtx)
		cancel()
		if shutdownErr != nil {
			_ = srv.Close()
		}
		serveErr = <-done
	case serveErr = <-done:
		if !errors.Is(serveErr, http.ErrServerClosed) {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			shutdownErr = srv.Shutdown(shutdownCtx)
			cancel()
			if shutdownErr != nil {
				_ = srv.Close()
			}
		}
	}

	drainCtx, drainCancel := context.WithTimeout(context.Background(), 15*time.Second)
	drainErr := h.Close(drainCtx)
	drainCancel()
	if drainErr != nil {
		stopHealth(cancelHealth, healthDone)
		return fmt.Errorf("article ingestion shutdown drain exceeded: %w", drainErr)
	}
	if shutdownErr != nil {
		stopHealth(cancelHealth, healthDone)
		return fmt.Errorf("article ingestion HTTP shutdown failed: %w", shutdownErr)
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		stopHealth(cancelHealth, healthDone)
		return errors.New("article ingestion listener failed")
	}
	stopHealth(cancelHealth, healthDone)
	db.Close()
	return nil
}

func stopHealth(cancel context.CancelFunc, done <-chan struct{}) {
	if cancel == nil {
		return
	}
	cancel()
	<-done
}

func watchPoolHealth(ctx context.Context, interval, timeout time.Duration, ping func(context.Context) error, onHealth func(time.Time)) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			checkCtx, cancel := context.WithTimeout(ctx, timeout)
			err := ping(checkCtx)
			cancel()
			if err == nil {
				onHealth(time.Now().UTC())
			}
		}
	}
}
