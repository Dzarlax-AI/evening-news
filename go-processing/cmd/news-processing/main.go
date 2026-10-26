package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/processing"
	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
func run() error {
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	token := strings.TrimSpace(os.Getenv("ARTICLE_PROCESSING_TOKEN"))
	aiURL := strings.TrimSpace(os.Getenv("ARTICLE_AI_URL"))
	aiToken := strings.TrimSpace(os.Getenv("ARTICLE_AI_TOKEN"))
	extractURL := strings.TrimSpace(os.Getenv("EXTRACTION_SERVICE_URL"))
	extractToken := strings.TrimSpace(os.Getenv("EXTRACTION_SERVICE_TOKEN"))
	if dsn == "" || len(token) < 16 || len(aiToken) < 16 || !validBaseURL(aiURL) || !validBaseURL(extractURL) || len(extractToken) < 16 {
		return errors.New("required article processing configuration is missing")
	}
	maxConns := int32(envInt("ARTICLE_PROCESSING_DB_MAX_CONNS", 5, 1, 5))
	workers := envInt("MAX_WORKERS", 5, 1, 32)
	batch := envInt("PROCESSING_BATCH_SIZE", 25, 1, 100)
	attempts := envInt("PROCESSING_MAX_ATTEMPTS", 3, 1, 10)
	timeout := envInt("PROCESSING_ATTEMPT_TIMEOUT_SECONDS", 600, 1, 3600)
	retry1 := envInt("PROCESSING_RETRY_INITIAL_SECONDS", 1800, 1, 604800)
	retryLater := envInt("PROCESSING_RETRY_LATER_SECONDS", 21600, 1, 604800)
	textLimit := envInt("EXTRACTION_TEXT_MAX_CHARS", 100000, 100, 100000)
	cycleMax, perSource := 0, 0
	var dateFrom, dateTo *time.Time
	if envBool("NEWS_LIMIT_ENABLED", false) {
		cycleMax = envInt("NEWS_LIMIT_MAX_ARTICLES", 50, 1, 100000)
		perSource = envInt("NEWS_LIMIT_PER_SOURCE", 50, 1, 100000)
		days := envInt("NEWS_LIMIT_DAYS", 1, 0, 36500)
		if days > 0 {
			v := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
			dateFrom = &v
		}
		if raw := os.Getenv("NEWS_LIMIT_NEWEST_DATE"); raw != "" {
			if v, e := time.Parse("2006-01-02", raw); e == nil {
				dateTo = &v
			}
		}
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	db, err := store.Open(ctx, dsn, maxConns)
	if err != nil {
		return err
	}
	closePool := true
	defer func() {
		if closePool {
			db.Close()
		}
	}()
	ai := processing.NewAIClient(aiURL, aiToken)
	extractor := processing.NewExtractorClient(extractURL, extractToken)
	extractor.MaxBytes = envInt("EXTRACTION_HTTP_MAX_BYTES", 5*1024*1024, 1024, 5*1024*1024)
	extractor.BrowserMaxBytes = envInt("EXTRACTION_BROWSER_MAX_BYTES", 5*1024*1024, 1024, 5*1024*1024)
	extractor.MaxChars = textLimit
	extractor.UseBrowser = envBool("EXTRACTION_SERVICE_NATIVE_BROWSER", false)
	extractor.AttemptTimeout = time.Duration(envInt("EXTRACTION_SERVICE_TIMEOUT_SECONDS", 30, 1, 30)) * time.Second
	extractor.TotalBudget = time.Duration(envInt("EXTRACTION_SERVICE_TOTAL_BUDGET_SECONDS", 120, 1, 600)) * time.Second
	proc := &processing.Processor{DB: db, AI: ai, Extractor: extractor, MaxWorkers: workers, MaxBatch: batch, MaxAttempts: attempts, AttemptTimeout: time.Duration(timeout) * time.Second, RetryInitial: time.Duration(retry1) * time.Second, RetryLater: time.Duration(retryLater) * time.Second, TextLimit: textLimit, CycleMax: cycleMax, PerSourceLimit: perSource, DateFrom: dateFrom, DateTo: dateTo, Duplicates: processing.NewDuplicateCache()}
	handler := processing.NewHandlerWithContext(ctx, db, proc, processing.Config{Token: token})
	server := &http.Server{Addr: env("ARTICLE_PROCESSING_LISTEN", ":8094"), Handler: handler.Routes(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = server.Shutdown(shutdown)
	}()
	listenErr := server.ListenAndServe()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := server.Shutdown(shutdownCtx)
	shutdownCancel()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 15*time.Second)
	drainErr := handler.Close(drainCtx)
	drainCancel()
	if drainErr != nil {
		log.Printf("article processing shutdown failed phase=drain error_code=shutdown_deadline")
		// The process is exiting; avoid blocking here on a pool whose users did
		// not drain within the bounded grace period. Process termination closes
		// remaining sockets, and active writes remain protected by DB fencing.
		closePool = false
		return errors.New("article processing shutdown drain exceeded")
	}
	if shutdownErr != nil {
		return errors.New("article processing HTTP shutdown failed")
	}
	if listenErr != nil && !errors.Is(listenErr, http.ErrServerClosed) {
		return errors.New("article processing server failed")
	}
	return nil
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func envInt(key string, fallback, min, max int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		log.Fatalf("invalid %s", key)
	}
	return n
}
func validBaseURL(raw string) bool {
	u, err := url.Parse(raw)
	return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}

func envBool(key string, fallback bool) bool {
	raw := os.Getenv(key)
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		log.Fatalf("invalid %s", key)
	}
	return v
}
