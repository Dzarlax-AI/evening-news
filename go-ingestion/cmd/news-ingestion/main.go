package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-ingestion/internal/ingest"
	"github.com/Dzarlax-AI/evening-news/go-ingestion/internal/store"
)

func main() {
	addr := env("ARTICLE_INGESTION_LISTEN", ":8093")
	dsn := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	token := strings.TrimSpace(os.Getenv("ARTICLE_INGESTION_TOKEN"))
	extractorURL := strings.TrimSpace(os.Getenv("EXTRACTION_SERVICE_URL"))
	extractorToken := strings.TrimSpace(os.Getenv("EXTRACTION_SERVICE_TOKEN"))
	nativeBrowser, err := strconv.ParseBool(env("EXTRACTION_SERVICE_NATIVE_BROWSER", "false"))
	if err != nil {
		log.Fatal("EXTRACTION_SERVICE_NATIVE_BROWSER must be true or false")
	}
	if dsn == "" || len(token) < 16 || extractorURL == "" || len(extractorToken) < 16 {
		log.Fatal("required article ingestion configuration is missing")
	}
	maxConns := int32(envInt("ARTICLE_INGESTION_DB_MAX_CONNS", 5, 1, 5))
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	db, err := store.Open(ctx, dsn, maxConns)
	if err != nil {
		log.Fatal("article ingestion database is unavailable")
	}
	defer db.Close()
	handler := ingest.NewHandler(db, ingest.Config{
		Token:                  token,
		ExtractorURL:           extractorURL,
		ExtractorToken:         extractorToken,
		ExtractorNativeBrowser: nativeBrowser,
		SelectorURL:            env("JOB_EXECUTOR_URL", "http://127.0.0.1:8100"),
		SelectorToken:          strings.TrimSpace(os.Getenv("JOB_EXECUTOR_TOKEN")),
	})
	srv := &http.Server{Addr: addr, Handler: handler.Routes(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, stop := context.WithTimeout(context.Background(), 10*time.Second)
		defer stop()
		_ = srv.Shutdown(shutdown)
	}()
	log.Printf("private article ingestion listener starting on %s", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatal("article ingestion listener failed")
	}
}

func env(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}
func envInt(key string, fallback, min, max int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < min || value > max {
		return fallback
	}
	return value
}
