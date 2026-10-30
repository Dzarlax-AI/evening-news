package runner

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-processing/internal/processing"
	"github.com/Dzarlax-AI/evening-news/go-processing/internal/store"
)

var ErrStaleFence = store.ErrStaleFence
var ErrRunConflict = store.ErrRunConflict

var ErrInvalidConfig = errors.New("component configuration is invalid")

type Config struct {
	DatabaseURL             string
	Token                   string
	AIURL                   string
	AIToken                 string
	ExtractorURL            string
	ExtractorToken          string
	ListenAddr              string
	DBMaxConns              int32
	MaxWorkers              int
	MaxBatch                int
	MaxAttempts             int
	AttemptTimeoutSeconds   int
	RetryInitialSeconds     int
	RetryLaterSeconds       int
	TextLimit               int
	MaxBytes                int
	BrowserMaxBytes         int
	ExtractorTimeoutSeconds int
	TotalBudgetSeconds      int
	CycleMax                int
	PerSourceLimit          int
	NativeBrowser           bool
	DateFrom                *time.Time
	DateTo                  *time.Time
	LoadOperationalSnapshot func(context.Context, string, int64) (OperationalSnapshot, error)
}

type OperationalSnapshot struct {
	Revision          int64
	PinnedAt          time.Time
	LimitsEnabled     bool
	MaxArticles       int
	PerSource         int
	Days              int
	NewestDate        string
	ProcessingEnabled bool
	DeliveryEnabled   bool
	SerbianEnabled    bool
}

type OperationalSnapshotReader func(context.Context, string, int64) (OperationalSnapshot, error)

func LoadConfig(env func(string) string) (Config, error) {
	get := func(k string, d int, lo, hi int) (int, error) {
		raw := strings.TrimSpace(env(k))
		if raw == "" {
			return d, nil
		}
		n, e := strconv.Atoi(raw)
		if e != nil || n < lo || n > hi {
			return 0, fmt.Errorf("%w: invalid %s", ErrInvalidConfig, k)
		}
		return n, nil
	}
	boolean := func(k string) (bool, error) {
		raw := strings.TrimSpace(env(k))
		if raw == "" {
			return false, nil
		}
		v, e := strconv.ParseBool(raw)
		if e != nil {
			return false, fmt.Errorf("%w: invalid %s", ErrInvalidConfig, k)
		}
		return v, nil
	}
	c := Config{
		DatabaseURL:    strings.TrimSpace(env("DATABASE_URL")),
		Token:          strings.TrimSpace(env("ARTICLE_PROCESSING_TOKEN")),
		AIURL:          strings.TrimSpace(env("ARTICLE_AI_URL")),
		AIToken:        strings.TrimSpace(env("ARTICLE_AI_TOKEN")),
		ExtractorURL:   strings.TrimSpace(env("EXTRACTION_SERVICE_URL")),
		ExtractorToken: strings.TrimSpace(env("EXTRACTION_SERVICE_TOKEN")),
		ListenAddr:     strings.TrimSpace(env("ARTICLE_PROCESSING_LISTEN")),
	}
	if c.ListenAddr == "" {
		c.ListenAddr = ":8094"
	}
	if c.DatabaseURL == "" || len(c.Token) < 16 || len(c.AIToken) < 16 || !validURL(c.AIURL) || !validURL(c.ExtractorURL) || len(c.ExtractorToken) < 16 {
		return c, fmt.Errorf("%w: required article processing configuration is missing", ErrInvalidConfig)
	}
	vals := []struct {
		key          string
		defaultValue int
		min          int
		max          int
		destination  *int
	}{
		{"ARTICLE_PROCESSING_DB_MAX_CONNS", 5, 1, 5, nil},
		{"MAX_WORKERS", 5, 1, 32, &c.MaxWorkers},
		{"PROCESSING_BATCH_SIZE", 25, 1, 100, &c.MaxBatch},
		{"PROCESSING_MAX_ATTEMPTS", 3, 1, 10, &c.MaxAttempts},
		{"PROCESSING_ATTEMPT_TIMEOUT_SECONDS", 600, 1, 3600, &c.AttemptTimeoutSeconds},
		{"PROCESSING_RETRY_INITIAL_SECONDS", 1800, 1, 604800, &c.RetryInitialSeconds},
		{"PROCESSING_RETRY_LATER_SECONDS", 21600, 1, 604800, &c.RetryLaterSeconds},
		{"EXTRACTION_TEXT_MAX_CHARS", 100000, 100, 100000, &c.TextLimit},
		{"EXTRACTION_HTTP_MAX_BYTES", 5 * 1024 * 1024, 1024, 5 * 1024 * 1024, &c.MaxBytes},
		{"EXTRACTION_BROWSER_MAX_BYTES", 5 * 1024 * 1024, 1024, 5 * 1024 * 1024, &c.BrowserMaxBytes},
		{"EXTRACTION_SERVICE_TIMEOUT_SECONDS", 30, 1, 30, &c.ExtractorTimeoutSeconds},
		{"EXTRACTION_SERVICE_TOTAL_BUDGET_SECONDS", 120, 1, 600, &c.TotalBudgetSeconds},
	}
	for _, v := range vals {
		n, e := get(v.key, v.defaultValue, v.min, v.max)
		if e != nil {
			return c, e
		}
		if v.destination == nil {
			c.DBMaxConns = int32(n)
		} else {
			*v.destination = n
		}
	}
	b, e := boolean("EXTRACTION_SERVICE_NATIVE_BROWSER")
	if e != nil {
		return c, e
	}
	c.NativeBrowser = b
	b, e = boolean("NEWS_LIMIT_ENABLED")
	if e != nil {
		return c, e
	}
	if b {
		c.CycleMax, e = get("NEWS_LIMIT_MAX_ARTICLES", 50, 1, 100000)
		if e != nil {
			return c, e
		}
		c.PerSourceLimit, e = get("NEWS_LIMIT_PER_SOURCE", 50, 1, 100000)
		if e != nil {
			return c, e
		}
		days, e := get("NEWS_LIMIT_DAYS", 1, 0, 36500)
		if e != nil {
			return c, e
		}
		if days > 0 {
			v := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
			c.DateFrom = &v
		}
		if raw := strings.TrimSpace(env("NEWS_LIMIT_NEWEST_DATE")); raw != "" {
			v, e := time.Parse("2006-01-02", raw)
			if e == nil {
				c.DateTo = &v
			}
		}
	}
	return c, nil
}
func ConfigFromEnv() (Config, error) { return LoadConfig(os.Getenv) }
func validURL(s string) bool {
	u, e := url.Parse(s)
	return e == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Hostname() != "" && u.User == nil && u.RawQuery == "" && u.Fragment == ""
}
func Run(ctx context.Context, c Config, onReady func()) error {
	if ctx == nil {
		return errors.New("processing context is required")
	}
	if c.DBMaxConns < 1 || c.ListenAddr == "" || c.Token == "" || !validURL(c.AIURL) || !validURL(c.ExtractorURL) || c.MaxWorkers < 1 || c.MaxBatch < 1 || c.MaxAttempts < 1 || c.AttemptTimeoutSeconds < 1 || c.RetryInitialSeconds < 1 || c.RetryLaterSeconds < 1 || c.TextLimit < 1 || c.MaxBytes < 1 || c.BrowserMaxBytes < 1 || c.ExtractorTimeoutSeconds < 1 || c.TotalBudgetSeconds < 1 {
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
	keepPool := true
	defer func() {
		if keepPool {
			db.Close()
		}
	}()
	ai := processing.NewAIClient(c.AIURL, c.AIToken)
	ex := processing.NewExtractorClient(c.ExtractorURL, c.ExtractorToken)
	ex.MaxBytes = c.MaxBytes
	ex.BrowserMaxBytes = c.BrowserMaxBytes
	ex.MaxChars = c.TextLimit
	ex.UseBrowser = c.NativeBrowser
	ex.AttemptTimeout = time.Duration(c.ExtractorTimeoutSeconds) * time.Second
	ex.TotalBudget = time.Duration(c.TotalBudgetSeconds) * time.Second
	p := &processing.Processor{
		DB:             db,
		AI:             ai,
		Extractor:      ex,
		MaxWorkers:     c.MaxWorkers,
		MaxBatch:       c.MaxBatch,
		MaxAttempts:    c.MaxAttempts,
		AttemptTimeout: time.Duration(c.AttemptTimeoutSeconds) * time.Second,
		RetryInitial:   time.Duration(c.RetryInitialSeconds) * time.Second,
		RetryLater:     time.Duration(c.RetryLaterSeconds) * time.Second,
		TextLimit:      c.TextLimit,
		CycleMax:       c.CycleMax,
		PerSourceLimit: c.PerSourceLimit,
		DateFrom:       c.DateFrom,
		DateTo:         c.DateTo,
		Duplicates:     processing.NewDuplicateCache(),
	}
	if c.LoadOperationalSnapshot != nil {
		p.LoadOperationalSnapshot = func(ctx context.Context, f store.Fence) (processing.OperationalSnapshot, error) {
			v, err := c.LoadOperationalSnapshot(ctx, f.Envelope.JobID, int64(f.Envelope.AttemptGeneration))
			if err != nil {
				return processing.OperationalSnapshot{}, err
			}
			return processing.OperationalSnapshot{Revision: v.Revision, PinnedAt: v.PinnedAt, LimitsEnabled: v.LimitsEnabled, MaxArticles: v.MaxArticles, PerSource: v.PerSource, Days: v.Days, NewestDate: v.NewestDate, ProcessingEnabled: v.ProcessingEnabled, DeliveryEnabled: v.DeliveryEnabled, SerbianEnabled: v.SerbianEnabled}, nil
		}
	}
	h := processing.NewHandlerWithContext(ctx, db, p, processing.Config{Token: c.Token})
	srv := &http.Server{
		Handler:           h.Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       30 * time.Second,
	}
	ln, e := net.Listen("tcp", c.ListenAddr)
	if e != nil {
		return e
	}
	if ctx.Err() != nil {
		_ = ln.Close()
		return nil
	}
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ln) }()
	if onReady != nil {
		onReady()
	}
	select {
	case e = <-done:
	case <-ctx.Done():
		sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		e = srv.Shutdown(sctx)
		cancel()
		if e == nil {
			e = <-done
		}
	}
	shctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	shutdownErr := srv.Shutdown(shctx)
	cancel()
	drainCtx, drainCancel := context.WithTimeout(context.Background(), 15*time.Second)
	drainErr := h.Close(drainCtx)
	drainCancel()
	if drainErr != nil {
		keepPool = false
		return errors.New("article processing shutdown drain exceeded")
	}
	if shutdownErr != nil {
		return errors.New("article processing HTTP shutdown failed")
	}
	if e != nil && !errors.Is(e, http.ErrServerClosed) {
		return errors.New("article processing server failed")
	}
	return nil
}
