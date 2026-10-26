package config

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"

	ingestion "github.com/Dzarlax-AI/evening-news/go-ingestion/runner"
	processing "github.com/Dzarlax-AI/evening-news/go-processing/runner"
	scheduler "github.com/Dzarlax-AI/evening-news/go-scheduler/runner"
	worker "github.com/Dzarlax-AI/evening-news/go-worker/runner"
)

var ErrInvalidConfig = errors.New("background configuration is invalid")
var configKeyPattern = regexp.MustCompile(`[A-Z][A-Z0-9_]{2,}`)

type Config struct {
	Worker          worker.Config
	Scheduler       scheduler.Config
	Ingestion       ingestion.Config
	Processing      processing.Config
	HealthAddr      string
	BuildID         string
	ShutdownTimeout time.Duration
	BackoffMin      time.Duration
	BackoffMax      time.Duration
	ProbeInterval   time.Duration
	ProbeTimeout    time.Duration
}

// Load merges the existing component contracts without mutating process
// environment. Each component sees only its own database URL.
func Load(getenv func(string) string, buildID string) (Config, error) {
	if getenv == nil {
		return Config{}, fmt.Errorf("%w: environment lookup is required", ErrInvalidConfig)
	}
	c := Config{HealthAddr: value(getenv, "BACKGROUND_HEALTH_LISTEN", ":8095"), BuildID: strings.TrimSpace(buildID),
		ShutdownTimeout: 60 * time.Second, BackoffMin: 250 * time.Millisecond, BackoffMax: 5 * time.Second,
		ProbeInterval: 5 * time.Second, ProbeTimeout: 2 * time.Second}
	if c.BuildID == "" {
		c.BuildID = "development"
	}
	var err error
	if c.ShutdownTimeout, err = seconds(getenv, "BACKGROUND_SHUTDOWN_TIMEOUT_SECONDS", 60, 1, 180); err != nil {
		return Config{}, err
	}
	if c.BackoffMin, err = duration(getenv, "BACKGROUND_WORKER_BACKOFF_MIN", 250*time.Millisecond, 50*time.Millisecond, 5*time.Second); err != nil {
		return Config{}, err
	}
	if c.BackoffMax, err = duration(getenv, "BACKGROUND_WORKER_BACKOFF_MAX", 5*time.Second, 50*time.Millisecond, 5*time.Second); err != nil {
		return Config{}, err
	}
	if c.BackoffMin > c.BackoffMax {
		return Config{}, fmt.Errorf("%w: worker backoff minimum exceeds maximum", ErrInvalidConfig)
	}

	lookups := []struct {
		name string
		load func(func(string) string) error
	}{
		{"worker", func(env func(string) string) error { x, e := worker.LoadConfig(env); c.Worker = x; return e }},
		{"scheduler", func(env func(string) string) error { x, e := scheduler.LoadConfig(env); c.Scheduler = x; return e }},
		{"ingestion", func(env func(string) string) error { x, e := ingestion.LoadConfig(env); c.Ingestion = x; return e }},
		{"processing", func(env func(string) string) error { x, e := processing.LoadConfig(env); c.Processing = x; return e }},
	}
	for _, item := range lookups {
		if err := item.load(scoped(getenv, item.name)); err != nil {
			return Config{}, fmt.Errorf("%w: component=%s reason=%s", ErrInvalidConfig, item.name, safeConfigReason(err))
		}
	}
	c.Scheduler.ListenAddr = value(getenv, "SCHEDULER_LISTEN_ADDR", ":8080")
	c.Scheduler.BuildID = c.BuildID
	if err := validateAddresses(c); err != nil {
		return Config{}, err
	}
	return c, nil
}

func safeConfigReason(err error) string {
	message := err.Error()
	if strings.Contains(message, "target ") {
		return "target settings missing or invalid"
	}
	if key := configKeyPattern.FindString(message); key != "" {
		return key + " missing or invalid"
	}
	if strings.Contains(message, "required") {
		return "required settings missing or invalid"
	}
	return "settings invalid"
}

func scoped(getenv func(string) string, component string) func(string) string {
	var dsnKey string
	switch component {
	case "worker":
		dsnKey = "WORKER_DATABASE_URL"
	case "scheduler":
		dsnKey = "SCHEDULER_DATABASE_URL"
	case "ingestion":
		dsnKey = "INGESTION_DATABASE_URL"
	case "processing":
		dsnKey = "PROCESSING_DATABASE_URL"
	}
	return func(key string) string {
		switch key {
		case "DATABASE_URL":
			return strings.TrimSpace(getenv(dsnKey))
		case "LISTEN_ADDR":
			if component == "scheduler" {
				return value(getenv, "SCHEDULER_LISTEN_ADDR", ":8080")
			}
		}
		return getenv(key)
	}
}

func validateAddresses(c Config) error {
	addrs := []struct{ name, addr string }{{"scheduler", c.Scheduler.ListenAddr}, {"ingestion", c.Ingestion.ListenAddr}, {"processing", c.Processing.ListenAddr}, {"health", c.HealthAddr}}
	seen := map[string]string{}
	for _, item := range addrs {
		_, port, err := net.SplitHostPort(item.addr)
		if err != nil {
			return fmt.Errorf("%w: %s listen address is invalid", ErrInvalidConfig, item.name)
		}
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("%w: %s listen port is invalid", ErrInvalidConfig, item.name)
		}
		if prior, ok := seen[port]; ok {
			return fmt.Errorf("%w: %s and %s listeners must use distinct ports", ErrInvalidConfig, prior, item.name)
		}
		seen[port] = item.name
	}
	return nil
}

func value(getenv func(string) string, key, fallback string) string {
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v
	}
	return fallback
}
func seconds(getenv func(string) string, key string, fallback, min, max int) (time.Duration, error) {
	raw := strings.TrimSpace(getenv(key))
	if raw == "" {
		return time.Duration(fallback) * time.Second, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%w: %s must be from %d through %d seconds", ErrInvalidConfig, key, min, max)
	}
	return time.Duration(n) * time.Second, nil
}
func duration(getenv func(string) string, key string, fallback, min, max time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(getenv(key))
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil || d < min || d > max {
		return 0, fmt.Errorf("%w: %s is outside the allowed range", ErrInvalidConfig, key)
	}
	return d, nil
}
