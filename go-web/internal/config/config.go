package config

import (
	"errors"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config contains only settings required by the HTTP process. Worker and
// provider credentials intentionally do not belong to the public web.
type Config struct {
	DatabaseURL               string
	ListenAddress             string
	AdminUsername             string
	AdminPassword             string
	JWTSecret                 string
	Development               bool
	TrustForwardAuth          bool
	ReadHeaderTimeout         time.Duration
	ReadTimeout               time.Duration
	WriteTimeout              time.Duration
	IdleTimeout               time.Duration
	ShutdownTimeout           time.Duration
	ScheduledJobsToken        string
	ScheduledJobsReceiptLimit int
	BackgroundExecutionMode   string
	ScheduleDispatchMode      string
	WebBackendURL             string
	WebBackendToken           string
	WebBackendTimeout         time.Duration
	AllowedOrigins            []string
	TrustedHosts              []string
	TrustedProxyCIDRs         []string
	SiteBaseURL               string
}

func Load() (Config, error) {
	development, err := boolEnv("DEVELOPMENT", false)
	if err != nil {
		return Config{}, err
	}
	trustForwardAuth, err := boolEnv("TRUST_FORWARD_AUTH", false)
	if err != nil {
		return Config{}, err
	}
	c := Config{
		DatabaseURL:               os.Getenv("DATABASE_URL"),
		ListenAddress:             envOr("WEB_LISTEN_ADDRESS", ":8000"),
		AdminUsername:             envOr("ADMIN_USERNAME", "admin"),
		AdminPassword:             os.Getenv("ADMIN_PASSWORD"),
		JWTSecret:                 os.Getenv("JWT_SECRET"),
		Development:               development,
		TrustForwardAuth:          trustForwardAuth,
		ReadHeaderTimeout:         5 * time.Second,
		ReadTimeout:               15 * time.Second,
		WriteTimeout:              30 * time.Second,
		IdleTimeout:               60 * time.Second,
		ShutdownTimeout:           10 * time.Second,
		ScheduledJobsToken:        os.Getenv("SCHEDULED_JOBS_TOKEN"),
		ScheduledJobsReceiptLimit: intEnv("SCHEDULED_JOBS_RECEIPT_LIMIT", 100000),
		BackgroundExecutionMode:   envOr("BACKGROUND_EXECUTION_MODE", "worker"),
		ScheduleDispatchMode:      envOr("SCHEDULE_DISPATCH_MODE", "service"),
		WebBackendURL:             os.Getenv("WEB_BACKEND_URL"),
		WebBackendToken:           os.Getenv("WEB_BACKEND_TOKEN"),
		WebBackendTimeout:         15 * time.Second,
		AllowedOrigins:            csvEnv("ALLOWED_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000,https://news.dzarlax.dev"),
		TrustedHosts:              csvEnv("TRUSTED_HOSTS", "localhost,127.0.0.1,news.dzarlax.dev"),
		TrustedProxyCIDRs:         csvEnv("TRUSTED_PROXY_CIDRS", ""),
		SiteBaseURL:               envOr("SITE_BASE_URL", "https://news.dzarlax.dev"),
	}
	if c.JWTSecret == "" {
		c.JWTSecret = c.AdminPassword
	}
	if c.DatabaseURL == "" {
		return Config{}, errors.New("DATABASE_URL is required")
	}
	if c.AdminUsername == "" {
		c.AdminUsername = "admin"
	}
	if c.AdminPassword == "" {
		return Config{}, errors.New("ADMIN_PASSWORD is required")
	}
	if c.ScheduledJobsReceiptLimit < 1 || c.ScheduledJobsReceiptLimit > 100000 {
		return Config{}, errors.New("SCHEDULED_JOBS_RECEIPT_LIMIT must be between 1 and 100000")
	}
	if c.BackgroundExecutionMode != "worker" {
		return Config{}, errors.New("BACKGROUND_EXECUTION_MODE must be worker for the Go web service")
	}
	if c.ScheduleDispatchMode != "service" {
		return Config{}, errors.New("SCHEDULE_DISPATCH_MODE must be service")
	}
	if c.TrustForwardAuth && len(c.TrustedProxyCIDRs) == 0 {
		return Config{}, errors.New("TRUSTED_PROXY_CIDRS is required when TRUST_FORWARD_AUTH is enabled")
	}
	for _, raw := range c.TrustedProxyCIDRs {
		if _, _, err := net.ParseCIDR(raw); err != nil {
			return Config{}, errors.New("TRUSTED_PROXY_CIDRS must contain valid CIDR values")
		}
	}
	if (c.WebBackendURL == "") != (c.WebBackendToken == "") {
		return Config{}, errors.New("WEB_BACKEND_URL and WEB_BACKEND_TOKEN must be configured together")
	}
	if c.WebBackendURL == "" || strings.TrimSpace(c.WebBackendToken) == "" {
		return Config{}, errors.New("WEB_BACKEND_URL and WEB_BACKEND_TOKEN are required")
	}
	return c, nil
}

func csvEnv(name, fallback string) []string {
	raw := os.Getenv(name)
	if raw == "" {
		raw = fallback
	}
	out := []string{}
	for _, part := range strings.Split(raw, ",") {
		if value := strings.TrimSpace(part); value != "" {
			out = append(out, value)
		}
	}
	return out
}

func intEnv(name string, fallback int) int {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return -1
	}
	return n
}

func envOr(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func boolEnv(name string, fallback bool) (bool, error) {
	v := os.Getenv(name)
	if v == "" {
		return fallback, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, errors.New(name + " must be a boolean")
	}
	return b, nil
}
