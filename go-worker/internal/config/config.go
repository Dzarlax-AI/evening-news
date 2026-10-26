package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL       string
	ExecutorURL       string
	ExecutorToken     string
	PollInterval      time.Duration
	ShutdownDrain     time.Duration
	JobRetention      time.Duration
	MaxJobAttempts    int
	RequestTimeout    time.Duration
	HeartbeatInterval time.Duration
}

func Load() (Config, error) { return LoadFrom(os.Getenv) }

func LoadFrom(getenv func(string) string) (Config, error) {
	var c Config
	dsn := strings.TrimSpace(getenv("DATABASE_URL"))
	if strings.HasPrefix(dsn, "postgresql+asyncpg://") {
		dsn = "postgresql://" + strings.TrimPrefix(dsn, "postgresql+asyncpg://")
	}
	parsedDB, err := url.Parse(dsn)
	if err != nil || parsedDB.Host == "" || (parsedDB.Scheme != "postgres" && parsedDB.Scheme != "postgresql") {
		return c, errors.New("DATABASE_URL must be a PostgreSQL URL")
	}
	c.DatabaseURL = dsn

	c.ExecutorURL = strings.TrimRight(strings.TrimSpace(getenv("JOB_EXECUTOR_URL")), "/")
	parsedExecutor, err := url.Parse(c.ExecutorURL)
	if err != nil || parsedExecutor.Host == "" || parsedExecutor.User != nil || parsedExecutor.RawQuery != "" || parsedExecutor.Fragment != "" ||
		(parsedExecutor.Scheme != "http" && parsedExecutor.Scheme != "https") || (parsedExecutor.Path != "" && parsedExecutor.Path != "/") {
		return c, errors.New("JOB_EXECUTOR_URL must be an absolute private HTTP(S) base URL")
	}
	c.ExecutorToken = getenv("JOB_EXECUTOR_TOKEN")
	if len(c.ExecutorToken) < 16 || len(c.ExecutorToken) > 4096 || !printableToken(c.ExecutorToken) {
		return c, errors.New("JOB_EXECUTOR_TOKEN must be 16 through 4096 printable ASCII bytes without spaces")
	}

	c.PollInterval, err = durationIntegerSeconds(getenv, "WORKER_POLL_INTERVAL_SECONDS", time.Second, time.Second, 10*time.Second)
	if err != nil {
		return c, err
	}
	c.ShutdownDrain, err = durationSeconds(getenv, "WORKER_SHUTDOWN_DRAIN_SECONDS", 20*time.Second, 0, 120*time.Second)
	if err != nil {
		return c, err
	}
	retentionDays, err := integer(getenv, "WORKER_JOB_RETENTION_DAYS", 7, 1, 90)
	if err != nil {
		return c, err
	}
	c.JobRetention = time.Duration(retentionDays) * 24 * time.Hour
	c.MaxJobAttempts, err = integer(getenv, "WORKER_MAX_JOB_ATTEMPTS", 3, 1, 10)
	if err != nil {
		return c, err
	}
	c.RequestTimeout, err = durationSeconds(getenv, "JOB_EXECUTOR_REQUEST_TIMEOUT_SECONDS", 5*time.Second, 250*time.Millisecond, 30*time.Second)
	if err != nil {
		return c, err
	}
	c.HeartbeatInterval = 5 * time.Second
	return c, nil
}

func printableToken(value string) bool {
	for _, b := range []byte(value) {
		if b < 0x21 || b > 0x7e {
			return false
		}
	}
	return true
}

func durationIntegerSeconds(getenv func(string) string, name string, fallback, min, max time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	seconds, err := strconv.Atoi(value)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	duration := time.Duration(seconds) * time.Second
	if duration < min || duration > max {
		return 0, fmt.Errorf("%s is outside the allowed range", name)
	}
	return duration, nil
}

func durationSeconds(getenv func(string) string, name string, fallback, min, max time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	seconds, err := strconv.ParseFloat(value, 64)
	if err != nil {
		return 0, fmt.Errorf("%s must be numeric", name)
	}
	duration := time.Duration(seconds * float64(time.Second))
	if duration < min || duration > max {
		return 0, fmt.Errorf("%s is outside the allowed range", name)
	}
	return duration, nil
}

func integer(getenv func(string) string, name string, fallback, min, max int) (int, error) {
	value := strings.TrimSpace(getenv(name))
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s must be from %d through %d", name, min, max)
	}
	return n, nil
}
