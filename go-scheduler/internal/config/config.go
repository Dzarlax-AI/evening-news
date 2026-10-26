package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Dzarlax-AI/evening-news/go-scheduler/internal/model"
)

var schemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

type Config struct {
	DatabaseURL string
	Schema      string
	APIToken    string
	Targets     map[string]model.Target
	ListenAddr  string
	DBMaxConns  int32
	MaxInflight int
	MaxOutbox   int
	PollEvery   time.Duration
	HTTPTimeout time.Duration
	StartPaused bool
	BuildID     string
}

func Load() (Config, error) { return LoadFrom(os.Getenv) }

func LoadFrom(getenv func(string) string) (Config, error) {
	var c Config
	c.DatabaseURL = strings.TrimSpace(getenv("SCHEDULER_DATABASE_URL"))
	c.Schema = strings.TrimSpace(getenv("SCHEDULER_DB_SCHEMA"))
	c.APIToken = getenv("SCHEDULER_API_TOKEN")
	c.ListenAddr = envOr(getenv, "LISTEN_ADDR", ":8080")
	c.BuildID = "development"
	c.StartPaused = strings.EqualFold(getenv("SCHEDULER_START_PAUSED"), "true")
	if c.DatabaseURL == "" || c.Schema == "" || c.APIToken == "" {
		return c, errors.New("SCHEDULER_DATABASE_URL, SCHEDULER_DB_SCHEMA and SCHEDULER_API_TOKEN are required")
	}
	parsedDB, err := url.Parse(c.DatabaseURL)
	if err != nil || (parsedDB.Scheme != "postgres" && parsedDB.Scheme != "postgresql") || parsedDB.Host == "" {
		return c, errors.New("SCHEDULER_DATABASE_URL must be a PostgreSQL URL")
	}
	if !schemaName.MatchString(c.Schema) {
		return c, errors.New("SCHEDULER_DB_SCHEMA is invalid")
	}
	if len(c.APIToken) < 16 || strings.ContainsAny(c.APIToken, "\r\n") {
		return c, errors.New("SCHEDULER_API_TOKEN must be at least 16 bytes and contain no line breaks")
	}
	maxConns, err := intEnv(getenv, "SCHEDULER_DB_MAX_CONNS", 8, 1, 64)
	if err != nil {
		return c, err
	}
	c.DBMaxConns = int32(maxConns)
	c.MaxInflight, err = intEnv(getenv, "SCHEDULER_MAX_INFLIGHT", 4, 1, 32)
	if err != nil {
		return c, err
	}
	c.MaxOutbox, err = intEnv(getenv, "SCHEDULER_MAX_OUTBOX", 10000, 100, 100000)
	if err != nil {
		return c, err
	}
	c.PollEvery, err = durationEnv(getenv, "SCHEDULER_POLL_INTERVAL", time.Second, 100*time.Millisecond, 5*time.Minute)
	if err != nil {
		return c, err
	}
	c.HTTPTimeout, err = durationEnv(getenv, "SCHEDULER_HTTP_TIMEOUT", 10*time.Second, 100*time.Millisecond, 2*time.Minute)
	if err != nil {
		return c, err
	}
	c.Targets, err = loadTargets(getenv)
	if err != nil {
		return c, err
	}
	if len(c.Targets) == 0 {
		return c, errors.New("SCHEDULER_TARGET_ALIASES must contain at least one configured target")
	}
	if strings.TrimSpace(c.BuildID) == "" {
		return c, errors.New("build ID cannot be empty")
	}
	return c, nil
}

func loadTargets(getenv func(string) string) (map[string]model.Target, error) {
	aliases := strings.Split(getenv("SCHEDULER_TARGET_ALIASES"), ",")
	if len(aliases) == 1 && strings.TrimSpace(aliases[0]) == "" {
		return nil, errors.New("SCHEDULER_TARGET_ALIASES is required")
	}
	targets := make(map[string]model.Target, len(aliases))
	allowHTTP := strings.EqualFold(getenv("SCHEDULER_ALLOW_HTTP_TARGETS"), "true")
	for _, rawAlias := range aliases {
		alias := strings.TrimSpace(rawAlias)
		if alias == "" || strings.ContainsAny(alias, " \t\r\n") || !regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`).MatchString(alias) {
			return nil, errors.New("invalid target alias configuration")
		}
		if _, exists := targets[alias]; exists {
			return nil, errors.New("duplicate target alias configuration")
		}
		key := strings.ToUpper(strings.ReplaceAll(alias, "-", "_"))
		endpoint := strings.TrimSpace(getenv("SCHEDULER_TARGET_" + key + "_URL"))
		token := getenv("SCHEDULER_TARGET_" + key + "_TOKEN")
		parsed, err := url.Parse(endpoint)
		if endpoint == "" || err != nil || parsed.Host == "" || parsed.User != nil || parsed.Fragment != "" || parsed.RawQuery != "" {
			return nil, fmt.Errorf("target %q must have an absolute URL without userinfo, query, or fragment", alias)
		}
		if parsed.Scheme != "https" && !(parsed.Scheme == "http" && allowHTTP) {
			return nil, fmt.Errorf("target %q must use HTTPS unless SCHEDULER_ALLOW_HTTP_TARGETS=true", alias)
		}
		if token == "" || strings.ContainsAny(token, "\r\n") {
			return nil, fmt.Errorf("target %q token is required", alias)
		}
		targets[alias] = model.Target{URL: strings.TrimRight(endpoint, "/"), Token: token}
	}
	return targets, nil
}

func envOr(getenv func(string) string, key, fallback string) string {
	if v := strings.TrimSpace(getenv(key)); v != "" {
		return v
	}
	return fallback
}

func intEnv(getenv func(string) string, key string, fallback, min, max int) (int, error) {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(value)
	if err != nil || n < min || n > max {
		return 0, fmt.Errorf("%s must be an integer from %d through %d", key, min, max)
	}
	return n, nil
}

func durationEnv(getenv func(string) string, key string, fallback, min, max time.Duration) (time.Duration, error) {
	value := strings.TrimSpace(getenv(key))
	if value == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(value)
	if err != nil || d < min || d > max {
		return 0, fmt.Errorf("%s must be a duration from %s through %s", key, min, max)
	}
	return d, nil
}
