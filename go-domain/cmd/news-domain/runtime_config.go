package main

import (
	"encoding/base64"
	"errors"
	"os"

	"github.com/Dzarlax-AI/evening-news/go-tools/runtimeconfig"
)

type configSource struct {
	get          func(string) string
	lookup       func(string) (string, bool)
	managed      bool
	installation string
	key          []byte
	keyID        string
}

func loadSource() (configSource, error) {
	path, present := os.LookupEnv("NEWS_RUNTIME_FILE")
	if !present {
		return configSource{get: os.Getenv, lookup: os.LookupEnv}, nil
	}
	allowed := []string{"DATABASE_URL", "DB_POOL_SIZE", "DB_MAX_OVERFLOW", "JOB_EXECUTOR_TOKEN", "ARTICLE_AI_TOKEN", "EXECUTION_SERVICE_URL", "EXTRACTION_SERVICE_URL", "EXTRACTION_SERVICE_TOKEN", "EXTRACTION_TEXT_MAX_CHARS", "EXTRACTION_HTTP_MAX_BYTES", "ALLOW_CREATE_ALL", "BACKGROUND_EXECUTION_MODE", "WEB_BACKEND_TOKEN", "JOB_EXECUTOR_HOST", "JOB_EXECUTOR_PORT", "GEMINI_API_ENDPOINT", "SUMMARIZATION_MODEL", "LOCAL_CATEGORY", "AI_PROVIDER", "OPENAI_ARTICLE_MODEL", "OPENAI_DIGEST_MODEL", "OPENAI_TEXT_MODEL", "TEXT_MODEL", "TELEGRAM_CHAT_ID", "TELEGRAM_SERVICE_CHAT_ID", "DIGEST_MODEL", "SITE_BASE_URL", "TELEGRAM_RICH_MESSAGES_ENABLED", "RPS", "NEWS_AI_DB_MAX_CONNS", "AI_MODEL_PRICES", "AI_INPUT_COST_PER_1M", "AI_OUTPUT_COST_PER_1M", "AI_CACHED_INPUT_COST_PER_1M", "NEWS_CREDENTIAL_KEY", "NEWS_CREDENTIAL_KEY_ID"}
	required := []string{"DATABASE_URL", "JOB_EXECUTOR_TOKEN", "ARTICLE_AI_TOKEN", "EXTRACTION_SERVICE_URL", "EXTRACTION_SERVICE_TOKEN", "WEB_BACKEND_TOKEN", "NEWS_CREDENTIAL_KEY", "NEWS_CREDENTIAL_KEY_ID"}
	m, err := runtimeconfig.Load(path, "domain", allowed, required)
	if err != nil {
		return configSource{}, err
	}
	key, err := base64.RawStdEncoding.DecodeString(m.Values["NEWS_CREDENTIAL_KEY"])
	if err != nil || len(key) != 32 {
		return configSource{}, errors.New("invalid credential key")
	}
	return configSource{get: func(k string) string { return m.Values[k] }, lookup: func(k string) (string, bool) { v, ok := m.Values[k]; return v, ok }, managed: true, installation: m.InstallationID, key: key, keyID: m.Values["NEWS_CREDENTIAL_KEY_ID"]}, nil
}
