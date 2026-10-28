# Docker README

This project ships with example Docker Compose files for production and development.

## Quick start (production)
```bash
cd v2
docker-compose -f docker-compose.example.yml up -d
```

## Environment variables
Edit `docker-compose.example.yml` and set these values:

Database:
- DATABASE_URL
- DB_PASSWORD
- DB_POOL_SIZE
- DB_MAX_OVERFLOW
- DB_POOL_TIMEOUT

AI / rate limiting:
- GEMINI_API_KEY
- GEMINI_API_ENDPOINT
- RPS
- SUMMARIZATION_MODEL
- CATEGORIZATION_MODEL
- DIGEST_MODEL

All three settings default to `gemini-3.5-flash-lite`. Combined article summarization and categorization use `SUMMARIZATION_MODEL`; editorial digest generation uses `DIGEST_MODEL`. `CATEGORIZATION_MODEL` is retained for compatibility and does not independently control the combined article-analysis request.

The private executor passes provider settings to one long-lived `news-ai-runtime`
child over an authenticated Unix socket. The child handles editorial snapshot,
generation and Telegram delivery, article analysis, and the legacy Gemini
text/structured prompts. Telegram and the existing AI consumers share the
configured `RPS` gate. The bot token and destinations are passed explicitly to
the private child, and receipt-backed digest delivery is described in
[Go digest delivery](go-digest-delivery.md). The runtime pool is reserved from the existing
`DB_POOL_SIZE + DB_MAX_OVERFLOW` total; configuration must leave at least two
Python connections for the executor guard and ordinary operations.

Telegram:
- TELEGRAM_TOKEN
- TELEGRAM_CHAT_ID
- TELEGRAM_RICH_MESSAGES_ENABLED
- SITE_BASE_URL

Admin auth:
- ADMIN_USERNAME
- ADMIN_PASSWORD
- JWT_SECRET

App settings:
- LOG_LEVEL
- DEVELOPMENT
- MAX_WORKERS
- CACHE_TTL
- CACHE_DIR
- ALLOW_CREATE_ALL
- ALLOWED_ORIGINS
- TRUSTED_HOSTS
- NEWS_CATEGORIES
- DEFAULT_CATEGORY

## Scheduler settings
These are optional environment variables you can tune in the compose file:

- SCHEDULER_CHECK_INTERVAL_SECONDS (default: 60)
- SCHEDULER_RESET_EVERY_CHECKS (default: 10)
- SCHEDULER_STUCK_HOURS (default: 4)
- SCHEDULER_TASK_TIMEOUT_SECONDS (default: 0 disables timeout)

Per-task override:
- task_config.timeout_seconds can override the global timeout for a specific task.

## Notes
- The example compose file includes defaults that are safe for local dev only.
