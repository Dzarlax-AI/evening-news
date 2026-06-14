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
