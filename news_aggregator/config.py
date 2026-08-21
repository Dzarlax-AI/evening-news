"""Configuration management."""

import os
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings."""
    
    # Database
    database_url: str = Field(default="postgresql+asyncpg://newsuser:newspass123@localhost:5432/newsdb")
    
    # Gemini API configuration
    gemini_api_key: Optional[str] = Field(default=None, alias="GEMINI_API_KEY")
    gemini_api_endpoint: str = Field(
        default="https://generativelanguage.googleapis.com/v1/models",
        alias="GEMINI_API_ENDPOINT"
    )
    
    # Specific AI models for different tasks
    summarization_model: str = Field(default="gemini-1.5-flash-latest", alias="SUMMARIZATION_MODEL")
    categorization_model: str = Field(default="gemini-1.5-flash-latest", alias="CATEGORIZATION_MODEL") 
    digest_model: str = Field(default="gemini-1.5-pro-latest", alias="DIGEST_MODEL")

    # AI usage pricing (per 1M tokens) - optional, configure per provider/model
    ai_input_cost_per_1m: Optional[float] = Field(default=None, alias="AI_INPUT_COST_PER_1M")
    ai_output_cost_per_1m: Optional[float] = Field(default=None, alias="AI_OUTPUT_COST_PER_1M")
    ai_cached_input_cost_per_1m: Optional[float] = Field(default=None, alias="AI_CACHED_INPUT_COST_PER_1M")
    
    # Browser (Chrome CDP endpoint, e.g. ws://chrome:9222)
    browser_ws_endpoint: Optional[str] = Field(default=None, alias="BROWSER_WS_ENDPOINT")
    browser_tab_acquire_timeout_seconds: float = Field(
        default=120.0, gt=0, allow_inf_nan=False, alias="BROWSER_TAB_ACQUIRE_TIMEOUT_SECONDS"
    )
    browser_tab_create_timeout_seconds: float = Field(
        default=30.0, gt=0, allow_inf_nan=False, alias="BROWSER_TAB_CREATE_TIMEOUT_SECONDS"
    )
    browser_tab_close_timeout_seconds: float = Field(
        default=5.0, gt=0, allow_inf_nan=False, alias="BROWSER_TAB_CLOSE_TIMEOUT_SECONDS"
    )
    browser_operation_timeout_seconds: float = Field(
        default=90.0, gt=0, allow_inf_nan=False, alias="BROWSER_OPERATION_TIMEOUT_SECONDS"
    )
    browser_failure_threshold: int = Field(
        default=3, gt=0, alias="BROWSER_FAILURE_THRESHOLD"
    )
    browser_restart_marker_path: Optional[str] = Field(
        default=None, alias="BROWSER_RESTART_MARKER_PATH"
    )

    # Telegram
    telegram_token: Optional[SecretStr] = Field(default=None, alias="TELEGRAM_TOKEN")
    telegram_chat_id: Optional[str] = Field(default=None, alias="TELEGRAM_CHAT_ID")
    telegram_service_chat_id: Optional[str] = Field(default=None, alias="TELEGRAM_SERVICE_CHAT_ID")
    telegram_rich_messages_enabled: bool = Field(default=True, alias="TELEGRAM_RICH_MESSAGES_ENABLED")
    site_base_url: str = Field(default="https://news.dzarlax.dev", alias="SITE_BASE_URL")
    # Deprecated: accepted so older .env files do not break settings loading after Telegraph removal.
    telegraph_access_token: Optional[str] = Field(default=None, alias="TELEGRAPH_ACCESS_TOKEN")
    
    # Application
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    development: bool = Field(default=False, alias="DEVELOPMENT")
    # Admin authentication
    admin_username: str = Field(default="admin", alias="ADMIN_USERNAME")
    admin_password: Optional[str] = Field(default=None, alias="ADMIN_PASSWORD")
    jwt_secret: Optional[str] = Field(default=None, alias="JWT_SECRET")
    trust_forward_auth: bool = Field(default=False, alias="TRUST_FORWARD_AUTH")
    scheduler_enabled: bool = Field(default=True, alias="SCHEDULER_ENABLED")

    # Local/regional category isolation
    # When set, articles matching this category are isolated from other feeds
    # (e.g., "Serbia" — local news won't pollute Business/Tech feeds for international readers)
    local_category: Optional[str] = Field(default=None, alias="LOCAL_CATEGORY")
    ga_measurement_id: Optional[str] = Field(default=None, alias="GA_MEASUREMENT_ID")
    
    # Processing
    max_workers: int = Field(default=5, alias="MAX_WORKERS")
    cache_ttl: int = Field(default=86400, alias="CACHE_TTL")
    cache_dir: str = Field(default="/tmp/rss_cache", alias="CACHE_DIR")
    cache_max_size_mb: float = Field(default=500.0, alias="CACHE_MAX_SIZE_MB")  # Maximum cache size in MB
    cache_max_entries: int = Field(default=10000, alias="CACHE_MAX_ENTRIES")  # Maximum number of cache entries
    
    # API Rate Limiting
    api_rate_limit: int = Field(default=3, alias="RPS")  # Requests per second
    
    
    # Database Connection Pool
    db_pool_size: int = Field(default=5, alias="DB_POOL_SIZE")  # Base pool size
    db_max_overflow: int = Field(default=10, alias="DB_MAX_OVERFLOW")  # Allow burst connections
    db_pool_timeout: int = Field(default=30, alias="DB_POOL_TIMEOUT")  # Reasonable timeout
    
    # Database Query Optimization (агрессивная очистка)
    db_statement_timeout: int = Field(default=15000, alias="DB_STATEMENT_TIMEOUT")  # 15s per query
    db_pool_pre_ping: bool = Field(default=True, alias="DB_POOL_PRE_PING")
    db_pool_recycle: int = Field(default=300, alias="DB_POOL_RECYCLE")  # 5min aggressive recycle

    # HTTP / proxy / CORS
    allowed_origins: Optional[str] = Field(
        default="http://localhost:8000,http://127.0.0.1:8000,https://news.dzarlax.dev",
        alias="ALLOWED_ORIGINS"
    )
    trusted_hosts: Optional[str] = Field(
        default="localhost,127.0.0.1,news.dzarlax.dev",
        alias="TRUSTED_HOSTS"
    )

    # Database init safety
    allow_create_all: bool = Field(default=True, alias="ALLOW_CREATE_ALL")

    # Digest Builder Configuration
    digest_telegram_limit: int = Field(default=3600, alias="DIGEST_TELEGRAM_LIMIT")
    digest_max_summary_retries: int = Field(default=2, alias="DIGEST_MAX_SUMMARY_RETRIES")
    digest_retry_delay: int = Field(default=1, alias="DIGEST_RETRY_DELAY")
    digest_max_articles_per_category: int = Field(default=10, alias="DIGEST_MAX_ARTICLES_PER_CATEGORY")
    digest_min_summary_length: int = Field(default=20, alias="DIGEST_MIN_SUMMARY_LENGTH")
    digest_max_summary_tokens: int = Field(default=1500, alias="DIGEST_MAX_SUMMARY_TOKENS")
    digest_parallel_categories: bool = Field(default=True, alias="DIGEST_PARALLEL_CATEGORIES")

    # News Processing Limits
    news_limit_enabled: bool = Field(default=False, alias="NEWS_LIMIT_ENABLED")
    news_limit_days: int = Field(default=1, alias="NEWS_LIMIT_DAYS")
    news_limit_max_articles: int = Field(default=50, alias="NEWS_LIMIT_MAX_ARTICLES")
    news_limit_per_source: int = Field(default=50, alias="NEWS_LIMIT_PER_SOURCE")
    news_limit_oldest_date: Optional[str] = Field(default=None, alias="NEWS_LIMIT_OLDEST_DATE")
    news_limit_newest_date: Optional[str] = Field(default=None, alias="NEWS_LIMIT_NEWEST_DATE")


    class Config:
        env_file = ".env"
        case_sensitive = False

    @field_validator("browser_restart_marker_path")
    @classmethod
    def validate_browser_restart_marker_path(cls, value: Optional[str]) -> Optional[str]:
        if value is None or not value.strip():
            return None
        normalized = value.strip()
        if not os.path.isabs(normalized):
            raise ValueError("BROWSER_RESTART_MARKER_PATH must be an absolute path")
        return normalized
    
    def get_allowed_origins_list(self) -> List[str]:
        """Return allowed origins as list."""
        if not self.allowed_origins:
            return []
        return [item.strip() for item in str(self.allowed_origins).split(',') if item.strip()]
    
    def get_trusted_hosts_list(self) -> List[str]:
        """Return trusted hosts as list."""
        if not self.trusted_hosts:
            return []
        return [item.strip() for item in str(self.trusted_hosts).split(',') if item.strip()]
        
    def get_legacy_config(self, key: str) -> Optional[str]:
        """Метод для совместимости со старой системой load_config."""
        legacy_mapping = {
            "TELEGRAM_BOT_TOKEN": self.telegram_token.get_secret_value() if self.telegram_token else None,
            "TELEGRAM_TOKEN": self.telegram_token.get_secret_value() if self.telegram_token else None,
            "TELEGRAM_CHAT_ID": self.telegram_chat_id,
            "TELEGRAM_SERVICE_CHAT_ID": self.telegram_service_chat_id or self.telegram_chat_id,
        }
        return legacy_mapping.get(key)

    def get_news_limit_config(self) -> Dict[str, Any]:
        """
        Get news processing limits configuration.

        Returns:
            Dictionary with limit settings for article processing
        """
        if not self.news_limit_enabled:
            return {
                'enabled': False,
                'max_articles': None,
                'per_source': None,
                'days': None,
                'oldest_date': None,
                'newest_date': None
            }

        config = {
            'enabled': True,
            'max_articles': self.news_limit_max_articles,
            'per_source': self.news_limit_per_source,
            'days': self.news_limit_days,
            'oldest_date': None,
            'newest_date': None
        }

        # Parse oldest date if provided
        if self.news_limit_oldest_date:
            try:
                config['oldest_date'] = datetime.strptime(
                    self.news_limit_oldest_date,
                    '%Y-%m-%d'
                )
            except ValueError:
                pass  # Invalid date format, ignore

        # Parse newest date if provided
        if self.news_limit_newest_date:
            try:
                config['newest_date'] = datetime.strptime(
                    self.news_limit_newest_date,
                    '%Y-%m-%d'
                )
            except ValueError:
                pass  # Invalid date format, ignore

        # Calculate date range based on days if provided
        if self.news_limit_days and self.news_limit_days > 0:
            config['date_from'] = datetime.now() - timedelta(days=self.news_limit_days)

        return config


# Глобальный экземпляр настроек
settings = Settings()


def get_settings() -> Settings:
    """Get application settings instance."""
    return settings
