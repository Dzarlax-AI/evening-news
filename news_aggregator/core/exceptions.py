"""Custom exceptions for Evening News v2."""


class NewsAggregatorError(Exception):
    """Base exception for Evening News."""
    pass


class ConfigurationError(NewsAggregatorError):
    """Configuration related errors."""
    pass


class DatabaseError(NewsAggregatorError):
    """Database related errors."""
    pass


class SourceError(NewsAggregatorError):
    """Source fetching errors."""
    pass


class ProcessingError(NewsAggregatorError):
    """Article processing errors."""
    pass


class APIError(NewsAggregatorError):
    """External API errors."""
    
    def __init__(self, message: str, status_code: int = None, response_text: str = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text


class CacheError(NewsAggregatorError):
    """Cache related errors."""
    pass


class TelegramError(NewsAggregatorError):
    """Telegram integration errors."""
    pass


class S3Error(NewsAggregatorError):
    """S3 storage errors."""
    pass


class ContentExtractionError(NewsAggregatorError):
    """Content extraction errors."""
    pass


class BrowserUnavailableError(ContentExtractionError):
    """Shared browser is unavailable or reconnecting is temporarily throttled."""

    pass


class BrowserBusyError(BrowserUnavailableError):
    """Shared browser capacity is busy; this is not a browser health failure."""

    pass
