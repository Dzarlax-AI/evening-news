"""SQLAlchemy models."""

from datetime import datetime
from typing import List, Optional
import uuid

from sqlalchemy import (
    Boolean, Column, DateTime, Date, Float, ForeignKey, Integer,
    String, Text, JSON, DECIMAL, UniqueConstraint, Index, text, Uuid, CheckConstraint,
    BigInteger, Identity
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from .database import Base


class Source(Base):
    """News source model."""
    __tablename__ = "sources"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(255), nullable=False)
    source_type = Column(String(50), nullable=False)  # rss, telegram, custom
    url = Column(Text, nullable=False)
    enabled = Column(Boolean, default=True)
    # Use default factory to avoid shared mutable default
    config = Column(JSON, default=dict)
    fetch_interval = Column(Integer, default=1800)  # seconds
    last_fetch = Column(DateTime)
    last_success = Column(DateTime)
    last_error = Column(Text)
    error_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())

    # Relationships
    articles = relationship("Article", back_populates="source")


class Article(Base):
    """Article model."""
    __tablename__ = "articles"

    id = Column(Integer, primary_key=True, index=True)
    source_id = Column(Integer, ForeignKey("sources.id", ondelete="CASCADE"))
    title = Column(Text, nullable=False)
    url = Column(Text, nullable=False, unique=True)
    content = Column(Text)
    summary = Column(Text)
    image_url = Column(Text)  # Legacy single image field for backward compatibility
    media_files = Column(JSON, default=list)  # List of media files: [{"url": "...", "type": "image|video|document", "thumbnail": "..."}]
    published_at = Column(DateTime)
    fetched_at = Column(DateTime, default=func.now())
    processed = Column(Boolean, default=False)  # Legacy field for backward compatibility
    summary_processed = Column(Boolean, default=False)  # True if AI summarization was attempted
    category_processed = Column(Boolean, default=False)  # True if AI categorization was attempted
    processing_state = Column(String(20), nullable=False, default="pending", server_default="pending")
    processing_attempts = Column(Integer, nullable=False, default=0, server_default="0")
    next_attempt_at = Column(DateTime)
    last_processing_error = Column(Text)
    hash_content = Column(String(64), index=True)
    
    # Advertising detection fields
    is_advertisement = Column(Boolean, default=False, index=True)  # True if content is advertising
    ad_confidence = Column(Float, default=0.0)  # Confidence score (0.0-1.0)
    ad_type = Column(String(50))  # Type of advertising (product_promotion, affiliate_marketing, etc.)
    ad_reasoning = Column(Text)  # AI reasoning for advertising classification
    ad_markers = Column(JSON, default=list)  # List of advertising markers found
    ad_processed = Column(Boolean, default=False)  # True if advertising detection was attempted

    # Relationships
    source = relationship("Source", back_populates="articles")
    article_categories = relationship("ArticleCategory", back_populates="article", cascade="all, delete-orphan")

    __table_args__ = (
        Index("idx_articles_processing_due", "processing_state", "next_attempt_at", "id",
              postgresql_where=text("processing_state IN ('pending', 'retry_wait')")),
    )

    @property
    def categories(self) -> List[str]:
        """Get list of category names for this article."""
        return [ac.category.name for ac in self.article_categories]
    
    @property
    def categories_with_confidence(self) -> List[dict]:
        """Get list of display categories with confidence scores and original AI categories."""
        # This is a simplified version that returns AI categories as-is
        # In real usage, this should be called through CategoryDisplayService for proper mapping
        return [
            {
                'name': ac.ai_category or 'Other',  # Use AI category name
                'display_name': ac.ai_category or 'Other',  # Will be mapped by display service
                'confidence': ac.confidence if ac.confidence is not None else 0.0,
                'color': '#6c757d',  # Default color, will be mapped by display service
                'ai_category': ac.ai_category  # Original AI category
            }
            for ac in self.article_categories
        ]
    
    @property 
    def primary_category(self) -> str:
        """Get primary AI category name (highest confidence) - not mapped."""
        if self.article_categories:
            # Sort by confidence descending and get the first one
            sorted_categories = sorted(self.article_categories, key=lambda ac: ac.confidence or 0, reverse=True)
            return sorted_categories[0].ai_category or 'Other'
        return 'Other'
    
    @property
    def images(self) -> List[dict]:
        """Get list of image media files."""
        if not self.media_files:
            # Fallback to legacy image_url for backward compatibility
            return [{"url": self.image_url, "type": "image"}] if self.image_url else []
        return [media for media in self.media_files if media.get('type') == 'image']
    
    @property
    def videos(self) -> List[dict]:
        """Get list of video media files."""
        if not self.media_files:
            return []
        return [media for media in self.media_files if media.get('type') == 'video']
    
    @property
    def documents(self) -> List[dict]:
        """Get list of document media files."""
        if not self.media_files:
            return []
        return [media for media in self.media_files if media.get('type') == 'document']
    
    @property
    def primary_image(self) -> Optional[str]:
        """Get primary image URL (first image or legacy image_url)."""
        images = self.images
        if images:
            return images[0].get('url')
        return self.image_url


class Category(Base):
    """Category model."""
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(50), nullable=False, unique=True)
    display_name = Column(String(100), nullable=False)
    description = Column(Text)
    color = Column(String(7), default='#6c757d')  # Hex color for UI
    is_local = Column(Boolean, default=False, server_default='false')  # Local/regional category — isolated from other feeds
    created_at = Column(DateTime, default=func.now())

    # Relationships
    article_categories = relationship("ArticleCategory", back_populates="category")


class ArticleCategory(Base):
    """Junction table for article-category many-to-many relationship."""
    __tablename__ = "article_categories"

    id = Column(Integer, primary_key=True, index=True)
    article_id = Column(Integer, ForeignKey("articles.id", ondelete="CASCADE"), nullable=False)
    category_id = Column(Integer, ForeignKey("categories.id", ondelete="CASCADE"), nullable=False)
    confidence = Column(Float, default=1.0)  # AI confidence for this categorization
    ai_category = Column(String(100))  # Original AI category name before mapping
    created_at = Column(DateTime, default=func.now())

    # Relationships
    article = relationship("Article", back_populates="article_categories")
    category = relationship("Category", back_populates="article_categories")

    # Unique constraint
    __table_args__ = (
        UniqueConstraint('article_id', 'category_id', name='unique_article_category'),
    )


class DailySummary(Base):
    """Daily summary by category model."""
    __tablename__ = "daily_summaries"
    
    id = Column(Integer, primary_key=True, index=True)
    # Persisted as DATE in DB schema – align ORM type
    date = Column(Date, nullable=False)
    category = Column(String(50), nullable=False)  # Business, Tech, Science, Nature, Serbia, Marketing, Other
    summary_text = Column(Text, nullable=False)
    articles_count = Column(Integer, default=0)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class Setting(Base):
    """Application settings model."""
    __tablename__ = "settings"

    key = Column(String(255), primary_key=True)
    value = Column(JSON, nullable=False)
    description = Column(Text)
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class ProcessingStat(Base):
    """Processing statistics model."""
    __tablename__ = "processing_stats"

    id = Column(Integer, primary_key=True, index=True)
    # Persisted as DATE in DB schema – align ORM type
    date = Column(Date, default=func.current_date(), unique=True)
    articles_fetched = Column(Integer, default=0)
    articles_processed = Column(Integer, default=0)
    api_calls_made = Column(Integer, default=0)
    errors_count = Column(Integer, default=0)
    processing_time_seconds = Column(Integer, default=0)


class BackgroundJob(Base):
    """Durable, content-free work accepted by the background worker."""
    __tablename__ = "background_jobs"

    id = Column(Uuid(as_uuid=True), primary_key=True, default=uuid.uuid4)
    queue_position = Column(BigInteger, Identity(), nullable=False, unique=True)
    kind = Column(String(40), nullable=False)
    payload = Column(JSON().with_variant(JSONB, "postgresql"), nullable=False, default=dict)
    idempotency_key = Column(String(200), nullable=False, unique=True)
    coalesce_key = Column(String(64))
    state = Column(String(24), nullable=False, server_default="queued")
    created_at = Column(DateTime, nullable=False, server_default=func.timezone("UTC", func.now()))
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    owner_generation = Column(Integer)
    attempt_generation = Column(Integer, nullable=False, server_default="0")
    initialized_at = Column(DateTime)
    schedule_key = Column(String(128))
    schedule_revision = Column(Integer)
    occurrence_id = Column(Uuid(as_uuid=True))
    scheduled_for = Column(DateTime)
    result = Column(JSON().with_variant(JSONB, "postgresql"))
    error_code = Column(String(40))

    __table_args__ = (
        CheckConstraint("kind IN ('news_processing', 'news_digest', 'telegram_digest', 'backup', 'reprocess_failed', 'article_reprocess', 'summaries_generate', 'source_test')", name="ck_background_jobs_kind"),
        CheckConstraint("state IN ('queued', 'running', 'succeeded', 'failed', 'interrupted', 'delivery_unknown')", name="ck_background_jobs_state"),
        CheckConstraint("error_code IS NULL OR error_code IN ('worker_error', 'worker_interrupted', 'worker_recovery_exhausted', 'external_outcome_unknown', 'invalid_job', 'resource_limit', 'database_unavailable')", name="ck_background_jobs_error_code"),
        CheckConstraint("jsonb_typeof(payload) = 'object'", name="ck_background_jobs_payload_object").ddl_if(dialect="postgresql"),
        CheckConstraint("result IS NULL OR jsonb_typeof(result) = 'object'", name="ck_background_jobs_result_object").ddl_if(dialect="postgresql"),
        Index("idx_background_jobs_queue", "created_at", "id", postgresql_where=text("state = 'queued'")),
        Index("idx_background_jobs_active_coalesce", "coalesce_key", unique=True,
              postgresql_where=text("state IN ('queued', 'running', 'interrupted') AND coalesce_key IS NOT NULL")),
        Index("idx_background_jobs_status", "state", "created_at"),
    )


class BackgroundJobIdempotencyKey(Base):
    """Durable request keys, including aliases coalesced to active work."""
    __tablename__ = "background_job_idempotency_keys"

    key = Column(String(200), primary_key=True)
    job_id = Column(Uuid(as_uuid=True), ForeignKey("background_jobs.id", ondelete="CASCADE"), nullable=False)
    created_at = Column(DateTime, nullable=False, server_default=func.timezone("UTC", func.now()))

    __table_args__ = (
        Index("idx_background_job_keys_job", "job_id"),
    )


class SchedulerOccurrenceReceipt(Base):
    """Durable idempotency receipts independent of background job retention."""
    __tablename__ = "scheduler_occurrence_receipts"

    occurrence_id = Column(Uuid(as_uuid=True), primary_key=True)
    envelope_sha256 = Column(String(64), nullable=False)
    job_id = Column(Uuid(as_uuid=True), nullable=False)
    accepted_at = Column(DateTime, nullable=False, server_default=func.timezone("UTC", func.now()))

    __table_args__ = (
        CheckConstraint("length(envelope_sha256) = 64", name="ck_scheduler_receipt_hash"),
    )


class BackgroundJobExecution(Base):
    """Idempotent Python executor receipt for one Go-owned job attempt."""
    __tablename__ = "background_job_executions"

    job_id = Column(Uuid(as_uuid=True), ForeignKey("background_jobs.id", ondelete="CASCADE"), primary_key=True)
    attempt_generation = Column(Integer, primary_key=True)
    envelope_sha256 = Column(String(64), nullable=False)
    executor_instance_id = Column(Uuid(as_uuid=True), nullable=False)
    owner_id = Column(Uuid(as_uuid=True))
    worker_generation = Column(Integer)
    deadline_at = Column(DateTime)
    state = Column(String(24), nullable=False, server_default="accepted")
    accepted_at = Column(DateTime, nullable=False, server_default=func.timezone("UTC", func.now()))
    started_at = Column(DateTime)
    finished_at = Column(DateTime)
    result = Column(JSON(none_as_null=True).with_variant(JSONB(none_as_null=True), "postgresql"))
    error_code = Column(String(40))
    legacy_completion_applied_at = Column(DateTime)
    legacy_alert_state = Column(String(16))

    __table_args__ = (
        CheckConstraint("attempt_generation >= 0", name="ck_background_execution_attempt_generation"),
        CheckConstraint("length(envelope_sha256) = 64", name="ck_background_execution_hash"),
        CheckConstraint("worker_generation IS NULL OR worker_generation > 0",
                        name="ck_background_execution_generation"),
        CheckConstraint("state IN ('accepted', 'running', 'succeeded', 'failed', 'interrupted', 'delivery_unknown')",
                        name="ck_background_execution_state"),
        CheckConstraint("error_code IS NULL OR error_code IN ('worker_error', 'worker_interrupted', 'worker_recovery_exhausted', 'external_outcome_unknown', 'invalid_job', 'resource_limit', 'database_unavailable')",
                        name="ck_background_execution_error_code"),
        CheckConstraint("result IS NULL OR jsonb_typeof(result) = 'object'",
                        name="ck_background_execution_result_object").ddl_if(dialect="postgresql"),
        CheckConstraint("legacy_alert_state IS NULL OR legacy_alert_state IN ('pending', 'attempting', 'delivered', 'unknown', 'not_needed')",
                        name="ck_background_execution_alert_state"),
    )


class BackgroundWorkerStatus(Base):
    """Singleton persisted worker lease and heartbeat."""
    __tablename__ = "background_worker_status"

    id = Column(Integer, primary_key=True, default=1)
    generation = Column(Integer, nullable=False, server_default="0")
    owner_id = Column(Uuid(as_uuid=True))
    lock_backend_pid = Column(Integer)
    heartbeat_at = Column(DateTime)
    status = Column(String(16), nullable=False, server_default="stopped")

    __table_args__ = (
        CheckConstraint("id = 1", name="ck_background_worker_singleton"),
        CheckConstraint("status IN ('stopped', 'running')", name="ck_background_worker_status"),
    )


class ArticleIngestionRun(Base):
    """Scalar progress and immutable execution fence for one ingestion attempt."""
    __tablename__ = "article_ingestion_runs"

    id = Column(Uuid(as_uuid=True), primary_key=True)
    job_id = Column(Uuid(as_uuid=True), ForeignKey("background_jobs.id", ondelete="CASCADE"), nullable=False)
    attempt_generation = Column(Integer, nullable=False)
    envelope_sha256 = Column(String(64), nullable=False)
    owner_id = Column(Uuid(as_uuid=True), nullable=False)
    worker_generation = Column(Integer, nullable=False)
    backend_pid = Column(Integer, nullable=False)
    executor_instance_id = Column(Uuid(as_uuid=True), nullable=False)
    deadline_at = Column(DateTime, nullable=False)
    state = Column(String(20), nullable=False, default="running", server_default="running")
    started_at = Column(DateTime, nullable=False, server_default=func.now())
    updated_at = Column(DateTime, nullable=False, server_default=func.now())
    finished_at = Column(DateTime)
    result = Column(JSON().with_variant(JSONB, "postgresql"), nullable=False,
                    default=dict, server_default=text("'{}'"))
    error_code = Column(String(40))

    __table_args__ = (
        UniqueConstraint("job_id", "attempt_generation"),
        CheckConstraint("attempt_generation > 0", name="ck_ingestion_run_attempt_positive"),
        CheckConstraint("worker_generation > 0", name="ck_ingestion_run_worker_generation_positive"),
        CheckConstraint("backend_pid > 0", name="ck_ingestion_run_backend_pid_positive"),
        CheckConstraint("length(envelope_sha256) = 64", name="ck_ingestion_run_digest_length"),
        CheckConstraint("state IN ('running','succeeded','failed','cancelled','interrupted')", name="ck_ingestion_run_state"),
        CheckConstraint("jsonb_typeof(result) = 'object'", name="ck_ingestion_run_result_object").ddl_if(dialect="postgresql"),
    )


class ArticleIngestionSourceRun(Base):
    """Bounded, content-free result for one source in an ingestion run."""
    __tablename__ = "article_ingestion_source_runs"

    run_id = Column(Uuid(as_uuid=True), ForeignKey("article_ingestion_runs.id", ondelete="CASCADE"), primary_key=True)
    source_id = Column(Integer, primary_key=True)
    source_type = Column(String(20), nullable=False)
    config_sha256 = Column(String(64), nullable=False)
    state = Column(String(20), nullable=False, default="pending", server_default="pending")
    candidates = Column(Integer, nullable=False, default=0, server_default="0")
    saved = Column(Integer, nullable=False, default=0, server_default="0")
    updated = Column(Integer, nullable=False, default=0, server_default="0")
    skipped = Column(Integer, nullable=False, default=0, server_default="0")
    fetched_at = Column(DateTime)
    error_code = Column(String(40))
    selector_study_sha256 = Column(String(64))

    __table_args__ = (
        CheckConstraint("source_type IN ('rss','telegram','custom')", name="ck_ingestion_source_type"),
        CheckConstraint("state IN ('pending','running','succeeded','partial','failed','skipped','busy','deferred','resource_limit','unavailable')", name="ck_ingestion_source_state"),
        CheckConstraint("candidates >= 0 AND saved >= 0 AND updated >= 0 AND skipped >= 0", name="ck_ingestion_source_counts_nonnegative"),
        CheckConstraint("length(config_sha256) = 64", name="ck_ingestion_source_digest_length"),
    )


class SourceCollectionState(Base):
    """Persistent bounded Page Monitor hashes and commit acknowledgements."""
    __tablename__ = "source_collection_state"

    source_id = Column(Integer, ForeignKey("sources.id", ondelete="CASCADE"), primary_key=True)
    config_sha256 = Column(String(64), nullable=False)
    scan_limit = Column(Integer, nullable=False, default=20, server_default="20")
    cursor_offset = Column(Integer, nullable=False, default=0, server_default="0")
    scan_active = Column(Boolean, nullable=False, default=False, server_default="false")
    previous_hashes = Column(JSON().with_variant(JSONB, "postgresql"), nullable=False,
                             default=dict, server_default=text("'{}'"))
    scan_hashes = Column(JSON().with_variant(JSONB, "postgresql"), nullable=False,
                         default=dict, server_default=text("'{}'"))
    acknowledged_hashes = Column(JSON().with_variant(JSONB, "postgresql"), nullable=False,
                                 default=dict, server_default=text("'{}'"))
    snapshot_at = Column(DateTime)
    content_sha256 = Column(String(64))
    updated_at = Column(DateTime, nullable=False, server_default=func.now())

    __table_args__ = (
        CheckConstraint("scan_limit BETWEEN 1 AND 100", name="ck_source_collection_scan_limit"),
        CheckConstraint("cursor_offset BETWEEN 0 AND 100", name="ck_source_collection_cursor"),
        CheckConstraint("cursor_offset <= scan_limit", name="ck_source_collection_cursor_within_limit"),
        CheckConstraint("jsonb_typeof(previous_hashes) = 'object' AND jsonb_typeof(scan_hashes) = 'object' AND jsonb_typeof(acknowledged_hashes) = 'object'", name="ck_source_collection_hash_objects").ddl_if(dialect="postgresql"),
        CheckConstraint("length(config_sha256) = 64", name="ck_source_collection_digest_length"),
    )



class ScheduleSettings(Base):
    """Schedule settings for automated tasks."""
    __tablename__ = "schedule_settings"

    id = Column(Integer, primary_key=True, index=True)
    task_name = Column(String(100), nullable=False, unique=True)
    enabled = Column(Boolean, default=False)
    
    # Schedule configuration
    schedule_type = Column(String(20), default="daily")  # daily, weekly, hourly, custom
    hour = Column(Integer, default=9)  # 0-23
    minute = Column(Integer, default=0)  # 0-59
    weekdays = Column(JSON, default=list)  # [1,2,3,4,5] for Mon-Fri
    timezone = Column(String(50), default="Europe/Belgrade")
    
    # Task specific settings
    task_config = Column(JSON, default=dict)
    
    last_run = Column(DateTime)
    last_finished_at = Column(DateTime)
    last_success_at = Column(DateTime)
    last_status = Column(String(20))
    last_error = Column(Text)
    last_duration_seconds = Column(DECIMAL(12, 3))
    next_run = Column(DateTime)
    is_running = Column(Boolean, default=False)
    
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


# Extraction learning models for AI-enhanced content extraction
class ExtractionPattern(Base):
    """Extraction pattern model for tracking learned selectors."""
    __tablename__ = "extraction_patterns"
    
    id = Column(Integer, primary_key=True, index=True)
    domain = Column(String(255), nullable=False, index=True)
    selector_pattern = Column(Text, nullable=False)
    extraction_strategy = Column(String(255), nullable=False)
    success_count = Column(Integer, default=0)
    failure_count = Column(Integer, default=0)
    quality_score_avg = Column(DECIMAL(5, 2), default=0)
    content_length_avg = Column(Integer, default=0)
    discovered_by = Column(String(20), default='manual')  # 'manual', 'ai', 'heuristic'
    is_stable = Column(Boolean, default=False)
    last_ai_analysis = Column(DateTime)
    consecutive_successes = Column(Integer, default=0)
    consecutive_failures = Column(Integer, default=0)
    first_success_at = Column(DateTime)
    last_success_at = Column(DateTime)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
    
    __table_args__ = (
        UniqueConstraint('domain', 'selector_pattern', 'extraction_strategy'),
    )


class DomainStability(Base):
    """Domain stability model for tracking extraction reliability."""
    __tablename__ = "domain_stability"
    
    id = Column(Integer, primary_key=True, index=True)
    domain = Column(String(255), unique=True, nullable=False, index=True)
    is_stable = Column(Boolean, default=False, index=True)
    success_rate_7d = Column(DECIMAL(5, 2), default=0)
    success_rate_30d = Column(DECIMAL(5, 2), default=0)
    total_attempts = Column(Integer, default=0)
    successful_attempts = Column(Integer, default=0)
    last_successful_extraction = Column(DateTime)
    last_failed_extraction = Column(DateTime)
    last_ai_analysis = Column(DateTime)
    consecutive_successes = Column(Integer, default=0)
    consecutive_failures = Column(Integer, default=0)
    stability_achieved_at = Column(DateTime)
    needs_reanalysis = Column(Boolean, default=False, index=True)
    ai_credits_saved = Column(Integer, default=0)
    reanalysis_triggers = Column(JSON, default=list)
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())


class ExtractionAttempt(Base):
    """Extraction attempt model for detailed logging."""
    __tablename__ = "extraction_attempts"
    
    id = Column(Integer, primary_key=True, index=True)
    article_url = Column(Text, nullable=False)
    domain = Column(String(255), nullable=False, index=True)
    extraction_strategy = Column(String(255), nullable=False, index=True)
    selector_used = Column(Text)
    success = Column(Boolean, nullable=False, index=True)
    content_length = Column(Integer)
    quality_score = Column(DECIMAL(5, 2))
    extraction_time_ms = Column(Integer)
    error_message = Column(Text)
    ai_analysis_triggered = Column(Boolean, default=False)
    ai_analysis = Column(JSON)
    user_agent = Column(String(500))
    http_status_code = Column(Integer)
    created_at = Column(DateTime, default=func.now(), index=True)


class AIUsageTracking(Base):
    """AI usage tracking model for monitoring costs and effectiveness."""
    __tablename__ = "ai_usage_tracking"
    
    id = Column(Integer, primary_key=True, index=True)
    domain = Column(String(255), nullable=False, index=True)
    analysis_type = Column(String(50), nullable=False, index=True)  # 'selector_discovery', 'pattern_analysis'
    tokens_used = Column(Integer)
    credits_cost = Column(DECIMAL(10, 4))
    analysis_result = Column(JSON)
    patterns_discovered = Column(Integer, default=0)
    patterns_successful = Column(Integer, default=0)
    cost_effectiveness = Column(DECIMAL(5, 2))  # successful patterns / cost ratio
    created_at = Column(DateTime, default=func.now(), index=True)


class CategoryMapping(Base):
    """Category mapping model for AI category to fixed category mapping."""
    __tablename__ = "category_mapping"
    
    id = Column(Integer, primary_key=True, index=True)
    ai_category = Column(String(100), nullable=False, unique=True, index=True)  # Original AI category name
    fixed_category = Column(String(50), nullable=False, index=True)  # One of our 7 fixed categories
    confidence_threshold = Column(Float, default=0.0)  # Minimum confidence to apply this mapping
    description = Column(Text)  # Optional description of why this mapping exists
    created_by = Column(String(100), default="system")  # Who created this mapping
    usage_count = Column(Integer, default=0)  # How many times this mapping was used
    last_used = Column(DateTime)  # When this mapping was last used
    is_active = Column(Boolean, default=True, index=True)  # Can be disabled without deleting
    created_at = Column(DateTime, default=func.now())
    updated_at = Column(DateTime, default=func.now(), onupdate=func.now())
