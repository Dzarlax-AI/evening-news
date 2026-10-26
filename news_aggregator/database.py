"""Database connection and models."""

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from .config import settings


class FencedAsyncSession(AsyncSession):
    """Keep the worker lease and job attempt locked through every commit."""

    async def commit(self):
        from .services.job_fence import current_job_fence

        fence = current_job_fence.get()
        if fence is None:
            return await super().commit()
        if not fence.active:
            raise RuntimeError("background job attempt is no longer active")
        async with fence.lease.transaction_fence(
            self, fence.job_id, fence.attempt_generation
        ):
            return await super().commit()

# Create async engine with enhanced connection pool settings
db_url = settings.database_url

engine = create_async_engine(
    db_url,
    echo=False,  # Disabled SQL logging for cleaner logs
    future=True,
    pool_size=settings.db_pool_size,
    max_overflow=settings.db_max_overflow,
    pool_timeout=settings.db_pool_timeout,
    pool_pre_ping=settings.db_pool_pre_ping,
    pool_recycle=settings.db_pool_recycle,
    pool_reset_on_return='rollback',
)

# Create async session factory with enhanced settings
AsyncSessionLocal = sessionmaker(
    bind=engine,
    class_=FencedAsyncSession,
    expire_on_commit=False,
    # Enhanced session settings to prevent connection leaks
    autoflush=True,
    autocommit=False
)

# Base class for models
Base = declarative_base()


async def init_db():
    """Initialize database using SQLAlchemy models."""
    import logging

    logger = logging.getLogger(__name__)
    logger.info("🔧 Checking database initialization...")

    try:
        # Import all models to ensure they're registered
        from . import models  # noqa

        # Optionally run create_all for fresh installs; disable in production by setting ALLOW_CREATE_ALL=false
        if settings.allow_create_all:
            async with engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
        else:
            logger.info("Skipping Base.metadata.create_all() because ALLOW_CREATE_ALL is false (use migrations).")

        # Verify database is accessible
        async with AsyncSessionLocal() as session:
            await session.execute(text("SELECT 1"))
            logger.info("✅ Database initialized and all tables created")

        # Schema bootstrap is owned by the explicit one-shot migration command.
        logger.info("Schema migrations are managed by news_aggregator.migrate")

    except Exception as init_error:
        logger.error(f"❌ Database initialization failed: {init_error}")
        raise RuntimeError(f"Database initialization failed: {init_error}")


async def get_db():
    """Dependency to get database session with proper cleanup."""
    import logging
    logger = logging.getLogger(__name__)
    
    # Log pool status before getting connection
    try:
        pool_status = engine.pool.status()
        logger.debug(f"📊 DB Pool before request: {pool_status}")
    except:
        pass
    
    session = AsyncSessionLocal()
    try:
        yield session
    except Exception as e:
        logger.error(f"Database session error: {e}")
        await session.rollback()
        raise
    finally:
        try:
            await session.close()
            logger.debug("🔒 Session closed successfully")
        except Exception as e:
            logger.warning(f"Error closing session: {e}")
            # Force cleanup if regular close fails
            try:
                await session.get_bind().dispose()
            except:
                pass


async def close_db_engine():
    """Properly close database engine and all connections."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Close all connections in the pool
        await engine.dispose()
        logger.info("✅ Database engine disposed and all connections closed")
    except Exception as e:
        logger.error(f"❌ Error closing database engine: {e}")


async def get_db_pool_status():
    """Get current database pool status for monitoring."""
    try:
        return {
            "pool_status": engine.pool.status(),
            "pool_size": engine.pool.size(),
            "checked_in": engine.pool.checkedin(),
            "checked_out": engine.pool.checkedout(),
            "overflow": engine.pool.overflow(),
            "invalidated_time": getattr(engine.pool, '_invalidated_time', 'unknown')
        }
    except Exception as e:
        return {"error": str(e)}


async def force_pool_cleanup():
    """Force cleanup of database connection pool."""
    import logging
    logger = logging.getLogger(__name__)
    
    try:
        # Get current status
        status_before = await get_db_pool_status()
        logger.info(f"🧹 Pool status before cleanup: {status_before}")
        
        # Dispose of all connections in pool
        await engine.dispose()
        
        # Get status after cleanup
        status_after = await get_db_pool_status()
        logger.info(f"✅ Pool status after cleanup: {status_after}")
        
        return {
            "status": "success",
            "before": status_before,
            "after": status_after
        }
        
    except Exception as e:
        logger.error(f"❌ Pool cleanup failed: {e}")
        return {"status": "failed", "error": str(e)}
