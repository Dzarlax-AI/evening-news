"""Main FastAPI application."""

from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.middleware.cors import CORSMiddleware
from uvicorn.middleware.proxy_headers import ProxyHeadersMiddleware

from .config import settings
from .database import init_db, AsyncSessionLocal
from .migrations.universal_migration_manager import create_migration_manager
from .utils.logging_config import setup_logging

import logging

# Initialize logging before anything else
setup_logging()
logger = logging.getLogger(__name__)


# Create migration manager
migration_manager = create_migration_manager(AsyncSessionLocal, "Evening News v2")

# Register migrations
# MediaFilesMigration removed - media caching functionality disabled

# Category migrations removed - already executed and managed via web interface

# Register performance optimization migration
from .migrations.feed_performance_optimization import FeedPerformanceOptimization
from .migrations.scheduler_run_outcomes import (
    SchedulerRunOutcomesMigration,
    ensure_scheduler_outcome_migration,
)
migration_manager.register_migration(FeedPerformanceOptimization())
migration_manager.register_migration(SchedulerRunOutcomesMigration())




@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    # Startup
    if not settings.admin_password:
        raise RuntimeError("ADMIN_PASSWORD environment variable is required for secure startup.")
    # Record application start time for health endpoints
    app.state.started_at = datetime.utcnow()
    await init_db()
    
    # Run automatic migrations
    try:
        logger.info("🔍 Checking for database migrations...")
        migration_results = await migration_manager.check_and_run_migrations()
        ensure_scheduler_outcome_migration(migration_results)
        
        if migration_results['migrations_run']:
            logger.info(f"✅ Applied {len(migration_results['migrations_run'])} migrations")
            for migration in migration_results['migrations_run']:
                logger.info(f"   - {migration['id']}: {migration['description']}")
        if migration_results['errors']:
            logger.warning(f"⚠️ Migration errors: {migration_results['errors']}")
    except Exception as e:
        logger.error(f"❌ Migration system error: {e}")
        raise
    
    # Start universal database queue system
    from .services.database_queue import get_database_queue
    db_queue = get_database_queue()
    try:
        logger.info("🔄 Starting database queue...")
        await db_queue.start()
        logger.info("✅ Database queue started successfully")
    except Exception as e:
        logger.error(f"❌ Database queue startup error: {e}")
        import traceback
        traceback.print_exc()
        # Don't prevent app startup on queue errors

    # Pre-warm category cache
    from .services.category_cache import get_category_cache
    try:
        logger.info("🔄 Loading category cache...")
        category_cache = get_category_cache()
        categories = await category_cache.get_categories(force_refresh=True)
        logger.info(f"✅ Category cache loaded with {len(categories)} categories")
    except Exception as e:
        logger.warning(f"⚠️ Category cache preload warning: {e}")
        # Don't prevent app startup
    
    # Start task scheduler
    if settings.scheduler_enabled:
        from .services.scheduler import start_scheduler
        try:
            logger.info("🔄 Starting task scheduler...")
            await start_scheduler()
            logger.info("✅ Task scheduler started successfully")
        except Exception as e:
            logger.error(f"❌ Task scheduler startup error: {e}")
            import traceback
            traceback.print_exc()
            # Don't prevent app startup on scheduler errors
    else:
        logger.info("⏸️ Task scheduler disabled (SCHEDULER_ENABLED=false)")
    
    # Start process monitor for browser health checks
    from .services.process_monitor import start_process_monitor
    try:
        logger.info("🔍 Starting process monitor...")
        await start_process_monitor()
        logger.info("✅ Process monitor started successfully")
    except Exception as e:
        logger.error(f"❌ Process monitor startup error: {e}")
    # Start pool cleanup task to prevent connection leaks
    import asyncio
    async def pool_cleanup_task():
        """Periodic pool cleanup to prevent connection leaks."""
        while True:
            await asyncio.sleep(300)  # Every 5 minutes
            try:
                from .database import force_pool_cleanup
                await force_pool_cleanup()
            except Exception as e:
                logger.info(f"🧹 Pool cleanup error: {e}")
    # Start cleanup task in background
    cleanup_task = asyncio.create_task(pool_cleanup_task())
    app.state.cleanup_task = cleanup_task
    
    yield
    
    # Shutdown
    # Cancel pool cleanup task
    if hasattr(app.state, 'cleanup_task'):
        app.state.cleanup_task.cancel()
        try:
            await app.state.cleanup_task
        except asyncio.CancelledError:
            pass
    
    from .services.scheduler import stop_scheduler
    await stop_scheduler()
    
    # Stop process monitor
    from .services.process_monitor import stop_process_monitor
    await stop_process_monitor()
    
    # Force cleanup ContentExtractor on shutdown
    from .extraction import cleanup_content_extractor
    try:
        await cleanup_content_extractor()
        logger.info("✅ ContentExtractor cleanup completed")
    except Exception as e:
        logger.warning(f"⚠️ ContentExtractor cleanup error: {e}")
    # Stop database queue system
    await db_queue.stop()
    
    # Close database connections
    try:
        from .database import close_db_engine
        await close_db_engine()
        logger.info("✅ Database connections closed")
    except Exception as e:
        logger.warning(f"⚠️ Database cleanup error: {e}")
app = FastAPI(
    title="Evening News v2",
    description="Modern news aggregator with deduplication and web management",
    version="2.0.0",
    lifespan=lifespan,
    redirect_slashes=False,  # Disable automatic slash redirects that can break HTTPS
    root_path="",  # Empty root path for proper URL generation behind proxy
    # Tell FastAPI we're behind a proxy
    servers=[
        {"url": "https://news.dzarlax.dev", "description": "Production server"},
        {"url": "http://localhost:8000", "description": "Local development"},
    ]
)

# Add CORS middleware to allow cross-origin requests
# Restrict to explicit origins; can be overridden via ALLOWED_ORIGINS env
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.get_allowed_origins_list(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Trust hosts explicitly to avoid host header spoofing
trusted_hosts = settings.get_trusted_hosts_list()
app.add_middleware(TrustedHostMiddleware, allowed_hosts=trusted_hosts + ["*"] if settings.development else trusted_hosts)

# Add ProxyHeadersMiddleware to handle HTTPS behind reverse proxy
# Note: keep only when running behind a trusted proxy configured above
app.add_middleware(ProxyHeadersMiddleware)

# Статические файлы
app.mount("/static", StaticFiles(directory="web/static"), name="static")


# Шаблоны
templates = Jinja2Templates(directory="web/templates")
templates.env.globals["config"] = settings

# API routes - New modular architecture
from .api import router as api_router
app.include_router(api_router, prefix="/api/v1")

# Auth routes (optional - only if JWT is available)
try:
    from .auth_api import router as auth_router
    app.include_router(auth_router)
    logger.info("✅ Authentication enabled")
except ImportError as e:
    logger.warning(f"⚠️ Authentication disabled (missing dependency): {e}")
    logger.info("ℹ️ Install PyJWT to enable authentication: pip install PyJWT")
# Admin routes (optional - only if auth dependencies are available)
try:
    from fastapi.responses import RedirectResponse
    from .admin import router as admin_router
    app.include_router(admin_router, prefix="/admin")

    # Add redirect from /admin to /admin/ (needed because redirect_slashes=False)
    @app.get("/admin")
    async def redirect_to_admin():
        return RedirectResponse(url="/admin/", status_code=307)

    logger.info("✅ Admin interface enabled")
except ImportError as e:
    logger.warning(f"⚠️ Admin interface disabled (missing dependency): {e}")
# Public routes
from .public import router as public_router
app.include_router(public_router)


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    """Main page - news list (default view)."""
    return templates.TemplateResponse(request, "public/list.html", {
        "title": "Лента новостей"
    })



@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "healthy"}


@app.get("/health/app")
async def health_app():
    """Application health info with version and start time."""
    started_at = getattr(app.state, "started_at", None)
    started_at_iso = started_at.isoformat() if started_at else None
    uptime_seconds = (datetime.utcnow() - started_at).total_seconds() if started_at else None
    return {
        "version": app.version,
        "started_at": started_at_iso,
        "uptime_seconds": uptime_seconds
    }


@app.get("/auth-status")
async def auth_status():
    """Check authentication configuration status."""
    from .auth import get_admin_auth_status
    return get_admin_auth_status()


@app.get("/api/public/admin-check")
async def admin_check(request: Request):
    """Check if current user has admin access (via Authentik or JWT cookie)."""
    from .config import settings
    from .security import verify_jwt_token
    # Authentik ForwardAuth
    if settings.trust_forward_auth and request.headers.get("X-authentik-username"):
        return {"admin": True, "user": request.headers.get("X-authentik-username")}
    # JWT cookie
    cookie_token = request.cookies.get("admin_token")
    if cookie_token:
        payload = verify_jwt_token(cookie_token)
        if payload and payload.get("level") == "admin":
            return {"admin": True, "user": payload.get("sub", "admin")}
    return {"admin": False}


if __name__ == "__main__":
    import sys
    
    # Check if running as CLI
    if len(sys.argv) > 1 and sys.argv[1] in ['process', 'sources', 'add-source', 'test-source', 'stats', 'config']:
        from .cli import cli
        cli()
    else:
        # Run as web server
        import uvicorn
        uvicorn.run(
            "news_aggregator.main:app",
            host="0.0.0.0",
            port=8000,
            reload=settings.development
        )
