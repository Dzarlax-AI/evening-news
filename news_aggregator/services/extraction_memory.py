import logging
"""Extraction memory service with database persistence."""

import asyncio
from collections import deque
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy import text
from ..database import AsyncSessionLocal

logger = logging.getLogger(__name__)

# Limits to prevent unbounded memory growth
MAX_ATTEMPTS_IN_MEMORY = 500
MAX_PATTERNS_PER_DOMAIN = 10
MAX_DOMAINS_IN_MEMORY = 500
MAX_PATTERNS_IN_MEMORY = 2000
MAX_DOMAINS_STATS = 500


@dataclass
class ExtractionMemoryEntry:
    """Memory entry for extraction patterns."""
    domain: str
    selector_pattern: str
    extraction_strategy: str
    success_count: int = 0
    failure_count: int = 0
    success_rate: float = 0.0
    quality_score_avg: float = 0.0
    content_length_avg: int = 0
    discovered_by: str = "manual"
    is_stable: bool = False
    consecutive_successes: int = 0
    consecutive_failures: int = 0
    first_success_at: Optional[datetime] = None
    last_success_at: Optional[datetime] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    id: Optional[int] = None


@dataclass
class ExtractionAttempt:
    """Record of an extraction attempt."""
    article_url: str
    domain: str
    extraction_strategy: str
    selector_used: Optional[str] = None
    success: bool = False
    content_length: Optional[int] = None
    quality_score: Optional[float] = None
    extraction_time_ms: Optional[int] = None
    error_message: Optional[str] = None
    ai_analysis_triggered: bool = False
    ai_analysis: Optional[Dict] = None
    user_agent: Optional[str] = None
    http_status_code: Optional[int] = None


class ExtractionMemoryService:
    """Extraction learning service with database persistence and in-memory cache."""

    def __init__(self):
        # In-memory cache for fast lookups
        self._patterns: Dict[str, List[ExtractionMemoryEntry]] = {}
        self._attempts: deque = deque(maxlen=MAX_ATTEMPTS_IN_MEMORY)
        self._domain_stats: Dict[str, Dict] = {}
        self._initialized = False
        self._init_lock = asyncio.Lock()

    async def _ensure_initialized(self):
        """Load patterns from database on first access."""
        if self._initialized:
            return

        async with self._init_lock:
            if self._initialized:
                return

            try:
                await self._load_patterns_from_db()
                self._initialized = True
                logger.info(f"  ✅ Loaded {sum(len(p) for p in self._patterns.values())} patterns from database")
            except Exception as e:
                logger.warning(f"  ⚠️ Failed to load patterns from DB, using empty cache: {e}")
                self._initialized = True

    async def _load_patterns_from_db(self):
        """Load the best bounded pattern set without materializing the full table."""
        async with AsyncSessionLocal() as session:
            result = await session.execute(text("""
                WITH pattern_stats AS (
                    SELECT id, domain, selector_pattern, extraction_strategy,
                           COALESCE(success_count, 0) AS success_count,
                           COALESCE(failure_count, 0) AS failure_count,
                           CASE WHEN COALESCE(success_count, 0) + COALESCE(failure_count, 0) > 0
                                THEN COALESCE(success_count, 0) * 1.0 /
                                     (COALESCE(success_count, 0) + COALESCE(failure_count, 0))
                                WHEN discovered_by = 'ai' THEN COALESCE(quality_score_avg, 0)
                                ELSE 0 END AS success_rate,
                           quality_score_avg,
                           content_length_avg, discovered_by, is_stable,
                           consecutive_successes, consecutive_failures,
                           first_success_at, last_success_at, created_at, updated_at
                    FROM extraction_patterns
                    WHERE success_count > 0 OR failure_count < 5
                ), eligible AS (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY domain
                               ORDER BY success_rate DESC,
                                        success_count DESC,
                                        COALESCE(consecutive_failures, 0) ASC,
                                        COALESCE(consecutive_successes, 0) DESC,
                                        last_success_at DESC NULLS LAST,
                                        COALESCE(quality_score_avg, 0) DESC,
                                        selector_pattern ASC,
                                        extraction_strategy ASC
                           ) AS pattern_rank
                    FROM pattern_stats
                ), domain_scores AS (
                    SELECT domain,
                           MAX(CASE WHEN pattern_rank = 1 THEN success_rate END) AS best_success_rate,
                           MAX(CASE WHEN pattern_rank = 1 THEN success_count END) AS best_success_count,
                           MIN(CASE WHEN pattern_rank = 1 THEN consecutive_failures END) AS best_consecutive_failures,
                           MAX(CASE WHEN pattern_rank = 1 THEN consecutive_successes END) AS best_consecutive_successes,
                           MAX(CASE WHEN pattern_rank = 1 THEN last_success_at END) AS best_last_success_at,
                           MAX(CASE WHEN pattern_rank = 1 THEN COALESCE(quality_score_avg, 0) END) AS best_quality,
                           MAX(CASE WHEN pattern_rank = 1 THEN selector_pattern END) AS best_selector,
                           MAX(CASE WHEN pattern_rank = 1 THEN extraction_strategy END) AS best_strategy
                    FROM eligible
                    WHERE pattern_rank <= :per_domain_limit
                    GROUP BY domain
                ), ranked_domains AS (
                    SELECT domain,
                           ROW_NUMBER() OVER (
                               ORDER BY best_success_rate DESC,
                                        best_success_count DESC,
                                        best_consecutive_failures ASC,
                                        best_consecutive_successes DESC,
                                        best_last_success_at DESC NULLS LAST,
                                        best_quality DESC,
                                        best_selector ASC,
                                        best_strategy ASC,
                                        domain ASC
                           ) AS domain_rank
                    FROM domain_scores
                )
                SELECT e.id, e.domain, e.selector_pattern, e.extraction_strategy,
                       e.success_count, e.failure_count, e.quality_score_avg,
                       e.content_length_avg, e.discovered_by, e.is_stable,
                       e.consecutive_successes, e.consecutive_failures,
                       e.first_success_at, e.last_success_at, e.created_at, e.updated_at
                FROM eligible e
                JOIN ranked_domains d ON d.domain = e.domain
                WHERE e.pattern_rank <= :per_domain_limit
                  AND d.domain_rank <= :domain_limit
                ORDER BY e.success_rate DESC,
                         e.success_count DESC,
                         COALESCE(e.consecutive_failures, 0) ASC,
                         COALESCE(e.consecutive_successes, 0) DESC,
                         e.last_success_at DESC NULLS LAST,
                         COALESCE(e.quality_score_avg, 0) DESC,
                         e.selector_pattern ASC,
                         e.extraction_strategy ASC,
                         e.domain ASC
                LIMIT :total_limit
            """), {
                "per_domain_limit": MAX_PATTERNS_PER_DOMAIN,
                "domain_limit": MAX_DOMAINS_IN_MEMORY,
                "total_limit": MAX_PATTERNS_IN_MEMORY,
            })

            for row in result.fetchall():
                domain = row[1]
                if domain not in self._patterns:
                    self._patterns[domain] = []

                success_count = row[4] or 0
                failure_count = row[5] or 0
                quality_score = float(row[6] or 0)
                discovered_by = row[8] or 'manual'
                if success_count + failure_count:
                    success_rate = self._calc_success_rate(success_count, failure_count)
                elif discovered_by == 'ai':
                    success_rate = quality_score * 100
                else:
                    success_rate = 0.0

                entry = ExtractionMemoryEntry(
                    id=row[0],
                    domain=domain,
                    selector_pattern=row[2],
                    extraction_strategy=row[3],
                    success_count=success_count,
                    failure_count=failure_count,
                    success_rate=success_rate,
                    quality_score_avg=quality_score,
                    content_length_avg=row[7] or 0,
                    discovered_by=discovered_by,
                    is_stable=row[9] or False,
                    consecutive_successes=row[10] or 0,
                    consecutive_failures=row[11] or 0,
                    first_success_at=row[12],
                    last_success_at=row[13],
                    created_at=row[14],
                    updated_at=row[15]
                )
                self._patterns[domain].append(entry)
            self._enforce_pattern_limits()

    @staticmethod
    def _pattern_rank(pattern: ExtractionMemoryEntry):
        """Rank patterns by evidence of successful, recent extraction."""
        successes = int(pattern.success_count or 0)
        failures = int(pattern.failure_count or 0)
        total_attempts = successes + failures
        if total_attempts:
            success_rate = successes / total_attempts
        elif pattern.discovered_by == "ai":
            # AI confidence is the admission signal until an extraction is observed.
            confidence = float(pattern.success_rate or 0) / 100
            if not confidence:
                confidence = float(pattern.quality_score_avg or 0)
            success_rate = confidence
        else:
            success_rate = 0.0
        last_success_at = pattern.last_success_at
        if isinstance(last_success_at, str):
            try:
                last_success_at = datetime.fromisoformat(last_success_at)
            except ValueError:
                last_success_at = None
        if last_success_at is not None and last_success_at.tzinfo is not None:
            last_success_at = last_success_at.replace(tzinfo=None)
        return (
            -success_rate,
            -successes,
            int(pattern.consecutive_failures or 0),
            -int(pattern.consecutive_successes or 0),
            last_success_at is None,
            -(last_success_at.timestamp()) if last_success_at is not None else 0,
            -float(pattern.quality_score_avg or 0),
            pattern.selector_pattern,
            pattern.extraction_strategy,
            pattern.domain,
        )

    def _enforce_pattern_limits(self) -> None:
        """Apply the same deterministic admission limits to every insertion path."""
        nonempty = {
            domain: sorted(self._unique_patterns(patterns), key=self._pattern_rank)[:MAX_PATTERNS_PER_DOMAIN]
            for domain, patterns in self._patterns.items()
            if patterns
        }
        domains_by_quality = sorted(
            nonempty,
            key=lambda domain: (self._pattern_rank(nonempty[domain][0]), domain),
        )[:MAX_DOMAINS_IN_MEMORY]
        candidates = [pattern for domain in domains_by_quality for pattern in nonempty[domain]]
        candidates.sort(key=self._pattern_rank)
        admitted = candidates[:MAX_PATTERNS_IN_MEMORY]
        rebuilt: Dict[str, List[ExtractionMemoryEntry]] = {}
        for pattern in admitted:
            rebuilt.setdefault(pattern.domain, []).append(pattern)
        self._patterns = rebuilt

    def _unique_patterns(self, patterns):
        """Keep one cumulative snapshot per identity, without doubling evidence."""
        unique = {}
        def snapshot_rank(entry):
            observations = (entry.success_count or 0) + (entry.failure_count or 0)
            return (-observations, self._pattern_rank(entry))
        for pattern in patterns:
            key = (pattern.selector_pattern, pattern.extraction_strategy)
            existing = unique.get(key)
            if existing is None or snapshot_rank(pattern) < snapshot_rank(existing):
                unique[key] = pattern
        return list(unique.values())

    def _calc_success_rate(self, success_count: int, failure_count: int) -> float:
        """Calculate success rate percentage."""
        total = success_count + failure_count
        return (success_count / total * 100) if total > 0 else 0.0

    async def _save_pattern_to_db(self, entry: ExtractionMemoryEntry):
        """Save or update a pattern in the database."""
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("""
                    INSERT INTO extraction_patterns
                        (domain, selector_pattern, extraction_strategy,
                         success_count, failure_count, quality_score_avg,
                         content_length_avg, discovered_by, is_stable,
                         consecutive_successes, consecutive_failures,
                         first_success_at, last_success_at)
                    VALUES
                        (:domain, :selector, :strategy,
                         :success_count, :failure_count, :quality_avg,
                         :length_avg, :discovered_by, :is_stable,
                         :consec_success, :consec_failure,
                         :first_success, :last_success)
                    ON CONFLICT (domain, selector_pattern, extraction_strategy) DO UPDATE SET
                    success_count = EXCLUDED.success_count,
                    failure_count = EXCLUDED.failure_count,
                    quality_score_avg = EXCLUDED.quality_score_avg,
                    content_length_avg = EXCLUDED.content_length_avg,
                    is_stable = EXCLUDED.is_stable,
                    consecutive_successes = EXCLUDED.consecutive_successes,
                    consecutive_failures = EXCLUDED.consecutive_failures,
                    last_success_at = EXCLUDED.last_success_at,
                    updated_at = NOW()
                """), {
                    'domain': entry.domain,
                    'selector': entry.selector_pattern,
                    'strategy': entry.extraction_strategy,
                    'success_count': entry.success_count,
                    'failure_count': entry.failure_count,
                    'quality_avg': Decimal(str(entry.quality_score_avg)),
                    'length_avg': entry.content_length_avg,
                    'discovered_by': entry.discovered_by,
                    'is_stable': entry.is_stable,
                    'consec_success': entry.consecutive_successes,
                    'consec_failure': entry.consecutive_failures,
                    'first_success': entry.first_success_at,
                    'last_success': entry.last_success_at
                })
                await session.commit()
        except Exception as e:
            logger.warning(f"  ⚠️ Failed to save pattern to DB: {e}")

    async def _save_ai_discovery_to_db(self, entry: ExtractionMemoryEntry):
        """Refresh an AI prior without erasing an observed row outside the cache."""
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("""
                    INSERT INTO extraction_patterns
                        (domain, selector_pattern, extraction_strategy, quality_score_avg, discovered_by)
                    VALUES (:domain, :selector, :strategy, :confidence, 'ai')
                    ON CONFLICT (domain, selector_pattern, extraction_strategy) DO UPDATE SET
                        quality_score_avg = CASE
                            WHEN COALESCE(extraction_patterns.success_count, 0) +
                                 COALESCE(extraction_patterns.failure_count, 0) = 0
                            THEN GREATEST(COALESCE(extraction_patterns.quality_score_avg, 0), EXCLUDED.quality_score_avg)
                            ELSE extraction_patterns.quality_score_avg END,
                        discovered_by = CASE
                            WHEN COALESCE(extraction_patterns.success_count, 0) +
                                 COALESCE(extraction_patterns.failure_count, 0) = 0
                            THEN 'ai' ELSE extraction_patterns.discovered_by END,
                        updated_at = NOW()
                """), {'domain': entry.domain, 'selector': entry.selector_pattern,
                        'strategy': entry.extraction_strategy,
                        'confidence': Decimal(str(entry.quality_score_avg))})
                await session.commit()
        except Exception as e:
            logger.warning(f"  ⚠️ Failed to save AI pattern discovery: {e}")
    async def _save_attempt_to_db(self, attempt: ExtractionAttempt):
        """Save extraction attempt to database for analytics."""
        try:
            async with AsyncSessionLocal() as session:
                await session.execute(text("""
                    INSERT INTO extraction_attempts
                        (article_url, domain, extraction_strategy, selector_used,
                         success, content_length, quality_score, extraction_time_ms,
                         error_message, ai_analysis_triggered, user_agent, http_status_code)
                    VALUES
                        (:url, :domain, :strategy, :selector,
                         :success, :length, :quality, :time_ms,
                         :error, :ai_triggered, :user_agent, :status_code)
                """), {
                    'url': attempt.article_url,
                    'domain': attempt.domain,
                    'strategy': attempt.extraction_strategy,
                    'selector': attempt.selector_used,
                    'success': attempt.success,
                    'length': attempt.content_length,
                    'quality': Decimal(str(attempt.quality_score)) if attempt.quality_score else None,
                    'time_ms': attempt.extraction_time_ms,
                    'error': attempt.error_message,
                    'ai_triggered': attempt.ai_analysis_triggered,
                    'user_agent': attempt.user_agent,
                    'status_code': attempt.http_status_code
                })
                await session.commit()
        except Exception as e:
            logger.warning(f"  ⚠️ Failed to save attempt to DB: {e}")
    async def record_extraction_attempt(self, attempt: ExtractionAttempt) -> bool:
        """Record an extraction attempt."""
        try:
            await self._ensure_initialized()

            self._attempts.append(attempt)

            # Save to database asynchronously
            asyncio.create_task(self._save_attempt_to_db(attempt))

            # Update in-memory stats
            domain = attempt.domain
            if domain not in self._domain_stats:
                # Evict least-active domain if over limit
                if len(self._domain_stats) >= MAX_DOMAINS_STATS:
                    least_active = min(
                        self._domain_stats,
                        key=lambda d: self._domain_stats[d]['total_attempts'],
                    )
                    del self._domain_stats[least_active]
                self._domain_stats[domain] = {
                    'total_attempts': 0,
                    'successful_attempts': 0,
                    'methods': {}
                }

            stats = self._domain_stats[domain]
            stats['total_attempts'] += 1

            if attempt.success:
                stats['successful_attempts'] += 1

                # Update method stats
                method = attempt.extraction_strategy
                if method not in stats['methods']:
                    stats['methods'][method] = {'attempts': 0, 'successes': 0}

                stats['methods'][method]['attempts'] += 1
                stats['methods'][method]['successes'] += 1

                # Add pattern if selector was used
                if attempt.selector_used:
                    await self._add_successful_pattern(
                        domain, attempt.selector_used, attempt.extraction_strategy,
                        attempt.quality_score or 0, attempt.content_length or 0
                    )
            else:
                # Update method stats for failures
                method = attempt.extraction_strategy
                if method not in stats['methods']:
                    stats['methods'][method] = {'attempts': 0, 'successes': 0}

                stats['methods'][method]['attempts'] += 1

            logger.info(f"  📝 Recorded {attempt.extraction_strategy} {'success' if attempt.success else 'failure'} for {domain}")
            return True

        except Exception as e:
            logger.error(f"❌ Error recording extraction attempt: {e}")
            return False
    
    async def _add_successful_pattern(
        self, domain: str, selector: str, strategy: str,
        quality_score: float, content_length: int
    ):
        """Add or update successful pattern."""
        if domain not in self._patterns:
            self._patterns[domain] = []
        self._patterns[domain] = self._unique_patterns(self._patterns[domain])
        
        # Find existing pattern
        existing = None
        for pattern in self._patterns[domain]:
            if pattern.selector_pattern == selector and pattern.extraction_strategy == strategy:
                existing = pattern
                break
        
        if existing:
            # Update existing pattern
            existing.success_count += 1
            existing.consecutive_successes += 1
            existing.consecutive_failures = 0

            # Update averages
            total_successes = existing.success_count
            existing.quality_score_avg = (
                (existing.quality_score_avg * (total_successes - 1) + quality_score) /
                total_successes
            )
            existing.content_length_avg = int(
                (existing.content_length_avg * (total_successes - 1) + content_length) /
                total_successes
            )

            # Recalculate success rate
            total_attempts = existing.success_count + existing.failure_count
            existing.success_rate = (existing.success_count / total_attempts) * 100 if total_attempts > 0 else 0

            existing.is_stable = existing.consecutive_successes >= 3
            existing.last_success_at = datetime.utcnow()
            existing.updated_at = datetime.utcnow()

            # Save to database
            asyncio.create_task(self._save_pattern_to_db(existing))
        else:
            # Create new pattern
            pattern = ExtractionMemoryEntry(
                domain=domain,
                selector_pattern=selector,
                extraction_strategy=strategy,
                success_count=1,
                failure_count=0,
                success_rate=100.0,
                quality_score_avg=quality_score,
                content_length_avg=content_length,
                consecutive_successes=1,
                consecutive_failures=0,
                is_stable=False,
                first_success_at=datetime.utcnow(),
                last_success_at=datetime.utcnow(),
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow()
            )
            self._patterns[domain].append(pattern)

            # Save to database
            asyncio.create_task(self._save_pattern_to_db(pattern))

        self._enforce_pattern_limits()

        logger.info(f"  🎯 Updated pattern: {selector[:40]}... for {domain}")

    def _evict_patterns(self, domain: str):
        """Compatibility wrapper for bounded deterministic admission."""
        self._enforce_pattern_limits()

    async def _add_failed_pattern(
        self, domain: str, selector: str, strategy: str
    ) -> None:
        """Record failed usage of an existing pattern to decrease its weight."""
        if not selector:
            return
        if domain not in self._patterns:
            self._patterns[domain] = []
        self._patterns[domain] = self._unique_patterns(self._patterns[domain])

        # Find existing pattern or create a new failed one
        existing = None
        for pattern in self._patterns[domain]:
            if pattern.selector_pattern == selector and pattern.extraction_strategy == strategy:
                existing = pattern
                break

        if existing:
            existing.failure_count += 1
            existing.consecutive_failures += 1
            existing.consecutive_successes = 0
            total_attempts = existing.success_count + existing.failure_count
            existing.success_rate = (existing.success_count / total_attempts) * 100 if total_attempts > 0 else 0
            existing.is_stable = False
            existing.updated_at = datetime.utcnow()

            # Save to database
            asyncio.create_task(self._save_pattern_to_db(existing))
        else:
            # Create a new record with one failure to track negatives
            new_pattern = ExtractionMemoryEntry(
                domain=domain,
                selector_pattern=selector,
                extraction_strategy=strategy,
                success_count=0,
                failure_count=1,
                success_rate=0.0,
                is_stable=False,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            self._patterns[domain].append(new_pattern)

            # Save to database
            asyncio.create_task(self._save_pattern_to_db(new_pattern))

        self._enforce_pattern_limits()

    async def degrade_pattern(self, domain: str, selector: str, strategy: str) -> None:
        """Public API to decrease a selector's weight after poor result."""
        try:
            await self._add_failed_pattern(domain, selector, strategy)
            logger.info(f"  ⬇️ Degraded pattern for {domain}: {selector[:40]}... ({strategy})")
        except Exception as e:
            logger.error(f"❌ Error degrading pattern: {e}")
    async def record_date_selector_success(self, domain: str, selector: str) -> None:
        """Record successful date extraction using a selector (stored as 'date_selector')."""
        await self._add_successful_pattern(domain, selector, 'date_selector', quality_score=0.0, content_length=0)

    async def record_date_selector_failure(self, domain: str, selector: str) -> None:
        """Record failed date extraction for a selector (stored as 'date_selector')."""
        await self._add_failed_pattern(domain, selector, 'date_selector')

    async def get_best_date_selectors_for_domain(self, domain: str, limit: int = 5) -> List[ExtractionMemoryEntry]:
        """Return best date selectors learned for the domain."""
        return await self.get_best_patterns_for_domain(domain, strategy='date_selector', limit=limit)
    
    async def get_best_patterns_for_domain(
        self, domain: str, strategy: Optional[str] = None, limit: int = 5
    ) -> List[ExtractionMemoryEntry]:
        """Get best extraction patterns for a domain."""
        await self._ensure_initialized()

        if domain not in self._patterns:
            return []

        patterns = self._patterns[domain]

        if strategy:
            patterns = [p for p in patterns if p.extraction_strategy == strategy]

        # Sort by success rate, then by consecutive successes
        patterns = sorted(
            patterns,
            key=lambda p: (p.success_rate, p.consecutive_successes, p.success_count),
            reverse=True
        )

        return patterns[:limit]
    
    async def get_domain_extraction_stats(self, domain: str) -> Dict:
        """Get extraction statistics for a domain."""
        if domain not in self._domain_stats:
            return {
                'domain': domain,
                'total_attempts': 0,
                'successful_attempts': 0,
                'success_rate': 0.0,
                'avg_extraction_time_ms': 0,
                'avg_quality_score': 0,
                'last_attempt': None,
                'methods': []
            }
        
        stats = self._domain_stats[domain]
        success_rate = (
            (stats['successful_attempts'] / stats['total_attempts'] * 100) 
            if stats['total_attempts'] > 0 else 0
        )
        
        methods = []
        for method_name, method_stats in stats['methods'].items():
            method_success_rate = (
                (method_stats['successes'] / method_stats['attempts'] * 100) 
                if method_stats['attempts'] > 0 else 0
            )
            methods.append({
                'strategy': method_name,
                'attempts': method_stats['attempts'],
                'successes': method_stats['successes'],
                'success_rate': method_success_rate,
                'avg_time_ms': 0  # Not tracked in simple version
            })
        
        return {
            'domain': domain,
            'total_attempts': stats['total_attempts'],
            'successful_attempts': stats['successful_attempts'],
            'success_rate': success_rate,
            'avg_extraction_time_ms': 0,  # Not tracked in simple version
            'avg_quality_score': 0,  # Not tracked in simple version
            'last_attempt': None,  # Not tracked in simple version
            'methods': methods
        }

    async def get_domain_success_rates(self, limit: int = 20, days: int = 7) -> List[Dict]:
        """Get per-domain extraction success rates from persisted attempts."""
        since_date = datetime.utcnow() - timedelta(days=days)
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                text("""
                    SELECT
                        domain,
                        COUNT(*) as total_attempts,
                        SUM(CASE WHEN success THEN 1 ELSE 0 END) as successful_attempts,
                        AVG(extraction_time_ms) as avg_time_ms,
                        AVG(quality_score) as avg_quality_score
                    FROM extraction_attempts
                    WHERE created_at >= :since_date
                    GROUP BY domain
                    ORDER BY total_attempts DESC
                    LIMIT :limit
                """),
                {"since_date": since_date, "limit": limit}
            )

            domain_stats = []
            for row in result.fetchall():
                domain = row.domain
                total_attempts = int(row.total_attempts or 0)
                successful_attempts = int(row.successful_attempts or 0)
                success_rate = (successful_attempts / total_attempts * 100) if total_attempts > 0 else 0.0

                domain_stats.append({
                    "domain": domain,
                    "total_attempts": total_attempts,
                    "successful_attempts": successful_attempts,
                    "success_rate": round(success_rate, 2),
                    "avg_extraction_time_ms": float(row.avg_time_ms or 0),
                    "avg_quality_score": float(row.avg_quality_score or 0)
                })

            return domain_stats
    
    async def record_page_structure(self, domain: str, analysis: 'PageAnalysis') -> bool:
        """Record page structure discovered by AI analysis."""
        try:
            patterns = []
            
            # Record content/container selectors
            for sel in analysis.content_selectors:
                patterns.append({
                    'selector': sel,
                    'strategy': 'source_container',
                    'confidence': analysis.confidence_scores.get(sel, 0.8)
                })
            
            # Record title selectors
            for sel in analysis.title_selectors:
                patterns.append({
                    'selector': sel,
                    'strategy': 'source_title',
                    'confidence': 0.9
                })
                
            # Record link selectors
            if hasattr(analysis, 'link_selectors') and analysis.link_selectors:
                for sel in analysis.link_selectors:
                    patterns.append({
                        'selector': sel,
                        'strategy': 'source_link',
                        'confidence': 0.9
                    })
            
            # Record date selectors
            for sel in analysis.date_selectors:
                patterns.append({
                    'selector': sel,
                    'strategy': 'source_date',
                    'confidence': 0.8
                })
                
            # Use record_ai_pattern_discovery to save them all
            return await self.record_ai_pattern_discovery(
                domain, 
                patterns, 
                analysis_type="page_structure_discovery"
            )
            
        except Exception as e:
            logger.error(f"❌ Error recording page structure: {e}")
            return False

    async def get_page_structure(self, domain: str) -> Dict[str, List[str]]:
        """Get best page structure selectors learned for a domain."""
        await self._ensure_initialized()
        
        result = {
            'container_selectors': [],
            'title_selectors': [],
            'link_selectors': [],
            'date_selectors': []
        }
        
        if domain not in self._patterns:
            return result
            
        patterns = self._patterns[domain]
        
        # Map strategy to result key
        mapping = {
            'source_container': 'container_selectors',
            'source_title': 'title_selectors',
            'source_link': 'link_selectors',
            'source_date': 'date_selectors'
        }
        
        for strategy, key in mapping.items():
            best = [
                p.selector_pattern for p in patterns 
                if p.extraction_strategy == strategy and p.success_rate >= 50
            ]
            # If none with >50% success (AI discovered are often high confidence), take all
            if not best:
                best = [
                    p.selector_pattern for p in patterns 
                    if p.extraction_strategy == strategy
                ]
            result[key] = best
            
        return result

    async def record_ai_pattern_discovery(
        self, domain: str, patterns: List[Dict], analysis_type: str = "selector_discovery"
    ) -> bool:
        """Record AI-discovered patterns."""
        try:
            await self._ensure_initialized()
            for pattern_data in patterns:
                selector = pattern_data.get('selector')
                strategy = pattern_data.get('strategy', 'ai_discovered')
                confidence = pattern_data.get('confidence', 0.5)
                
                if selector:
                    if domain not in self._patterns:
                        self._patterns[domain] = []
                    self._patterns[domain] = self._unique_patterns(self._patterns[domain])
                    existing = next((entry for entry in self._patterns[domain]
                                     if entry.selector_pattern == selector
                                     and entry.extraction_strategy == strategy), None)
                    if existing is not None:
                        if not ((existing.success_count or 0) + (existing.failure_count or 0)):
                            if existing.discovered_by == 'ai':
                                confidence = max(confidence, (existing.success_rate or 0) / 100,
                                                 existing.quality_score_avg or 0)
                            existing.success_rate = confidence * 100
                            existing.quality_score_avg = confidence
                            existing.discovered_by = 'ai'
                        existing.updated_at = datetime.utcnow()
                        asyncio.create_task(self._save_ai_discovery_to_db(existing))
                        self._enforce_pattern_limits()
                        continue
                    
                    # Add AI-discovered pattern
                    pattern = ExtractionMemoryEntry(
                        domain=domain,
                        selector_pattern=selector,
                        extraction_strategy=strategy,
                        success_count=0,
                        failure_count=0,
                        success_rate=confidence * 100,
                        # success_rate is a generated database column; retain
                        # zero-attempt AI confidence in this persisted score.
                        quality_score_avg=confidence,
                        discovered_by='ai',
                        created_at=datetime.utcnow(),
                        updated_at=datetime.utcnow()
                    )
                    self._patterns[domain].append(pattern)
                    asyncio.create_task(self._save_ai_discovery_to_db(pattern))
                    self._enforce_pattern_limits()

            logger.info(f"  🤖 Recorded {len(patterns)} AI-discovered patterns for {domain}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error recording AI patterns: {e}")
            return False
    
    async def get_domains_needing_ai_analysis(self, limit: int = 10) -> List[str]:
        """Get domains that need AI analysis."""
        domains_needing_help = []
        
        for domain, stats in self._domain_stats.items():
            success_rate = (
                (stats['successful_attempts'] / stats['total_attempts'] * 100) 
                if stats['total_attempts'] > 0 else 0
            )
            
            # Domains with low success rate or many attempts need help
            if (success_rate < 50 and stats['total_attempts'] >= 3) or stats['total_attempts'] == 0:
                domains_needing_help.append(domain)
        
        return domains_needing_help[:limit]
    
    async def get_extraction_efficiency_stats(self) -> Dict:
        """Get overall extraction efficiency statistics."""
        if not self._domain_stats:
            return {
                'period': '7 days',
                'total_attempts': 0,
                'successful_attempts': 0,
                'success_rate': 0,
                'domains_processed': 0,
                'avg_extraction_time_ms': 0,
                'ai_analyses': 0,
                'patterns_discovered': 0,
                'patterns_successful': 0,
                'ai_cost_effectiveness': 0
            }
        
        total_attempts = sum(stats['total_attempts'] for stats in self._domain_stats.values())
        total_successes = sum(stats['successful_attempts'] for stats in self._domain_stats.values())
        success_rate = (total_successes / total_attempts * 100) if total_attempts > 0 else 0
        
        ai_patterns_count = sum(
            len([p for p in patterns if p.discovered_by == 'ai'])
            for patterns in self._patterns.values()
        )
        
        return {
            'period': 'session',
            'total_attempts': total_attempts,
            'successful_attempts': total_successes,
            'success_rate': success_rate,
            'domains_processed': len(self._domain_stats),
            'avg_extraction_time_ms': 0,
            'ai_analyses': 0,  # Not tracked in simple version
            'patterns_discovered': ai_patterns_count,
            'patterns_successful': 0,  # Not tracked in simple version
            'ai_cost_effectiveness': 0  # Not tracked in simple version
        }
    
    async def get_successful_pattern(self, domain: str) -> Optional[Dict]:
        """Get the most successful pattern for a domain as dict."""
        await self._ensure_initialized()

        if domain not in self._patterns:
            return None

        # Find patterns with success rate > 50% and success count > 0
        successful_patterns = [
            pattern for pattern in self._patterns[domain]
            if pattern.success_count > 0 and pattern.success_rate > 50.0
        ]

        if not successful_patterns:
            return None

        # Sort by success rate, then by success count
        successful_patterns.sort(
            key=lambda p: (p.success_rate, p.success_count),
            reverse=True
        )

        best = successful_patterns[0]
        return {
            'selector': best.selector_pattern,
            'method': best.extraction_strategy,
            'success_rate': best.success_rate,
            'success_count': best.success_count
        }


# Global instance
_extraction_memory_service = None

async def get_extraction_memory() -> ExtractionMemoryService:
    """Get or create extraction memory service instance."""
    global _extraction_memory_service
    if _extraction_memory_service is None:
        _extraction_memory_service = ExtractionMemoryService()
    return _extraction_memory_service
