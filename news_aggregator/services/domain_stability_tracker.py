import logging
"""Domain stability tracking for content extraction optimization."""

import time
import json
from typing import Dict, List, Optional, NamedTuple
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


@dataclass
class DomainExtractionStats:
    """Statistics for domain extraction performance."""
    domain: str
    total_attempts: int = 0
    successful_attempts: int = 0
    failed_attempts: int = 0
    last_success_timestamp: Optional[float] = None
    last_failure_timestamp: Optional[float] = None
    average_extraction_time_ms: float = 0.0
    success_rate: float = 0.0
    
    # Method performance tracking
    method_success_counts: Dict[str, int] = None
    method_failure_counts: Dict[str, int] = None
    method_avg_times: Dict[str, float] = None
    
    # Quality metrics
    average_content_length: float = 0.0
    average_quality_score: float = 0.0
    
    # Retry logic for complete failures
    consecutive_all_methods_failures: int = 0
    last_all_methods_failure_timestamp: Optional[float] = None
    
    def __post_init__(self):
        if self.method_success_counts is None:
            self.method_success_counts = {}
        if self.method_failure_counts is None:
            self.method_failure_counts = {}
        if self.method_avg_times is None:
            self.method_avg_times = {}
    
    def update_success(self, method: str, extraction_time_ms: int, 
                      content_length: int, quality_score: float):
        """Update stats after successful extraction."""
        self.total_attempts += 1
        self.successful_attempts += 1
        self.last_success_timestamp = time.time()
        
        # Update method stats
        self.method_success_counts[method] = self.method_success_counts.get(method, 0) + 1
        
        # Update timing
        current_avg = self.method_avg_times.get(method, 0.0)
        current_count = self.method_success_counts[method]
        self.method_avg_times[method] = (current_avg * (current_count - 1) + extraction_time_ms) / current_count
        
        # Update overall metrics
        self._recalculate_metrics(extraction_time_ms, content_length, quality_score)
    
    def update_failure(self, method: str, extraction_time_ms: int):
        """Update stats after failed extraction."""
        self.total_attempts += 1
        self.failed_attempts += 1
        self.last_failure_timestamp = time.time()
        
        # Update method stats
        self.method_failure_counts[method] = self.method_failure_counts.get(method, 0) + 1
        
        # Update overall metrics
        self._recalculate_metrics(extraction_time_ms, 0, 0)
    
    def _recalculate_metrics(self, extraction_time_ms: int, content_length: int, quality_score: float):
        """Recalculate derived metrics."""
        if self.total_attempts > 0:
            self.success_rate = self.successful_attempts / self.total_attempts
        
        # Update average extraction time
        if self.total_attempts > 1:
            self.average_extraction_time_ms = (
                (self.average_extraction_time_ms * (self.total_attempts - 1) + extraction_time_ms) / 
                self.total_attempts
            )
        else:
            self.average_extraction_time_ms = extraction_time_ms
        
        # Update content quality metrics (only for successful extractions)
        if content_length > 0:
            if self.successful_attempts > 1:
                self.average_content_length = (
                    (self.average_content_length * (self.successful_attempts - 1) + content_length) / 
                    self.successful_attempts
                )
                self.average_quality_score = (
                    (self.average_quality_score * (self.successful_attempts - 1) + quality_score) / 
                    self.successful_attempts
                )
            else:
                self.average_content_length = content_length
                self.average_quality_score = quality_score
    
    def get_best_method(self) -> Optional[str]:
        """Get the most successful extraction method for this domain."""
        if not self.method_success_counts:
            return None
        
        # Sort by success count, then by average time
        methods = []
        for method, success_count in self.method_success_counts.items():
            failure_count = self.method_failure_counts.get(method, 0)
            total_method_attempts = success_count + failure_count
            method_success_rate = success_count / total_method_attempts if total_method_attempts > 0 else 0
            avg_time = self.method_avg_times.get(method, float('inf'))
            
            methods.append((method, success_count, method_success_rate, avg_time))
        
        # Sort by success rate, then by success count, then by time
        methods.sort(key=lambda x: (x[2], x[1], -x[3]), reverse=True)
        return methods[0][0] if methods else None
    
    def get_method_performance(self) -> Dict[str, Dict[str, float]]:
        """Get detailed performance metrics for each method."""
        performance = {}
        
        for method in set(list(self.method_success_counts.keys()) + list(self.method_failure_counts.keys())):
            success_count = self.method_success_counts.get(method, 0)
            failure_count = self.method_failure_counts.get(method, 0)
            total_method_attempts = success_count + failure_count
            
            performance[method] = {
                'success_count': success_count,
                'failure_count': failure_count,
                'total_attempts': total_method_attempts,
                'success_rate': success_count / total_method_attempts if total_method_attempts > 0 else 0,
                'average_time_ms': self.method_avg_times.get(method, 0.0)
            }
        
        return performance
    
    def is_stable(self, min_attempts: int = 5, min_success_rate: float = 0.7) -> bool:
        """Check if domain extraction is considered stable."""
        return (
            self.total_attempts >= min_attempts and 
            self.success_rate >= min_success_rate
        )
    
    def needs_optimization(self, max_avg_time_ms: float = 5000, min_success_rate: float = 0.5) -> bool:
        """Check if domain needs extraction optimization."""
        return (
            self.success_rate < min_success_rate or 
            self.average_extraction_time_ms > max_avg_time_ms
        )


class DomainStabilityTracker:
    """Tracks extraction stability and performance across domains."""
    
    def __init__(self, persistence_file: str = "/app/data/domain_stats.json"):
        self.domain_stats: Dict[str, DomainExtractionStats] = {}
        self.ai_analysis_credits_used = 0
        self.ai_analysis_credits_saved = 0
        self.last_cleanup_time = time.time()
        self.persistence_file = persistence_file
        
        # Configuration
        self.cleanup_interval_hours = 24
        self.max_domain_age_days = 30
        self.min_attempts_for_stability = 5
        
        # Load existing stats on startup
        self._load_stats()
    
    def get_domain_stats(self, domain: str) -> DomainExtractionStats:
        """Get or create domain statistics."""
        if domain not in self.domain_stats:
            self.domain_stats[domain] = DomainExtractionStats(domain=domain)
        return self.domain_stats[domain]
    
    def update_domain_stats(self, domain: str, success: bool, 
                           extraction_time_ms: int = 0, content_length: int = 0, 
                           quality_score: float = 0.0, method: str = "unknown"):
        """Update domain extraction statistics."""
        stats = self.get_domain_stats(domain)
        
        if success:
            stats.update_success(method, extraction_time_ms, content_length, quality_score)
            # Reset consecutive failures counter on any success
            self.reset_all_methods_failures(domain)
        else:
            stats.update_failure(method, extraction_time_ms)
        
        # Save stats after each update
        self._save_stats()
        
        # Periodic cleanup
        if time.time() - self.last_cleanup_time > self.cleanup_interval_hours * 3600:
            self._cleanup_old_domains()
    
    def get_best_method_for_domain(self, domain: str) -> Optional[str]:
        """Get the best extraction method for a specific domain."""
        if domain not in self.domain_stats:
            return None
        return self.domain_stats[domain].get_best_method()
    
    def get_ineffective_methods_for_domain(self, domain: str, failure_threshold: float = 0.8, min_attempts: int = 2) -> List[str]:
        """Get methods that consistently fail for this domain."""
        if domain not in self.domain_stats:
            return []
        
        stats = self.domain_stats[domain]
        
        # If we had a recent success but it just failed, reset and try all methods
        current_time = time.time()
        if (stats.last_success_timestamp and stats.last_failure_timestamp and 
            stats.last_failure_timestamp > stats.last_success_timestamp and
            current_time - stats.last_failure_timestamp < 3600):  # Last failure within 1 hour
            # Recent success followed by failure - don't mark any methods as ineffective
            return []
        
        ineffective_methods = []
        
        for method, failure_count in stats.method_failure_counts.items():
            success_count = stats.method_success_counts.get(method, 0)
            total_attempts = failure_count + success_count
            
            # Lower threshold for immediate session learning
            if total_attempts >= min_attempts:
                failure_rate = failure_count / total_attempts
                
                # Special handling for Playwright - be less aggressive on challenging domains
                if method == 'playwright' and success_count > 0:
                    # For Playwright, require higher failure rate if it has had recent successes
                    # This allows Playwright to keep trying on anti-bot protected sites
                    playwright_threshold = 0.95  # Only blacklist if >95% failure rate
                    if failure_rate >= playwright_threshold:
                        ineffective_methods.append(method)
                elif failure_rate >= failure_threshold:
                    ineffective_methods.append(method)
        
        return ineffective_methods
    
    def should_skip_domain_temporarily(self, domain: str) -> tuple[bool, str]:
        """Check if domain should be temporarily skipped due to consecutive failures."""
        if domain not in self.domain_stats:
            return False, ""
        
        stats = self.domain_stats[domain]
        current_time = time.time()
        
        # No consecutive failures recorded
        if stats.consecutive_all_methods_failures == 0:
            return False, ""
        
        # Calculate exponential backoff delay
        # 1st failure: 10 min, 2nd: 30 min, 3rd: 1 hour, 4th: 2 hours, max: 6 hours
        base_delay = 600  # 10 minutes
        max_delay = 21600  # 6 hours
        
        failures = min(stats.consecutive_all_methods_failures, 8)  # Cap at reasonable level
        delay_seconds = min(base_delay * (1.5 ** (failures - 1)), max_delay)
        
        if stats.last_all_methods_failure_timestamp:
            time_since_last_failure = current_time - stats.last_all_methods_failure_timestamp
            if time_since_last_failure < delay_seconds:
                remaining_minutes = int((delay_seconds - time_since_last_failure) / 60)
                return True, f"Exponential backoff: {remaining_minutes} minutes remaining"
        
        return False, ""
    
    def record_all_methods_failure(self, domain: str):
        """Record when all extraction methods failed for a domain."""
        if domain not in self.domain_stats:
            self.domain_stats[domain] = DomainExtractionStats(domain=domain)
        
        stats = self.domain_stats[domain]
        stats.consecutive_all_methods_failures += 1
        stats.last_all_methods_failure_timestamp = time.time()
        
        self._save_stats()
        
        logger.info(f"  📈 Consecutive all-methods failures for {domain}: {stats.consecutive_all_methods_failures}")
    def reset_all_methods_failures(self, domain: str):
        """Reset consecutive failure counter when any method succeeds."""
        if domain not in self.domain_stats:
            return
        
        stats = self.domain_stats[domain]
        if stats.consecutive_all_methods_failures > 0:
            logger.info(f"  ✅ Resetting failure counter for {domain} (was {stats.consecutive_all_methods_failures})")
            stats.consecutive_all_methods_failures = 0
            stats.last_all_methods_failure_timestamp = None
            self._save_stats()
    
    def get_recently_failed_methods(self, domain: str, recent_failures_threshold: int = 2) -> List[str]:
        """Get methods that failed recently and should be deprioritized."""
        if domain not in self.domain_stats:
            return []
        
        stats = self.domain_stats[domain]
        recently_failed = []
        
        # If we had a recent success but it just failed, reset and try all methods
        current_time = time.time()
        if (stats.last_success_timestamp and stats.last_failure_timestamp and 
            stats.last_failure_timestamp > stats.last_success_timestamp and
            current_time - stats.last_failure_timestamp < 3600):  # Last failure within 1 hour
            # Recent success followed by failure - don't skip any methods, try everything
            return []
        
        # Check methods that have recent consecutive failures
        for method, failure_count in stats.method_failure_counts.items():
            success_count = stats.method_success_counts.get(method, 0)
            
            # If method has recent failures without recent successes, deprioritize
            if failure_count >= recent_failures_threshold and success_count == 0:
                recently_failed.append(method)
        
        return recently_failed
    
    def should_use_ai_optimization(self, domain: str) -> tuple[bool, str]:
        """Determine if AI optimization should be used for this domain."""
        stats = self.get_domain_stats(domain)
        
        # Always try AI for completely unknown domains
        if stats.total_attempts == 0:
            return True, "Unknown domain - trying AI discovery"
        
        # Skip AI for stable, well-performing domains
        if stats.is_stable() and not stats.needs_optimization():
            return False, f"Domain stable (success rate: {stats.success_rate:.1%})"
        
        # Use AI for problematic domains
        if stats.needs_optimization():
            return True, f"Domain needs optimization (success rate: {stats.success_rate:.1%})"
        
        # Use AI for domains with insufficient data
        if stats.total_attempts < self.min_attempts_for_stability:
            return True, f"Insufficient data ({stats.total_attempts} attempts)"
        
        return False, "Domain performance adequate"
    
    def mark_ai_analysis_completed(self, domain: str, selectors_discovered: int, 
                                  tokens_used: int, credits_cost: float):
        """Record AI analysis completion."""
        self.ai_analysis_credits_used += credits_cost
        logger.info(f"AI analysis completed for {domain}: {selectors_discovered} selectors, {tokens_used} tokens, {credits_cost:.3f} credits")
    
    def increment_credits_saved(self, domain: str):
        """Record credits saved by skipping AI analysis."""
        # Estimate credits that would have been used
        estimated_credits = 0.01  # Rough estimate
        self.ai_analysis_credits_saved += estimated_credits
    
    def get_domain_performance_summary(self) -> Dict[str, any]:
        """Get overall performance summary across all domains."""
        if not self.domain_stats:
            return {
                'total_domains': 0,
                'stable_domains': 0,
                'problematic_domains': 0,
                'ai_credits_used': self.ai_analysis_credits_used,
                'ai_credits_saved': self.ai_analysis_credits_saved
            }
        
        stable_count = sum(1 for stats in self.domain_stats.values() if stats.is_stable())
        problematic_count = sum(1 for stats in self.domain_stats.values() if stats.needs_optimization())
        
        return {
            'total_domains': len(self.domain_stats),
            'stable_domains': stable_count,
            'problematic_domains': problematic_count,
            'overall_success_rate': self._calculate_overall_success_rate(),
            'ai_credits_used': self.ai_analysis_credits_used,
            'ai_credits_saved': self.ai_analysis_credits_saved,
            'net_credits': self.ai_analysis_credits_saved - self.ai_analysis_credits_used
        }
    
    def get_top_performing_domains(self, limit: int = 10) -> List[Dict[str, any]]:
        """Get top performing domains by success rate."""
        domains = []
        for domain, stats in self.domain_stats.items():
            if stats.total_attempts >= 3:  # Only include domains with sufficient data
                domains.append({
                    'domain': domain,
                    'success_rate': stats.success_rate,
                    'total_attempts': stats.total_attempts,
                    'average_time_ms': stats.average_extraction_time_ms,
                    'best_method': stats.get_best_method()
                })
        
        domains.sort(key=lambda x: x['success_rate'], reverse=True)
        return domains[:limit]
    
    def get_problematic_domains(self, limit: int = 10) -> List[Dict[str, any]]:
        """Get domains that need optimization."""
        domains = []
        for domain, stats in self.domain_stats.items():
            if stats.needs_optimization() and stats.total_attempts >= 2:
                domains.append({
                    'domain': domain,
                    'success_rate': stats.success_rate,
                    'total_attempts': stats.total_attempts,
                    'average_time_ms': stats.average_extraction_time_ms,
                    'last_failure': stats.last_failure_timestamp
                })

        domains.sort(key=lambda x: (x['success_rate'], -x['total_attempts']))
        return domains[:limit]

    def get_optimal_timeout(self, domain: str, base_timeout_ms: int = 25000) -> int:
        """Calculate optimal timeout based on domain history.

        Returns timeout in milliseconds, adjusted based on:
        - Average extraction time for this domain
        - Success rate (lower success = longer timeout)
        - Number of previous attempts
        """
        if domain not in self.domain_stats:
            return base_timeout_ms  # Unknown domain, use default

        stats = self.domain_stats[domain]

        if stats.total_attempts == 0:
            return base_timeout_ms

        # Start with average time + 50% buffer
        avg_time = stats.average_extraction_time_ms
        if avg_time > 0:
            optimal = int(avg_time * 1.5)
        else:
            optimal = base_timeout_ms

        # Adjust based on success rate
        # Low success rate = increase timeout to give more time
        if stats.success_rate < 0.3:
            optimal = int(optimal * 1.5)  # Very problematic domain
        elif stats.success_rate < 0.5:
            optimal = int(optimal * 1.25)  # Somewhat problematic

        # For stable, fast domains, reduce timeout
        if stats.is_stable() and avg_time < 5000:
            optimal = max(int(avg_time * 2), 10000)  # At least 10s

        # Clamp to reasonable bounds
        min_timeout = 10000  # 10 seconds minimum
        max_timeout = 60000  # 60 seconds maximum
        optimal = max(min_timeout, min(optimal, max_timeout))

        return optimal

    def get_method_timeout(self, domain: str, method: str, base_timeout_ms: int = 25000) -> int:
        """Get timeout optimized for specific extraction method on this domain."""
        if domain not in self.domain_stats:
            return base_timeout_ms

        stats = self.domain_stats[domain]
        method_avg_time = stats.method_avg_times.get(method, 0)

        if method_avg_time > 0:
            # Use method-specific average + buffer
            optimal = int(method_avg_time * 1.5)
        else:
            # Fall back to domain-level timeout
            return self.get_optimal_timeout(domain, base_timeout_ms)

        # Clamp to reasonable bounds
        min_timeout = 10000
        max_timeout = 60000
        return max(min_timeout, min(optimal, max_timeout))
    
    def _calculate_overall_success_rate(self) -> float:
        """Calculate overall success rate across all domains."""
        total_attempts = sum(stats.total_attempts for stats in self.domain_stats.values())
        total_successes = sum(stats.successful_attempts for stats in self.domain_stats.values())
        
        return total_successes / total_attempts if total_attempts > 0 else 0.0
    
    def _cleanup_old_domains(self):
        """Remove statistics for domains not seen recently."""
        current_time = time.time()
        cutoff_time = current_time - (self.max_domain_age_days * 24 * 3600)
        
        domains_to_remove = []
        for domain, stats in self.domain_stats.items():
            last_activity = max(
                stats.last_success_timestamp or 0,
                stats.last_failure_timestamp or 0
            )
            if last_activity < cutoff_time:
                domains_to_remove.append(domain)
        
        for domain in domains_to_remove:
            del self.domain_stats[domain]
        
        if domains_to_remove:
            logger.info(f"  🧹 Cleaned up {len(domains_to_remove)} old domain statistics")
        self.last_cleanup_time = current_time
    
    def export_stats(self) -> Dict[str, any]:
        """Export all statistics for persistence."""
        return {
            'domain_stats': {
                domain: asdict(stats) for domain, stats in self.domain_stats.items()
            },
            'ai_credits_used': self.ai_analysis_credits_used,
            'ai_credits_saved': self.ai_analysis_credits_saved,
            'last_cleanup_time': self.last_cleanup_time
        }
    
    def import_stats(self, data: Dict[str, any]):
        """Import statistics from persistent storage."""
        if 'domain_stats' in data:
            for domain, stats_dict in data['domain_stats'].items():
                stats = DomainExtractionStats(**stats_dict)
                self.domain_stats[domain] = stats
        
        self.ai_analysis_credits_used = data.get('ai_credits_used', 0)
        self.ai_analysis_credits_saved = data.get('ai_credits_saved', 0)
        self.last_cleanup_time = data.get('last_cleanup_time', time.time())
    
    def _load_stats(self):
        """Load statistics from persistent storage."""
        try:
            import os
            if os.path.exists(self.persistence_file):
                with open(self.persistence_file, 'r') as f:
                    data = json.loads(f.read())
                    self.import_stats(data)
                logger.info(f"📊 Loaded domain stats from {self.persistence_file}: {len(self.domain_stats)} domains")
        except Exception as e:
            logger.warning(f"⚠️ Failed to load domain stats: {e}")
    def _save_stats(self):
        """Save statistics to persistent storage."""
        try:
            from .job_fence import current_job_fence
            fence = current_job_fence.get()
            if fence is not None and not fence.active:
                return
            if fence is not None:
                fence.pending_state_publish = True
                return
            import os
            os.makedirs(os.path.dirname(self.persistence_file), exist_ok=True)
            
            data = self.export_stats()
            with open(self.persistence_file, 'w') as f:
                f.write(json.dumps(data, indent=2))
        except Exception as e:
            logger.warning(f"⚠️ Failed to save domain stats: {e}")

    async def flush_fenced_state(self, fence):
        """Atomically publish accumulated cache state while the current job is fenced."""
        if not fence.pending_state_publish:
            return
        import os
        from pathlib import Path
        from ..database import AsyncSessionLocal

        target = Path(self.persistence_file)
        target.parent.mkdir(parents=True, exist_ok=True)
        temp = target.with_name(
            f".{target.name}.{fence.job_id.hex}.a{fence.attempt_generation}.working"
        )
        try:
            temp.write_text(json.dumps(self.export_stats(), indent=2))
            async with AsyncSessionLocal() as session:
                async with fence.lease.publication_fence(session):
                    os.replace(temp, target)
                    await session.commit()
            fence.pending_state_publish = False
        finally:
            temp.unlink(missing_ok=True)
# Global instance
_stability_tracker = None

async def get_stability_tracker() -> DomainStabilityTracker:
    """Get or create the global domain stability tracker."""
    global _stability_tracker
    if _stability_tracker is None:
        _stability_tracker = DomainStabilityTracker()
    return _stability_tracker
