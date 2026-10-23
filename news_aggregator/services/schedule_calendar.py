"""Compatibility calendar helper used when recovering legacy accepted jobs."""

import logging
from datetime import datetime, timedelta
from typing import Optional
import pytz

logger = logging.getLogger(__name__)


def calculate_next_run(
    schedule_type: str,
    hour: int,
    minute: int,
    weekdays: list,
    timezone: str,
    interval_minutes: int = 30
) -> Optional[datetime]:
    """Calculate the next run time for a scheduled task.

    Args:
        schedule_type: Type of schedule ('daily', 'hourly', 'interval')
        hour: Hour for daily schedule (0-23)
        minute: Minute for daily/hourly schedule (0-59)
        weekdays: List of weekday numbers (1=Monday, 7=Sunday)
        timezone: Timezone string (e.g., 'Europe/Belgrade')
        interval_minutes: Minutes between runs for interval schedule

    Returns:
        Next run datetime in UTC, or None if error
    """
    try:
        tz = pytz.timezone(timezone)
        now = datetime.now(tz)

        if schedule_type == "daily":
            # Next run at specified time
            next_run = now.replace(
                hour=hour,
                minute=minute,
                second=0,
                microsecond=0
            )

            # If time has passed today, schedule for tomorrow
            if next_run <= now:
                next_run += timedelta(days=1)

            # Check weekdays if specified
            if weekdays:
                while next_run.isoweekday() not in weekdays:
                    next_run += timedelta(days=1)

        elif schedule_type == "hourly":
            # Next run at specified minute of next hour
            next_run = now.replace(minute=minute, second=0, microsecond=0)
            if next_run <= now:
                next_run += timedelta(hours=1)

            # Check weekdays if specified
            if weekdays and next_run.isoweekday() not in weekdays:
                # Skip to next valid weekday
                while next_run.isoweekday() not in weekdays:
                    next_run += timedelta(days=1)
                next_run = next_run.replace(hour=0, minute=minute)

        elif schedule_type == "interval":
            # Generic interval in minutes
            interval_minutes = max(1, min(interval_minutes, 24*60))
            next_run = now + timedelta(minutes=interval_minutes)

        else:
            logger.warning(f"Unknown schedule type: {schedule_type}")
            return None

        return next_run.astimezone(pytz.UTC).replace(tzinfo=None)

    except Exception as e:
        logger.error(f"Error calculating next run: {e}", exc_info=True)
        return None
