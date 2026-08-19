"""Deduplicated operational alerts for the Telegram service chat."""

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Awaitable, Callable, Dict, Optional


logger = logging.getLogger(__name__)


@dataclass
class _AlertState:
    active: bool = False
    last_sent_at: float = 0.0
    suppressed: int = 0


class OperationalAlertManager:
    """Send failures once, suppress repeats, and announce recovery once."""

    def __init__(
        self,
        sender: Callable[[str, str], Awaitable[object]],
        repeat_interval_seconds: float = 3600.0,
    ):
        self._sender = sender
        self._repeat_interval_seconds = repeat_interval_seconds
        self._states: Dict[str, _AlertState] = {}
        self._lock = asyncio.Lock()

    async def failure(self, key: str, title: str, message: str) -> bool:
        """Send the first failure and periodic reminders for a continuing incident."""
        async with self._lock:
            state = self._states.setdefault(key, _AlertState())
            now = time.monotonic()
            should_send = (
                not state.active
                or now - state.last_sent_at >= self._repeat_interval_seconds
            )
            state.active = True
            if not should_send:
                state.suppressed += 1
                return False

            suffix = ""
            if state.suppressed:
                suffix = f"\n\nПовторов с прошлого уведомления: {state.suppressed}"
            alert_message = (message + suffix)[:3200]
            try:
                sent = bool(await self._sender(title[:200], alert_message))
            except Exception as exc:
                logger.error("Operational alert sender failed for %s: %s", key, exc)
                sent = False
            if sent:
                state.last_sent_at = now
                state.suppressed = 0
            else:
                # Retry the next observation and never announce recovery for an
                # incident that the service chat did not receive.
                state.active = False
                logger.error("Could not deliver operational alert %s", key)
            return sent

    async def recovery(self, key: str, title: str, message: str) -> bool:
        """Send recovery only for an incident previously observed as active."""
        async with self._lock:
            state = self._states.get(key)
            if state is None or not state.active:
                return False
            suffix = f"\n\nПодавлено повторов: {state.suppressed}" if state.suppressed else ""
            recovery_message = (message + suffix)[:3200]
            try:
                sent = bool(await self._sender(title[:200], recovery_message))
            except Exception as exc:
                logger.error("Operational recovery sender failed for %s: %s", key, exc)
                sent = False
            if sent:
                state.active = False
                state.last_sent_at = time.monotonic()
                state.suppressed = 0
            else:
                logger.error("Could not deliver operational recovery %s", key)
            return sent

    def is_active(self, key: str) -> bool:
        state: Optional[_AlertState] = self._states.get(key)
        return bool(state and state.active)
