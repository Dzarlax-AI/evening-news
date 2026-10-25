"""Durable hysteresis for retryable source warnings on scheduled runs."""

from collections import Counter
from copy import deepcopy
import hashlib
import json
import re

from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from ..models import ArticleIngestionRun, ArticleIngestionSourceRun, BackgroundJob, Setting

RETRYABLE_SOURCE_CODES = frozenset({
    "transport_failed", "deadline_exceeded", "upstream_unavailable", "upstream_rate_limited",
})
MAX_SOURCES = 64
MAX_STATE_BYTES = 12 * 1024
MAX_ACTIVE_ALERT_JOBS = 128
_HASH_RE = re.compile(r"[a-f0-9]{64}\Z")
_TYPE_NAMES = {"rss": "RSS", "telegram": "Telegram", "custom": "Page Monitor"}


def schedule_identity(job):
    """Return a stable identity only for scheduled jobs, never manual work."""
    schedule_key = getattr(job, "schedule_key", None)
    if isinstance(schedule_key, str) and schedule_key and len(schedule_key) <= 128:
        return "key:" + schedule_key
    payload = job.payload if isinstance(job.payload, dict) else {}
    schedule_id = payload.get("schedule_id")
    if isinstance(schedule_id, (str, int)) and not isinstance(schedule_id, bool):
        value = str(schedule_id)
        if value and len(value) <= 128:
            return "id:" + value
    return None


async def read_current_go_observation(session, job, attempt):
    """Load only this job attempt's completed Go receipt and source rows."""
    run = await session.scalar(select(ArticleIngestionRun).where(
        ArticleIngestionRun.job_id == job.id,
        ArticleIngestionRun.attempt_generation == attempt,
    ))
    if run is None:
        return None, []
    sources = (await session.scalars(select(ArticleIngestionSourceRun).where(
        ArticleIngestionSourceRun.run_id == run.id,
    ).order_by(ArticleIngestionSourceRun.source_id))).all()
    return run, sources


async def previous_scheduled_position(session, job, identity):
    """Find the immediately preceding news-processing job for this schedule."""
    query = select(BackgroundJob.queue_position).where(
        BackgroundJob.kind == "news_processing",
        BackgroundJob.queue_position < job.queue_position,
    )
    if identity.startswith("key:"):
        query = query.where(BackgroundJob.schedule_key == identity[4:])
    elif identity.startswith("id:"):
        query = query.where(
            or_(BackgroundJob.schedule_key.is_(None), BackgroundJob.schedule_key == ""),
            BackgroundJob.payload["schedule_id"].as_string() == identity[3:],
        )
    else:
        return None
    return await session.scalar(query.order_by(BackgroundJob.queue_position.desc()).limit(1))


def classify_observation(job, run, sources):
    """Return a compact healthy/retryable observation, or None to fail safe."""
    if (job.kind != "news_processing" or job.state != "succeeded" or run is None
            or run.state != "succeeded" or not 0 < len(sources) <= MAX_SOURCES):
        return None
    run_result = run.result if isinstance(run.result, dict) else None
    job_result = job.result if isinstance(job.result, dict) else None
    if not run_result or not job_result:
        return None
    go_count = _bounded_count(run_result.get("errors_count"))
    job_count = _bounded_count(job_result.get("errors_count"))
    outcomes = run_result.get("source_outcomes", {} if go_count == 0 else None)
    completed = _bounded_count(run_result.get("sources_completed"), maximum=MAX_SOURCES)
    total = _bounded_count(run_result.get("sources_total"), maximum=MAX_SOURCES)
    if (go_count is None or job_count is None or completed is None or total is None
            or completed != len(sources) or total != len(sources)
            or not isinstance(outcomes, dict) or len(outcomes) > MAX_SOURCES):
        return None

    parsed_outcomes = {}
    for code, count in outcomes.items():
        if (not isinstance(code, str) or not re.fullmatch(r"[a-z0-9_-]{1,40}", code)
                or _bounded_count(count, maximum=1000) is None):
            return None
        parsed_outcomes[code] = count
    if sum(parsed_outcomes.values()) != go_count:
        return None

    normalized = []
    codes = Counter()
    seen_ids = set()
    for source in sources:
        source_id = source.source_id
        source_type = source.source_type
        code = source.error_code
        if (not isinstance(source_id, int) or isinstance(source_id, bool) or source_id <= 0
                or source_id in seen_ids or source_type not in _TYPE_NAMES
                or not isinstance(source.config_sha256, str)
                or _HASH_RE.fullmatch(source.config_sha256) is None):
            return None
        seen_ids.add(source_id)
        if source.state == "succeeded" and code is None:
            normalized.append({"id": source_id, "type": source_type,
                               "config": source.config_sha256, "state": "healthy"})
            continue
        if (source.state not in {"partial", "failed"} or not isinstance(code, str)
                or not re.fullmatch(r"[a-z0-9_-]{1,40}", code)):
            return None
        codes[code] += 1
        normalized.append({"id": source_id, "type": source_type,
                           "config": source.config_sha256, "state": "warning", "code": code})

    if dict(codes) != parsed_outcomes or sum(codes.values()) != go_count or job_count != go_count:
        return None
    warning_count = sum(codes.values())
    expected_status = "warning" if warning_count else "success"
    if run_result.get("status") != expected_status or job_result.get("status") != expected_status:
        return None

    if warning_count == 0:
        return {"sources": normalized, "successful": len(sources), "total": len(sources), "warning": None}
    if warning_count != 1:
        return None
    warning = next(source for source in normalized if source["state"] == "warning")
    if warning["code"] not in RETRYABLE_SOURCE_CODES:
        return None
    return {
        "sources": normalized, "successful": len(sources) - 1, "total": len(sources),
        "warning": warning,
    }


async def prepare_alert(session, identity, queue_position, job_id, observation, previous_position,
                        active_job_ids):
    """Update per-schedule counters under a serialized Setting row lock."""
    key = _setting_key(identity)
    await session.execute(pg_insert(Setting).values(
        key=key, value={"v": 1, "last_position": 0, "sources": {}},
        description="Reserved scheduled source alert hysteresis state",
    ).on_conflict_do_nothing(index_elements=[Setting.key]))
    setting = await session.scalar(select(Setting).where(Setting.key == key).with_for_update())
    if setting is None:
        return None
    state = _valid_state(setting.value)
    if state is None:
        return None
    reconciled = _reconcile_orphaned_deliveries(state, active_job_ids)
    if queue_position <= state["last_position"]:
        if reconciled:
            setting.value = state
        return {"event": None, "identity": identity, "source_ids": [], "recovery": False}

    sources_state = state["sources"]
    if state["last_position"] and previous_position != state["last_position"]:
        # A finalized scheduled job was skipped, never callback-replayed, or
        # completed out of order. The current result starts a fresh streak.
        for prior in sources_state.values():
            prior["failed_streak"] = 0
            prior["healthy_streak"] = 0
            prior["last_state"] = "missing"
    current = {item["id"]: item for item in observation["sources"]}
    events = []

    # A missing source is not evidence of recovery or a consecutive failure.
    for raw_id, prior in list(sources_state.items()):
        if int(raw_id) not in current:
            if prior.get("incident") == "none" and prior.get("event_job") is None:
                sources_state.pop(raw_id)
                continue
            prior["failed_streak"] = 0
            prior["healthy_streak"] = 0
            prior["last_state"] = "missing"

    for item in observation["sources"]:
        source_id = str(item["id"])
        prior = sources_state.get(source_id)
        if prior is None or prior.get("config") != item["config"]:
            prior = {"config": item["config"], "failed_streak": 0, "healthy_streak": 0,
                     "last_state": "new", "incident": "none", "event_job": None}
            sources_state[source_id] = prior
        if item["state"] == "warning":
            prior["healthy_streak"] = 0
            if prior.get("last_state") == "warning":
                prior["failed_streak"] = min(2, prior.get("failed_streak", 0) + 1)
            else:
                prior["failed_streak"] = 1
            prior["last_state"] = "warning"
            if prior["failed_streak"] >= 2 and prior.get("incident") == "none":
                prior["incident"] = "attempting"
                prior["event_job"] = str(job_id)
                events.append({"source_id": item["id"], "type": item["type"],
                               "code": item["code"], "recovery": False})
        else:
            prior["failed_streak"] = 0
            if prior.get("last_state") == "healthy":
                prior["healthy_streak"] = min(2, prior.get("healthy_streak", 0) + 1)
            else:
                prior["healthy_streak"] = 1
            prior["last_state"] = "healthy"
            if prior["healthy_streak"] >= 2 and prior.get("incident") == "active":
                prior["incident"] = "recovering"
                prior["event_job"] = str(job_id)
                events.append({"source_id": item["id"], "type": item["type"],
                               "recovery": True})
            elif prior["healthy_streak"] >= 2 and prior.get("incident") == "unknown":
                # Two confirmed healthy observations retire an uncertain delivery
                # silently, without claiming that a recovery message was sent.
                prior["incident"] = "none"
                prior["event_job"] = None

    state["last_position"] = queue_position
    state["sources"] = sources_state
    if len(sources_state) > MAX_SOURCES:
        return None
    encoded = json.dumps(state, separators=(",", ":"), sort_keys=True)
    if len(encoded.encode("utf-8")) > MAX_STATE_BYTES:
        return None
    setting.value = state
    if not events:
        return {"event": None, "identity": identity, "source_ids": [], "recovery": False}
    descriptions = []
    for item in events:
        source = f"Source #{item['source_id']} ({_TYPE_NAMES[item['type']]})"
        if item["recovery"]:
            descriptions.append(f"{source} recovered")
        else:
            descriptions.append(f"{source} reported {item['code']}")
    if all(item["recovery"] for item in events):
        title = "Scheduled source recovered"
    elif all(not item["recovery"] for item in events):
        title = "Scheduled source warning"
    else:
        title = "Scheduled source updates"
    message = ("; ".join(descriptions)[:350] + ". Sources successful: "
               f"{observation['successful']}/{observation['total']}.")
    return {"event": {"title": title, "message": message[:500]}, "identity": identity,
            "events": events}


async def break_streaks(session, identity, queue_position, active_job_ids):
    """A scheduled warning outside the accepted source case breaks consecutiveness."""
    setting = await session.scalar(select(Setting).where(
        Setting.key == _setting_key(identity),
    ).with_for_update())
    state = _valid_state(setting.value) if setting else None
    if state is None:
        return
    reconciled = _reconcile_orphaned_deliveries(state, active_job_ids)
    if queue_position <= state["last_position"]:
        if reconciled:
            setting.value = state
        return
    state["last_position"] = queue_position
    for prior in state["sources"].values():
        prior["failed_streak"] = 0
        prior["healthy_streak"] = 0
        prior["last_state"] = "missing"
    setting.value = state


def _reconcile_orphaned_deliveries(state, active_job_ids):
    """Downgrade durable pre-send markers after their process is no longer active."""
    if active_job_ids is None:
        return False
    changed = False
    for prior in state["sources"].values():
        if (prior.get("incident") in {"attempting", "recovering"}
                and prior.get("event_job") not in active_job_ids):
            prior["incident"] = "unknown"
            prior["event_job"] = None
            changed = True
    return changed


async def finish_alert(session, identity, events, job_id, delivered):
    """Persist a confirmed send only; failures remain uncertain and are not replayed."""
    key = _setting_key(identity)
    setting = await session.scalar(select(Setting).where(Setting.key == key).with_for_update())
    state = _valid_state(setting.value) if setting else None
    if state is None:
        return
    sources = state["sources"]
    for event in events:
        prior = sources.get(str(event["source_id"]))
        if not prior or prior.get("event_job") != str(job_id):
            continue
        recovery = event["recovery"]
        expected = "recovering" if recovery else "attempting"
        if prior.get("incident") != expected:
            continue
        prior["incident"] = ("none" if recovery else "active") if delivered else "unknown"
        prior["event_job"] = None
        if recovery:
            prior["healthy_streak"] = 0
    setting.value = state


async def abandon_inflight(session, identity, job_id):
    """Mark a prior process's interrupted send uncertain without replaying it."""
    key = _setting_key(identity)
    setting = await session.scalar(select(Setting).where(Setting.key == key).with_for_update())
    state = _valid_state(setting.value) if setting else None
    if state is None:
        return
    for prior in state["sources"].values():
        if prior.get("event_job") == str(job_id) and prior.get("incident") in {"attempting", "recovering"}:
            prior["incident"] = "unknown"
            prior["event_job"] = None
    setting.value = state


def _setting_key(identity):
    digest = hashlib.sha256(identity.encode("utf-8")).hexdigest()
    return "scheduled_source_alerts:v1:" + digest


def _valid_state(value):
    value = deepcopy(value)
    if not isinstance(value, dict) or value.get("v") != 1:
        return None
    sources = value.get("sources")
    position = value.get("last_position")
    if (not isinstance(sources, dict) or len(sources) > MAX_SOURCES
            or not isinstance(position, int) or isinstance(position, bool) or position < 0):
        return None
    for source_id, source in sources.items():
        failed_streak = source.get("failed_streak") if isinstance(source, dict) else None
        healthy_streak = source.get("healthy_streak") if isinstance(source, dict) else None
        if (not isinstance(source_id, str) or not source_id.isdigit() or not isinstance(source, dict)
                or not isinstance(source.get("config"), str) or _HASH_RE.fullmatch(source["config"]) is None
                or source.get("incident") not in {"none", "active", "attempting", "recovering", "unknown", "retired"}
                or source.get("last_state") not in {"new", "warning", "healthy", "missing"}
                or not isinstance(failed_streak, int) or isinstance(failed_streak, bool)
                or failed_streak not in {0, 1, 2}
                or not isinstance(healthy_streak, int) or isinstance(healthy_streak, bool)
                or healthy_streak not in {0, 1, 2}
                or (source.get("event_job") is not None and not isinstance(source.get("event_job"), str))):
            return None
    if len(json.dumps(value, separators=(",", ":"), sort_keys=True).encode("utf-8")) > MAX_STATE_BYTES:
        return None
    return value


def _bounded_count(value, maximum=1_000_000_000):
    if not isinstance(value, int) or isinstance(value, bool) or not 0 <= value <= maximum:
        return None
    return value
