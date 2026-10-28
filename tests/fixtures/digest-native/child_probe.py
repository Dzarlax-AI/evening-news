"""Run only against the parent test's isolated DB and fake providers."""
import asyncio
import hashlib
import json
import os
from datetime import datetime, timezone
from types import SimpleNamespace

from pydantic import SecretStr
from news_aggregator.services.ai_runtime import AIRuntime
from news_aggregator.config import settings


async def main():
    data = json.loads(os.environ["DIGEST_PROBE_INPUT"])
    settings.database_url = data["database_url"]
    settings.gemini_api_key = "fixture-key"
    settings.gemini_api_endpoint = data["provider"] + "/v1/models"
    settings.summarization_model = "fixture-model"
    settings.digest_model = "fixture-model"
    settings.site_base_url = "https://news.fixture.test"
    settings.telegram_token = SecretStr("fixture-token")
    settings.telegram_chat_id = "fixture-second-channel"
    settings.telegram_service_chat_id = None
    settings.telegram_rich_messages_enabled = True
    os.environ["NEWS_FIXTURE_MODE"] = "1"
    os.environ["NEWS_TEST_TELEGRAM_ENDPOINT"] = data["provider"] + "/telegram"
    runtime = AIRuntime(os.environ["NEWS_AI_RUNTIME_BINARY"])
    await runtime.start()
    try:
        context = SimpleNamespace(kind="news_digest", payload={}, executor_backend_pid=data["executor_pid"])
        envelope = data["envelope"]
        deadline = datetime.fromisoformat(envelope["deadline_at"].replace("Z", "+00:00"))
        async def operation(name, operation_id, value):
            encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
            record = SimpleNamespace(job_id=envelope["job_id"], attempt_generation=1, operation=name,
                operation_id=operation_id, domain_instance_id="00000000-0000-0000-0000-000000000007",
                expected_envelope=envelope, input_fingerprint=hashlib.sha256(encoded).hexdigest(),
                input=value, deadline=deadline)
            return await runtime.digest_operation(record, context)
        assert (await operation("summaries_generate", "00000000-0000-0000-0000-000000000016", {}))["summaries_generated"] == 1
        assert (await operation("summary_status", "00000000-0000-0000-0000-000000000012", {}))["summaries_count"] == 2
        prepared = await operation("digest_prepare", "00000000-0000-0000-0000-000000000013", {})
        result = await operation("digest_publish", "00000000-0000-0000-0000-000000000014", {"prepared_token": prepared["prepared_token"]})
        assert result["success"] is True and result["parts_sent"] == 1
    finally:
        await runtime.stop()


try:
    asyncio.run(main())
except Exception as exc:
    import traceback
    print("probe_error", type(exc).__name__)
    for frame in traceback.extract_tb(exc.__traceback__):
        print("probe_frame", frame.name, frame.lineno)
    raise SystemExit(1)
