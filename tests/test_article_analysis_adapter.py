from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest

from news_aggregator.services import ai_client
from news_aggregator.services import article_analysis as adapter
from news_aggregator.services.ai_runtime import AIRuntimeError, AIRuntimeProviderError


@pytest.mark.asyncio
async def test_article_adapter_delegates_to_native_runtime(monkeypatch):
    runtime = type("Runtime", (), {"analyze": AsyncMock(return_value={"summary": "synthetic"})})()
    monkeypatch.setattr(adapter, "get_ai_runtime", lambda: runtime)
    deadline = datetime.now(timezone.utc) + timedelta(seconds=10)
    result = await adapter.analyze_article(
        "title", "content", "https://example.test/story", "context",
        model="fixture-model", max_text_chars=1000, deadline=deadline,
    )
    assert result == {"summary": "synthetic"}
    runtime.analyze.assert_awaited_once_with(
        "title", "content", "https://example.test/story", "context", 1000, "fixture-model", deadline
    )


@pytest.mark.asyncio
async def test_article_adapter_preserves_provider_status_but_not_local_rpc_status(monkeypatch):
    async def provider(*_args):
        raise AIRuntimeProviderError("provider_http_error", 422, 7)

    runtime = type("Runtime", (), {"analyze": AsyncMock(side_effect=provider)})()
    monkeypatch.setattr(adapter, "get_ai_runtime", lambda: runtime)
    with pytest.raises(adapter.ArticleAnalysisError) as caught:
        await adapter.analyze_article("t", "c", "https://example.test", None,
                                      model="m", max_text_chars=100, deadline=datetime.now(timezone.utc)+timedelta(seconds=5))
    assert caught.value.status_code == 422
    assert caught.value.retry_after_seconds == 7

    runtime.analyze.side_effect = AIRuntimeError("runtime_invalid_input")
    with pytest.raises(adapter.ArticleAnalysisError) as caught:
        await adapter.analyze_article("t", "c", "https://example.test", None,
                                      model="m", max_text_chars=100, deadline=datetime.now(timezone.utc)+timedelta(seconds=5))
    assert caught.value.code == "runtime_invalid_input"
    assert caught.value.status_code is None


@pytest.mark.asyncio
async def test_ai_client_text_and_structured_methods_keep_legacy_result_shapes(monkeypatch):
    calls = []

    class Runtime:
        async def generate(self, **kwargs):
            calls.append(kwargs)
            if kwargs["mode"] == "text":
                return {"choices": [{"message": {"content": "text"}}],
                        "usage": {"prompt_tokens": 2, "completion_tokens": 3,
                                  "cached_tokens": 0, "total_tokens": 5}}
            return {"result": ["accepted-array"], "usage": {"prompt_tokens": 2,
                    "completion_tokens": 3, "cached_tokens": 0, "total_tokens": 5}}

    monkeypatch.setattr("news_aggregator.services.ai_runtime.get_ai_runtime", lambda: Runtime())
    client = object.__new__(ai_client.AIClient)
    text = await client._make_gemini_request("prompt", "fixture-model", "digest", "example.test", 0.25, 321)
    structured = await client._make_structured_ai_request(
        "structured prompt", "fixture-model", {"type": "array"}, "category", "example.test"
    )
    assert text["choices"][0]["message"]["content"] == "text"
    assert structured["result"] == ["accepted-array"]
    assert calls[0]["timeout"] == 30
    assert calls[0]["temperature"] == 0.25
    assert calls[0]["max_tokens"] == 321
    assert calls[1]["timeout"] == 40
    assert calls[1]["schema"] == {"type": "array"}


@pytest.mark.asyncio
async def test_unknown_runtime_outcome_is_not_replayed_by_legacy_retry_decorator(monkeypatch):
    calls = 0

    class Runtime:
        async def generate(self, **_kwargs):
            nonlocal calls
            calls += 1
            raise AIRuntimeError("runtime_unavailable")

    monkeypatch.setattr("news_aggregator.services.ai_runtime.get_ai_runtime", lambda: Runtime())
    client = object.__new__(ai_client.AIClient)
    client.summarization_model = "fixture-model"
    with pytest.raises(AIRuntimeError):
        await client._make_raw_ai_request("prompt", model="fixture-model")
    assert calls == 1
