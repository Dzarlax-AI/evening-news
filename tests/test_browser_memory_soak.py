"""Loopback CDP soak: real nodriver websockets/listeners without launching Chrome."""

import asyncio
import gc
import json
import os
import tracemalloc
import urllib.parse
import weakref
from collections import Counter
from pathlib import Path

import psutil
import pytest
import websockets
from nodriver.core import util as nodriver_util
from nodriver.core.config import Config

import news_aggregator.core.browser_pool as bp


def _pool_counts():
    return bp.get_browser_resource_stats()


def _filtered_allocated_bytes(snapshot, package_pattern):
    """Sum live traced bytes for matching source paths without exposing values."""
    filtered = snapshot.filter_traces([
        tracemalloc.Filter(inclusive=True, filename_pattern=package_pattern)
    ])
    return sum(stat.size for stat in filtered.statistics("filename"))


@pytest.mark.asyncio
async def test_browser_memory_returns_to_baseline_after_tab_and_browser_cycles(
    tmp_path, monkeypatch, request
):
    """Exercise real Connection.aopen/_listener/aclose against a local CDP stub.

    This isolates transport ownership from Chrome itself. The fake endpoint only
    implements the small Target domain subset used by Browser.get and tab cleanup.
    """
    target_counter = Counter()
    active_clients = set()

    async def fake_cdp(websocket):
        active_clients.add(websocket)
        try:
            async for payload in websocket:
                command = json.loads(payload)
                method = command["method"]
                if method == "Target.createTarget":
                    target_counter["created"] += 1
                    result = {"targetId": f"target-{target_counter['created']}"}
                elif method == "Target.attachToBrowserTarget":
                    result = {"sessionId": "browser-session"}
                elif method == "Target.attachToTarget":
                    result = {"sessionId": f"session-{target_counter['created']}"}
                elif method == "Target.getTargetInfo":
                    target_id = command["params"]["targetId"]
                    result = {"targetInfo": {
                        "targetId": target_id,
                        "type": "page",
                        "title": "loopback fixture",
                        "url": "https://fixture.test/",
                        "attached": True,
                        "canAccessOpener": False,
                    }}
                elif method == "Target.closeTarget":
                    target_counter["closed"] += 1
                    result = {"success": True}
                else:
                    raise AssertionError(f"Unexpected CDP command: {method}")
                await websocket.send(json.dumps({"id": command["id"], "result": result}))
        finally:
            active_clients.discard(websocket)

    original = _pool_counts()
    tracing_started_here = not tracemalloc.is_tracing()
    if tracing_started_here:
        tracemalloc.start(10)
        request.addfinalizer(tracemalloc.stop)
    process = psutil.Process()
    gc.collect()
    await asyncio.sleep(0)
    if tracing_started_here:
        tracemalloc.reset_peak()
    baseline_traced_current, baseline_traced_peak = tracemalloc.get_traced_memory()
    baseline_managed = len(bp._managed_connections)
    baseline_registry = set(nodriver_util.get_registered_instances())
    baseline_rss = process.memory_info().rss
    cycle_metrics = []
    collected_tabs = []
    tab_counts = [17, 17, 17, 17, 16, 16]

    async with websockets.serve(fake_cdp, "127.0.0.1", 0) as server:
        baseline_handles = process.num_handles() if hasattr(process, "num_handles") else None
        baseline_tasks = len(asyncio.all_tasks())
        port = server.sockets[0].getsockname()[1]
        endpoint = f"ws://127.0.0.1:{port}"
        for cycle, tab_count in enumerate(tab_counts, start=1):
            browser = bp.Browser(Config(
                host="127.0.0.1", port=port, browser_executable_path="loopback-only",
            ))
            browser.websocket_url = endpoint
            await browser.aopen()
            bp._browser = browser
            bp._managed_connections[id(browser)] = browser
            nodriver_util.get_registered_instances().add(browser)
            cycle_refs = []

            for tab_number in range(tab_count):
                async with bp.browser_tab(f"https://fixture.test/{cycle}/{tab_number}") as tab:
                    # Approximate a retained response graph without creating a huge fixture.
                    tab.transactions[-1].result = {"page_payload": "x" * 256_000}
                    cycle_refs.append(weakref.ref(tab))
                del tab

            await bp.close_browser()
            del browser
            gc.collect()
            await asyncio.sleep(0)
            collected_tabs.extend(cycle_refs)

            if tracing_started_here:
                tracemalloc.reset_peak()
            traced_current, traced_peak = tracemalloc.get_traced_memory()
            allocation_snapshot = tracemalloc.take_snapshot()

            counts = _pool_counts()
            handles = process.num_handles() if baseline_handles is not None else None
            cycle_metrics.append({
                "cycle": cycle,
                "tabs_closed": tab_count,
                "pool_counts": counts,
                "active_cdp_clients": len(active_clients),
                "live_asyncio_tasks": len(asyncio.all_tasks()),
                "process_handles": handles,
                "rss_bytes": process.memory_info().rss,
                "tracemalloc_current_bytes": traced_current,
                "tracemalloc_peak_bytes": traced_peak if tracing_started_here else None,
                "project_live_allocated_bytes": _filtered_allocated_bytes(
                    allocation_snapshot, "*news_aggregator*"
                ),
                "nodriver_live_allocated_bytes": _filtered_allocated_bytes(
                    allocation_snapshot, "*nodriver*"
                ),
                "collected_tab_refs": sum(ref() is None for ref in cycle_refs),
            })

            assert counts == original
            assert len(active_clients) == 0
            assert all(ref() is None for ref in cycle_refs)
            assert len(nodriver_util.get_registered_instances()) == len(baseline_registry)
            assert len(asyncio.all_tasks()) == baseline_tasks

        metrics = {
            "transport": "installed nodriver Connection over loopback WebSocket",
            "browser_engine_launched": False,
            "total_tab_close_cases": sum(tab_counts),
            "sequential_cycles": len(tab_counts),
            "target_commands": dict(target_counter),
            "baseline_pool_counts": original,
            "baseline_managed_connections": baseline_managed,
            "baseline_asyncio_tasks": baseline_tasks,
            "baseline_process_handles": baseline_handles,
            "baseline_rss_bytes": baseline_rss,
            "tracemalloc_started_here": tracing_started_here,
            "baseline_tracemalloc_current_bytes": baseline_traced_current,
            "baseline_tracemalloc_peak_bytes": baseline_traced_peak,
            "allocation_measurement": (
                "post-gc live snapshot; project and nodriver fields are byte totals "
                "filtered by source filename, including retained package caches"
            ),
            "cycles": cycle_metrics,
        }
        metrics_path = Path(os.environ.get(
            "MEMORY_SOAK_REPORT_PATH",
            str(tmp_path / "browser-memory-soak-metrics.json"),
        ))
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        metrics_path.write_text(
            json.dumps(metrics, indent=2), encoding="utf-8",
        )

    assert target_counter["created"] == sum(tab_counts)
    assert target_counter["closed"] == sum(tab_counts)
    assert all(ref() is None for ref in collected_tabs)
    assert len(bp._managed_connections) == baseline_managed
    assert len(asyncio.all_tasks()) <= baseline_tasks + 1
    if baseline_handles is not None:
        assert process.num_handles() <= baseline_handles + 1


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.environ.get("RUN_MEMORY_BROWSER_TESTS") != "1"
    or not os.environ.get("MEMORY_BROWSER_ENDPOINT"),
    reason="requires explicit opt-in and an isolated local Chrome CDP endpoint",
)
async def test_real_chrome_browser_html_is_bounded_and_preserves_source_markup(monkeypatch):
    """Opt-in local Chrome proof for bounded HTML transfer and source attributes."""
    from news_aggregator.config import settings
    from news_aggregator.core.resource_limits import ResourceLimitError, read_browser_html

    endpoint = os.environ["MEMORY_BROWSER_ENDPOINT"]
    monkeypatch.setattr(settings, "browser_ws_endpoint", endpoint)
    monkeypatch.setattr(settings, "extraction_browser_max_bytes", 8192)
    browser = None
    try:
        browser = await bp.get_browser()
        html = (
            '<!doctype html><html><head><title>Loopback fixture</title>'
            '<meta name="description" content="source metadata"></head><body>'
            '<article data-story-id="story-7" data-custom-flag="preserve">'
            '<time datetime="2026-10-15T12:00:00Z">Today</time>'
            '<img src="data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs=" '
            'alt="source image" data-image-kind="hero">Headline</article>'
            '</body></html>'
        )
        data_url = "data:text/html," + urllib.parse.quote(html)
        async with bp.browser_tab(data_url) as tab:
            await asyncio.sleep(0.1)
            extracted = await read_browser_html(tab)
            assert '<meta name="description" content="source metadata">' in extracted
            assert 'data-story-id="story-7"' in extracted
            assert 'data-custom-flag="preserve"' in extracted
            assert 'datetime="2026-10-15T12:00:00Z"' in extracted
            assert 'data-image-kind="hero"' in extracted

        oversized = "<html><body><article>" + ("x" * 5000) + "</article></body></html>"
        oversized_url = "data:text/html," + urllib.parse.quote(oversized)
        monkeypatch.setattr(settings, "extraction_browser_max_bytes", 1024)
        async with bp.browser_tab(oversized_url) as tab:
            await asyncio.sleep(0.1)
            with pytest.raises(ResourceLimitError, match="browser_limit"):
                await read_browser_html(tab)
            methods = [transaction.request["method"] for transaction in tab.transactions]
            assert "DOM.getDocument" not in methods
            assert not any(method.startswith("DOM.") for method in methods)
    finally:
        await bp.close_browser()
