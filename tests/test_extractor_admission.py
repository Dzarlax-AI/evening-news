"""Behavioral checks for bounded extractor admission and cleanup."""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import pytest

from news_aggregator.extraction.admission import AdmissionWaitTimeout, endpoint_admission


@pytest.mark.asyncio
async def test_waiters_enter_in_fifo_order_without_starvation():
    endpoint = 'http://extractor/private'
    first_entered = asyncio.Event()
    release_first = asyncio.Event()
    order = []

    async def worker(name, block=False):
        async with endpoint_admission(endpoint + '/v1/extract', 1):
            order.append(name)
            if block:
                first_entered.set()
                await release_first.wait()

    first = asyncio.create_task(worker('first', block=True))
    await first_entered.wait()
    second = asyncio.create_task(worker('second'))
    third = asyncio.create_task(worker('third'))
    await asyncio.sleep(0)
    release_first.set()
    await asyncio.gather(first, second, third)

    assert order == ['first', 'second', 'third']


@pytest.mark.asyncio
async def test_waiter_cap_and_cancellation_release_the_queue():
    endpoint = 'http://extractor/private'
    active = asyncio.Event()
    release_active = asyncio.Event()

    async def hold():
        async with endpoint_admission(endpoint + '/v1/extract', 2):
            active.set()
            await release_active.wait()

    owner = asyncio.create_task(hold())
    await active.wait()

    admitted = 0

    async def waiter():
        nonlocal admitted
        async with endpoint_admission(endpoint + '/v1/telegram/fetch', 2):
            admitted += 1

    waiters = [asyncio.create_task(waiter()) for _ in range(8)]
    await asyncio.sleep(0)
    with pytest.raises(AdmissionWaitTimeout):
        async with endpoint_admission(endpoint + '/v1/sources/collect', 1):
            pass

    waiters[0].cancel()
    with pytest.raises(asyncio.CancelledError):
        await waiters[0]
    release_active.set()
    await owner
    await asyncio.gather(*waiters[1:])
    assert admitted == 7


@pytest.mark.asyncio
async def test_route_prefix_and_other_endpoint_are_independent():
    first = endpoint_admission('http://extractor/private/v1/extract', 1)
    async with first:
        async with endpoint_admission('http://other/private/v1/telegram/fetch', 0.1):
            pass
        with pytest.raises(AdmissionWaitTimeout):
            async with endpoint_admission('http://extractor/private/v1/sources/collect', 0.01):
                pass


@pytest.mark.asyncio
async def test_canceling_active_request_does_not_strand_next_waiter():
    endpoint = 'http://extractor/private/v1/extract'
    active = asyncio.Event()
    next_active = asyncio.Event()

    async def owner():
        async with endpoint_admission(endpoint, 1):
            active.set()
            await asyncio.Future()

    first = asyncio.create_task(owner())
    await active.wait()

    async def waiter():
        async with endpoint_admission('http://extractor/private/v1/telegram/fetch', 1):
            next_active.set()

    second = asyncio.create_task(waiter())
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    await second
    assert next_active.is_set()


def test_same_base_is_independent_across_event_loops():
    rendezvous = Barrier(2)

    def enter_from_thread(route):
        async def run():
            async with endpoint_admission(f'http://extractor/private/{route}', 1):
                await asyncio.to_thread(rendezvous.wait, 1)
        asyncio.run(run())

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(enter_from_thread, route)
                   for route in ('v1/extract', 'v1/telegram/fetch')]
        for future in futures:
            future.result(timeout=2)
