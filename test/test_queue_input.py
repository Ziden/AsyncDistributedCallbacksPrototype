import pytest

from cron_o import storage, stream
from cron_o.async_queue import AsyncCallbackQueue
from cron_o.time_utils import get_current_time_millis
from test.utilities import generate_call, run_all_coroutines_from


@pytest.mark.asyncio
async def test_worker_adding_to_storage(call):
    async_worker = AsyncCallbackQueue()
    await stream.add(call)
    await async_worker._input_tick()

    assert call.call_id in await storage.get_next_calls()
    assert await storage.get_call(call.call_id)


@pytest.mark.asyncio
async def test_worker_knowing_next_task(call):
    async_worker = AsyncCallbackQueue()
    await stream.add(call)
    await async_worker._input_tick()

    assert async_worker.next_call.call_id == call.call_id


@pytest.mark.asyncio
async def test_input_not_picking_up():
    async_worker = AsyncCallbackQueue()
    await async_worker._input_tick()

    assert not async_worker.next_call


@pytest.mark.asyncio
async def test_input_multiple(call):
    async_worker = AsyncCallbackQueue()
    now = get_current_time_millis()
    call_1 = generate_call(now + 5)
    call_2 = generate_call(now + 10)

    await stream.add(call_2)
    await stream.add(call_1)
    await async_worker._input_tick()

    assert async_worker.next_call.call_id == call_1.call_id
    assert len(await storage.get_all_calls()) == 2


@pytest.mark.asyncio
async def test_input_catchup(call):
    async_worker = AsyncCallbackQueue()
    now = get_current_time_millis()

    await stream.add(generate_call(now - 10))
    await stream.add(generate_call(now - 20))
    await stream.add(generate_call(now - 30))
    await async_worker._input_tick()

    await run_all_coroutines_from(AsyncCallbackQueue._run_call_and_delete_from_stream)

    assert not async_worker.next_call, "Call should have been executed"
    assert len(await stream.read_all()) == 0, "Call should be removed from stream"
    assert len(await storage.get_all_calls()) == 0, "Call should not be in calls sorted set"


