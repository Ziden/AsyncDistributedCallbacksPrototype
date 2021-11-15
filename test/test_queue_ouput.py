import pytest

from cron_o import stream, storage
from cron_o.async_queue import AsyncCallbackQueue
from cron_o.time_utils import get_current_time_millis
from test.utilities import generate_call, run_all_coroutines_from, added_time_millis


@pytest.mark.asyncio
async def test_output_lock_when_no_tasks():
    async_worker = AsyncCallbackQueue()
    await async_worker._output_tick()

    assert async_worker.output_lock.locked()


@pytest.mark.asyncio
async def test_new_input_unlocks_output():
    async_worker = AsyncCallbackQueue()

    await async_worker._output_tick()
    await stream.add(generate_call(0))
    await async_worker._input_tick()

    assert not async_worker.output_lock.locked()


@pytest.mark.asyncio
async def test_output_without_next():
    async_worker = AsyncCallbackQueue()
    now = get_current_time_millis()
    call_1 = generate_call(now - 10)
    call_2 = generate_call(now + 10)

    await stream.add(call_1)
    await stream.add(call_2)

    await async_worker._input_tick()
    await async_worker._output_tick()

    assert async_worker.next_call.call_id == call_2.call_id, "First call should have been executed"
    assert len(await stream.read_all()) == 1, "Executed tasks should be removed from stream"
    assert len(await storage.get_all_calls()) == 1, "Executed tasks should be removed from stream"


@pytest.mark.asyncio
async def test_output_past_task_instantly_ran():
    async_worker = AsyncCallbackQueue()
    now = get_current_time_millis()
    call_1 = generate_call(now - 10)

    await stream.add(call_1)
    await async_worker._input_tick()
    await async_worker._output_tick()

    await run_all_coroutines_from(AsyncCallbackQueue._run_call_and_delete_from_stream)

    assert not async_worker.next_call
    assert len(await stream.read_all()) == 0
    assert len(await storage.get_all_calls()) == 0


@pytest.mark.asyncio
async def test_output_runs_future():
    async_worker = AsyncCallbackQueue()
    now = get_current_time_millis()
    call_1 = generate_call(now + 5)
    await stream.add(call_1)

    # Task received and registered but not ran
    await async_worker._input_tick()

    # Task only picked up after some time
    with added_time_millis(50):
        await async_worker._output_tick()

    assert not async_worker.next_call
    assert len(await stream.read_all()) == 0


@pytest.mark.asyncio
async def test_output_catch_up():
    async_worker = AsyncCallbackQueue()
    now = get_current_time_millis()
    c1 = generate_call(now + 5, b'\xff')
    c2 = generate_call(now + 10, b'\xcc')
    c3 = generate_call(now + 35, b'\xee')

    await stream.add(c1)
    await stream.add(c2)
    await stream.add(c3)

    # Tasks received and registered but not ran
    await async_worker._input_tick()
    with added_time_millis(50):
        await async_worker._output_tick()

    assert async_worker.next_call.call_id == c2.call_id, "Should have only picked up the next task"


@pytest.mark.asyncio
async def test_output_picking_up_from_past():
    async_worker = AsyncCallbackQueue()
    for i in range(10):
        await stream.add(generate_call(get_current_time_millis() + 50))

    # Tasks received and registered but not ran
    await async_worker._input_tick()

    with added_time_millis(100):
        for i in range(5):
            await async_worker._output_tick()

    assert len(await stream.read_all()) == 5, "Should have finished processing 5 elements"

