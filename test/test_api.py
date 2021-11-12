
from uuid import uuid4
import pytest
from cron_o import dao, api, ScheduledCall


@pytest.mark.asyncio
async def test_adding_call_creates_queue():
    call = ScheduledCall()
    call.call_id = uuid4()
    queue_id = uuid4()
    await api.add_scheduled_call_transaction(queue_id, call)
    queues = await dao.get_all_queues()

    assert queue_id.bytes in queues


@pytest.mark.asyncio
async def test_adding_call_inserts_in_sorted_set():
    call = ScheduledCall()
    call.call_id = uuid4()
    queue_id = uuid4()
    await api.add_scheduled_call_transaction(queue_id, call)
    queues = await dao.get_all_queues()

    assert queue_id.bytes in queues
