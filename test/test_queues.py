
from uuid import uuid4
import pytest
from cron_o import dao, server_nodes


@pytest.mark.asyncio
async def test_creating_queue():
    queue_id = uuid4()
    await dao.create_queue(queue_id.bytes)
    assert queue_id.bytes in await dao.get_all_queues()


@pytest.mark.asyncio
async def test_new_queue_is_not_watched():
    queue_id = uuid4()
    await dao.create_queue(queue_id.bytes)
    queues = await dao.get_all_queues()

    assert not queues[queue_id.bytes]


@pytest.mark.asyncio
async def test_getting_unwatched_queue():
    queue_id = uuid4()
    await dao.create_queue(queue_id.bytes)

    unwatched_queues = await server_nodes._get_unwatched_queues_transaction()
    assert queue_id.bytes in unwatched_queues


