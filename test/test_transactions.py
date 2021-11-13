import asyncio
from time import sleep
from uuid import uuid4
import pytest
from cron_o import dao, server_nodes, api
from cron_o.dao import redis_transaction, RedisKeys


@pytest.mark.asyncio
async def test_write_transaction():
    async with redis_transaction(RedisKeys.QUEUE_WATCHERS):
        queue_id = uuid4()
        await dao.create_queue(queue_id.bytes)

    node = await server_nodes.create_server_node()
    assert queue_id in node.watching_queues


@pytest.mark.asyncio
async def test_read_transaction():
    queue_id = uuid4()
    async with redis_transaction(RedisKeys.QUEUE_WATCHERS):
        await dao.create_queue(queue_id.bytes)
        await server_nodes.create_server_node()

        assert queue_id.bytes not in await dao.get_all_queues()
    assert queue_id.bytes in await dao.get_all_queues()

