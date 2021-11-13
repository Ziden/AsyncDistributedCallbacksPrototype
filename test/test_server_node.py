import asyncio
from time import sleep
from uuid import uuid4
import pytest
from cron_o import dao, server_nodes, api


@pytest.mark.asyncio
async def test_new_node_with_no_queues():
    node = await server_nodes.create_server_node()

    assert len(node.watching_queues) == 0


@pytest.mark.asyncio
async def test_new_node_picks_unwatched_queues():
    queue_id = uuid4()
    await dao.create_queue(queue_id.bytes)

    node = await server_nodes.create_server_node()
    assert queue_id in node.watching_queues


@pytest.mark.asyncio
async def test_node_creating_one_task_per_queue():
    for i in range(10):
        queue_id = uuid4()
        await dao.create_queue(queue_id.bytes)

    node = await server_nodes.create_server_node()
    assert len(node.workers) == 10


@pytest.mark.asyncio
async def test_max_queue_limit_per_node():
    server_nodes.QUEUES_PER_NODE = 5
    for i in range(server_nodes.QUEUES_PER_NODE + 5):
        queue_id = uuid4()
        await dao.create_queue(queue_id.bytes)

    node = await server_nodes.create_server_node()
    assert len(node.watching_queues) == server_nodes.QUEUES_PER_NODE