from uuid import uuid4
import pytest
from cron_o import dao, server_nodes, api, ScheduledCall


call = ScheduledCall()
call.call_id = uuid4()
call.queue_id = uuid4()


"""
@pytest.mark.asyncio
async def test_node_created_waiting():
    node = await server_nodes.create_server_node()

    assert len(node.watching_queues) == 0
    assert len(node.workers) == 0
"""


@pytest.mark.asyncio
async def test_worker_creates_async_worker_when_new_tasks_arrives():
    node = await server_nodes.create_server_node()
    api.add_scheduled_call_transaction(call)

    assert len(node.watching_queues) == 1
    assert len(node.workers) == 1





