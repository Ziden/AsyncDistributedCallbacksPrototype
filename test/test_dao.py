from uuid import uuid4
import pytest
from cron_o import dao


@pytest.mark.asyncio
async def test_dao_heartbeat():
    node_id = uuid4()
    await dao.do_heartbeat(node_id)

    heartbeats = await dao.get_all_server_node_heartbeats()
    assert node_id.bytes in heartbeats
