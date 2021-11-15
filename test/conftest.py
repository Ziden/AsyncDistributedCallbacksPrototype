import asyncio
import sys
from uuid import uuid4

import pytest

from cron_o import dao
from cron_o.models import ScheduledCall


@pytest.fixture(scope="session")
def event_loop(request):
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True, scope="function")
def redis_flush():
    asyncio.get_event_loop().run_until_complete(dao.wipe())
    yield None


@pytest.fixture()
def call():
    call = ScheduledCall(uuid4().bytes)
    call.queue_id = uuid4().bytes
    call.call_timestamp_millis = sys.maxsize
    call.call_params = {
        "wololo": 123,
        "walala": "321"
    }
    return call
