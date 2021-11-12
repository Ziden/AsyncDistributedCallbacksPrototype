import asyncio

import pytest

from cron_o import dao


@pytest.fixture(scope="session")
def event_loop(request):
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(autouse=True, scope="function")
def redis_flush():
    asyncio.get_event_loop().run_until_complete(dao.wipe())
    yield None
