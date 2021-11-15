import random
import sys

import pytest

from cron_o import storage
from cron_o.dao import async_transaction
from cron_o.time_utils import get_current_time_millis
from test.utilities import generate_call


@pytest.mark.skip
@pytest.mark.asyncio
async def test_big_sorted_set_performance():
    async with async_transaction():
        for i in range(1000000):
            storage.add_call_to_sorted_set(generate_call(random.randrange(0, sys.maxsize)))

    before = get_current_time_millis()
    async with async_transaction():
        storage.add_call_to_sorted_set(generate_call(random.randrange(0, sys.maxsize)))
    after = get_current_time_millis()

    assert after - before < 3, "Inserting in the sorted set is taking more then 3ms"
