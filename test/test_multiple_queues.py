from uuid import uuid4

import pytest

from cron_o import stream, time_utils
from cron_o.async_queue import AsyncCallbackQueue
from test.utilities import generate_call


@pytest.mark.asyncio
async def test_worker_adding_to_storage(call):
    now = time_utils.get_current_time_millis()
    queue_1 = AsyncCallbackQueue(uuid4().bytes)
    queue_2 = AsyncCallbackQueue(uuid4().bytes)

    call_1 = generate_call(timestamp=now+500)
    call_2 = generate_call(timestamp=now+500)

    call_1.queue_id = queue_1.queue_id
    call_2.queue_id = queue_2.queue_id



