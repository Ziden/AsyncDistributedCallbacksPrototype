from uuid import uuid4

import pytest

from cron_o.models import ScheduledCall
from cron_o import stream


stream.setup_context(b'/x00')


@pytest.mark.asyncio
async def test_add_multiple(call):
    await stream.add(call)
    await stream.add(call)
    await stream.add(call)

    in_stream = await stream.read()

    assert len(in_stream) == 3


@pytest.mark.asyncio
async def test_stream_data(call):
    await stream.add(call)

    in_stream = await stream.read()

    assert in_stream[0].queue_id == call.queue_id
    assert in_stream[0].call_timestamp_millis == call.call_timestamp_millis
    assert in_stream[0].call_params == call.call_params


