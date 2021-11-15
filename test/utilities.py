import asyncio
import sys
from contextlib import contextmanager
from typing import Type, Optional
from unittest.mock import patch
from uuid import uuid4

from cron_o import time_utils
from cron_o.models import ScheduledCall


def generate_call(timestamp: int = 0, defined_id: Optional[bytes] = None) -> ScheduledCall:
    call = ScheduledCall(defined_id or uuid4().bytes)
    call.queue_id = uuid4().bytes
    call.call_timestamp_millis = timestamp
    call.call_params = {}
    return call


async def run_all_coroutines_from(cls: Type):
    await asyncio.gather(*[
        t for t in asyncio.all_tasks() if cls.__name__ in t.get_coro().__qualname__
    ])


@contextmanager
def added_time_millis(millis: int):
    with patch("cron_o.time_utils.get_current_time_millis", return_value=time_utils.get_current_time_millis() + millis):
        yield



