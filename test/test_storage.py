import asyncio
import random
import sys
from datetime import time
from uuid import uuid4

import pytest

from cron_o.dao import async_transaction
from cron_o.models import ScheduledCall
from cron_o import storage
from cron_o.time_utils import get_current_time_millis
from test.utilities import generate_call


@pytest.mark.asyncio
async def test_adding_to_set(call):
    async with async_transaction():
        await storage.add_call_to_sorted_set(call)

    assert call.call_id in await storage.get_next_calls()


@pytest.mark.asyncio
async def test_creates_object_key(call):
    async with async_transaction():
        await storage.add_call_to_sorted_set(call)

    saved_call = await storage.get_call(call.call_id)
    assert saved_call.call_params == call.call_params


@pytest.mark.asyncio
async def test_creates_object_key(call):
    ids = [uuid4().bytes, uuid4().bytes, uuid4().bytes]
    async with async_transaction():
        for i in ids:
            await storage.add_call_to_sorted_set(generate_call(0, i))

    for i in ids:
        assert (await storage.get_call(i)).call_id == i


@pytest.mark.asyncio
async def test_set_ordering():
    c1 = generate_call(50)
    c2 = generate_call(60)
    c3 = generate_call(70)

    async with async_transaction():
        await asyncio.gather(
            storage.add_call_to_sorted_set(c1),
            storage.add_call_to_sorted_set(c2),
            storage.add_call_to_sorted_set(c3)
        )

    next = await storage.get_next_calls()
    assert next[0] == c1.call_id


@pytest.mark.asyncio
async def test_reodering():
    c1 = generate_call(50)
    c2 = generate_call(60)
    c3 = generate_call(70)

    async with async_transaction():
        await asyncio.gather(
            storage.add_call_to_sorted_set(c1),
            storage.add_call_to_sorted_set(c2),
            storage.add_call_to_sorted_set(c3)
        )

        await storage.delete_from_sorted_set(c1)

    next = await storage.get_next_calls()
    assert next[0] == c2.call_id
