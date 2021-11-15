import asyncio
from time import sleep
from uuid import uuid4

import pytest
from aioredis import WatchError

from cron_o.dao import async_transaction, writer, reader, _dao


@pytest.mark.asyncio
async def test_write_transaction():
    async with async_transaction("A"):
        await writer().set("A", "1")

    assert await reader().get("A")


@pytest.mark.asyncio
async def test_read_transaction_no_cache():
    async with async_transaction("A"):
        await writer().set("A", "1")
        assert not await reader().get("A")

    assert await reader().get("A")


@pytest.mark.asyncio
async def test_watch_error():
    with pytest.raises(WatchError):
        async with async_transaction("A"):
                await _dao.get_connection().set("A", 1)

