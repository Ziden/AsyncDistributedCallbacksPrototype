import asyncio
from dataclasses import dataclass
from typing import Union, Dict
from uuid import UUID, uuid4
import logging

import async_timeout
from msgpack import packb, unpackb

import aioredis

from cron_o.models import ScheduledCall
from node import pubsub

LIST_KEY = "scheduled_calls_queue"
RECEIVED_LIST_KEY = "scheduled_calls_queue"

redis = aioredis.Redis.from_url(
    "redis://localhost", max_connections=10
)


async def job_receiver():
    while True:
        try:
            async with async_timeout.timeout(1):
                # todo transactions
                _, obj = await redis.blpop(LIST_KEY, timeout=5)
                redis.lpush(RECEIVED_LIST_KEY, obj)
                await receive_scheduled_call(unpackb(obj))
        except asyncio.TimeoutError:
            pass


async def receive_scheduled_call(scheduled_call: ScheduledCall):
    logging.info(f"Received scheduled call {scheduled_call}")


async def add_scheduled_call(scheduled_call: ScheduledCall):
    logging.info(f"Adding scheduled call {scheduled_call}", scheduled_call)
    await redis.lpush(LIST_KEY, packb(scheduled_call.__dict__))


async def main():
    await add_scheduled_call(ScheduledCall(uuid4().bytes, "", {}, 50))
    await job_receiver()

if __name__ == "__main__":
    import os

    if "redis_version:2.6" not in os.environ.get("REDIS_VERSION", ""):
        asyncio.run(main())
