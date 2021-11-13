import asyncio
from contextlib import contextmanager, asynccontextmanager
from enum import Enum
from typing import Dict, Optional, List
from uuid import UUID

import aioredis
from aioredis import Redis
from cron_o.time_utils import get_current_time_millis
from main import ScheduledCall


class RedisKeys:
    # All nodes and their last ticks
    NODES_LAST_TICKS = "server-nodes"
    # All queues and which node is watching it
    QUEUE_WATCHERS = "job-queues"
    # Fifo list of new calls to be added
    NEW_CALLS = "new-calls"
    # Queue sorted sets of scheduled calls
    SORTED_SET = "queue-{}"

    @staticmethod
    def queue(queue_id: bytes):
        queue_id_int = int.from_bytes(queue_id, byteorder='big', signed=True)
        return RedisKeys.SORTED_SET.format(queue_id_int)


class RedisDao:
    _connection: Redis
    _pipeline: Optional[Redis]

    def __init__(self):
        self._pipeline = None
        self._connection = None

    def connect(self):
        self._connection = aioredis.Redis.from_url(
            "redis://localhost", max_connections=10, encoding='utf-8'
        )

    def get_connection(self):
        if not self._connection:
            self.connect()
        return self._pipeline or self._connection


_dao = RedisDao()


async def connect():
    await _dao.connect()


async def wipe():
    await _dao.get_connection().flushdb(asynchronous=False)


async def watch_queues(node_id: bytes, queue_ids: List[bytes]):
    await _dao.get_connection().hmset(RedisKeys.QUEUE_WATCHERS, mapping={
        queue_id: node_id for queue_id in queue_ids
    })


async def get_all_queues() -> Dict[bytes, bytes]:
    """ Returns queue_id : watching_node_id"""
    return await _dao.get_connection().hgetall(RedisKeys.QUEUE_WATCHERS)


async def add_scheduled_call(queue_id: bytes, scheduled_call: ScheduledCall):
    if not await _dao.get_connection().hexists(RedisKeys.QUEUE_WATCHERS, queue_id):
        await create_queue(queue_id)
    await _dao.get_connection().zadd(RedisKeys.queue(queue_id), {scheduled_call.pack(): scheduled_call.call_timestamp_millis})


async def get_all_scheduled_calls(queue_id: bytes):
    return await _dao.get_connection().zrange(RedisKeys.queue(queue_id), "-inf", "+inf")


async def create_queue(queue_id: bytes):
    await _dao.get_connection().hset(RedisKeys.QUEUE_WATCHERS, queue_id, "")


async def add_new_call_to_queue():
    await _dao.get_connection().lpush()

##############
# Heartbeats #
##############


async def do_heartbeat(node_id: bytes):
    await _dao.get_connection().hset(RedisKeys.NODES_LAST_TICKS, node_id.bytes, get_current_time_millis())


async def get_all_server_node_heartbeats() -> Dict[bytes, int]:
    """ Returns a dictionary of node_ids to the last timestamp in millis that node has sent a beat """
    all_nodes = await _dao.get_connection().hgetall(RedisKeys.NODES_LAST_TICKS)
    return {k: int(v) for k, v in all_nodes.items()}


@asynccontextmanager
async def redis_transaction(*watched_keys):
    pipe = _dao.get_connection().pipeline(True)
    await pipe.watch(*watched_keys)
    pipe.multi()
    _dao._pipeline = pipe
    yield
    await pipe.execute(True)
    _dao._pipeline = None

