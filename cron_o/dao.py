import asyncio
from contextlib import contextmanager, asynccontextmanager
from enum import Enum
from typing import Dict, Optional, List
from uuid import UUID

import aioredis
from aioredis import Redis

from cron_o import ScheduledCall
from cron_o.time_utils import get_current_time_millis


class RedisKeys:
    # All nodes and their last ticks
    NODES_LAST_TICKS = "node::ticks"
    # All queues and which node is watching it
    QUEUE_WATCHERS = "queue::node"
    # Fifo list of new calls to be added
    NEW_CALLS = "calls:list"
    # Queue sorted sets of scheduled calls
    @staticmethod
    def queue_sorted_set(queue_id: bytes):
        return f"queue:{queue_id}"

    @staticmethod
    def queue_modification_list(queue_id: bytes):
        return f"queue:mod:{queue_id}"


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
        return self._connection

    def get_connection(self):
        return self._connection or self.connect()

    def writer(self):
        return self._pipeline or self.get_connection()

    def reader(self):
        return self.get_connection()


_dao = RedisDao()


@asynccontextmanager
async def redis_transaction(*watched_keys):
    pipe = _dao.writer().pipeline(True)
    if watched_keys:
        await pipe.watch(*watched_keys)
    pipe.multi()
    _dao._pipeline = pipe
    yield
    await pipe.execute(True)
    _dao._pipeline = None


@contextmanager
def require_transaction():
    if not _dao._pipeline:
        raise Exception("Not in transaction")
    yield


async def connect():
    await _dao.connect()


def get_dao():
    return _dao


async def wipe():
    await _dao.writer().flushdb(asynchronous=False)


async def watch_queues(node_id: bytes, queue_ids: List[bytes]):
    await _dao.writer().hmset(RedisKeys.QUEUE_WATCHERS, mapping={
        queue_id: node_id for queue_id in queue_ids
    })


async def get_all_queues() -> Dict[bytes, bytes]:
    """ Returns queue_id : watching_node_id"""
    return await _dao.reader().hgetall(RedisKeys.QUEUE_WATCHERS)


@require_transaction()
async def add_scheduled_call(queue_id: bytes, scheduled_call: ScheduledCall):
    if not await _dao.reader().hexists(RedisKeys.QUEUE_WATCHERS, queue_id):
        await create_queue(queue_id)
    sorted_set_data = {
        scheduled_call.call_id.bytes: scheduled_call.call_timestamp_millis
    }
    await _dao.writer().zadd(RedisKeys.queue_sorted_set(queue_id), sorted_set_data)
    await _flag_queue_modified(queue_id)


async def get_scheduled_calls(queue_id: bytes, min_timestamp: int, max_timestamp: int):
    return await _dao.reader().zrange(RedisKeys.queue_sorted_set(queue_id), min_timestamp, max_timestamp)


async def _flag_queue_modified(queue_id: bytes):
    await _dao.writer().lpush(RedisKeys.NEW_CALLS, queue_id)
    await _dao.writer().lpush(RedisKeys.NEW_CALLS, queue_id)


async def get_modified_queues():
    return await _dao.reader().lrange(RedisKeys.NEW_CALLS, 0, 5)


async def create_queue(queue_id: bytes):
    await _dao.writer().hset(RedisKeys.QUEUE_WATCHERS, queue_id, "")


##############
# Heartbeats #
##############


async def do_heartbeat(node_id: bytes):
    await _dao.writer().hset(RedisKeys.NODES_LAST_TICKS, node_id.bytes, get_current_time_millis())


async def get_all_server_node_heartbeats() -> Dict[bytes, int]:
    """ Returns a dictionary of node_ids to the last timestamp in millis that node has sent a beat """
    all_nodes = await _dao.reader().hgetall(RedisKeys.NODES_LAST_TICKS)
    return {k: int(v) for k, v in all_nodes.items()}

