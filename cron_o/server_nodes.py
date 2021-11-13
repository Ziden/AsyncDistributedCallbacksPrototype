import asyncio
from typing import List
from uuid import uuid4, UUID

from cron_o.async_worker import AsyncWorker, create_new_async_worker
from .dao import redis_transaction
from . import dao
from . import time_utils
import logging


MILLIS_TILL_DIED = 3000

QUEUES_PER_NODE = 2000
AMT_WORKERS = 4

logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)


class ServerNode:
    node_id: UUID
    watching_queues: List[UUID]
    workers: List[AsyncWorker]
    looping: bool = False

    def __init__(self, node_id: UUID):
        self.node_id = node_id
        self.watching_queues = []
        self.workers = []
        self.looping = False


async def create_server_node() -> ServerNode:
    node = ServerNode(uuid4())
    logging.info("New Server Node Spawned")
    await _watch_available_queues_transaction(node)
    return node


async def _watch_available_queues_transaction(node: ServerNode):
    unwatched_queues = await _get_unwatched_queues_transaction()
    will_watch = unwatched_queues[:QUEUES_PER_NODE]
    if will_watch:
        await dao.watch_queues(node.node_id.bytes, will_watch)
        for queue_id in will_watch:
            node.workers.append(create_new_async_worker(queue_id))
    logging.info("Watching queues", queues=will_watch)
    node.watching_queues = [UUID(bytes=b) for b in will_watch]


async def _get_unwatched_queues_transaction() -> List[UUID]:
    all_queues = await dao.get_all_queues()
    last_heartbeats = await dao.get_all_server_node_heartbeats()
    unwatched_queues = []
    for queue_id, watching_node_id in all_queues.items():
        last_beat = last_heartbeats.get(watching_node_id, 0)
        if not watching_node_id or time_utils.is_due(last_beat + MILLIS_TILL_DIED):
            unwatched_queues.append(queue_id)
    return unwatched_queues
