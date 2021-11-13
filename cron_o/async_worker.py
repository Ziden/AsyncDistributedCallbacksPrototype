import asyncio
from asyncio import Task
from uuid import UUID, uuid4


class AsyncWorker:
    worker_id: UUID
    watching_queue: UUID
    task: Task
    looping: bool = False

    def __init__(self, queue_to_watch: UUID):
        self.worker_id = uuid4()
        self.watching_queue = queue_to_watch


async def async_worker_task(async_worker: AsyncWorker):
    while async_worker.looping:
        asyncio.wait(1)


def create_new_async_worker(queue_id: UUID):
    new_async_worker = AsyncWorker(queue_id)
    loop = asyncio.get_event_loop()
    new_async_worker.task = loop.create_task(async_worker_task(new_async_worker))
    return new_async_worker
