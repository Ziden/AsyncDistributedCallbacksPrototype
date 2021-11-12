import asyncio
from asyncio import Task
from uuid import UUID, uuid4


class AsyncWorker:
    worker_id: UUID
    watching_queue: UUID
    task: Task

    def __init__(self, queue_to_watch: UUID):
        self.worker_id = uuid4()
        self.watching_queue = queue_to_watch
