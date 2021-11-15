import asyncio
import logging
from asyncio import AbstractEventLoop
from uuid import uuid4

from cron_o import stream, time_utils
from cron_o.async_queue import AsyncCallbackQueue
from cron_o.models import ScheduledCall

logging.root.setLevel(logging.NOTSET)
logging.basicConfig(level=logging.NOTSET)


# one worker example
async def main_loop():
    logging.info("Starting main loop")
    queue_id = b'\xFF\x00'
    worker = AsyncCallbackQueue(queue_id)
    worker.blocking_read_time = 10000
    await worker.start_listening()
    while worker.running:
        await asyncio.sleep(10000000)

if __name__ == "__main__":
    asyncio.run(main_loop())
