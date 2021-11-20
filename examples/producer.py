import asyncio
import logging
from asyncio import AbstractEventLoop
from uuid import uuid4

from cron_o import stream, time_utils, dao
from cron_o.async_queue import AsyncCallbackQueue
from cron_o.models import ScheduledCall
from test.utilities import generate_call
import random
logging.root.setLevel(logging.NOTSET)
logging.basicConfig(level=logging.NOTSET)



# one worker example
async def main_loop():
    logging.info("Starting main loop")
    queue_id = b'\xFF\x00'
    for i in range(100000):
        call = generate_call(time_utils.get_current_time_millis() + 500 - random.randrange(1000))
        call.queue_id = queue_id
        stream.setup_context(queue_id)
        print("Sending call")
        await stream.add(call)

    print("DONE")
    await asyncio.sleep(1000)


if __name__ == "__main__":
    asyncio.run(main_loop())
