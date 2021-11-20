import asyncio
import logging
from typing import Optional

from asyncio import Task

from cron_o import stream, storage
from cron_o.dao import async_transaction
from cron_o import time_utils

from .models import ScheduledCall


class AsyncCallbackQueue:

    def __init__(self, queue_id: bytes = b'\x00'):
        self.queue_id = queue_id
        self.next_call: Optional[ScheduledCall] = None
        self.running = True
        self.main_loop = asyncio.get_event_loop()
        self.output_lock = asyncio.Lock()
        self.blocking_read_time: Optional[int] = None
        self.async_callbacks = True
        self.unlock_task: Optional[Task] = None
        self.input_task: Optional[Task] = None
        self.output_task: Optional[Task] = None

    async def start_listening(self):
        self.input_task = self.main_loop.create_task(self.input_loop())
        self.output_task = self.main_loop.create_task(self.output_loop())
        logging.info(f"Async queue started for queue {self.queue_id}")

    async def input_loop(self):
        stream.setup_context(self.queue_id)
        while self.running:
            self.debug("Ticking input")
            await self._input_tick()

    async def output_loop(self):
        stream.setup_context(self.queue_id)
        while self.running:
            self.debug("Ticking output")
            await self._output_tick()

    async def _input_tick(self):
        new_scheduled_calls = await stream.read(blocking_seconds=self.blocking_read_time)
        if not new_scheduled_calls:
            return

        self.info(
            "Received messages",
            ids=[c.call_id for c in new_scheduled_calls]
        )
        async with async_transaction():
            for call in new_scheduled_calls:
                if call.is_due():
                    self.main_loop.create_task(self._run_call_and_delete_from_stream(call))
                    continue
                await storage.add_call_to_sorted_set(call)
                if not self.next_call or self.next_call.call_timestamp_millis > call.call_timestamp_millis:
                    self.next_call = call
        await self._unlock_output()

    async def _output_tick(self):
        await self.output_lock.acquire()
        if not self.next_call:
            return

        now = time_utils.get_current_time_millis()
        if now > self.next_call.call_timestamp_millis:
            await self._run_and_remove_call(self.next_call)
        else:
            self.unlock_task = self.main_loop.create_task(
                self._unlock_output_task(self.next_call.call_timestamp_millis)
            )

    async def _unlock_output_task(self, millis: int):
        await asyncio.sleep(millis / 1000)
        self._unlock_output()

    async def _unlock_output(self):
        if self.output_lock.locked():
            self.output_lock.release()
        if self.unlock_task and not self.unlock_task.cancelled() and not self.unlock_task.done():
            self.unlock_task.cancel()
        self.unlock_task = None

    async def _run_and_remove_call(self, call: ScheduledCall):
        async with async_transaction():
            await storage.delete_from_sorted_set(call)
            await stream.delete(call)
            self.info("Removing call", call_id=call.call_id)
            if self.async_callbacks:
                self.main_loop.create_task(self._run_call_task(call))
            else:
                await self._run_call_task(call)

        possible_next = await storage.get_next_calls()
        if possible_next:
            self.next_call = await storage.get_call(possible_next[0])
            await self._unlock_output()
        else:
            self.next_call = None

    async def _run_call_and_delete_from_stream(self, call: ScheduledCall):
        await self._run_call_task(call)
        await stream.delete(call)

    async def _run_call_task(self, call: ScheduledCall):
        self.info("Running call", call_id=call.call_id)
        await asyncio.sleep(10)

    def next_call_time(self):
        return self.next_call.call_timestamp_millis if self.next_call else 0

    def info(self, msg: str, **kw):
        logging.info(msg + str(kw or ""))

    def debug(self, msg: str, **kw):
        logging.info(msg + str(kw or ""))