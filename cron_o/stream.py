import logging
from _contextvars import ContextVar
from typing import List, Generator, Iterator, Optional

from .models import ScheduledCall
from . import dao

_queue_id = ContextVar("queue_id")
_last_id = ContextVar("last_id")


def get_queue_id() -> bytes:
    return _queue_id.get()


def setup_context(queue_id: bytes, last_id: str = "0-0"):
    _last_id.set(last_id)
    _queue_id.set(queue_id)


def get_stream_key():
    return f"calls:stream:{str(_queue_id.get())}"


async def add(call: ScheduledCall):
    await dao.writer().xadd(get_stream_key(), call.to_dict())
    logging.info(f"Added call to stream {call.call_id}")


async def read(blocking_seconds: Optional[int] = None) -> List[ScheduledCall]:
    response = await dao.reader().xread({get_stream_key(): _last_id.get()}, block=blocking_seconds)
    if not response:
        return response
    return _parse_redis_stream_response(response)


async def set_last_read_id(last_id):
    await dao.writer().set("")


def _parse_redis_stream_response(response) -> List[ScheduledCall]:
    elements = response[0][1]
    ret = []
    for element_id, element_data in elements:
        call = ScheduledCall().from_dict(element_data)
        call.stream_id = element_id
        ret.append(call)
        _last_id.set(element_id)
    return ret


async def read_all() -> List[ScheduledCall]:
    response = await dao.reader().xread({get_stream_key(): "0-0"})
    if not response:
        return response
    return _parse_redis_stream_response(response)


async def delete(call: ScheduledCall):
    await dao.writer().xdel(get_stream_key(), call.stream_id)
