from .models import ScheduledCall
from . import dao


class RedisKeys:
    CALLS_SORTED_SET = "calls:sortedset"
    CALL = "call:{}"
    CALL_STREAM = "calls:stream"

    @staticmethod
    def call_key(call_id: bytes):
        return RedisKeys.CALL.format(call_id)


@dao.require_transaction()
async def add_call_to_sorted_set(call: ScheduledCall):
    await dao.writer().zadd(RedisKeys.CALLS_SORTED_SET, {call.call_id: call.call_timestamp_millis})
    await call.serialize(dao.writer())


async def delete_from_sorted_set(call: ScheduledCall):
    await dao.writer().zrem(RedisKeys.CALLS_SORTED_SET, call.call_id)


async def get_next_calls() -> bytes:
    return await dao.reader().zrange(RedisKeys.CALLS_SORTED_SET, 0, 0)


async def get_all_calls() -> bytes:
    return await dao.reader().zrange(RedisKeys.CALLS_SORTED_SET, 0, -1)


async def get_call(call_id: bytes) -> ScheduledCall:
    return await ScheduledCall(call_id).deserialize(dao.reader())


async def delete_call(call_id: bytes):
    dao.writer().hdel(RedisKeys.CALL, call_id)
