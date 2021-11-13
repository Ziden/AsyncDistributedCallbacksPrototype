from uuid import UUID

from cron_o import dao
from cron_o.dao import redis_transaction, RedisKeys
from cron_o.models import ScheduledCall


async def add_scheduled_call_transaction(queue_id: UUID, call: ScheduledCall):
    async with redis_transaction():
        await dao.add_scheduled_call(queue_id.bytes, call)
