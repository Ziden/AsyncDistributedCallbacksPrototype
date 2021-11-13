from uuid import UUID

from cron_o import dao
from cron_o.dao import redis_transaction, RedisKeys
from cron_o.models import ScheduledCall


async def add_scheduled_call_transaction(call: ScheduledCall):
    async with redis_transaction():
        await dao.add_scheduled_call(call)
