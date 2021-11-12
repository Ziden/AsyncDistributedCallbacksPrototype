import aioredis
import async_timeout
import asyncio

from aioredis import Redis


async def pubsub():
    redis: Redis = aioredis.Redis.from_url(
        "redis://localhost", max_connections=10, decode_responses=True
    )
    psub = redis.pubsub()

    async def reader(channel: aioredis.client.PubSub):
        while True:
            try:
                async with async_timeout.timeout(1):
                    message = await channel.get_message(ignore_subscribe_messages=True)
                    if message is not None:
                        print(f"(Reader) Message Received: {message}")
                    await asyncio.sleep(0.01)
            except asyncio.TimeoutError:
                pass

    async with psub as p:
        await p.subscribe("channel:1")
        await reader(p)  # wait for reader to complete
        await p.unsubscribe("channel:1")

    # closing all open connections
    await psub.close()
