import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as aioredis
from redis import ResponseError

from redis_streams import redis
from redis_streams.pending_handler import pending_handler
from redis_streams.producer import producer
from redis_streams.worker import worker
from settings import settings

REDIS_URL = str(settings.REDIS_URL)


@asynccontextmanager
async def redis_db() -> AsyncGenerator:
    pool = aioredis.BlockingConnectionPool.from_url(
        REDIS_URL,
        max_connections=1_000,
        decode_responses=True,
    )
    redis.redis_client = aioredis.Redis(connection_pool=pool)
    yield
    await pool.disconnect()


async def create_group(skey: str, gname: str) -> None:
    """
    Create consumer group
    :param skey: stream name
    :param gname: consumer group name
    """
    try:
        await redis.redis_client.xgroup_create(name=skey, groupname=gname, id=0)
    except ResponseError as e:
        print(f"Raised: {e}")


async def run():
    async with redis_db():
        tasks = [
            asyncio.create_task(producer.producer(settings.STREAM), name="Producer")
        ]

        # Create consumer group
        await create_group(settings.STREAM, settings.GROUP)

        # Create the worker (consumer) tasks
        for _ in range(settings.NUM_WORKERS):
            name = f"Worker-{_}"
            tasks.append(
                asyncio.create_task(
                    worker.worker(settings.STREAM, settings.GROUP, name),
                    name=name,
                )
            )

        # Create the pending handler worker
        for _ in range(settings.NUM_HANDLERS):
            name = f"Handler-{_}"
            tasks.append(
                asyncio.create_task(
                    pending_handler.pending_handler(
                        settings.STREAM, settings.GROUP, name
                    ),
                    name=name,
                )
            )

        try:
            await asyncio.wait(tasks, timeout=20)
        finally:
            for task in tasks:
                print(f"Cancel task={task.get_name()}")
                task.cancel()


if __name__ == "__main__":
    asyncio.run(run())
