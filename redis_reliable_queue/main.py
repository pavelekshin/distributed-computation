import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import redis.asyncio as aioredis

from redis_reliable_queue import redis
from redis_reliable_queue.producer import producer
from redis_reliable_queue.worker import worker
from settings import settings

REDIS_URL = str(settings.REDIS_URL)


@asynccontextmanager
async def redis_db() -> AsyncGenerator:
    pool = aioredis.BlockingConnectionPool.from_url(
        REDIS_URL,
        max_connections=10,
        decode_responses=True,
    )
    redis.redis_client = aioredis.Redis(connection_pool=pool)
    yield
    await pool.disconnect()


async def run():
    async with redis_db():
        tasks = [asyncio.create_task(producer.producer("queue"), name="Producer")]

        # Create the worker (consumer) tasks
        for _ in range(settings.NUM_WORKERS):
            tasks.append(
                asyncio.create_task(
                    worker.worker("queue", "processing"), name=f"Worker-{_}"
                )
            )

        try:
            await asyncio.wait(tasks, timeout=10)
        finally:
            for task in tasks:
                print(f"Cancel task={task.get_name()}")
                task.cancel()


if __name__ == "__main__":
    asyncio.run(run())
