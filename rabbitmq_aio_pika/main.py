import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import aio_pika
from aio_pika.abc import AbstractQueue, AbstractRobustConnection
from aio_pika.pool import Pool

from rabbitmq_aio_pika import rabbit
from rabbitmq_aio_pika.producer import producer
from rabbitmq_aio_pika.worker import worker
from settings import settings

RABBIT_URL = str(settings.RABBIT_URL)


@asynccontextmanager
async def rabbitmq() -> AsyncGenerator:
    async def get_connection() -> AbstractRobustConnection:
        return await aio_pika.connect_robust(RABBIT_URL)

    connection_pool: Pool = Pool(get_connection, max_size=2)

    async def get_channel() -> aio_pika.Channel:
        async with connection_pool.acquire() as connection:
            return await connection.channel()

    rabbit.rabbit_client = Pool(get_channel, max_size=10)
    yield
    await connection_pool.close()
    await rabbit.rabbit_client.close()


async def create_queue(qname: str) -> AbstractQueue:
    """
    Create queue
    :param qname: queue name
    """
    async with rabbit.rabbit_client.acquire() as channel:  # type: aio_pika.Channel
        return await channel.declare_queue(
            qname,
            durable=False,
            auto_delete=False,
        )


async def run():
    async with rabbitmq():
        tasks = [
            asyncio.create_task(producer.producer(settings.QUEUE), name="Producer")
        ]

        # Create queue
        await create_queue(settings.QUEUE)

        # Create the worker (consumer) tasks
        for _ in range(settings.NUM_WORKERS):
            name = f"Worker-{_}"
            tasks.append(
                asyncio.create_task(
                    worker.worker(settings.QUEUE),
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
