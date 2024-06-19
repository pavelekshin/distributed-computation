import asyncio
from datetime import datetime
from json import dumps
from random import randrange
from uuid import uuid4

from redis_streams import redis
from settings import settings


async def data_generator(start: int, job_id: str) -> list[dict[str, str | int]]:
    """
    Generates data
    :param start: range
    :param job_id: job uuid
    :return:
    """
    return [
        {
            "id": job_id,
            "ts": datetime.now().isoformat(),
            "data": {
                "message_number": i,
                "x": randrange(0, 100),
                "y": randrange(0, 100),
            },
        }
        for i in range(start, start + randrange(settings.NUM_TASKS))
    ]


async def producer(stream: str) -> None:
    """
    Puts all the requested work into the work queue.
    :param stream: stream name
    """
    start = 0
    while True:
        job_id = str(uuid4())
        print(f"New Job {job_id}")
        for data in await data_generator(start, job_id):
            message_json = dumps(data)
            await redis.redis_client.xadd(stream, {"json": message_json})
        await asyncio.sleep(1)
        start += settings.NUM_TASKS
