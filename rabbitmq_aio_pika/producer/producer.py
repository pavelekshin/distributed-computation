import asyncio
from datetime import datetime
from json import dumps
from random import randrange
from uuid import uuid4

import aio_pika
from aio_pika import DeliveryMode

from rabbitmq_aio_pika import rabbit
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


async def producer(qname: str) -> None:
    """
    Puts all the requested work into the work queue.
    :param qname: producer queue
    """
    start = 0
    async with rabbit.rabbit_client.acquire() as channel:  # type: aio_pika.Channel
        while not channel.is_closed:  # prevent publish to closed channel
            job_id = str(uuid4())
            print(f"New Job {job_id}")
            for data in await data_generator(start, job_id):
                message_json = dumps(data)
                timestamp = datetime.timestamp(datetime.now())
                await channel.default_exchange.publish(  # publish message to queue
                    aio_pika.Message(
                        body=message_json.encode(),
                        timestamp=timestamp,
                        correlation_id=job_id,
                        delivery_mode=DeliveryMode.PERSISTENT,
                        # PERSISTENT messages prevent message lose while RabbitMQ is restarted
                    ),
                    routing_key=qname,
                )
            await asyncio.sleep(1)
            start += settings.NUM_TASKS
