import asyncio
import json
import random

import aio_pika  # noqa
from aio_pika.abc import AbstractIncomingMessage

from rabbitmq_aio_pika import rabbit


async def process_message(incoming_message: AbstractIncomingMessage) -> None:
    """
    Process messages retrieved from queue
    :param incoming_message: AbstractIncomingMessage
    """
    message = json.loads(incoming_message.body.decode())
    print(
        f"Message received: id={message['id']}, from={incoming_message.consumer_tag}, "
        f"message_number={message['data']['message_number']}"
    )

    # mimic potential processing errors
    processed_ok = random.choices((True, False), weights=(10, 1), k=1)[0]
    if processed_ok:
        print("\tProcessed successfully")
        await incoming_message.ack()
    else:
        print("\tProcessing failed - requeuing...")


async def worker(queue_name: str, name: str) -> None:
    """
    Consumes items from the RabbitMQ queue
    :param name: worker name
    :param queue_name: worker queue
    """
    async with rabbit.rabbit_client.acquire() as channel:  # type: aio_pika.Channel
        await channel.set_qos(prefetch_count=100)  # number of messages per worker
        queue = await channel.declare_queue(
            queue_name,
            durable=True,  # Durable queue survive broker restart
            auto_delete=False,
        )
        await queue.consume(process_message, consumer_tag=name)
        await asyncio.Future()
