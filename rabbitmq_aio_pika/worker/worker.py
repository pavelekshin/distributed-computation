import random
from json import loads

from aio_pika.abc import AbstractIncomingMessage

from rabbitmq_aio_pika.main import create_queue


async def process_message(incoming_message: AbstractIncomingMessage) -> None:
    """
    Process messages retrieved from queue
    :param incoming_message: AbstractIncomingMessage
    """

    message = loads(incoming_message.body.decode())
    print(
        f"Message received: id={message['id']}, message_number={message['data']['message_number']}"
    )

    # mimic potential processing errors
    processed_ok = random.choices((True, False), weights=(10, 1), k=1)[0]
    if processed_ok:
        print("\tProcessed successfully")
        await incoming_message.ack()
    else:
        print("\tProcessing failed - requeuing...")


async def worker(qname: str) -> None:
    """
    Consumes items from the Redis queue
    :param qname: worker queue
    """
    queue = await create_queue(qname)
    await queue.consume(process_message)
