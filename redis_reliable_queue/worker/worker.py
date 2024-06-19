import random
from json import loads

from redis_reliable_queue import redis


async def process_message(queue: str, message_json: str):
    """
    Process messages retrieved from queue
    :param queue:  - queue name
    :param message_json:  - JSON message for processing
    """
    message = loads(message_json)
    print(
        f"Message received: id={message['id']}, message_number={message['data']['message_number']}"
    )

    # mimic potential processing errors
    processed_ok = random.choices((True, False), weights=(5, 1), k=1)[0]
    if processed_ok:
        print("\tProcessed successfully")
        await redis.redis_client.lrem("processing", count=0, value=f"{message_json}")
    else:
        print("\tProcessing failed - requeuing...")
        await redis.redis_client.lpush(queue, message_json)


async def worker(queue: str, processing_queue: str):
    """
    Consumes items from the Redis queue
    :param queue:  - queue name
    :param processing_queue:  - processing queue name
    """

    while True:
        message_json = await redis.redis_client.blmove(
            queue, processing_queue, timeout=0
        )
        await process_message(queue, message_json)
