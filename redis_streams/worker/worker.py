import random
from json import loads

from redis_streams import redis


async def process_message(message_json: str, ids: str, skey: str, gname: str) -> None:
    """
    Process messages retrieved from queue
    :param message_json:  - JSON message for processing
    :param ids: message ids
    :param skey: stream name
    :param gname: group name
    """
    message = loads(message_json)
    print(
        f"Message received: id={message['id']}, message_number={message['data']['message_number']}"
    )

    # mimic potential processing errors
    processed_ok = random.choices((True, False), weights=(10, 1), k=1)[0]
    if processed_ok:
        print("\tProcessed successfully")
        await redis.redis_client.xack(skey, gname, ids)
    else:
        print("\tProcessing failed - requeuing...")


async def print_pending_info(skey: str, gname: str) -> None:
    """
    Print pending items for stream and consumer group
    :param skey: stream name
    :param gname: consumer group
    """
    pr = await redis.redis_client.xpending(name=skey, groupname=gname)
    print(f"{pr.get('pending')} pending messages on '{skey=}' for group '{gname=}'")


async def worker(skey: str, gname: str, name: str) -> None:
    """
    Consumes items from the Redis queue
    :param skey: stream name
    :param gname: consumer group name
    :param name: worker name
    """
    while True:
        for stream, messages in await redis.redis_client.xreadgroup(
            groupname=gname, consumername=name, count=1, streams={skey: ">"}
        ):
            for ids, message in messages:
                await process_message(message["json"], ids, skey, gname)
                await print_pending_info(stream, gname)
