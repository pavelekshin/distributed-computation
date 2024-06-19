from redis_streams import redis
from redis_streams.worker import worker


async def pending_handler(skey: str, gname: str, name: str):
    """
    Consumes pending item from the Redis queue
    """
    while True:
        pr = await redis.redis_client.xpending_range(
            name=skey, groupname=gname, min="-", max="+", count=1_000, idle=15_000
        )
        if message_ids := [message["message_id"] for message in pr]:
            for ids, message in await redis.redis_client.xclaim(
                    name=skey,
                    groupname=gname,
                    consumername=name,
                    min_idle_time=15_000,
                    message_ids=message_ids,
            ):
                await worker.process_message(message["json"], ids, skey, gname)
