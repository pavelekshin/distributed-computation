import dataclasses
from datetime import timedelta

from redis.asyncio import Redis

redis_client: Redis = None  # type: ignore


@dataclasses.dataclass
class RedisData:
    key: bytes | str
    value: bytes | str | None = None
    ttl: int | timedelta = None
