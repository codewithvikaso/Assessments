import redis.asyncio as redis

from app.core.config import settings


class RedisManager:

    def __init__(self):
        self.client = None

    async def connect(self):
        self.client = redis.from_url(
            settings.REDIS_URL,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5,
        )

        await self.client.ping()

        print("Redis connected successfully")

    async def disconnect(self):
        if self.client:
            await self.client.aclose()

        print("Redis connection closed")

    def get_client(self):
        if self.client is None:
            raise RuntimeError("Redis is not connected")

        return self.client


redis_manager = RedisManager()


def get_redis():
    return redis_manager.get_client()