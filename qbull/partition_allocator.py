import asyncio
import uuid
import redis.asyncio as aioredis
import logging

class PartitionAllocator:
    def __init__(self, redis_url: str, total_partitions: int, ttl_sec=60):
        self.redis_url = redis_url
        self.total_partitions = total_partitions
        self.ttl_sec = ttl_sec
        self.token = str(uuid.uuid4())
        self.partition = None
        self.redis = None

    async def connect(self):
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)

    async def acquire_n(self, count):
        acquired = []
        while len(acquired) < count:
            for i in range(self.total_partitions):
                if i in acquired:
                    continue
                key = f"partition_lock:{i}"
                ok = await self.redis.set(key, self.token, ex=self.ttl_sec, nx=True)
                if ok:
                    logging.info(f"✅ Acquired partition {i}")
                    acquired.append(i)
                    if len(acquired) == count:
                        break
            if len(acquired) < count:
                logging.warning("⏳ Not enough free partitions, waiting...")
                await asyncio.sleep(3)
        return acquired

    async def acquire_partition(self):
        while True:
            for i in range(self.total_partitions):
                key = f"partition_lock:{i}"
                ok = await self.redis.set(key, self.token, ex=self.ttl_sec, nx=True)
                if ok:
                    self.partition = i
                    logging.info(f"✅ Acquired partition {i}")
                    return i
            logging.warning("⏳ All partitions are busy, waiting...")
            await asyncio.sleep(3)

    async def refresh_lock(self):
        if self.partition is not None:
            key = f"partition_lock:{self.partition}"
            await self.redis.expire(key, self.ttl_sec)

    async def release(self):
        if self.partition is not None:
            key = f"partition_lock:{self.partition}"
            val = await self.redis.get(key)
            if val == self.token:
                await self.redis.delete(key)
                logging.info(f"🔓 Released partition {self.partition}")

    async def close(self):
        await self.release()
        if self.redis:
            await self.redis.close()