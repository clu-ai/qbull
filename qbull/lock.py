import asyncio
import aioredis
from pathlib import Path

class RedisLock:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis = None
        self.lock_extend_sha = None
        self.lock_release_sha = None

    async def connect(self):
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)

        base = Path(__file__).parent / "lua"
        extend_script = (base / "lock_extend.lua").read_text()
        release_script = (base / "lock_release.lua").read_text()

        self.lock_extend_sha = await self.redis.script_load(extend_script)
        self.lock_release_sha = await self.redis.script_load(release_script)

    async def acquire_or_extend(self, key: str, token: str, ttl_ms: int) -> bool:
        return await self.redis.evalsha(
            self.lock_extend_sha, 1, key, token, str(ttl_ms)
        ) == 1

    async def release(self, key: str, token: str) -> bool:
        return await self.redis.evalsha(
            self.lock_release_sha, 1, key, token
        ) == 1

    async def close(self):
        if self.redis:
            await self.redis.close()