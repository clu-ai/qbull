import asyncio
import redis.asyncio as aioredis
from redis.exceptions import RedisError
from pathlib import Path
import logging


class RedisLock:
    def __init__(self, redis_url: str):
        self.redis_url = redis_url
        self.redis = None
        self._lock_extend_sha = None
        self._lock_release_sha = None
        self._connected = False

    async def connect(self):
        if self._connected:
            return
        try:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()  # Verify connection

            base = Path(__file__).parent / "lua"
            extend_script = (base / "lock_extend.lua").read_text()
            release_script = (base / "lock_release.lua").read_text()

            # Load scripts using script_load for efficiency
            self._lock_extend_sha, self._lock_release_sha = await asyncio.gather(
                self.redis.script_load(extend_script),
                self.redis.script_load(release_script),
            )
            self._connected = True
            logging.info("RedisLock connected and Lua scripts loaded.")
        except (RedisError.exceptions.ConnectionError, OSError) as e:
            logging.error(f"RedisLock connection failed: {e}")
            self.redis = None
            self._connected = False
            raise  # Re-raise exception after logging

    async def acquire_or_extend(self, key: str, token: str, ttl_ms: int) -> bool:
        if not self._connected or not self.redis:
            logging.error("Cannot acquire lock: RedisLock not connected.")
            return False
        try:
            result = await self.redis.evalsha(
                self._lock_extend_sha, 1, key, token, str(ttl_ms)
            )
            return result == 1
        except RedisError.exceptions.RedisError as e:
            logging.error(f"Error acquiring/extending lock {key}: {e}")
            return False

    async def release(self, key: str, token: str) -> bool:
        if not self._connected or not self.redis:
            logging.warning("Cannot release lock: RedisLock not connected.")
            return False
        try:
            result = await self.redis.evalsha(self._lock_release_sha, 1, key, token)
            return result == 1
        except RedisError.exceptions.RedisError as e:
            logging.error(f"Error releasing lock {key}: {e}")
            return False  # Indicate failure

    async def close(self):
        if self.redis:
            try:
                await self.redis.close()
                logging.info("RedisLock connection closed.")
            except RedisError.exceptions.RedisError as e:
                logging.error(f"Error closing RedisLock connection: {e}")
            finally:
                self.redis = None
                self._connected = False
