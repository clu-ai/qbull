import asyncio
import aioredis
import uuid
import json
from datetime import datetime
from qbull.lock import RedisLock


class Publisher:
    def __init__(self, redis_url: str, stream_name: str):
        self.redis_url = redis_url
        self.stream_name = stream_name
        self.redis = None

    async def connect(self):
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)

    async def publish_job(self, data: dict):
        job_id = str(uuid.uuid4())
        payload = {
            "job_id": job_id,
            "created_at": datetime.utcnow().isoformat(),
            **data,
        }
        await self.redis.xadd(self.stream_name, {"job": json.dumps(payload)})
        return job_id

    async def close(self):
        if self.redis:
            await self.redis.close()


class Consumer:
    def __init__(
        self, redis_url: str, stream_name: str, group: str, consumer_name: str
    ):
        self.redis_url = redis_url
        self.stream_name = stream_name
        self.group = group
        self.consumer_name = consumer_name
        self.redis = None
        self.handlers = {}
        self.locker = RedisLock(redis_url)
        self.token = str(uuid.uuid4())

    def handler(self, cmd: str):
        def decorator(func):
            self.handlers[cmd] = func
            return func

        return decorator

    async def connect(self):
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
        await self.locker.connect()
        try:
            await self.redis.xgroup_create(
                self.stream_name, self.group, id="$", mkstream=True
            )
        except aioredis.exceptions.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    async def listen(self):
        while True:
            response = await self.redis.xreadgroup(
                groupname=self.group,
                consumername=self.consumer_name,
                streams={self.stream_name: ">"},
                count=1,
                block=5000,
            )
            if response:
                for stream, messages in response:
                    for message_id, fields in messages:
                        job_data = json.loads(fields["job"])
                        job_id = job_data.get("job_id")
                        lock_key = f"lock:{job_id}"

                        acquired = await self.locker.acquire_or_extend(
                            lock_key, self.token, 10000
                        )
                        if not acquired:
                            print(f"🔒 Lock ocupado para job_id {job_id}, saltando...")
                            continue

                        try:
                            cmd = job_data.get("cmd")
                            handler = self.handlers.get(cmd)
                            if handler:
                                await handler(job_data)
                            else:
                                print(f"⚠️ No handler para cmd: {cmd}")
                            await self.redis.xack(
                                self.stream_name, self.group, message_id
                            )
                        finally:
                            await self.locker.release(lock_key, self.token)

    async def close(self):
        if self.redis:
            await self.redis.close()
        await self.locker.close()
