import asyncio
import redis.asyncio as aioredis
import uuid
import json
from datetime import datetime, timezone
import redis.exceptions
import logging

from src.qbull.lock import RedisLock
from src.qbull.partitioner import get_partition
from src.qbull.config import DEFAULT_JOB_LOCK_TTL_MS  # Import default TTL


class Publisher:
    def __init__(self, redis_url: str, stream_name: str, partitions: int = 1):
        self.redis_url = redis_url
        self.stream_name = stream_name
        self.partitions = max(1, partitions)  # Ensure at least 1 partition
        self.redis = None
        self._connected = False

    async def connect(self):
        if self._connected:
            return
        try:
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            self._connected = True
            logging.info(f"Publisher connected for stream '{self.stream_name}'.")
        except (redis.exceptions.ConnectionError, OSError) as e:
            logging.error(f"Publisher connection failed: {e}")
            self.redis = None
            self._connected = False
            raise

    async def publish_job(
        self, data: dict, partition_key: str | None = None
    ) -> str | None:
        if not self._connected or not self.redis:
            logging.error("Cannot publish job: Publisher not connected.")
            return None

        job_id = str(uuid.uuid4())
        payload = {
            "job_id": job_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
            **data,
        }

        # Determine partition key: explicit partition_id, 'to' field, or job_id as fallback
        key_source = (
            partition_key or data.get("partition_id") or data.get("to") or job_id
        )
        partition = get_partition(str(key_source), self.partitions)
        stream_partition = f"{self.stream_name}:{partition}"

        try:
            job_json = json.dumps(payload)
            # XADD stream_key * field value [field value ...]
            await self.redis.xadd(stream_partition, {"job": job_json})
            logging.info(
                f"Published job {job_id} to partition {partition} ({stream_partition}) using key '{key_source}'"
            )
            return job_id
        except redis.exceptions.RedisError as e:
            logging.error(f"Failed to publish job {job_id} to {stream_partition}: {e}")
            return None
        except TypeError as e:
            logging.error(f"Failed to serialize job data for job {job_id}: {e}")
            return None

    async def close(self):
        if self.redis:
            try:
                await self.redis.close()
                logging.info(
                    f"Publisher connection closed for stream '{self.stream_name}'."
                )
            except redis.exceptions.RedisError as e:
                logging.error(f"Error closing Publisher connection: {e}")
            finally:
                self.redis = None
                self._connected = False


class Consumer:
    def __init__(
        self,
        redis_url: str,
        stream_name: str,
        group: str,
        consumer_name: str,
        partition: int = 0,
        job_lock_ttl_ms: int = DEFAULT_JOB_LOCK_TTL_MS,
        read_block_ms: int = 5000,
    ):
        self.redis_url = redis_url
        self.stream_name = stream_name
        self.group = group
        self.consumer_name = (
            f"{consumer_name}-{partition}"  # Ensure unique consumer name per partition
        )
        self.partition = partition
        self.stream_partition = f"{stream_name}:{partition}"
        self.job_lock_ttl_ms = job_lock_ttl_ms
        self.read_block_ms = read_block_ms

        self.redis = None
        self._connected = False
        self.handlers = {}
        self.locker = RedisLock(redis_url)
        self.token = str(uuid.uuid4())  # Unique token for this consumer instance
        self._stop_event = asyncio.Event()

    def handler(self, cmd: str):
        def decorator(func):
            if cmd in self.handlers:
                logging.warning(f"Handler for command '{cmd}' is being overwritten.")
            self.handlers[cmd] = func
            return func

        return decorator

    async def connect(self):
        if self._connected:
            return
        try:
            # Connect main redis client and locker client
            self.redis = await aioredis.from_url(self.redis_url, decode_responses=True)
            await self.redis.ping()
            await self.locker.connect()  # Connects its own redis instance

            # Ensure consumer group exists
            await self.redis.xgroup_create(
                self.stream_partition,
                self.group,
                id="0",
                mkstream=True,  # Read from start if group new
            )
            logging.info(
                f"Consumer group '{self.group}' ensured for stream '{self.stream_partition}'."
            )
            self._connected = True
            logging.info(
                f"Consumer '{self.consumer_name}' connected (Token: {self.token[:8]}...)."
            )

        except redis.exceptions.ResponseError as e:
            if "BUSYGROUP Consumer Group name already exists" in str(e):
                logging.info(
                    f"Consumer group '{self.group}' already exists for stream '{self.stream_partition}'."
                )
                self._connected = True  # Group exists, connection is fine
                logging.info(
                    f"Consumer '{self.consumer_name}' connected (Token: {self.token[:8]}...)."
                )
            else:
                logging.error(
                    f"Failed to create/verify consumer group '{self.group}': {e}"
                )
                await self.close_internal()  # Cleanup partial connections
                raise
        except (redis.exceptions.ConnectionError, OSError) as e:
            logging.error(f"Consumer connection failed: {e}")
            await self.close_internal()
            raise

    async def listen(self):
        if not self._connected or not self.redis:
            raise ConnectionError("Consumer is not connected.")

        logging.info(
            f"Consumer '{self.consumer_name}' starting listening on '{self.stream_partition}'..."
        )
        self._stop_event.clear()

        while not self._stop_event.is_set():
            try:
                # XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
                response = await self.redis.xreadgroup(
                    groupname=self.group,
                    consumername=self.consumer_name,
                    streams={self.stream_partition: ">"},  # '>' means new messages only
                    count=1,
                    block=self.read_block_ms,
                )

                if not response:
                    continue  # Timeout, loop again

                # Response format: [[stream_name, [[message_id, {field: value, ...}], ...]]]
                for stream, messages in response:
                    for message_id, fields in messages:
                        await self.process_message(message_id, fields)

            except redis.exceptions.ConnectionError as e:
                logging.error(
                    f"Redis connection error in '{self.consumer_name}': {e}. Attempting to reconnect..."
                )
                await asyncio.sleep(
                    5
                )  # Wait before potentially reconnecting (aioredis might handle this)
                # Consider adding explicit reconnect logic if needed
            except Exception as e:
                logging.exception(
                    f"Unexpected error in consumer '{self.consumer_name}' listen loop: {e}"
                )
                await asyncio.sleep(5)  # Prevent rapid failing loop

        logging.info(f"Consumer '{self.consumer_name}' listener stopped.")

    async def process_message(self, message_id: str, fields: dict):
        job_data = None
        job_id = None
        lock_key = None
        acquired = False

        try:
            job_raw = fields.get("job")
            if not job_raw:
                logging.warning(
                    f"[{self.consumer_name}] Message {message_id} has no 'job' field. Acknowledging."
                )
                await self.redis.xack(self.stream_partition, self.group, message_id)
                return

            job_data = json.loads(job_raw)
            job_id = job_data.get("job_id")
            if not job_id:
                logging.warning(
                    f"[{self.consumer_name}] Job data in message {message_id} missing 'job_id'. Acknowledging."
                )
                await self.redis.xack(self.stream_partition, self.group, message_id)
                return

            lock_key = f"qbull:job_lock:{job_id}"
            acquired = await self.locker.acquire_or_extend(
                lock_key, self.token, self.job_lock_ttl_ms
            )

            if not acquired:
                logging.debug(
                    f"[{self.consumer_name}] Lock busy for job {job_id} ({message_id}), skipping."
                )
                # Do not ACK, let another consumer or PEL handle it after timeout
                return

            logging.info(
                f"[{self.consumer_name}] Processing job {job_id} ({message_id})..."
            )
            cmd = job_data.get("cmd")
            handler = self.handlers.get(cmd)

            if handler:
                # Consider adding job lock refresh task here if handlers can be very long
                await handler(job_data)
                logging.info(
                    f"[{self.consumer_name}] Job {job_id} ({message_id}) processed successfully."
                )
                # Acknowledge message AFTER successful processing
                await self.redis.xack(self.stream_partition, self.group, message_id)
                logging.debug(
                    f"[{self.consumer_name}] Message {message_id} acknowledged."
                )
            else:
                logging.warning(
                    f"[{self.consumer_name}] No handler found for command '{cmd}' in job {job_id} ({message_id}). Acknowledging."
                )
                await self.redis.xack(self.stream_partition, self.group, message_id)

        except json.JSONDecodeError as e:
            logging.error(
                f"[{self.consumer_name}] Failed to decode JSON for message {message_id}: {e}. Acknowledging."
            )
            await self.redis.xack(
                self.stream_partition, self.group, message_id
            )  # ACK invalid message
        except Exception as e:
            # Handler raised an exception
            logging.exception(
                f"[{self.consumer_name}] Error processing job {job_id} ({message_id}): {e}"
            )
            # Do NOT ACK. Let it be redelivered or handled by PEL recovery.
        finally:
            if acquired and lock_key:
                released = await self.locker.release(lock_key, self.token)
                if not released:
                    logging.warning(
                        f"[{self.consumer_name}] Failed to release lock for job {job_id} (maybe expired or released by other)."
                    )

    async def close_internal(self):
        # Helper for closing resources without external signaling
        if self.redis:
            try:
                await self.redis.close()
            except redis.exceptions.RedisError as e:
                logging.error(f"Error closing consumer Redis connection: {e}")
            finally:
                self.redis = None
        await self.locker.close()  # Close locker's connection
        self._connected = False

    async def stop(self):
        logging.info(f"Stopping consumer '{self.consumer_name}'...")
        self._stop_event.set()

    async def close(self):
        await self.stop()
        await self.close_internal()
        logging.info(f"Consumer '{self.consumer_name}' closed.")
