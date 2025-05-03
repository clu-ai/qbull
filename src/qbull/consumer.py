# consumer.py
import asyncio
import redis.asyncio as aioredis
import uuid
import json
from datetime import datetime, timezone
import redis.exceptions
import logging
import signal
import socket # For default consumer name

# Assuming other modules are in the same package or PYTHONPATH
from .lock import RedisLock
from .partition_allocator import PartitionAllocator
from .config import (
    DEFAULT_JOB_LOCK_TTL_MS,
    DEFAULT_PARTITION_LOCK_TTL_SEC,
    PARTITION_REFRESH_INTERVAL_SEC,
    DEFAULT_CONSUMER_GROUP_NAME,
    DEFAULT_READ_BLOCK_MS,
    PARTITIONS as TOTAL_PARTITIONS, # Import total partitions config
    PARTITIONS_PER_POD, # Import partitions per pod config
)

logger = logging.getLogger(__name__)

class Consumer:
    """
    Manages consuming jobs from multiple stream partitions concurrently.
    It automatically acquires, manages, and refreshes locks for a subset of
    partitions based on configuration. Handlers are registered per stream name.
    """
    def __init__(
        self,
        redis_url: str,
        group_name: str = DEFAULT_CONSUMER_GROUP_NAME,
        total_partitions: int = TOTAL_PARTITIONS,
        partitions_to_acquire: int = PARTITIONS_PER_POD,
        job_lock_ttl_ms: int = DEFAULT_JOB_LOCK_TTL_MS,
        partition_lock_ttl_sec: int = DEFAULT_PARTITION_LOCK_TTL_SEC,
        partition_refresh_interval_sec: int = PARTITION_REFRESH_INTERVAL_SEC,
        read_block_ms: int = DEFAULT_READ_BLOCK_MS,
    ):
        """
        Initializes the Consumer manager.

        Args:
            redis_url: Redis connection URL.
            group_name: The consumer group name to use for all streams.
            total_partitions: Total number of partitions in the system.
            partitions_to_acquire: How many partitions this instance should try to acquire.
            job_lock_ttl_ms: TTL for individual job locks (milliseconds).
            partition_lock_ttl_sec: TTL for the partition locks (seconds).
            partition_refresh_interval_sec: How often to refresh partition locks (seconds).
            read_block_ms: XREADGROUP block timeout (milliseconds).
        """
        if partitions_to_acquire > total_partitions:
             logger.warning(f"partitions_to_acquire ({partitions_to_acquire}) > total_partitions ({total_partitions}). Will attempt to acquire all {total_partitions}.")
             partitions_to_acquire = total_partitions
        if partitions_to_acquire <= 0:
             raise ValueError("partitions_to_acquire must be positive.")
        if partition_refresh_interval_sec >= partition_lock_ttl_sec:
            raise ValueError("partition_refresh_interval_sec must be less than partition_lock_ttl_sec")

        self.redis_url = redis_url
        self.group_name = group_name
        self.total_partitions = total_partitions
        self.partitions_to_acquire = partitions_to_acquire
        self.job_lock_ttl_ms = job_lock_ttl_ms
        self.partition_lock_ttl_sec = partition_lock_ttl_sec
        self.partition_refresh_interval_sec = partition_refresh_interval_sec
        self.read_block_ms = read_block_ms

        # Unique identifier for this consumer instance process
        self.consumer_base_name = f"consumer-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
        self.token = str(uuid.uuid4()) # Unique token for job locks acquired by this instance

        # Internal components
        self.redis = None # Main connection for XREAD/XACK etc.
        self.locker = RedisLock(redis_url) # For job locks
        self.partition_allocator = PartitionAllocator(
            redis_url=redis_url,
            total_partitions=self.total_partitions,
            ttl_sec=self.partition_lock_ttl_sec
        )

        # State
        self._connected = False
        self._running = False
        self.handlers = {} # { "stream_base_name": async_handler_func }
        self.acquired_partitions = []
        self._listener_tasks = {} # { partition_num: asyncio.Task }
        self._refresh_task = None
        self._stop_event = asyncio.Event()
        self._shutdown_signals_handled = False

        logger.info(f"Consumer '{self.consumer_base_name}' initialized. Group: '{group_name}', Acquiring: {partitions_to_acquire}/{total_partitions}, Job Lock TTL: {job_lock_ttl_ms}ms, Part Lock TTL: {partition_lock_ttl_sec}s")

    def handler(self, stream_name: str):
        """Decorator to register a handler for a specific base stream name."""
        if not isinstance(stream_name, str) or not stream_name:
             raise ValueError("stream_name for handler must be a non-empty string.")

        def decorator(func):
            if not asyncio.iscoroutinefunction(func):
                 raise TypeError(f"Handler for '{stream_name}' must be an async function (coroutine).")
            if stream_name in self.handlers:
                logger.warning(f"Overwriting handler for stream '{stream_name}'.")
            self.handlers[stream_name] = func
            logger.info(f"Registered handler for stream '{stream_name}': {func.__name__}")
            return func
        return decorator

    async def connect(self):
        """Connects internal components (Redis client, Locker, Allocator)."""
        if self._connected:
            logger.debug("Consumer already connected.")
            return
        try:
            logger.info("Consumer connecting...")
            # Connect main redis, locker, and allocator concurrently
            results = await asyncio.gather(
                aioredis.from_url(self.redis_url, decode_responses=True),
                self.locker.connect(),
                self.partition_allocator.connect(),
                return_exceptions=True # Allow gathering results even if one fails initially
            )

            # Check results
            redis_conn, locker_res, allocator_res = results
            exceptions = []
            if isinstance(redis_conn, Exception):
                 exceptions.append(f"Main Redis connection failed: {redis_conn}")
            else:
                 self.redis = redis_conn
                 # Ping main connection
                 try:
                      await self.redis.ping()
                      logger.info("Main Redis connection successful.")
                 except (redis.exceptions.ConnectionError, OSError) as e:
                      exceptions.append(f"Main Redis ping failed: {e}")
                      self.redis = None # Reset if ping fails

            if isinstance(locker_res, Exception):
                 exceptions.append(f"Locker connection failed: {locker_res}")
            if isinstance(allocator_res, Exception):
                 exceptions.append(f"Partition Allocator connection failed: {allocator_res}")


            # If any connection failed, log, cleanup and raise
            if exceptions or not self.redis:
                 error_msg = "Consumer connection failed. Errors: " + "; ".join(exceptions)
                 logger.error(error_msg)
                 await self.close() # Attempt cleanup
                 raise ConnectionError(error_msg)

            self._connected = True
            logger.info("Consumer connection established successfully.")

        except Exception as e:
             logger.exception(f"Unexpected error during consumer connection: {e}")
             await self.close() # Attempt cleanup
             self._connected = False
             raise # Re-raise the exception

    async def _ensure_groups_for_partition(self, partition_num: int):
        """Ensures consumer groups exist for all registered streams on a given partition."""
        if not self.redis or not self._connected:
             logger.error(f"[P{partition_num}] Cannot ensure groups: Consumer not connected.")
             return False
        if not self.handlers:
             logger.warning(f"[P{partition_num}] No handlers registered, skipping group creation.")
             return True # Nothing to do

        group_creation_tasks = []
        for stream_name in self.handlers.keys():
            stream_partition_key = f"{stream_name}:{partition_num}"
            # Create group if not exists for this specific stream-partition
            group_creation_tasks.append(
                self._create_group_if_not_exists(stream_partition_key, self.group_name)
            )

        results = await asyncio.gather(*group_creation_tasks, return_exceptions=True)

        all_ok = True
        for i, result in enumerate(results):
             stream_name = list(self.handlers.keys())[i]
             stream_partition_key = f"{stream_name}:{partition_num}"
             if isinstance(result, Exception):
                  logger.error(f"[P{partition_num}] Failed to ensure group '{self.group_name}' for stream '{stream_partition_key}': {result}")
                  all_ok = False
             elif result is False: # Our helper function indicates an issue
                  logger.error(f"[P{partition_num}] Failed to ensure group '{self.group_name}' for stream '{stream_partition_key}' (unknown error).")
                  all_ok = False
             # else: Group exists or was created successfully

        return all_ok


    async def _create_group_if_not_exists(self, stream_key: str, group_name: str) -> bool:
        """Helper to create consumer group, ignoring 'already exists' errors."""
        try:
            # Try creating the group starting from the beginning of the stream ('0')
            # mkstream=True creates the stream if it doesn't exist yet
            await self.redis.xgroup_create(stream_key, group_name, id='0', mkstream=True)
            logger.info(f"Created consumer group '{group_name}' for stream '{stream_key}'.")
            return True
        except redis.exceptions.ResponseError as e:
            # Check if the error message indicates the group already exists
            if "BUSYGROUP Consumer Group name already exists" in str(e):
                logger.debug(f"Group '{group_name}' already exists for stream '{stream_key}'.")
                return True # Group exists, which is the desired state
            else:
                # Log other Redis response errors
                logger.error(f"Failed to create group '{group_name}' for stream '{stream_key}': {e}")
                return False
        except redis.exceptions.RedisError as e:
            # Log other Redis errors (connection issues, etc.)
            logger.error(f"Redis error ensuring group '{group_name}' for stream '{stream_key}': {e}")
            return False


    async def _listen_on_partition(self, partition_num: int):
        """Task function to listen for messages on a specific partition."""
        if not self.handlers:
            logger.warning(f"[P{partition_num}] No handlers registered. Listener task exiting.")
            return

        consumer_name_partition = f"{self.consumer_base_name}-{partition_num}"
        logger.info(f"[P{partition_num}] Listener task started. Consumer name: '{consumer_name_partition}', Group: '{self.group_name}'")

        # Ensure groups exist for all streams on this partition before starting loop
        if not await self._ensure_groups_for_partition(partition_num):
             logger.error(f"[P{partition_num}] Failed to ensure consumer groups. Listener task stopping.")
             # Consider signaling failure back to the main process
             return

        # Prepare the streams dictionary for XREADGROUP
        # Format: { "stream_name:partition": ">" } for all registered handlers
        streams_to_read = {
            f"{stream_name}:{partition_num}": ">" # ">" means read only new messages
            for stream_name in self.handlers.keys()
        }
        logger.info(f"[P{partition_num}] Will listen on streams: {list(streams_to_read.keys())}")

        while not self._stop_event.is_set():
            try:
                # XREADGROUP GROUP group consumer [COUNT count] [BLOCK milliseconds] STREAMS key [key ...] ID [ID ...]
                # Read 1 message at a time per stream-partition this consumer handles within this partition
                response = await self.redis.xreadgroup(
                    groupname=self.group_name,
                    consumername=consumer_name_partition,
                    streams=streams_to_read,
                    count=1, # Read one message per stream requested, if available
                    block=self.read_block_ms,
                )

                if not response:
                    # logger.debug(f"[P{partition_num}] XREADGROUP timed out or no new messages.")
                    continue # Timeout or no new messages, loop again

                # Response format: [ [stream_key, [ [message_id, {field: value, ...}] ] ], ... ]
                for stream_key, messages in response:
                    # Extract base stream name and confirm partition matches (sanity check)
                    try:
                         base_stream_name, p_str = stream_key.rsplit(':', 1)
                         msg_partition = int(p_str)
                         if msg_partition != partition_num:
                              logger.error(f"[P{partition_num}] Received message for wrong partition {msg_partition} on stream key {stream_key}! Skipping.")
                              continue # Should not happen with XREADGROUP structure
                    except ValueError:
                         logger.error(f"[P{partition_num}] Could not parse partition from stream key '{stream_key}'. Skipping.")
                         continue

                    if base_stream_name not in self.handlers:
                         logger.warning(f"[P{partition_num}] Received message for stream '{base_stream_name}' but no handler is registered. Skipping.")
                         # Consider ACK'ing here to remove unhandled messages or let them pend
                         continue

                    for message_id, fields in messages:
                        logger.debug(f"[P{partition_num}] Received message {message_id} from stream '{stream_key}'")
                        # Process the message using the registered handler
                        await self.process_message(message_id, fields, base_stream_name, partition_num, consumer_name_partition)
                        # Brief yield to allow other tasks to run, preventing CPU hogging if stream is very busy
                        await asyncio.sleep(0.001)


            except redis.exceptions.ConnectionError as e:
                logger.error(f"[P{partition_num}] Redis connection error in listener: {e}. Will retry...")
                await asyncio.sleep(5) # Wait before retrying loop
                # Consider adding explicit reconnection check/attempt for self.redis if needed
            except redis.exceptions.ResponseError as e:
                 # e.g., NOGROUP error if group was deleted externally
                 logger.error(f"[P{partition_num}] Redis response error in listener: {e}. Re-ensuring groups and retrying...")
                 await asyncio.sleep(2)
                 await self._ensure_groups_for_partition(partition_num) # Try to fix group issue
            except Exception as e:
                logger.exception(f"[P{partition_num}] Unexpected error in listener loop: {e}")
                await asyncio.sleep(5) # Prevent rapid fail loop

        logger.info(f"[P{partition_num}] Listener task stopped for partition {partition_num}.")


    async def process_message(self, message_id: str, fields: dict, stream_name: str, partition_num: int, consumer_name: str):
        """Processes a single message: locks job, calls handler, ACKs/releases lock."""
        job_data = None
        job_id = None
        lock_key = None
        acquired = False
        stream_partition_key = f"{stream_name}:{partition_num}" # For logging and ACK

        try:
            job_raw = fields.get("job")
            if not job_raw:
                logger.warning(f"[{consumer_name}] Message {message_id} on '{stream_partition_key}' has no 'job' field. Acknowledging.")
                await self.redis.xack(stream_partition_key, self.group_name, message_id)
                return

            try:
                 job_data = json.loads(job_raw)
            except json.JSONDecodeError as e:
                 logger.error(f"[{consumer_name}] Failed to decode JSON for message {message_id} on '{stream_partition_key}': {e}. Acknowledging.")
                 await self.redis.xack(stream_partition_key, self.group_name, message_id)
                 return

            job_id = job_data.get("job_id")
            if not job_id:
                logger.warning(f"[{consumer_name}] Job data in message {message_id} on '{stream_partition_key}' missing 'job_id'. Acknowledging.")
                await self.redis.xack(stream_partition_key, self.group_name, message_id)
                return

            # Acquire lock for the specific job_id
            lock_key = f"qbull:job_lock:{job_id}"
            acquired = await self.locker.acquire_or_extend(lock_key, self.token, self.job_lock_ttl_ms)

            if not acquired:
                # Another consumer instance likely processing this job_id (e.g., from redelivery)
                logger.debug(f"[{consumer_name}] Lock busy for job {job_id} ({message_id} on '{stream_partition_key}'), skipping. Another consumer might be processing.")
                # Do NOT ACK. If the other consumer fails, it might get redelivered.
                # If the other consumer succeeds, this message remains pending until claimed/timeout.
                # Consider XCLAIM mechanisms if strict exactly-once is needed over at-least-once.
                return

            # --- Lock Acquired ---
            logger.info(f"[{consumer_name}] Processing job {job_id} ({message_id} from '{stream_partition_key}')...")

            handler = self.handlers.get(stream_name) # Find handler by base stream name
            if not handler:
                 # This check should ideally be done before locking, but double-check here
                 logger.error(f"[{consumer_name}] No handler found for stream '{stream_name}' for job {job_id} ({message_id}). This shouldn't happen if check before lock works. Acknowledging.")
                 await self.redis.xack(stream_partition_key, self.group_name, message_id)
                 return # Release lock in finally block

            # Execute the registered handler
            # Consider adding heartbeat/lock extension if handler is long-running
            await handler(job_data) # Pass the deserialized job data

            logger.info(f"[{consumer_name}] Job {job_id} ({message_id} from '{stream_partition_key}') processed successfully.")

            # Acknowledge message AFTER successful processing
            ack_result = await self.redis.xack(stream_partition_key, self.group_name, message_id)
            if ack_result == 1:
                 logger.debug(f"[{consumer_name}] Message {message_id} acknowledged successfully.")
            else:
                 # Should ideally be 1 if successful. 0 might mean message ID doesn't exist in PEL (already acked?)
                 logger.warning(f"[{consumer_name}] Message {message_id} acknowledgement returned {ack_result} (expected 1).")


        except Exception as e:
            # Exception raised by the handler or during processing logic
            logger.exception(f"[{consumer_name}] Error processing job {job_id} ({message_id} from '{stream_partition_key}'): {e}")
            # Do NOT ACK. Let the message be redelivered after visibility timeout,
            # potentially handled by Pending Entries List (PEL) recovery logic later.

        finally:
            # Release the job lock if it was acquired
            if acquired and lock_key:
                released = await self.locker.release(lock_key, self.token)
                if not released:
                    # This could happen if the lock TTL expired during processing
                    logger.warning(f"[{consumer_name}] Failed to release lock '{lock_key}' for job {job_id}. It might have expired or been released by another process.")
                # else: Lock released successfully


    async def _refresh_partition_locks(self):
        """Task function to periodically refresh acquired partition locks."""
        logger.info("Partition lock refresh task started.")
        while not self._stop_event.is_set():
            await asyncio.sleep(self.partition_refresh_interval_sec)
            if not self.acquired_partitions:
                # logger.debug("No partitions acquired, skipping refresh.")
                continue
            if not self.partition_allocator or not self.partition_allocator._connected:
                 logger.warning("Partition allocator not connected, cannot refresh locks.")
                 continue

            logger.debug(f"Refreshing locks for partitions: {self.acquired_partitions}")
            try:
                success = await self.partition_allocator.refresh_n(self.acquired_partitions)
                if not success:
                    logger.warning("Potential issue refreshing partition locks (check allocator logs). Some locks might be lost.")
                    # Consider adding logic here to try and re-acquire lost partitions?
                    # For simplicity now, it just logs the warning.
            except Exception as e:
                 logger.exception(f"Unexpected error during partition lock refresh: {e}")

        logger.info("Partition lock refresh task stopped.")

    async def start(self):
        """Acquires partitions, starts listener tasks and refresh task."""
        if not self._connected:
            raise ConnectionError("Consumer is not connected. Call connect() first.")
        if self._running:
             logger.warning("Consumer is already running.")
             return
        if not self.handlers:
             logger.warning("No handlers registered. Consumer will start but won't process anything.")
             # Allow starting without handlers, maybe they are registered later? Or raise error?
             # raise ValueError("Cannot start Consumer: No handlers registered.")

        logger.info(f"Starting consumer '{self.consumer_base_name}'...")
        self._stop_event.clear()
        self._running = True

        try:
            # 1. Acquire Partitions
            logger.info(f"Attempting to acquire {self.partitions_to_acquire} partitions...")
            self.acquired_partitions = await self.partition_allocator.acquire_n(self.partitions_to_acquire)
            if not self.acquired_partitions:
                 logger.warning("Failed to acquire any partitions. Consumer will run but be idle.")
                 # Keep running? Or stop? Let's keep running, maybe partitions become free later.
                 # Needs logic to periodically retry acquisition if desired.
            else:
                 logger.info(f"Successfully acquired partitions: {self.acquired_partitions}")

                 # 2. Start Listener Tasks for acquired partitions
                 logger.info("Starting listener tasks...")
                 for p_num in self.acquired_partitions:
                     if p_num not in self._listener_tasks or self._listener_tasks[p_num].done():
                         task = asyncio.create_task(self._listen_on_partition(p_num))
                         self._listener_tasks[p_num] = task
                         logger.info(f"Listener task created for partition {p_num}.")
                     else:
                          logger.warning(f"Listener task for partition {p_num} seems to be already running.")

                 # 3. Start Refresh Task
                 if self.acquired_partitions and (not self._refresh_task or self._refresh_task.done()):
                     logger.info("Starting partition lock refresh task...")
                     self._refresh_task = asyncio.create_task(self._refresh_partition_locks())
                 elif not self.acquired_partitions:
                      logger.info("No partitions acquired, refresh task not needed.")


            logger.info(f"Consumer '{self.consumer_base_name}' started successfully.")
            # Keep running until stop() is called
            await self._stop_event.wait() # Wait indefinitely until event is set

        except Exception as e:
             logger.exception(f"Error during consumer startup or main loop: {e}")
             # Ensure stop is called to attempt cleanup
             await self.stop()
        finally:
            self._running = False
            logger.info(f"Consumer '{self.consumer_base_name}' main loop finished.")


    def _setup_signal_handlers(self):
        """Sets up OS signal handlers for graceful shutdown."""
        if self._shutdown_signals_handled:
             return
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
             loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(self.stop(signal_name=s.name)))
        self._shutdown_signals_handled = True
        logger.info("OS signal handlers (SIGINT, SIGTERM) configured for graceful shutdown.")


    async def run(self):
        """Connects, sets up signal handlers, and starts the consumer."""
        try:
             await self.connect()
             self._setup_signal_handlers() # Setup signals after successful connection
             await self.start() # This blocks until stop() is called
        except ConnectionError as e:
             logger.error(f"Failed to connect, consumer cannot run: {e}")
        except Exception as e:
             logger.exception(f"Consumer failed to run: {e}")
        finally:
             # Ensure cleanup happens even if start() fails or exits unexpectedly
             await self.close()


    async def stop(self, signal_name: str = "programmatic"):
        """Signals the consumer to stop gracefully."""
        if not self._running and not self._stop_event.is_set():
             logger.info(f"Stop requested ({signal_name}), but consumer wasn't actively running. Ensuring cleanup.")
             # Set event anyway to ensure close logic runs fully
             self._stop_event.set()
             await self.close() # Trigger cleanup directly
             return

        if self._stop_event.is_set():
             logger.info(f"Stop requested ({signal_name}), but stop process already initiated.")
             return # Avoid concurrent stop calls piling up

        logger.info(f"Stop requested ({signal_name}). Initiating graceful shutdown...")
        self._stop_event.set()

        # Give tasks some time to finish processing current message / read loop
        await asyncio.sleep(1)

        # Cancel tasks explicitly
        logger.info("Cancelling listener and refresh tasks...")
        if self._refresh_task and not self._refresh_task.done():
            self._refresh_task.cancel()
        listener_tasks_to_cancel = [task for task in self._listener_tasks.values() if task and not task.done()]
        for task in listener_tasks_to_cancel:
            task.cancel()

        # Wait for tasks to finish cancellation
        cancelled_tasks = listener_tasks_to_cancel + ([self._refresh_task] if self._refresh_task else [])
        if cancelled_tasks:
             logger.info("Waiting for tasks to finish cancellation...")
             await asyncio.gather(*cancelled_tasks, return_exceptions=True)
             logger.info("Tasks finished cancellation.")

        # Final cleanup is handled by close() which should be called after start() finishes
        logger.info("Graceful shutdown initiated. Waiting for main loop to exit...")


    async def close(self):
        """Releases resources (partitions, connections). Call after stop()."""
        logger.info("Closing consumer resources...")

        # 1. Release acquired partitions (important!)
        if self.acquired_partitions and self.partition_allocator and self.partition_allocator._connected:
            logger.info(f"Releasing acquired partitions: {self.acquired_partitions}")
            try:
                await self.partition_allocator.release_n(self.acquired_partitions)
                logger.info("Partitions released.")
                self.acquired_partitions = []
            except Exception as e:
                 logger.exception(f"Error releasing partitions: {e}")

        # 2. Close connections concurrently
        close_tasks = []
        if self.partition_allocator:
            close_tasks.append(self.partition_allocator.close())
        if self.locker:
             close_tasks.append(self.locker.close())
        if self.redis:
            # Wrap redis close in its own try/except within the task
            async def close_main_redis():
                 try:
                      await self.redis.close()
                      logger.info("Main Redis connection closed.")
                 except redis.exceptions.RedisError as e:
                      logger.error(f"Error closing main Redis connection: {e}")
                 finally:
                      self.redis = None
            close_tasks.append(close_main_redis())

        if close_tasks:
             await asyncio.gather(*close_tasks, return_exceptions=True)
             logger.info("Connections closed.")

        self._connected = False
        self._running = False
        # Reset state
        self._listener_tasks = {}
        self._refresh_task = None
        # Don't clear handlers here, they might be needed if restarting

        logger.info(f"Consumer '{self.consumer_base_name}' closed.")