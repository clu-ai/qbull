import asyncio
import json
import random
import logging
import signal
import typing
from datetime import datetime
from qbull.publisher import Consumer, Publisher
from qbull.config import (
    PARTITIONS,
    REDIS_URL,
    PARTITIONS_PER_POD,
    DEFAULT_PARTITION_LOCK_TTL_SEC,
    PARTITION_REFRESH_INTERVAL_SEC,
    DEFAULT_JOB_LOCK_TTL_MS,
)
from qbull.partition_allocator import PartitionAllocator

log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
logging.basicConfig(level=logging.INFO, format=log_format, force=True)
logger = logging.getLogger("run_workers")
# -----------------------------------------

STREAM_NAME = "WHATSAPP"
GROUP_NAME = "workers"

# Global list to keep track of running tasks for graceful shutdown
running_tasks: list[asyncio.Task] = []
shutdown_event = asyncio.Event()


async def _refresh_partition_locks(
    allocator: PartitionAllocator, partitions: list[int], interval_sec: int
):
    """Background task to periodically refresh partition locks."""
    while not shutdown_event.is_set():
        try:
            # Mover sleep al final para refrescar inmediatamente al iniciar la tarea si es necesario
            # await asyncio.sleep(interval_sec)
            if shutdown_event.is_set():
                break
            logger.info(f"Refreshing partition locks for: {partitions}")
            refreshed = await allocator.refresh_n(partitions)
            if not refreshed:
                logger.warning(
                    f"Failed to refresh some partition locks for: {partitions}. Instance might lose partitions."
                )
            await asyncio.sleep(interval_sec)
        except asyncio.CancelledError:
            logger.info("Partition refresh task cancelled.")
            break
        except Exception as e:
            logger.exception(f"Error in partition refresh task: {e}")
            # Wait longer before retrying after an unexpected error
            await asyncio.sleep(interval_sec * 2)


async def _run_consumer(consumer: Consumer):
    """Wrapper to run consumer listen loop and handle potential errors."""
    logger.info(f"Starting consumer task for {consumer.consumer_name}...")
    try:
        logger.info(f"Consumer {consumer.consumer_name} entering listen loop...")
        await consumer.listen()
        logger.info(f"Consumer {consumer.consumer_name} listen loop exited.")
    except asyncio.CancelledError:
        logger.info(f"Consumer task {consumer.consumer_name} cancelled.")
    except Exception as e:
        logger.exception(f"Consumer {consumer.consumer_name} failed unexpectedly: {e}")
    finally:
        logger.warning(f"Closing consumer {consumer.consumer_name}...")
        await consumer.close()
        logger.warning(f"Consumer {consumer.consumer_name} closed.")


async def publish_test_jobs(publisher: Publisher):
    """Publishes a few test jobs."""
    logger.info("Starting test job publisher task...")
    try:
        for i in range(5):
            if shutdown_event.is_set():
                logger.info("Shutdown signaled, stopping test job publisher.")
                break
            wait_time = random.uniform(2, 5)
            logger.debug(f"Publisher waiting {wait_time:.2f}s before job #{i+1}")
            to_number = random.choice(
                ["3205104418", "3205104419", "3115550011", "3115550022"]
            )
            logger.info(f"Publisher attempting to send job #{i+1} to {to_number}")
            job_id = await publisher.publish_job(
                {
                    "cmd": "SEND_MESSAGE",
                    "to": to_number,
                    "content": f"Test message #{i+1} from run_workers @ {datetime.now()}",
                },
                partition_key=to_number,  # Use 'to' number for partitioning
            )
            if job_id:
                logger.info(
                    f"✅ Test job #{i+1} published with ID: {job_id} (to: {to_number})"
                )
            else:
                logger.error(f"❌ Failed to publish test job #{i+1}")
    except asyncio.CancelledError:
         logger.info("Test job publisher task cancelled.")
    except Exception as e:
        logger.exception(f"Error in test job publisher task: {e}")
    finally:
        logger.info("Test job publisher task finished. Closing publisher.")
        if publisher:
             await publisher.close()


def _create_handler(partition_id: int):
    """Creates a handler closure capturing the correct partition id."""
    logger.debug(f"Creating handler for partition {partition_id}")
    async def handle_send_message(job: dict):
        job_id = job.get('job_id', 'N/A')
        to_num = job.get('to', 'N/A')
        content_snip = job.get('content', '')[:30]
        logger.info(f"[P{partition_id}] Received job {job_id}. Processing SEND_MESSAGE to {to_num}...")
        # Simulate work
        work_time = random.uniform(0.1, 0.5)
        await asyncio.sleep(work_time)
        logger.info(
            f"[P{partition_id}] Finished processing job {job_id} (SEND_MESSAGE to {to_num}) in {work_time:.2f}s. Content: '{content_snip}...'"
        )

    return handle_send_message


async def main():
    logger.info("--- Starting main() execution ---")
    global running_tasks
    allocator = None
    consumers: list[Consumer] = []
    acquired_partitions: list[int] = []
    refresh_task: asyncio.Task | None = None
    publisher: Publisher | None = None
    publisher_task: asyncio.Task | None = None

    try:
        logger.info("Instantiating PartitionAllocator...")
        allocator = PartitionAllocator(
            REDIS_URL, total_partitions=PARTITIONS, ttl_sec=DEFAULT_PARTITION_LOCK_TTL_SEC
        )

        logger.info("Connecting PartitionAllocator...")
        await allocator.connect()

        logger.info(f"Attempting to acquire {PARTITIONS_PER_POD} partitions...")
        acquired_partitions = await allocator.acquire_n(PARTITIONS_PER_POD)

        if not acquired_partitions:
            logger.error("Could not acquire any partitions after retries. Exiting main.")
            return

        logger.info("Starting partition refresh task...")
        refresh_task = asyncio.create_task(
            _refresh_partition_locks(
                allocator, acquired_partitions, PARTITION_REFRESH_INTERVAL_SEC
            )
        )
        running_tasks.append(refresh_task)
        logger.info("Partition refresh task started.")

        logger.info("Setting up and connecting consumers...")
        for partition in acquired_partitions:
            logger.info(f"Setting up consumer for partition {partition}...")
            consumer = Consumer(
                redis_url=REDIS_URL,
                stream_name=STREAM_NAME,
                group=GROUP_NAME,
                consumer_name="worker",
                partition=partition,
                job_lock_ttl_ms=DEFAULT_JOB_LOCK_TTL_MS,
            )
            handler_func = _create_handler(partition)
            consumer.handler("SEND_MESSAGE")(handler_func)

            logger.info(f"Connecting consumer for partition {partition}...")
            await consumer.connect() 
            consumers.append(consumer)
            logger.info(f"Consumer for partition {partition} connected.")
        logger.info("All consumers set up and connected.")

        logger.info("Starting consumer listening tasks...")
        if not consumers:
             logger.warning("No consumers were created (likely no partitions acquired).")
        for c in consumers:
            logger.info(f"Creating listener task for consumer {c.consumer_name}...")
            task = asyncio.create_task(_run_consumer(c))
            running_tasks.append(task)
        logger.info("All consumer listening tasks created.")

        if acquired_partitions and min(acquired_partitions) == 0:
            logger.info("Condition met: This worker acquired partition 0. Starting test publisher...")
            publisher = Publisher(REDIS_URL, STREAM_NAME, partitions=PARTITIONS)
            await publisher.connect()
            publisher_task = asyncio.create_task(publish_test_jobs(publisher))
            running_tasks.append(publisher_task)
            logger.info("Test publisher task created.")
        else:
             logger.info("Condition NOT met: This worker did not acquire partition 0 (or acquired none). Test publisher will not run.")

        # --- Log antes de esperar ---
        logger.info(
            f"--- Worker setup complete. Acquired Partitions: {acquired_partitions}. Entering main wait loop (shutdown_event.wait()). ---"
        )
        # ---------------------------
        await shutdown_event.wait()

        logger.info("--- Shutdown signal received. Exiting main wait loop. ---")

    except Exception as e:
        logger.exception(f"--- CRITICAL ERROR in main worker loop: {e} ---")
        shutdown_event.set()
    finally:
        logger.info("--- Initiating shutdown sequence (finally block)... ---")

        # 1. Stop publisher task if running
        if publisher_task and not publisher_task.done():
            logger.info("Cancelling publisher task...")
            publisher_task.cancel()
            logger.info("Publisher task cancelled.")

        # 2. Consumers stop themselves via _run_consumer when task is cancelled or loop exits

        # 3. Cancel the refresh task
        if refresh_task and not refresh_task.done():
            logger.info("Cancelling refresh task...")
            refresh_task.cancel()
            logger.info("Refresh task cancelled.")

        # 4. Wait for tasks to complete (with a timeout)
        if running_tasks:
             logger.info(f"Waiting for {len(running_tasks)} background tasks to finish...")
             done, pending = await asyncio.wait(running_tasks, timeout=15.0, return_when=asyncio.ALL_COMPLETED) # Aumentar timeout
             logger.info(f"Wait finished. Done tasks: {len(done)}, Pending tasks: {len(pending)}")

             if pending:
                  logger.warning(f"{len(pending)} tasks did not finish gracefully after timeout, cancelling forcefully:")
                  for task in pending:
                       logger.warning(f" - Cancelling task: {task.get_name()}") # Intentar obtener nombre
                       task.cancel()
                  logger.info("Waiting briefly for forceful cancellations...")
                  await asyncio.sleep(1) # Espera corta
             else:
                  logger.info("All tasks finished gracefully.")
        else:
             logger.info("No running tasks to wait for.")

        # 5. Release acquired partitions
        if allocator and allocator._connected and acquired_partitions:
            logger.info(f"Releasing acquired partitions: {acquired_partitions}")
            try:
                 await allocator.release_n(acquired_partitions)
            except Exception as e:
                 logger.exception(f"Error during partition release: {e}")
        elif not acquired_partitions:
             logger.info("No partitions were acquired, skipping release.")
        elif not allocator:
             logger.warning("Allocator object not found, cannot release partitions.")
        elif not allocator._connected:
             logger.warning("Allocator not connected, cannot release partitions.")


        # 6. Close allocator connection
        if allocator:
            logger.info("Closing allocator connection...")
            await allocator.close()
            logger.info("Allocator connection closed.")

        logger.info("--- Shutdown sequence complete. ---")


def handle_shutdown_signal(sig, frame):
    logger.warning(f"--- Received signal {sig}. Triggering graceful shutdown (setting shutdown_event)... ---")
    if shutdown_event.is_set():
         logger.warning("Shutdown already in progress.")
         return
    shutdown_event.set()


if __name__ == "__main__":
    print(f"--- Script {__file__} started ---")
    logger.info(f"--- Configuring signal handlers for SIGINT, SIGTERM ---")
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    logger.info(f"--- Signal handlers configured ---")

    try:
        logger.info(f"--- Running asyncio.run(main()) ---")
        asyncio.run(main())
        logger.info(f"--- asyncio.run(main()) finished ---")
    except KeyboardInterrupt:
        logger.warning(
            "KeyboardInterrupt caught directly in __main__ (Signal handler might have issues?)."
        )
    except Exception as e:
        logger.exception("--- Unhandled exception during asyncio.run ---")
    finally:
         logger.info(f"--- Script {__file__} finished ---")