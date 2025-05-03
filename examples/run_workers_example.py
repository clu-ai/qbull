import asyncio
import logging
import signal
import os
import random
from datetime import datetime
from dotenv import load_dotenv # Example app can use dotenv for config
from typing import List # Use List from typing for Task list compatibility < 3.9

# --- Import FROM the qbull library ---
# Assumes the qbull library is installed (e.g., pip install -e . or from GitHub)
# or that the 'src' directory is in the PYTHONPATH for local testing.
try:
    from src.qbull import Consumer, Publisher, PartitionAllocator
except ImportError:
    # Fallback for local testing without installation
    # This assumes you run like: python -m examples.run_workers_example from the repo root
    import sys
    from pathlib import Path
    # Add the 'src' directory to the path
    sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))
    from qbull import Consumer, Publisher, PartitionAllocator
    print("WARN: qbull library not found in site-packages, using relative path from src/. "
          "Install the library for standard usage (`pip install .` or `pip install git+...`).")


# --- Configuration for this Example Application ---
# Load from a .env file in the project root (if it exists)
# The application (this script) is responsible for loading its config, not the library.
dotenv_path = Path(__file__).parent.parent / '.env' # Assumes .env is in the repo root
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)
    print(f"Loaded config from {dotenv_path}")
else:
    print(f"WARN: .env file not found at {dotenv_path}, using defaults or environment variables.")

# Get configuration values from environment or defaults
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
# Total partitions defined for the system (needed by publisher and allocator)
PARTITIONS = int(os.getenv("PARTITIONS", 8))
# How many partitions each instance of this script should try to acquire
PARTITIONS_PER_POD = int(os.getenv("PARTITIONS_PER_POD", 2))
# TTL (Time-To-Live) for partition locks in seconds
PARTITION_LOCK_TTL_SEC = int(os.getenv("DEFAULT_PARTITION_LOCK_TTL_SEC", 60))
# How often the worker should refresh its partition locks (should be < TTL/2 or TTL/3)
PARTITION_REFRESH_INTERVAL_SEC = int(os.getenv("PARTITION_REFRESH_INTERVAL_SEC", 20))
# TTL for the job-specific lock acquired by the consumer before processing
JOB_LOCK_TTL_MS = int(os.getenv("DEFAULT_JOB_LOCK_TTL_MS", 60000))
# Application-specific constants for this example
STREAM_NAME = "WHATSAPP_WORKERS_EXAMPLE" # Use a specific stream for this example
GROUP_NAME = "example_workers" # Consumer group name for this example
CONSUMER_NAME_BASE = "worker" # Base name for consumers (partition number will be appended)
# -------------------------------------------------

# --- Logging Configuration (The application configures logging) ---
log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
# Set level to INFO or DEBUG (DEBUG will show more internal library logs)
logging.basicConfig(level=logging.INFO, format=log_format, force=True)
# Get a specific logger for this application script
logger = logging.getLogger("run_workers_example")
# ----------------------------------------------------------------

# --- Global Variables for managing tasks and shutdown ---
running_tasks: List[asyncio.Task] = [] # Keep track of background tasks for cleanup
shutdown_event = asyncio.Event() # Event to signal graceful shutdown
# -------------------------------------------------------


# --- Background Task Functions ---

async def _refresh_partition_locks(
    allocator: PartitionAllocator, partitions: list[int], interval_sec: int
):
    """Background task that periodically refreshes acquired partition locks."""
    logger.info(f"Refresh task started for partitions {partitions}, interval {interval_sec}s.")
    while not shutdown_event.is_set():
        try:
            # Wait for the specified interval *after* the refresh attempt
            await asyncio.sleep(interval_sec)

            # Check for shutdown signal immediately after waking up
            if shutdown_event.is_set():
                logger.debug("Refresh task: Shutdown detected after sleep, stopping.")
                break

            logger.info(f"Refreshing partition locks for: {partitions}")
            # Call the allocator's refresh method
            refreshed = await allocator.refresh_n(partitions)
            if not refreshed:
                # Log a warning if refresh potentially failed (e.g., lock expired)
                logger.warning(
                    f"Failed to refresh some partition locks for: {partitions}. Worker might lose partitions if this persists."
                )
                # Optional: Add logic here to stop consumers if refresh fails consistently

        except asyncio.CancelledError:
            # Log when the task is cancelled during shutdown
            logger.info("Partition refresh task cancelled.")
            break # Exit the loop cleanly
        except Exception as e:
            # Log any unexpected errors in the refresh loop
            logger.exception(f"Error in partition refresh task: {e}")
            # Wait longer before retrying after an unexpected error to avoid spamming logs
            logger.info(f"Waiting {interval_sec * 2}s after refresh error...")
            await asyncio.sleep(interval_sec * 2)

    logger.info(f"Refresh task for partitions {partitions} finished.")


async def _run_consumer(consumer: Consumer):
    """Task wrapper to run the consumer's listen loop and handle cleanup."""
    logger.info(f"Starting consumer task wrapper for {consumer.consumer_name}...")
    try:
        logger.info(f"Consumer {consumer.consumer_name} entering listen loop...")
        # Start the main listening loop of the consumer
        await consumer.listen()
        # If listen() returns normally (it shouldn't with the current while True), log it
        logger.info(f"Consumer {consumer.consumer_name} listen loop exited normally (unexpected).")
    except asyncio.CancelledError:
        # Log when the consumer task is cancelled (usually during shutdown)
        logger.info(f"Consumer task {consumer.consumer_name} received cancellation.")
        # Cleanup (closing connections) happens in the finally block
    except Exception as e:
        # Log unexpected errors originating from within the consumer's listen/process loop
        logger.exception(f"Consumer {consumer.consumer_name} failed unexpectedly: {e}")
    finally:
        # This block executes when the _run_consumer task finishes for any reason
        # (cancellation, error, or unexpected loop exit)
        logger.warning(f"Closing consumer {consumer.consumer_name} from wrapper's finally block...")
        # Ensure the consumer's resources (Redis connections) are closed
        await consumer.close() # consumer.close() logs its own steps internally
        logger.warning(f"Consumer {consumer.consumer_name} closed via wrapper.")


async def publish_test_jobs(publisher: Publisher):
    """Publishes a few test jobs periodically (if this worker runs the publisher)."""
    logger.info("Starting test job publisher task...")
    try:
        for i in range(5): # Publish 5 test jobs
            # Check if shutdown has been signalled before proceeding
            if shutdown_event.is_set():
                logger.info("Shutdown signaled, stopping test job publisher.")
                break

            # Wait a random interval before publishing the next job
            wait_time = random.uniform(2, 5)
            logger.debug(f"Publisher waiting {wait_time:.2f}s before publishing job #{i+1}")
            await asyncio.sleep(wait_time)

            # Prepare test job data
            to_number = random.choice(
                ["3205104418", "3205104419", "3115550011", "3115550022", "3001234567"] # Added more variety
            )
            job_data = {
                "cmd": "SEND_MESSAGE",
                "to": to_number,
                "content": f"Test message #{i+1} from {__file__} @ {datetime.now()}",
            }
            logger.info(f"Publisher attempting to send job #{i+1} to {to_number}")

            # Publish the job using the provided Publisher instance
            job_id = await publisher.publish_job(
                data=job_data,
                partition_key=to_number,  # Use the 'to' number to determine the partition
            )
            # Log success or failure
            if job_id:
                logger.info(
                    f"✅ Test job #{i+1} published with ID: {job_id} (to: {to_number})"
                )
            else:
                logger.error(f"❌ Failed to publish test job #{i+1}")

    except asyncio.CancelledError:
        # Log when the publisher task is cancelled during shutdown
         logger.info("Test job publisher task cancelled.")
    except Exception as e:
        # Log any unexpected errors during publishing
        logger.exception(f"Error in test job publisher task: {e}")
    finally:
        # This block executes when the task finishes or is cancelled
        logger.info("Test job publisher task finished. Closing publisher...")
        # Ensure the publisher object exists and has a close method before calling it
        if publisher and hasattr(publisher, 'close'):
             await publisher.close() # Publisher.close() logs its own messages


def _create_handler(partition_id: int):
    """Factory function to create a unique handler for each consumer/partition."""
    logger.debug(f"Creating handler function for partition {partition_id}")
    # This closure captures the partition_id for logging purposes
    async def handle_send_message(job: dict):
        # Extract job details safely using .get()
        job_id = job.get('job_id', 'N/A')
        to_num = job.get('to', 'N/A')
        content_snip = job.get('content', '')[:30] # Get first 30 chars
        logger.info(f"[P{partition_id}] Received job {job_id}. Processing SEND_MESSAGE to {to_num}...")

        # Simulate some processing time
        work_time = random.uniform(0.1, 0.5)
        await asyncio.sleep(work_time)

        logger.info(
            f"[P{partition_id}] Finished processing job {job_id} (SEND_MESSAGE to {to_num}) in {work_time:.2f}s. Content: '{content_snip}...'"
        )
        # Optional: print full job data for debugging
        # import json
        # logger.debug(f"[P{partition_id}] Full job data:\n{json.dumps(job, indent=2)}")

    return handle_send_message
# ----------------------------------


# --- Signal Handling ---
def handle_shutdown_signal(sig, frame):
    """Signal handler for SIGINT (Ctrl+C) and SIGTERM."""
    # Log immediately upon receiving the signal
    logger.warning(f"--- Received signal {sig}. Triggering graceful shutdown (setting shutdown_event)... ---")
    # Prevent triggering shutdown multiple times if signal is sent repeatedly
    if shutdown_event.is_set():
         logger.warning("Shutdown already in progress.")
         return
    # Set the asyncio event to signal the main loop and other tasks to stop
    shutdown_event.set()
# -------------------------


# --- Main Application Logic ---
async def main():
    """Main coroutine: Sets up allocator, consumers, tasks, and waits for shutdown."""
    logger.info("--- Starting Worker Example Main Function ---")
    # Use global variables to track tasks and shutdown state
    global running_tasks, shutdown_event
    # Initialize resource variables to None for clarity in the finally block
    allocator: PartitionAllocator | None = None
    consumers: list[Consumer] = []
    acquired_partitions: list[int] = []
    refresh_task: asyncio.Task | None = None
    publisher: Publisher | None = None
    publisher_task: asyncio.Task | None = None

    try:
        # 1. Setup Partition Allocator
        logger.info("Instantiating PartitionAllocator...")
        # Pass configuration explicitly
        allocator = PartitionAllocator(
            redis_url=REDIS_URL,
            total_partitions=PARTITIONS,
            ttl_sec=PARTITION_LOCK_TTL_SEC
        )

        logger.info("Connecting PartitionAllocator...")
        await allocator.connect()
        # If connect() fails here, it raises an exception, stopping execution before proceeding.
        logger.info("PartitionAllocator connected.")

        # 2. Acquire Partitions
        logger.info(f"Attempting to acquire {PARTITIONS_PER_POD} partitions...")
        # Pass configuration explicitly (wait_interval uses its default)
        acquired_partitions = await allocator.acquire_n(PARTITIONS_PER_POD)

        # Check if partitions were successfully acquired
        if not acquired_partitions:
            logger.error("Could not acquire any partitions after retries. Exiting main function.")
            return # Exit cleanly if no partitions could be acquired

        # Success is logged internally by acquire_n: "🏆 Successfully acquired..."

        # 3. Start Partition Lock Refresh Task
        logger.info("Starting partition lock refresh task...")
        refresh_task = asyncio.create_task(
            _refresh_partition_locks( # Pass necessary arguments
                allocator, acquired_partitions, PARTITION_REFRESH_INTERVAL_SEC
            )
        )
        running_tasks.append(refresh_task) # Track the task for shutdown
        logger.info(f"Partition refresh task scheduled for {acquired_partitions}.")

        # 4. Setup and Connect Consumers (one per acquired partition)
        logger.info("Setting up and connecting consumers...")
        for partition in acquired_partitions:
            logger.info(f"Setting up consumer for partition {partition}...")
            # Pass configuration explicitly
            consumer = Consumer(
                redis_url=REDIS_URL,
                stream_name=STREAM_NAME,
                group=GROUP_NAME,
                consumer_name_base=CONSUMER_NAME_BASE, # Pass the base name
                partition=partition,
                job_lock_ttl_ms=JOB_LOCK_TTL_MS, # Pass specific TTL
                # read_block_ms uses its default (5000ms)
            )
            # Create and register the handler for this consumer/partition
            handler_func = _create_handler(partition)
            consumer.handler("SEND_MESSAGE")(handler_func)

            logger.info(f"Connecting consumer for partition {partition}...")
            await consumer.connect() # Connect this consumer
            consumers.append(consumer) # Keep track of consumer instances
            logger.info(f"Consumer for partition {partition} connected ({consumer.consumer_name}).")
        logger.info(f"All {len(consumers)} consumers set up and connected.")

        # 5. Start Consumer Listening Tasks
        logger.info("Starting consumer listening tasks...")
        if not consumers:
             logger.warning("No consumers were created (this should not happen if partitions were acquired).")
        else:
            for c in consumers:
                logger.info(f"Creating listener task for consumer {c.consumer_name}...")
                # Start the _run_consumer wrapper task for each consumer
                task = asyncio.create_task(_run_consumer(c))
                running_tasks.append(task) # Track the task for shutdown
            logger.info(f"All {len(consumers)} consumer listening tasks scheduled.")

        # 6. Conditionally Start Test Publisher
        # Start the publisher only if this worker instance acquired partition 0
        if acquired_partitions and min(acquired_partitions) == 0:
            logger.info("Condition met: This worker acquired partition 0. Starting test publisher...")
            # Pass configuration explicitly
            publisher = Publisher(
                redis_url=REDIS_URL,
                stream_name=STREAM_NAME,
                partitions=PARTITIONS # Publisher needs total partition count
            )
            await publisher.connect() # Connect the publisher
            publisher_task = asyncio.create_task(publish_test_jobs(publisher)) # Pass the instance
            running_tasks.append(publisher_task) # Track the task for shutdown
            logger.info("Test publisher task scheduled.")
        else:
             # Log if publisher is not started
             logger.info(f"Condition NOT met: This worker acquired partitions {acquired_partitions}. Test publisher will not run.")

        # --- Worker is Ready ---
        logger.info(
            f"--- Worker setup complete. Acquired Partitions: {acquired_partitions}. Entering main wait loop (shutdown_event.wait()). Press Ctrl+C to stop. ---"
        )
        # -----------------------

        # 7. Wait Indefinitely for Shutdown Signal
        await shutdown_event.wait() # This blocks until shutdown_event.set() is called by the signal handler
        logger.info("--- Shutdown signal received or event set. Exiting main wait loop. ---")

    except Exception as e:
        # Catch critical errors during setup or the main wait loop
        logger.exception(f"--- CRITICAL ERROR in main worker loop: {e} ---")
        # Trigger shutdown sequence on critical error
        shutdown_event.set()
    finally:
        # --- Graceful Shutdown Sequence ---
        # This block executes when the try block finishes (normally via shutdown_event)
        # or if an exception occurred.
        logger.info("--- Initiating shutdown sequence (finally block)... ---")

        # Step 1: Signal consumers to stop listening EARLY
        # This sets their internal _stop_event, helping them exit their loop
        # faster once their current xreadgroup call returns.
        logger.info(f"Signalling {len(consumers)} consumers to stop...")
        for c in consumers:
            try:
                # Directly set the event, no need to await stop() itself.
                if hasattr(c, '_stop_event') and isinstance(c._stop_event, asyncio.Event):
                     c._stop_event.set()
                     logger.debug(f"Stop event set for consumer {c.consumer_name}")
                else:
                     logger.warning(f"Cannot set stop event for consumer {getattr(c, 'consumer_name', 'UNKNOWN')}")
            except Exception as e:
                 logger.error(f"Error trying to signal consumer {getattr(c, 'consumer_name', 'UNKNOWN')} to stop: {e}")
        logger.info("Stop signal sent to all consumers.")

        # Step 2: Cancel other background tasks (publisher, refresh) explicitly
        # Consumer tasks are in running_tasks too and will be handled by wait below.
        if publisher_task and not publisher_task.done():
            logger.info("Cancelling publisher task...")
            publisher_task.cancel()
            # Cancellation is requested; asyncio.wait manages the completion/timeout.

        if refresh_task and not refresh_task.done():
            logger.info("Cancelling refresh task...")
            refresh_task.cancel()
            # Cancellation is requested; asyncio.wait manages the completion/timeout.

        # Step 3: Wait for *all* running tasks (consumers, publisher, refresh)
        # Give tasks time to finish cleanly after stop signals/cancellations.
        if running_tasks:
            # Timeout: Should be > consumer read_block_ms + buffer. e.g., 5s + 3s = 8s
            wait_timeout = 8.0
            logger.info(f"Waiting up to {wait_timeout}s for {len(running_tasks)} background tasks to finish gracefully...")

            # Wait for tasks to complete or timeout
            done, pending = await asyncio.wait(running_tasks, timeout=wait_timeout)
            logger.info(f"Initial wait finished. Done tasks: {len(done)}, Pending tasks: {len(pending)}")

            # Step 4: Force Cancel if any task is still pending after the wait period
            if pending:
                logger.warning(f"{len(pending)} tasks did not finish gracefully, cancelling forcefully:")
                for task in pending:
                    # Try getting a meaningful name for the task
                    task_name = task.get_name() if hasattr(task, 'get_name') else str(task)
                    logger.warning(f" - Forcefully cancelling task: {task_name}")
                    task.cancel() # Request forceful cancellation

                # Wait briefly *again* to allow these forceful cancellations to process
                logger.info("Waiting briefly (1s) for forceful cancellations...")
                await asyncio.sleep(1.0)
                logger.info("Brief wait for forceful cancellations complete.")
            else:
                logger.info("All tasks finished gracefully after initial wait.")
        else:
            logger.info("No running tasks found to wait for.")

        # Step 5: Release acquired partitions (Checking connection and existence)
        # This should happen after tasks are stopped/cancelled.
        if allocator and acquired_partitions: # Check if allocator exists and partitions were acquired
            logger.info(f"Checking allocator connection before releasing partitions: {acquired_partitions}")
            if allocator._connected: # Check connection status before attempting release
                logger.info(f"Releasing acquired partitions: {acquired_partitions}")
                try:
                    # Call the allocator's release method
                    await allocator.release_n(acquired_partitions)
                except Exception as e:
                    # Log exception during release but continue shutdown
                    logger.exception(f"Error during partition release: {e}")
            else:
                # Log if allocator is not connected, cannot release reliably
                logger.warning("Allocator not connected, cannot reliably release partitions.")
        elif not acquired_partitions:
            logger.info("No partitions were acquired, skipping release.")
        else: # allocator object itself is None
            logger.warning("Allocator object was not initialized, cannot release partitions.")


        # Step 6: Close allocator connection (only if it exists)
        if allocator:
            logger.info("Closing allocator connection...")
            await allocator.close() # allocator.close() logs its own messages now
        else:
             logger.info("Allocator object was not initialized, skipping close.")

        # Consumer connections are closed within their own _run_consumer finally block

        logger.info("--- Shutdown sequence complete. ---")
        # --- End of Robust Finally Block ---
# --------------------------------


# --- Script Entry Point ---
if __name__ == "__main__":
    # Use print here for immediate feedback before logging might be fully configured elsewhere
    print(f"--- Script {__file__} started ---")
    logger.info(f"--- Configuring signal handlers for SIGINT (Ctrl+C), SIGTERM ---")
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, handle_shutdown_signal)
    signal.signal(signal.SIGTERM, handle_shutdown_signal)
    logger.info(f"--- Signal handlers configured ---")

    try:
        logger.info(f"--- Starting asyncio event loop with asyncio.run(main()) ---")
        # Run the main asynchronous function
        asyncio.run(main())
        logger.info(f"--- asyncio.run(main()) finished normally ---")
    except KeyboardInterrupt:
        # This block should ideally not be reached if the signal handler works correctly
        logger.warning(
            "KeyboardInterrupt caught directly in __main__ (Signal handler might have failed?)."
        )
    except Exception as e:
        # Catch any other unhandled exceptions during the main execution
        logger.exception("--- Unhandled top-level exception during asyncio.run ---")
    finally:
         # Final log message when the script exits
         logger.info(f"--- Script {__file__} finished ---")
# -------------------------