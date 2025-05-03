import asyncio
import json  # For potentially printing job data
import logging  # Standard Python logging
import os  # To access environment variables
from datetime import datetime  # For timestamp in test message
from dotenv import load_dotenv  # To load config from .env file for the example
from pathlib import Path  # To construct path to .env file

# --- Import FROM the qbull library ---
# Assumes the qbull library is installed (e.g., pip install -e . or from GitHub)
# or that the 'src' directory is in the PYTHONPATH for local testing.
try:
    # Standard import if library is installed
    from qbull import Publisher, Consumer
except ImportError:
    # Fallback for local testing without installation
    # This assumes you run like: python -m examples.main_example from the repo root
    import sys

    print(
        "WARN: Could not import 'qbull'. Attempting relative import from '../src'. "
        "Install the library (`pip install .` or `pip install git+...`) for standard usage.",
        file=sys.stderr,
    )
    # Add the 'src' directory (parent of 'examples' parent) to the Python path
    sys.path.insert(0, str(Path(__file__).parent.parent / "src"))
    from qbull import Publisher, Consumer

# --- Configuration for this Example Application ---
# Load config from a .env file located in the project root (one level up from 'examples')
# This demonstrates how an application using the library might load its config.
# The library itself does NOT load .env files.
dotenv_path = Path(__file__).parent.parent / ".env"  # Path to root .env
if dotenv_path.exists():
    load_dotenv(dotenv_path=dotenv_path)
    print(
        f"Loaded config from {dotenv_path}"
    )  # Use print before logging might be fully set
else:
    print(
        f"WARN: .env file not found at {dotenv_path}, using defaults or environment variables."
    )

# Get configuration values (the application is responsible for this)
# It reads from environment variables, falling back to defaults if not set.
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
# Total number of partitions the system uses (the publisher needs this to calculate target partition)
PARTITIONS = int(os.getenv("PARTITIONS", 8))
# Specific parameters for this simple example
STREAM_NAME = (
    "WHATSAPP_EXAMPLE"  # Use a different stream name to avoid interfering with workers
)
GROUP_NAME = "main_example_workers"  # Consumer group for this example
CONSUMER_NAME_BASE = "main-test-worker"  # Base name for the consumer instance
EXAMPLE_PARTITION = 0  # This example will focus on partition 0
EXAMPLE_JOB_LOCK_TTL_MS = 15000  # Use a shorter job lock TTL for quick testing
# -------------------------------------------------

# --- Logging Configuration (The application configures its own logging) ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
# Get a specific logger for this example script
logger = logging.getLogger("main_example")
# -------------------------------------------------------------------------


# --- Instantiate Consumer by passing config explicitly ---
# Note: We now pass 'consumer_name_base' instead of the final 'consumer_name'.
# The Consumer class will append the partition number internally.
logger.info(f"Instantiating Consumer for partition {EXAMPLE_PARTITION}")
consumer = Consumer(
    redis_url=REDIS_URL,
    stream_name=STREAM_NAME,
    group=GROUP_NAME,
    consumer_name_base=CONSUMER_NAME_BASE,  # Base name provided here
    partition=EXAMPLE_PARTITION,  # Specify which partition this instance handles
    job_lock_ttl_ms=EXAMPLE_JOB_LOCK_TTL_MS,  # Pass the specific TTL
    # read_block_ms uses its library default (e.g., 5000ms)
)


# --- Define and Register a Job Handler ---
# Use the @consumer.handler decorator provided by the library's Consumer class
# to register a function that handles jobs with the command "SEND_MESSAGE".
@consumer.handler("SEND_MESSAGE")
async def handle_send_message(job):
    # This function will be called by the consumer when it receives a job
    # with 'cmd': 'SEND_MESSAGE' after acquiring the job lock.
    logger.info(
        # Use the application's logger
        f"📩 [P{EXAMPLE_PARTITION}] Handling SEND_MESSAGE for job {job.get('job_id')} to {job.get('to')}"
    )
    await asyncio.sleep(1)  # Simulate doing some work (e.g., calling an API)
    # Optional: Print the full job data for debugging
    # logger.debug(f"Full job data:\n{json.dumps(job, indent=2)}")
    logger.info(
        f"✅ [P{EXAMPLE_PARTITION}] Finished processing job {job.get('job_id')}"
    )


# --- Function to Publish a Test Message ---
async def publish_test_message():
    """Instantiates Publisher, connects, publishes one message, and closes."""
    publisher = None  # Initialize variable
    logger.info("Attempting to publish a test message...")
    try:
        # Instantiate Publisher by passing config explicitly
        publisher = Publisher(
            redis_url=REDIS_URL,
            stream_name=STREAM_NAME,
            partitions=PARTITIONS,  # Publisher needs total partitions to calculate the target
        )
        # Connect the publisher to Redis
        await publisher.connect()
        logger.info("Publisher connected for example.")

        # Prepare job data
        test_phone = "3201112233"  # Example phone number
        job_data = {  # 'data' is the argument for the job payload
            "cmd": "SEND_MESSAGE",
            "to": test_phone,
            "content": f"Hello from main_example.py test @ {datetime.now()}",
        }
        # Publish the job, providing a key for partitioning
        job_id = await publisher.publish_job(
            data=job_data,
            partition_key=test_phone,  # Use the phone number as the partition key
        )
        # Log success or failure
        if job_id:
            logger.info(f"Published test job with ID: {job_id} (key: {test_phone})")
        else:
            logger.error("Failed to publish test job.")

    except Exception as e:
        # Log any exceptions during publishing
        logger.exception("Error publishing test message")
    finally:
        # Ensure the publisher connection is closed
        if publisher:
            logger.info("Closing example publisher...")
            await publisher.close()  # close() logs its own messages
            logger.info("Example publisher closed.")


# --- Function to Run Publisher and Consumer Concurrently (for demo) ---
async def run_all():
    """Connects consumer, publishes a message in the background, and starts listening."""
    # Access the globally defined consumer instance
    global consumer
    logger.info("Starting run_all (connect consumer, publish, start listening)...")
    try:
        # Connect the consumer instance
        logger.info("Connecting example consumer...")
        await consumer.connect()
        logger.info("Example consumer connected.")

        # Start the publish_test_message function as a background task
        logger.info("Creating task to publish test message...")
        publish_task = asyncio.create_task(publish_test_message())

        # Start the consumer listening loop as a background task
        logger.info(
            f"Creating task for consumer listen loop on partition {EXAMPLE_PARTITION}..."
        )
        listen_task = asyncio.create_task(consumer.listen())

        # Wait for tasks. In this simple example, listen_task runs forever until stopped.
        # We'll wait only for the publisher task to finish first to see the message published.
        logger.info("Waiting for publisher task to complete...")
        done, pending = await asyncio.wait(
            {publish_task, listen_task},
            return_when=asyncio.FIRST_COMPLETED,  # Wait until at least one task finishes (should be publisher)
        )
        logger.info(
            f"Publisher task likely finished (in 'done': {publish_task in done}). Listener task running: {listen_task in pending}"
        )

        # Now, keep the script running by waiting for the listener task.
        # The listener task will only stop if it encounters an unhandled error
        # or if the script receives an interrupt signal (Ctrl+C).
        if listen_task in pending:
            logger.info(
                "Listener task still running (expected). Waiting indefinitely (or until Ctrl+C)..."
            )
            await listen_task  # Wait for the listener task to complete or raise an error/cancellation

    except asyncio.CancelledError:
        # This might happen if the main task itself is cancelled externally
        logger.info("run_all task was cancelled.")
    except Exception as e:
        # Log any other unexpected errors in run_all
        logger.exception("Error in run_all orchestration")
    finally:
        # Cleanup: Close the consumer.
        # This will be called if run_all finishes normally, encounters an error, or is cancelled.
        # The consumer.listen() task's wrapper (_run_consumer in worker example) would
        # normally handle closing, but since we run listen() directly here, we close manually.
        logger.info("Closing example consumer in run_all finally block...")
        # Check if consumer exists and is connected before closing
        # Note: `consumer.close()` calls `consumer.stop()` internally which sets the event.
        if consumer and hasattr(consumer, "close"):
            # Checking consumer._connected might be unreliable during shutdown, just try closing.
            await consumer.close()
        logger.info("run_all finished.")


# --- Script Entry Point ---
if __name__ == "__main__":
    # This block executes when the script is run directly (e.g., python examples/main_example.py)
    logger.info("Running simple main_example.py...")
    logger.info(f"Using Redis URL: {REDIS_URL}")
    logger.info(f"Target Stream: {STREAM_NAME} (Total System Partitions: {PARTITIONS})")
    logger.info(f"This example focuses on Partition: {EXAMPLE_PARTITION}")

    # Get the asyncio event loop
    # loop = asyncio.get_event_loop() # Deprecated since Python 3.10
    try:
        # Run the main async function 'run_all' until it completes
        # 'run_all' is designed to run indefinitely until interrupted
        logger.info(
            "Starting asyncio event loop with run_all()... Press Ctrl+C to stop."
        )
        asyncio.run(run_all())
    except KeyboardInterrupt:
        # Handle Ctrl+C gracefully
        logger.info("\nKeyboardInterrupt received. Stopping example...")
        # asyncio.run() typically handles loop cleanup on KeyboardInterrupt,
        # but explicit task cancellation might be needed in more complex scenarios.
        # The finally block in run_all should handle consumer cleanup.
    except Exception as e:
        logger.exception("An unexpected error occurred at the top level.")
    finally:
        # Optional: Explicit loop closing (often not needed with asyncio.run)
        # loop.close()
        logger.info("Main example script finished.")
