import asyncio
import json
import logging
from datetime import datetime

# Assuming qbull package structure
from qbull.publisher import Publisher, Consumer
from qbull.config import PARTITIONS, REDIS_URL, DEFAULT_JOB_LOCK_TTL_MS

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

STREAM_NAME = "WHATSAPP"
GROUP_NAME = "workers"
CONSUMER_NAME = "main-test-worker"
PARTITION = 0

# Keep example simple, uses partition 0
consumer = Consumer(
    REDIS_URL,
    STREAM_NAME,
    GROUP_NAME,
    CONSUMER_NAME,
    partition=PARTITION,
    job_lock_ttl_ms=15000,  # Shorter TTL for testing example
)


@consumer.handler("SEND_MESSAGE")
async def handle_send_message(job):
    logging.info(
        f"📩 Handling SEND_MESSAGE for job {job.get('job_id')} to {job.get('to')}"
    )
    await asyncio.sleep(1)  # Simulate work
    # print(json.dumps(job, indent=2))
    logging.info(f"✅ Finished job {job.get('job_id')}")


async def publish_test_message():
    publisher = None
    try:
        publisher = Publisher(REDIS_URL, STREAM_NAME, partitions=PARTITIONS)
        await publisher.connect()
        job_id = await publisher.publish_job(
            {
                "cmd": "SEND_MESSAGE",
                "to": "3201112233",
                "content": f"Hello from main.py test @ {datetime.now()}",
            },
            partition_key="3201112233",  # Explicit key for partitioning
        )
        if job_id:
            logging.info(f"Published test job with ID: {job_id}")
        else:
            logging.error("Failed to publish test job.")
    except Exception as e:
        logging.exception("Error publishing test message")
    finally:
        if publisher:
            await publisher.close()


async def run_all():
    try:
        await consumer.connect()
        # Run publisher concurrently
        publish_task = asyncio.create_task(publish_test_message())
        # Start listening
        listen_task = asyncio.create_task(consumer.listen())
        # Wait for both tasks (or until listener stops/fails)
        await asyncio.gather(publish_task, listen_task)

    except Exception as e:
        logging.exception("Error in run_all")
    finally:
        await consumer.close()


if __name__ == "__main__":
    logging.info("Running simple main.py example...")
    try:
        asyncio.run(run_all())
    except KeyboardInterrupt:
        logging.info("Main example interrupted.")
    logging.info("Main example finished.")
