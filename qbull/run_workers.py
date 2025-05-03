import asyncio
import json
import random
import logging
from qbull.publisher import Consumer, Publisher
from qbull.config import PARTITIONS, REDIS_URL, PARTITIONS_PER_POD
from qbull.partition_allocator import PartitionAllocator

STREAM_NAME = "WHATSAPP"
GROUP_NAME = "workers"

logging.basicConfig(level=logging.INFO)


async def publish_test_jobs(publisher):
    for i in range(5):
        await asyncio.sleep(random.uniform(2, 5))
        to_number = random.choice(["3205104418", "3205104419"])
        job_id = await publisher.publish_job(
            {
                "cmd": "SEND_MESSAGE",
                "to": to_number,
                "content": f"Mensaje de prueba #{i+1} desde run_workers",
                "partition_id": to_number,
            }
        )
        logging.info(f"✅ Test job #{i+1} published with ID: {job_id}")


async def main():
    allocator = PartitionAllocator(REDIS_URL, total_partitions=PARTITIONS)
    await allocator.connect()
    acquired = await allocator.acquire_n(PARTITIONS_PER_POD)

    consumers = []
    for partition in acquired:
        consumer = Consumer(
            REDIS_URL,
            STREAM_NAME,
            GROUP_NAME,
            f"worker-{partition}",
            partition=partition,
        )

        @consumer.handler("SEND_MESSAGE")
        async def handle_send_message(job, partition=partition):
            logging.info(f"[P{partition}] 📩 To {job['to']}: {job['content']}")
            print(json.dumps(job, indent=2))

        await consumer.connect()
        consumers.append(consumer)

    publisher_task = None
    if 0 in acquired:
        publisher = Publisher(REDIS_URL, STREAM_NAME, partitions=PARTITIONS)
        await publisher.connect()
        publisher_task = asyncio.create_task(publish_test_jobs(publisher))

    try:
        await asyncio.gather(
            *(c.listen() for c in consumers),
            *([publisher_task] if publisher_task else []),
        )
    finally:
        for p in acquired:
            await allocator.release_partition(p)


if __name__ == "__main__":
    asyncio.run(main())
