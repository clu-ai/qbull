import asyncio
import json
from qbull.publisher import Publisher, Consumer
from qbull.config import PARTITIONS, REDIS_URL

STREAM_NAME = "WHATSAPP"
GROUP_NAME = "workers"
CONSUMER_NAME = "worker-1"
PARTITION = 0

consumer = Consumer(
    REDIS_URL, STREAM_NAME, GROUP_NAME, CONSUMER_NAME, partition=PARTITION
)


@consumer.handler("SEND_MESSAGE")
async def handle_send_message(job):
    print(f"📩 Enviando mensaje a {job['to']}: {job['content']}")
    print(json.dumps(job, indent=2))


async def publish_test_message():
    publisher = Publisher(REDIS_URL, STREAM_NAME, partitions=PARTITIONS)
    await publisher.connect()
    job_id = await publisher.publish_job(
        {"cmd": "SEND_MESSAGE", "to": "3205104418", "content": "Hola desde el test"}
    )
    print(f"✅ Job publicado con ID: {job_id}")
    await publisher.close()


async def run_all():
    await consumer.connect()
    asyncio.create_task(publish_test_message())
    await consumer.listen()


if __name__ == "__main__":
    asyncio.run(run_all())
