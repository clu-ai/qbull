import asyncio
import json
from qbull.publisher import Consumer, Publisher
from qbull.config import PARTITIONS, REDIS_URL

STREAM_NAME = "WHATSAPP"
GROUP_NAME = "workers"


def create_consumer(partition: int):
    consumer = Consumer(
        REDIS_URL, STREAM_NAME, GROUP_NAME, f"worker-{partition}", partition=partition
    )

    @consumer.handler("SEND_MESSAGE")
    async def handle_send_message(job):
        print(f"[P{partition}] 📩 Enviando mensaje a {job['to']}: {job['content']}")
        print(json.dumps(job, indent=2))

    return consumer


async def publish_test_job():
    publisher = Publisher(REDIS_URL, STREAM_NAME, partitions=PARTITIONS)
    await publisher.connect()
    job_id = await publisher.publish_job(
        {
            "cmd": "SEND_MESSAGE",
            "to": "3205104418",
            "content": "Mensaje de prueba desde run_workers",
        }
    )
    print(f"✅ Test job published with ID: {job_id}")
    await publisher.close()


async def main():
    consumers = [create_consumer(i) for i in range(PARTITIONS)]
    await asyncio.gather(*(c.connect() for c in consumers))

    # Publicar trabajo de prueba
    await publish_test_job()

    await asyncio.gather(*(c.listen() for c in consumers))


if __name__ == "__main__":
    asyncio.run(main())
