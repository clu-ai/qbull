import asyncio
import json
from qbull.publisher import Consumer

from qbull.config import PARTITIONS, REDIS_URL

STREAM_NAME = "WHATSAPP"
GROUP_NAME = "workers"

def create_consumer(partition: int):
    consumer = Consumer(
        REDIS_URL,
        STREAM_NAME,
        GROUP_NAME,
        f"worker-{partition}",
        partition=partition
    )

    @consumer.handler("SEND_MESSAGE")
    async def handle_send_message(job):
        print(f"[P{partition}] 📩 Enviando mensaje a {job['to']}: {job['content']}")
        print(json.dumps(job, indent=2))

    return consumer


async def main():
    consumers = [create_consumer(i) for i in range(PARTITIONS)]
    await asyncio.gather(*(c.connect() for c in consumers))
    await asyncio.gather(*(c.listen() for c in consumers))


if __name__ == "__main__":
    asyncio.run(main())