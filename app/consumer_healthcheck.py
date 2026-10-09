from __future__ import annotations

import asyncio

from aio_pika import connect_robust

from app.config import Settings
from app.messaging.broker import DLQ_QUEUE, PAYMENTS_QUEUE, RETRY_QUEUE


async def check_topology_declared(rabbitmq_url: str) -> None:
    connection = await connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()
        for queue in (PAYMENTS_QUEUE, RETRY_QUEUE, DLQ_QUEUE):
            await channel.declare_queue(queue.name, durable=True, passive=True)
    finally:
        await connection.close()


def main() -> None:
    settings = Settings.from_env()
    asyncio.run(check_topology_declared(settings.rabbitmq_url))


if __name__ == "__main__":
    main()
