from __future__ import annotations

import asyncio

from aio_pika import connect_robust

from app.config import Settings
from app.messaging.broker import DLQ_QUEUE, PAYMENTS_QUEUE, retry_tier_queue, retry_tiers


async def check_topology_declared(rabbitmq_url: str, settings: Settings) -> None:
    connection = await connect_robust(rabbitmq_url)
    try:
        channel = await connection.channel()
        retry_queues = (
            retry_tier_queue(tier, settings.message_retry_base_delay_ms)
            for tier in retry_tiers(settings.message_max_attempts)
        )
        for queue in (PAYMENTS_QUEUE, *retry_queues, DLQ_QUEUE):
            await channel.declare_queue(queue.name, durable=True, passive=True)
    finally:
        await connection.close()


def main() -> None:
    settings = Settings.from_env()
    asyncio.run(check_topology_declared(settings.rabbitmq_url, settings))


if __name__ == "__main__":
    main()
