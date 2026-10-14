from __future__ import annotations

import asyncio
from pathlib import Path

from aio_pika import connect_robust

from app.config import Settings
from app.consumer_heartbeat import DEFAULT_HEARTBEAT_PATH, is_heartbeat_fresh
from app.messaging.broker import DLQ_QUEUE, PAYMENTS_QUEUE, retry_tier_queue, retry_tiers

HEARTBEAT_STALE_MULTIPLIER = 3


class ConsumerHeartbeatStaleError(RuntimeError):
    pass


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


def check_heartbeat_fresh(
    settings: Settings, heartbeat_path: Path = DEFAULT_HEARTBEAT_PATH
) -> None:
    max_age_seconds = settings.consumer_heartbeat_interval_seconds * HEARTBEAT_STALE_MULTIPLIER
    if not is_heartbeat_fresh(heartbeat_path, max_age_seconds):
        raise ConsumerHeartbeatStaleError(
            f"consumer heartbeat file {heartbeat_path} is missing or older than "
            f"{max_age_seconds} seconds; this consumer process may be hung or stopped, "
            "even though RabbitMQ still reports its queues as declared"
        )


def main() -> None:
    settings = Settings.from_env()
    asyncio.run(check_topology_declared(settings.rabbitmq_url, settings))
    check_heartbeat_fresh(settings)


if __name__ == "__main__":
    main()
