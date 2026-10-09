from __future__ import annotations

import asyncio

import pytest
from aio_pika import connect_robust

from app.messaging.broker import DLQ_QUEUE, PAYMENTS_EXCHANGE, RETRY_QUEUE, create_broker
from tests.integration.conftest import integration_settings

pytestmark = pytest.mark.integration


async def test_message_for_nonexistent_payment_ends_up_in_dead_letter_queue() -> None:
    settings = integration_settings()
    broker = create_broker(settings.rabbitmq_url)
    await broker.start()
    try:
        await broker.publish(
            {"payment_id": "00000000-0000-0000-0000-000000000000"},
            queue="payments.new",
            exchange=PAYMENTS_EXCHANGE,
            routing_key="payments.new",
        )
    finally:
        await broker.stop()

    connection = await connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        dlq = await channel.declare_queue(DLQ_QUEUE.name, durable=True, passive=True)

        async def poll_for_dlq_message() -> bool:
            for _ in range(60):
                message = await dlq.get(fail=False)
                if message is not None:
                    await message.ack()
                    return True
                await asyncio.sleep(1)
            return False

        assert await poll_for_dlq_message() is True

        retry_queue = await channel.declare_queue(RETRY_QUEUE.name, durable=True, passive=True)
        assert retry_queue.declaration_result.message_count == 0
    finally:
        await connection.close()
