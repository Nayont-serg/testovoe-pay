from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from aio_pika import connect_robust

from app.messaging.broker import DLQ_QUEUE, PAYMENTS_EXCHANGE, RETRY_QUEUE, create_broker
from tests.integration.conftest import integration_settings
from tests.integration.environment import assert_test_rabbitmq_vhost

pytestmark = pytest.mark.integration


async def test_message_for_nonexistent_payment_ends_up_in_dead_letter_queue() -> None:
    settings = integration_settings()
    nonexistent_payment_id = str(uuid.uuid4())

    connection = await connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        dlq = await channel.declare_queue(DLQ_QUEUE.name, durable=True, passive=True)
        stale_retry_queue = await channel.declare_queue(
            RETRY_QUEUE.name, durable=True, passive=True
        )
        assert_test_rabbitmq_vhost(settings.rabbitmq_url)
        await dlq.purge()
        await stale_retry_queue.purge()

        broker = create_broker(settings.rabbitmq_url)
        await broker.start()
        try:
            await broker.publish(
                {"payment_id": nonexistent_payment_id},
                queue="payments.new",
                exchange=PAYMENTS_EXCHANGE,
                routing_key="payments.new",
            )
        finally:
            await broker.stop()

        async def poll_for_dlq_message() -> bool:
            for _ in range(60):
                message = await dlq.get(fail=False)
                if message is None:
                    await asyncio.sleep(1)
                    continue
                payload = json.loads(message.body.decode())
                if payload.get("payment_id") == nonexistent_payment_id:
                    await message.ack()
                    return True
            return False

        assert await poll_for_dlq_message() is True

        retry_queue = await channel.declare_queue(RETRY_QUEUE.name, durable=True, passive=True)
        assert retry_queue.declaration_result.message_count == 0
    finally:
        await connection.close()
