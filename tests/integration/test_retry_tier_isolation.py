from __future__ import annotations

import asyncio
import json
import time
import uuid

import pytest
from aio_pika import connect_robust

from app.messaging.broker import (
    PAYMENTS_EXCHANGE,
    create_broker,
    declare_retry_topology,
    retry_tier_delay_ms,
    retry_tier_exchange,
)
from tests.integration.conftest import integration_settings
from tests.integration.environment import assert_test_rabbitmq_vhost

pytestmark = pytest.mark.integration


async def test_short_tier_retry_is_not_blocked_by_a_longer_tier_retry_ahead_of_it() -> None:
    settings = integration_settings()
    assert_test_rabbitmq_vhost(settings.rabbitmq_url)

    long_tier, short_tier = 2, 1
    long_delay_ms = retry_tier_delay_ms(long_tier, settings.message_retry_base_delay_ms)
    short_delay_ms = retry_tier_delay_ms(short_tier, settings.message_retry_base_delay_ms)
    assert short_delay_ms < long_delay_ms

    long_payment_id = str(uuid.uuid4())
    short_payment_id = str(uuid.uuid4())

    connection = await connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        payments_exchange = await channel.declare_exchange(
            PAYMENTS_EXCHANGE.name, durable=True, passive=True
        )
        observer_queue = await channel.declare_queue(exclusive=True, auto_delete=True)
        await observer_queue.bind(payments_exchange, routing_key="payments.new")

        broker = create_broker(settings.rabbitmq_url)
        await broker.start()
        await declare_retry_topology(broker, settings)
        try:
            publish_time = time.monotonic()
            await broker.publish(
                {"payment_id": long_payment_id},
                exchange=retry_tier_exchange(long_tier),
                routing_key="payments.new",
                mandatory=True,
                persist=True,
            )
            await broker.publish(
                {"payment_id": short_payment_id},
                exchange=retry_tier_exchange(short_tier),
                routing_key="payments.new",
                mandatory=True,
                persist=True,
            )
        finally:
            await broker.stop()

        arrivals: dict[str, float] = {}
        deadline = publish_time + (long_delay_ms / 1000) + 3.0
        while len(arrivals) < 2 and time.monotonic() < deadline:
            message = await observer_queue.get(fail=False)
            if message is None:
                await asyncio.sleep(0.05)
                continue
            payload = json.loads(message.body.decode())
            payment_id = payload.get("payment_id")
            await message.ack()
            if payment_id in (long_payment_id, short_payment_id):
                arrivals.setdefault(payment_id, time.monotonic() - publish_time)

        assert long_payment_id in arrivals, "long-tier retry never returned to payments.new"
        assert short_payment_id in arrivals, "short-tier retry never returned to payments.new"

        short_elapsed = arrivals[short_payment_id]
        long_elapsed = arrivals[long_payment_id]

        assert short_elapsed < long_elapsed, (
            "short-tier retry was blocked behind the longer-tier retry ahead of it"
        )
        assert short_elapsed < (long_delay_ms / 1000) - 0.5, (
            "short-tier retry waited almost as long as the long-tier retry, "
            "indicating head-of-line blocking on a shared queue"
        )
        assert abs(short_elapsed - short_delay_ms / 1000) < 1.0
        assert abs(long_elapsed - long_delay_ms / 1000) < 1.5
    finally:
        await connection.close()
