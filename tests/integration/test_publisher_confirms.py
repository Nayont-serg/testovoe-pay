from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from aio_pika import connect_robust
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging.broker import (
    DLQ_QUEUE,
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    create_broker,
    declare_payments_topology,
    declare_retry_topology,
    retry_tier_queue_name,
)
from app.messaging.outbox_relay import OutboxRelay
from app.models.outbox import OutboxMessage
from app.models.payment import Currency, Payment, PaymentStatus
from app.repositories.payment_repository import PaymentRepository
from app.schemas.payment import PaymentCreateRequest
from app.services.payment_service import PaymentService
from tests.integration.conftest import integration_settings
from tests.integration.environment import PROJECT_ROOT, assert_test_rabbitmq_vhost

pytestmark = pytest.mark.integration


async def _run_compose(*args: str) -> None:
    process = await asyncio.create_subprocess_exec(
        "docker",
        "compose",
        *args,
        cwd=PROJECT_ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(f"docker compose {' '.join(args)} failed: {stderr.decode()}")


async def _fetch_outbox_message(session: AsyncSession, payment_id: uuid.UUID) -> OutboxMessage:
    result = await session.execute(
        select(OutboxMessage).where(OutboxMessage.payment_id == payment_id)
    )
    return result.scalar_one()


async def _poll_for_terminal_status(
    session_factory: async_sessionmaker[AsyncSession], payment_id: uuid.UUID
) -> Payment:
    for _ in range(100):
        async with session_factory() as session:
            fetched = await session.get(Payment, payment_id)
            if fetched is not None and fetched.status != PaymentStatus.PENDING:
                return fetched
        await asyncio.sleep(0.2)
    raise AssertionError("payment never left pending status")


async def test_relay_leaves_message_unpublished_and_self_heals_after_binding_is_deleted(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = integration_settings()
    assert_test_rabbitmq_vhost(settings.rabbitmq_url)

    await _run_compose("stop", "api")
    try:
        connection = await connect_robust(settings.rabbitmq_url)
        try:
            channel = await connection.channel()
            exchange = await channel.declare_exchange(
                PAYMENTS_EXCHANGE.name, durable=True, passive=True
            )
            queue = await channel.declare_queue(PAYMENTS_QUEUE.name, durable=True, passive=True)
            await queue.unbind(exchange, routing_key="payments.new")

            broker = create_broker(settings.rabbitmq_url)
            await broker.start()
            try:
                async with session_factory() as session:
                    service = PaymentService(PaymentRepository(session), settings)
                    payment = await service.create_payment(
                        PaymentCreateRequest(
                            amount="10.00",
                            currency=Currency.USD,
                            description="r1 binding loss repro",
                            metadata={},
                            webhook_url="https://example.com/hook",
                        ),
                        idempotency_key=str(uuid.uuid4()),
                    )

                relay = OutboxRelay(
                    session_factory,
                    broker,
                    settings,
                    declare_topology=lambda: declare_payments_topology(broker),
                )

                await relay._publish_pending_batch()

                async with session_factory() as session:
                    outbox_message = await _fetch_outbox_message(session, payment.id)
                    assert outbox_message.published_at is None

                queue_state = await channel.declare_queue(
                    PAYMENTS_QUEUE.name, durable=True, passive=True
                )
                assert queue_state.declaration_result.message_count == 0

                await relay._publish_pending_batch()

                async with session_factory() as session:
                    outbox_message = await _fetch_outbox_message(session, payment.id)
                    assert outbox_message.published_at is not None
            finally:
                await broker.stop()
        finally:
            await queue.bind(exchange, routing_key="payments.new")
            await connection.close()
    finally:
        await _run_compose("start", "api")

    final_payment = await _poll_for_terminal_status(session_factory, payment.id)
    assert final_payment.status in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)


async def test_consumer_dead_letters_original_message_when_retry_tier_queue_is_gone() -> None:
    settings = integration_settings()
    assert_test_rabbitmq_vhost(settings.rabbitmq_url)
    nonexistent_payment_id = str(uuid.uuid4())

    connection = await connect_robust(settings.rabbitmq_url)
    try:
        channel = await connection.channel()
        dlq = await channel.declare_queue(DLQ_QUEUE.name, durable=True, passive=True)
        await dlq.purge()

        tier_one_queue = await channel.declare_queue(
            retry_tier_queue_name(1), durable=True, passive=True
        )
        await tier_one_queue.delete()

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
    finally:
        restore_broker = create_broker(settings.rabbitmq_url)
        await restore_broker.start()
        try:
            await declare_retry_topology(restore_broker, settings)
        finally:
            await restore_broker.stop()
        await connection.close()
