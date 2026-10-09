from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.messaging.broker import PAYMENTS_EXCHANGE, create_broker
from app.models.payment import Currency, Payment, PaymentStatus
from app.repositories.payment_repository import OutboxRepository, PaymentRepository
from app.schemas.payment import PaymentCreateRequest
from app.services.payment_service import PaymentService
from tests.integration.conftest import integration_settings

pytestmark = pytest.mark.integration


async def test_payment_is_created_published_and_reaches_terminal_status(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    settings = integration_settings()
    broker = create_broker(settings.rabbitmq_url)
    await broker.start()
    try:
        async with session_factory() as session:
            service = PaymentService(PaymentRepository(session))
            payment = await service.create_payment(
                PaymentCreateRequest(
                    amount="10.00",
                    currency=Currency.USD,
                    description="integration test",
                    metadata={},
                    webhook_url="https://example.com/hook",
                ),
                idempotency_key=str(uuid.uuid4()),
            )

        async with session_factory() as session, session.begin():
            repository = OutboxRepository(session)
            messages = await repository.fetch_unpublished(10)
            assert any(m.payment_id == payment.id for m in messages)
            for message in messages:
                await broker.publish(
                    message.payload,
                    queue="payments.new",
                    exchange=PAYMENTS_EXCHANGE,
                    routing_key=message.routing_key,
                )
                message.published_at = datetime.now(UTC)

        async def poll_for_terminal_status() -> Payment:
            for _ in range(100):
                async with session_factory() as session:
                    fetched = await session.get(Payment, payment.id)
                    if fetched is not None and fetched.status != PaymentStatus.PENDING:
                        return fetched
                await asyncio.sleep(0.2)
            raise AssertionError("payment never left pending status")

        final_payment = await poll_for_terminal_status()
        assert final_payment.status in (PaymentStatus.SUCCEEDED, PaymentStatus.FAILED)
        assert final_payment.processed_at is not None
    finally:
        await broker.stop()
