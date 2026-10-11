from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy import delete

from app.db.session import create_engine, create_session_factory
from app.models.payment import Currency, Payment, PaymentStatus
from tests.integration.environment import (
    UnsafeTestEnvironmentError,
    ensure_database_exists,
    ensure_rabbitmq_vhost_exists,
)

pytestmark = pytest.mark.integration

DEV_DATABASE_URL = "postgresql+asyncpg://payments:payments@localhost:5433/payments"
DEV_RABBITMQ_URL = "amqp://payments:payments@localhost:5673/"


async def test_ensure_database_exists_refuses_non_test_database_and_preserves_dev_data() -> None:
    engine = create_engine(DEV_DATABASE_URL)
    session_factory = create_session_factory(engine)
    sentinel = Payment(
        id=uuid.uuid4(),
        amount=Decimal("1.00"),
        currency=Currency.USD,
        description="R5 regression sentinel",
        metadata_={},
        status=PaymentStatus.PENDING,
        idempotency_key=str(uuid.uuid4()),
        webhook_url="https://example.com/sentinel",
        created_at=datetime.now(UTC),
    )
    try:
        async with session_factory() as session:
            session.add(sentinel)
            await session.commit()

        with pytest.raises(UnsafeTestEnvironmentError):
            await ensure_database_exists(DEV_DATABASE_URL)

        async with session_factory() as session:
            preserved = await session.get(Payment, sentinel.id)
        assert preserved is not None
    finally:
        async with session_factory() as session:
            await session.execute(delete(Payment).where(Payment.id == sentinel.id))
            await session.commit()
        await engine.dispose()


async def test_ensure_rabbitmq_vhost_exists_refuses_non_test_vhost() -> None:
    with pytest.raises(UnsafeTestEnvironmentError):
        await ensure_rabbitmq_vhost_exists(DEV_RABBITMQ_URL, "http://localhost:15673")
