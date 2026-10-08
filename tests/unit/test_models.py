from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

from app.models.outbox import OutboxMessage
from app.models.payment import Currency, Payment, PaymentStatus


def test_payment_defaults_to_pending_status() -> None:
    payment = Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.50"),
        currency=Currency.USD,
        description="test",
        metadata_={"order_id": "abc"},
        idempotency_key="key-1",
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
    )
    assert payment.status == PaymentStatus.PENDING
    assert payment.processed_at is None


def test_outbox_message_starts_unpublished() -> None:
    message = OutboxMessage(
        id=uuid.uuid4(),
        payment_id=uuid.uuid4(),
        routing_key="payments.new",
        payload={"payment_id": "abc"},
        created_at=datetime.now(UTC),
    )
    assert message.published_at is None
