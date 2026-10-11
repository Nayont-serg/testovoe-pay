from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest

from app.config import Settings
from app.models.outbox import OutboxMessage
from app.models.payment import Currency, Payment, PaymentStatus
from app.repositories.payment_repository import PaymentNotFoundError
from app.schemas.payment import PaymentCreateRequest
from app.services.payment_service import IdempotencyKeyConflictError, PaymentService


def make_settings() -> Settings:
    return Settings.from_env(
        {
            "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db",
            "RABBITMQ_URL": "amqp://u:p@localhost:5672/",
            "API_KEY": "secret",
            "WEBHOOK_ALLOW_PRIVATE_DESTINATIONS": "true",
        }
    )


class FakeRepository:
    def __init__(self) -> None:
        self.payments_by_key: dict[str, Payment] = {}
        self.payments_by_id: dict[uuid.UUID, Payment] = {}
        self.created_outbox_messages: list[OutboxMessage] = []

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        return self.payments_by_key.get(key)

    async def get_by_id(self, payment_id: uuid.UUID) -> Payment | None:
        return self.payments_by_id.get(payment_id)

    async def create_with_outbox_event(
        self, payment: Payment, outbox_message: OutboxMessage
    ) -> Payment:
        winner = self.payments_by_key.get(payment.idempotency_key)
        if winner is not None:
            return winner
        self.payments_by_key[payment.idempotency_key] = payment
        self.payments_by_id[payment.id] = payment
        self.created_outbox_messages.append(outbox_message)
        return payment

    async def mark_processed(
        self, payment_id: uuid.UUID, status: PaymentStatus, processed_at: datetime
    ) -> tuple[Payment, bool]:
        payment = self.payments_by_id.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        if payment.status != PaymentStatus.PENDING:
            return payment, False
        payment.status = status
        payment.processed_at = processed_at
        return payment, True


def make_request() -> PaymentCreateRequest:
    return PaymentCreateRequest(
        amount=Decimal("100.00"),
        currency=Currency.USD,
        description="order #1",
        metadata={"order_id": "abc"},
        webhook_url="https://example.com/hook",
    )


async def test_create_payment_persists_payment_and_outbox_event() -> None:
    repository = FakeRepository()
    service = PaymentService(repository, make_settings())

    payment = await service.create_payment(make_request(), idempotency_key="idem-1")

    assert payment.status == PaymentStatus.PENDING
    assert payment.idempotency_key == "idem-1"
    assert len(repository.created_outbox_messages) == 1
    assert repository.created_outbox_messages[0].payment_id == payment.id
    assert repository.created_outbox_messages[0].routing_key == "payments.new"


async def test_create_payment_is_idempotent_for_same_key() -> None:
    repository = FakeRepository()
    service = PaymentService(repository, make_settings())

    first = await service.create_payment(make_request(), idempotency_key="idem-2")
    second = await service.create_payment(make_request(), idempotency_key="idem-2")

    assert first.id == second.id
    assert len(repository.created_outbox_messages) == 1


async def test_get_payment_returns_none_when_missing() -> None:
    repository = FakeRepository()
    service = PaymentService(repository, make_settings())

    assert await service.get_payment(uuid.uuid4()) is None


async def test_mark_processed_raises_for_unknown_payment() -> None:
    repository = FakeRepository()
    with pytest.raises(PaymentNotFoundError):
        await repository.mark_processed(uuid.uuid4(), PaymentStatus.SUCCEEDED, datetime.now(UTC))


async def test_create_payment_raises_conflict_when_replay_body_differs() -> None:
    repository = FakeRepository()
    service = PaymentService(repository, make_settings())

    await service.create_payment(make_request(), idempotency_key="idem-3")

    mismatched_request = PaymentCreateRequest(
        amount=Decimal("999.00"),
        currency=Currency.USD,
        description="order #1",
        metadata={"order_id": "abc"},
        webhook_url="https://example.com/hook",
    )

    with pytest.raises(IdempotencyKeyConflictError):
        await service.create_payment(mismatched_request, idempotency_key="idem-3")

    assert len(repository.created_outbox_messages) == 1


async def test_create_payment_skips_destination_check_on_exact_match_replay(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    repository = FakeRepository()
    service = PaymentService(repository, make_settings())

    first = await service.create_payment(make_request(), idempotency_key="idem-4")

    async def fail_if_called(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("destination check must not run again for an exact-match replay")

    monkeypatch.setattr("app.services.payment_service.validate_webhook_destination", fail_if_called)

    replayed = await service.create_payment(make_request(), idempotency_key="idem-4")

    assert replayed.id == first.id
    assert len(repository.created_outbox_messages) == 1
