from __future__ import annotations

import uuid
from datetime import UTC, datetime
from decimal import Decimal

import pytest
from sqlalchemy.exc import IntegrityError

from app.models.outbox import OutboxMessage
from app.models.payment import Currency, Payment, PaymentStatus
from app.repositories.payment_repository import PaymentNotFoundError, PaymentRepository


def make_payment(idempotency_key: str, status: PaymentStatus = PaymentStatus.PENDING) -> Payment:
    return Payment(
        id=uuid.uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.USD,
        description="order",
        metadata_={},
        status=status,
        idempotency_key=idempotency_key,
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
    )


def make_outbox_message(payment: Payment) -> OutboxMessage:
    return OutboxMessage(
        id=uuid.uuid4(),
        payment_id=payment.id,
        routing_key="payments.new",
        payload={"payment_id": str(payment.id)},
        created_at=datetime.now(UTC),
    )


class FakeResult:
    def __init__(self, payment: Payment | None) -> None:
        self._payment = payment

    def scalar_one_or_none(self) -> Payment | None:
        return self._payment


class FakeSession:
    def __init__(
        self,
        stored: Payment | None = None,
        lookup_result: Payment | None = None,
        commit_error: IntegrityError | None = None,
    ) -> None:
        self.stored = stored
        self.lookup_result = lookup_result
        self.commit_error = commit_error
        self.added: list[object] = []
        self.commits = 0
        self.rollbacks = 0

    def add(self, instance: object) -> None:
        self.added.append(instance)

    async def flush(self) -> None:
        return None

    async def commit(self) -> None:
        if self.commit_error is not None:
            raise self.commit_error
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def execute(self, _statement: object) -> FakeResult:
        return FakeResult(self.lookup_result)

    async def get(
        self, _entity: object, _primary_key: uuid.UUID, with_for_update: bool = False
    ) -> Payment | None:
        assert with_for_update is True
        return self.stored


def make_integrity_error() -> IntegrityError:
    return IntegrityError("INSERT INTO payments", (), Exception("duplicate key"))


async def test_create_with_outbox_event_returns_created_payment() -> None:
    payment = make_payment("idem-1")
    session = FakeSession()
    repository = PaymentRepository(session)

    created = await repository.create_with_outbox_event(payment, make_outbox_message(payment))

    assert created is payment
    assert session.commits == 1
    assert session.rollbacks == 0


async def test_create_with_outbox_event_returns_winner_on_idempotency_key_conflict() -> None:
    payment = make_payment("idem-2")
    winner = make_payment("idem-2")
    session = FakeSession(lookup_result=winner, commit_error=make_integrity_error())
    repository = PaymentRepository(session)

    returned = await repository.create_with_outbox_event(payment, make_outbox_message(payment))

    assert returned is winner
    assert session.rollbacks == 1


async def test_create_with_outbox_event_reraises_unrelated_integrity_error() -> None:
    payment = make_payment("idem-3")
    session = FakeSession(lookup_result=None, commit_error=make_integrity_error())
    repository = PaymentRepository(session)

    with pytest.raises(IntegrityError):
        await repository.create_with_outbox_event(payment, make_outbox_message(payment))

    assert session.rollbacks == 1


async def test_mark_processed_transitions_pending_payment() -> None:
    payment = make_payment("idem-4")
    session = FakeSession(stored=payment)
    repository = PaymentRepository(session)
    processed_at = datetime.now(UTC)

    returned, transitioned = await repository.mark_processed(
        payment.id, PaymentStatus.SUCCEEDED, processed_at
    )

    assert transitioned is True
    assert returned.status == PaymentStatus.SUCCEEDED
    assert returned.processed_at == processed_at
    assert session.commits == 1


async def test_mark_processed_is_noop_for_already_terminal_payment() -> None:
    payment = make_payment("idem-5", status=PaymentStatus.SUCCEEDED)
    payment.processed_at = datetime(2026, 1, 1, tzinfo=UTC)
    session = FakeSession(stored=payment)
    repository = PaymentRepository(session)

    returned, transitioned = await repository.mark_processed(
        payment.id, PaymentStatus.FAILED, datetime.now(UTC)
    )

    assert transitioned is False
    assert returned.status == PaymentStatus.SUCCEEDED
    assert returned.processed_at == datetime(2026, 1, 1, tzinfo=UTC)
    assert session.commits == 0


async def test_mark_processed_raises_for_unknown_payment() -> None:
    session = FakeSession(stored=None)
    repository = PaymentRepository(session)

    with pytest.raises(PaymentNotFoundError):
        await repository.mark_processed(uuid.uuid4(), PaymentStatus.SUCCEEDED, datetime.now(UTC))
