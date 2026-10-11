from __future__ import annotations

import uuid
from datetime import UTC, datetime

from app.config import Settings
from app.messaging.webhook_destination import validate_webhook_destination
from app.models.outbox import OutboxMessage
from app.models.payment import Payment, PaymentStatus
from app.repositories.payment_repository import PaymentRepository
from app.schemas.payment import PaymentCreateRequest


class IdempotencyKeyConflictError(Exception):
    def __init__(self, idempotency_key: str) -> None:
        super().__init__(
            f"idempotency key {idempotency_key!r} was already used with a different request body"
        )
        self.idempotency_key = idempotency_key


def _request_matches_payment(request: PaymentCreateRequest, payment: Payment) -> bool:
    return (
        request.amount == payment.amount
        and request.currency == payment.currency
        and request.description == payment.description
        and request.metadata == payment.metadata_
        and str(request.webhook_url) == payment.webhook_url
    )


class PaymentService:
    def __init__(self, repository: PaymentRepository, settings: Settings) -> None:
        self._repository = repository
        self._settings = settings

    async def create_payment(self, request: PaymentCreateRequest, idempotency_key: str) -> Payment:
        await validate_webhook_destination(
            str(request.webhook_url),
            allow_private_destinations=self._settings.webhook_allow_private_destinations,
        )

        existing = await self._repository.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return self._reconcile_replay(request, existing, idempotency_key)

        now = datetime.now(UTC)
        payment_id = uuid.uuid4()
        payment = Payment(
            id=payment_id,
            amount=request.amount,
            currency=request.currency,
            description=request.description,
            metadata_=request.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            webhook_url=str(request.webhook_url),
            created_at=now,
        )
        outbox_message = OutboxMessage(
            id=uuid.uuid4(),
            payment_id=payment_id,
            routing_key="payments.new",
            payload={"payment_id": str(payment_id)},
            created_at=now,
        )
        created = await self._repository.create_with_outbox_event(payment, outbox_message)
        if created.id != payment_id:
            return self._reconcile_replay(request, created, idempotency_key)
        return created

    async def get_payment(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._repository.get_by_id(payment_id)

    @staticmethod
    def _reconcile_replay(
        request: PaymentCreateRequest, existing: Payment, idempotency_key: str
    ) -> Payment:
        if not _request_matches_payment(request, existing):
            raise IdempotencyKeyConflictError(idempotency_key)
        return existing
