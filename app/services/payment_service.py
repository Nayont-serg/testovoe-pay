from __future__ import annotations

import uuid
from datetime import UTC, datetime

from app.models.outbox import OutboxMessage
from app.models.payment import Payment, PaymentStatus
from app.repositories.payment_repository import PaymentRepository
from app.schemas.payment import PaymentCreateRequest


class PaymentService:
    def __init__(self, repository: PaymentRepository) -> None:
        self._repository = repository

    async def create_payment(self, request: PaymentCreateRequest, idempotency_key: str) -> Payment:
        existing = await self._repository.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            return existing

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
        await self._repository.create_with_outbox_event(payment, outbox_message)
        return payment

    async def get_payment(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._repository.get_by_id(payment_id)
