from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.outbox import OutboxMessage
from app.models.payment import Payment, PaymentStatus


class PaymentNotFoundError(Exception):
    def __init__(self, payment_id: uuid.UUID) -> None:
        super().__init__(f"payment {payment_id} not found")
        self.payment_id = payment_id


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        result = await self._session.execute(select(Payment).where(Payment.idempotency_key == key))
        return result.scalar_one_or_none()

    async def get_by_id(self, payment_id: uuid.UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def create_with_outbox_event(
        self, payment: Payment, outbox_message: OutboxMessage
    ) -> Payment:
        try:
            self._session.add(payment)
            await self._session.flush()
            self._session.add(outbox_message)
            await self._session.commit()
        except IntegrityError:
            await self._session.rollback()
            winner = await self.get_by_idempotency_key(payment.idempotency_key)
            if winner is None:
                raise
            return winner
        return payment

    async def mark_processed(
        self, payment_id: uuid.UUID, status: PaymentStatus, processed_at: datetime
    ) -> tuple[Payment, bool]:
        payment = await self._session.get(Payment, payment_id, with_for_update=True)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        if payment.status != PaymentStatus.PENDING:
            return payment, False
        payment.status = status
        payment.processed_at = processed_at
        await self._session.commit()
        return payment, True


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def fetch_unpublished(self, limit: int) -> list[OutboxMessage]:
        result = await self._session.execute(
            select(OutboxMessage)
            .where(OutboxMessage.published_at.is_(None))
            .order_by(OutboxMessage.created_at)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return list(result.scalars())
