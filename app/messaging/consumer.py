from __future__ import annotations

import asyncio
import logging
import random
import uuid
from datetime import UTC, datetime
from typing import Protocol

from aio_pika.exceptions import PublishError
from sqlalchemy.exc import SQLAlchemyError

from app.config import Settings
from app.messaging.broker import DLX_EXCHANGE, retry_tier_exchange
from app.messaging.webhook_client import WebhookDeliveryError
from app.models.payment import Payment, PaymentStatus
from app.repositories.payment_repository import PaymentNotFoundError

logger = logging.getLogger("payment_processing_service.consumer")

_RETRY_COUNT_HEADER = "x-retry-count"
_PAYMENTS_ROUTING_KEY = "payments.new"


class _RepositoryLike(Protocol):
    async def mark_processed(
        self, payment_id: uuid.UUID, status: PaymentStatus, processed_at: datetime
    ) -> tuple[Payment, bool]: ...


class _WebhookClientLike(Protocol):
    async def send(self, webhook_url: str, payload: dict[str, object]) -> None: ...


class RetryPublisher(Protocol):
    async def publish_retry(
        self, payload: dict[str, object], headers: dict[str, object], retry_count: int
    ) -> None: ...

    async def publish_dead_letter(
        self, payload: dict[str, object], headers: dict[str, object]
    ) -> None: ...


class _BrokerLike(Protocol):
    async def publish(
        self,
        message: dict[str, object],
        *,
        exchange: object,
        routing_key: str,
        headers: dict[str, object],
        mandatory: bool = True,
        persist: bool = False,
    ) -> object: ...


def next_retry_count(headers: dict[str, object]) -> int:
    return int(headers.get(_RETRY_COUNT_HEADER, 0))


def build_webhook_payload(payment: Payment) -> dict[str, object]:
    return {
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "processed_at": payment.processed_at.isoformat(),
    }


async def simulate_gateway_and_update(
    repository: _RepositoryLike, payment_id: uuid.UUID, settings: Settings
) -> tuple[Payment, bool]:
    await asyncio.sleep(
        random.uniform(settings.processing_min_seconds, settings.processing_max_seconds)
    )
    status = (
        PaymentStatus.SUCCEEDED
        if random.random() >= settings.processing_failure_rate
        else PaymentStatus.FAILED
    )
    return await repository.mark_processed(payment_id, status, datetime.now(UTC))


async def handle_payment_message(
    payload: dict[str, object],
    headers: dict[str, object],
    repository: _RepositoryLike,
    webhook_client: _WebhookClientLike,
    retry_publisher: RetryPublisher,
    settings: Settings,
) -> None:
    payment_id = uuid.UUID(str(payload["payment_id"]))
    retry_count = next_retry_count(headers)

    try:
        payment, transitioned = await simulate_gateway_and_update(repository, payment_id, settings)
    except (PaymentNotFoundError, SQLAlchemyError):
        next_headers = {**headers, _RETRY_COUNT_HEADER: retry_count + 1}
        if retry_count + 1 >= settings.message_max_attempts:
            logger.error(
                "payment %s processing failed after %d attempts, dead-lettering",
                payment_id,
                retry_count + 1,
            )
            await retry_publisher.publish_dead_letter(payload, next_headers)
        else:
            delay_ms = settings.message_retry_base_delay_ms * (2**retry_count)
            logger.warning(
                "payment %s processing failed (attempt %d), requeueing to retry tier %d "
                "with %dms delay",
                payment_id,
                retry_count + 1,
                retry_count + 1,
                delay_ms,
            )
            await retry_publisher.publish_retry(payload, next_headers, retry_count)
        return

    if not transitioned:
        logger.info(
            "payment %s is already in terminal status %s, skipping webhook for redelivered message",
            payment_id,
            payment.status.value,
        )
        return

    try:
        await webhook_client.send(payment.webhook_url, build_webhook_payload(payment))
    except WebhookDeliveryError:
        logger.error("webhook delivery failed for payment %s", payment_id)


class BrokerRetryPublisher:
    def __init__(self, broker: _BrokerLike) -> None:
        self._broker = broker

    async def publish_retry(
        self, payload: dict[str, object], headers: dict[str, object], retry_count: int
    ) -> None:
        tier = retry_count + 1
        try:
            await self._broker.publish(
                payload,
                exchange=retry_tier_exchange(tier),
                routing_key=_PAYMENTS_ROUTING_KEY,
                headers=headers,
                mandatory=True,
                persist=True,
            )
        except PublishError:
            logger.error(
                "retry publish for payment %s was not routed to any queue (tier %d)",
                payload.get("payment_id"),
                tier,
            )
            raise

    async def publish_dead_letter(
        self, payload: dict[str, object], headers: dict[str, object]
    ) -> None:
        try:
            await self._broker.publish(
                payload,
                exchange=DLX_EXCHANGE,
                routing_key=_PAYMENTS_ROUTING_KEY,
                headers=headers,
                mandatory=True,
                persist=True,
            )
        except PublishError:
            logger.error(
                "dead-letter publish for payment %s was not routed to any queue",
                payload.get("payment_id"),
            )
            raise
