from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

from app.config import Settings
from app.messaging.consumer import (
    build_webhook_payload,
    handle_payment_message,
    next_retry_count,
)
from app.messaging.webhook_client import WebhookDeliveryError
from app.models.payment import Currency, Payment, PaymentStatus
from app.repositories.payment_repository import PaymentNotFoundError


def make_settings(**overrides: str) -> Settings:
    base = {
        "DATABASE_URL": "postgresql+asyncpg://u:p@localhost/db",
        "RABBITMQ_URL": "amqp://u:p@localhost/",
        "API_KEY": "key",
        "MESSAGE_MAX_ATTEMPTS": "3",
        "PROCESSING_MIN_SECONDS": "0",
        "PROCESSING_MAX_SECONDS": "0",
    }
    return Settings.from_env({**base, **overrides})


def make_payment(payment_id: uuid.UUID) -> Payment:
    return Payment(
        id=payment_id,
        amount=10,
        currency=Currency.USD,
        description="order",
        metadata_={},
        status=PaymentStatus.PENDING,
        idempotency_key=str(payment_id),
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
    )


class SucceedingRepository:
    def __init__(self, payment: Payment) -> None:
        self._payment = payment

    async def mark_processed(
        self, _payment_id: uuid.UUID, status: PaymentStatus, processed_at: datetime
    ) -> Payment:
        self._payment.status = status
        self._payment.processed_at = processed_at
        return self._payment


class RaisingRepository:
    async def mark_processed(
        self, payment_id: uuid.UUID, _status: PaymentStatus, _processed_at: datetime
    ) -> Payment:
        raise PaymentNotFoundError(payment_id)


class RecordingWebhookClient:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, object]]] = []

    async def send(self, webhook_url: str, payload: dict[str, object]) -> None:
        self.sent.append((webhook_url, payload))


class FailingWebhookClient:
    async def send(self, _webhook_url: str, _payload: dict[str, object]) -> None:
        raise WebhookDeliveryError("boom")


class RecordingRetryPublisher:
    def __init__(self) -> None:
        self.retries: list[tuple[dict[str, object], dict[str, object], int]] = []
        self.dead_letters: list[tuple[dict[str, object], dict[str, object]]] = []

    async def publish_retry(
        self, payload: dict[str, object], headers: dict[str, object], delay_ms: int
    ) -> None:
        self.retries.append((payload, headers, delay_ms))

    async def publish_dead_letter(
        self, payload: dict[str, object], headers: dict[str, object]
    ) -> None:
        self.dead_letters.append((payload, headers))


def test_next_retry_count_defaults_to_zero() -> None:
    assert next_retry_count({}) == 0
    assert next_retry_count({"x-retry-count": 2}) == 2


def test_build_webhook_payload_serializes_processed_at() -> None:
    payment = make_payment(uuid.uuid4())
    payment.status = PaymentStatus.SUCCEEDED
    payment.processed_at = datetime(2026, 1, 1, tzinfo=UTC)
    payload = build_webhook_payload(payment)
    assert payload == {
        "payment_id": str(payment.id),
        "status": "succeeded",
        "processed_at": "2026-01-01T00:00:00+00:00",
    }


async def test_handle_payment_message_sends_webhook_on_successful_processing() -> None:
    payment_id = uuid.uuid4()
    payment = make_payment(payment_id)
    webhook_client = RecordingWebhookClient()
    retry_publisher = RecordingRetryPublisher()

    await handle_payment_message(
        payload={"payment_id": str(payment_id)},
        headers={},
        repository=SucceedingRepository(payment),
        webhook_client=webhook_client,
        retry_publisher=retry_publisher,
        settings=make_settings(),
    )

    assert len(webhook_client.sent) == 1
    assert not retry_publisher.retries
    assert not retry_publisher.dead_letters


async def test_handle_payment_message_sends_webhook_on_simulated_business_failure() -> None:
    payment_id = uuid.uuid4()
    payment = make_payment(payment_id)
    webhook_client = RecordingWebhookClient()
    retry_publisher = RecordingRetryPublisher()

    await handle_payment_message(
        payload={"payment_id": str(payment_id)},
        headers={},
        repository=SucceedingRepository(payment),
        webhook_client=webhook_client,
        retry_publisher=retry_publisher,
        settings=make_settings(PROCESSING_FAILURE_RATE="1"),
    )

    assert payment.status == PaymentStatus.FAILED
    assert len(webhook_client.sent) == 1
    assert webhook_client.sent[0][1]["status"] == "failed"
    assert not retry_publisher.retries
    assert not retry_publisher.dead_letters


async def test_handle_payment_message_requeues_on_processing_error_below_max_attempts() -> None:
    payment_id = uuid.uuid4()
    retry_publisher = RecordingRetryPublisher()

    await handle_payment_message(
        payload={"payment_id": str(payment_id)},
        headers={"x-retry-count": 1},
        repository=RaisingRepository(),
        webhook_client=RecordingWebhookClient(),
        retry_publisher=retry_publisher,
        settings=make_settings(MESSAGE_MAX_ATTEMPTS="3"),
    )

    assert len(retry_publisher.retries) == 1
    _payload, headers, delay_ms = retry_publisher.retries[0]
    assert headers["x-retry-count"] == 2
    assert delay_ms == 2000 * (2**1)
    assert not retry_publisher.dead_letters


async def test_handle_payment_message_dead_letters_after_max_attempts() -> None:
    payment_id = uuid.uuid4()
    retry_publisher = RecordingRetryPublisher()

    await handle_payment_message(
        payload={"payment_id": str(payment_id)},
        headers={"x-retry-count": 2},
        repository=RaisingRepository(),
        webhook_client=RecordingWebhookClient(),
        retry_publisher=retry_publisher,
        settings=make_settings(MESSAGE_MAX_ATTEMPTS="3"),
    )

    assert len(retry_publisher.dead_letters) == 1
    assert not retry_publisher.retries


async def test_handle_payment_message_logs_and_continues_on_webhook_failure(
    caplog: pytest.LogCaptureFixture,
) -> None:
    payment_id = uuid.uuid4()
    payment = make_payment(payment_id)

    with caplog.at_level("ERROR"):
        await handle_payment_message(
            payload={"payment_id": str(payment_id)},
            headers={},
            repository=SucceedingRepository(payment),
            webhook_client=FailingWebhookClient(),
            retry_publisher=RecordingRetryPublisher(),
            settings=make_settings(),
        )

    assert "webhook" in caplog.text.lower()
