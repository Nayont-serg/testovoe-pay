from __future__ import annotations

import uuid
from datetime import UTC, datetime

import aiormq
import pytest
from aio_pika.exceptions import PublishError
from aiormq.abc import DeliveredMessage

from app.config import Settings
from app.messaging.broker import DLX_EXCHANGE, retry_tier_exchange
from app.messaging.consumer import (
    BrokerRetryPublisher,
    build_webhook_payload,
    handle_payment_message,
    next_retry_count,
)
from app.messaging.webhook_client import WebhookDeliveryError
from app.models.payment import Currency, Payment, PaymentStatus
from app.repositories.payment_repository import PaymentNotFoundError


def make_publish_error(routing_key: str) -> PublishError:
    delivery = aiormq.spec.Basic.Return(
        reply_code=312, reply_text="NO_ROUTE", routing_key=routing_key
    )
    message = DeliveredMessage(delivery=delivery, header=None, body=b"", channel=None)
    return PublishError(message, delivery)


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
    ) -> tuple[Payment, bool]:
        if self._payment.status != PaymentStatus.PENDING:
            return self._payment, False
        self._payment.status = status
        self._payment.processed_at = processed_at
        return self._payment, True


class RaisingRepository:
    async def mark_processed(
        self, payment_id: uuid.UUID, _status: PaymentStatus, _processed_at: datetime
    ) -> tuple[Payment, bool]:
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
        self, payload: dict[str, object], headers: dict[str, object], retry_count: int
    ) -> None:
        self.retries.append((payload, headers, retry_count))

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
    _payload, headers, retry_count = retry_publisher.retries[0]
    assert headers["x-retry-count"] == 2
    assert retry_count == 1
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


async def test_handle_payment_message_skips_webhook_when_payment_already_terminal() -> None:
    payment_id = uuid.uuid4()
    payment = make_payment(payment_id)
    payment.status = PaymentStatus.SUCCEEDED
    payment.processed_at = datetime(2026, 1, 1, tzinfo=UTC)
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

    assert payment.status == PaymentStatus.SUCCEEDED
    assert payment.processed_at == datetime(2026, 1, 1, tzinfo=UTC)
    assert not webhook_client.sent
    assert not retry_publisher.retries
    assert not retry_publisher.dead_letters


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


class RecordingPublishBroker:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def publish(self, message: dict[str, object], **kwargs: object) -> None:
        self.calls.append({"message": message, **kwargs})


class UnroutableBroker:
    async def publish(self, _message: dict[str, object], **kwargs: object) -> None:
        raise make_publish_error(str(kwargs.get("routing_key")))


async def test_broker_retry_publisher_publishes_to_the_tier_matching_retry_count() -> None:
    broker = RecordingPublishBroker()
    publisher = BrokerRetryPublisher(broker)

    await publisher.publish_retry({"payment_id": "p1"}, {"x-retry-count": 1}, retry_count=0)

    assert len(broker.calls) == 1
    assert broker.calls[0]["exchange"] == retry_tier_exchange(1)
    assert broker.calls[0]["routing_key"] == "payments.new"
    assert broker.calls[0]["mandatory"] is True


async def test_broker_retry_publisher_selects_the_second_tier_for_the_second_retry() -> None:
    broker = RecordingPublishBroker()
    publisher = BrokerRetryPublisher(broker)

    await publisher.publish_retry({"payment_id": "p1"}, {"x-retry-count": 2}, retry_count=1)

    assert broker.calls[0]["exchange"] == retry_tier_exchange(2)


async def test_broker_retry_publisher_publishes_dead_letter_to_the_dlx() -> None:
    broker = RecordingPublishBroker()
    publisher = BrokerRetryPublisher(broker)

    await publisher.publish_dead_letter({"payment_id": "p1"}, {})

    assert broker.calls[0]["exchange"] == DLX_EXCHANGE
    assert broker.calls[0]["mandatory"] is True


async def test_broker_retry_publisher_reraises_and_logs_on_unroutable_retry(
    caplog: pytest.LogCaptureFixture,
) -> None:
    publisher = BrokerRetryPublisher(UnroutableBroker())

    with caplog.at_level("ERROR"), pytest.raises(PublishError):
        await publisher.publish_retry({"payment_id": "p1"}, {}, retry_count=0)

    assert "p1" in caplog.text


async def test_broker_retry_publisher_reraises_and_logs_on_unroutable_dead_letter(
    caplog: pytest.LogCaptureFixture,
) -> None:
    publisher = BrokerRetryPublisher(UnroutableBroker())

    with caplog.at_level("ERROR"), pytest.raises(PublishError):
        await publisher.publish_dead_letter({"payment_id": "p1"}, {})

    assert "p1" in caplog.text
