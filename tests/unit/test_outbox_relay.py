from __future__ import annotations

import asyncio
import uuid
from datetime import datetime

import pytest
from sqlalchemy.exc import OperationalError

from app.messaging.outbox_relay import OutboxRelay


class FakeOutboxMessage:
    def __init__(self, payload: dict[str, object], routing_key: str) -> None:
        self.id = uuid.uuid4()
        self.payload = payload
        self.routing_key = routing_key
        self.published_at: datetime | None = None


class FakeOutboxRepository:
    def __init__(self, messages: list[FakeOutboxMessage]) -> None:
        self._messages = messages

    async def fetch_unpublished(self, limit: int) -> list[FakeOutboxMessage]:
        return [m for m in self._messages if m.published_at is None][:limit]


class FlakyOutboxRepository:
    def __init__(self, messages: list[FakeOutboxMessage]) -> None:
        self._messages = messages
        self.calls = 0

    async def fetch_unpublished(self, limit: int) -> list[FakeOutboxMessage]:
        self.calls += 1
        if self.calls == 1:
            raise OperationalError("SELECT 1", (), Exception("connection lost"))
        return [m for m in self._messages if m.published_at is None][:limit]


class FakeSession:
    def __init__(self, repository: FakeOutboxRepository) -> None:
        self.repository = repository
        self.committed = False

    def begin(self) -> FakeSession:
        return self

    async def __aenter__(self) -> FakeSession:
        return self

    async def __aexit__(self, *_args: object) -> None:
        self.committed = True


class FakeBroker:
    def __init__(self) -> None:
        self.published: list[dict[str, object]] = []

    async def publish(self, payload: dict[str, object], **kwargs: object) -> None:
        self.published.append({"payload": payload, **kwargs})


class FakeSettings:
    outbox_poll_interval_seconds = 0.01
    outbox_batch_size = 10


async def test_publish_pending_batch_publishes_and_marks_published() -> None:
    message = FakeOutboxMessage({"payment_id": "abc"}, "payments.new")
    repository = FakeOutboxRepository([message])
    session = FakeSession(repository)
    broker = FakeBroker()

    relay = OutboxRelay(
        session_factory=lambda: session,
        broker=broker,
        settings=FakeSettings(),
        outbox_repository_factory=lambda _session: repository,
    )
    await relay._publish_pending_batch()

    assert len(broker.published) == 1
    assert broker.published[0]["payload"] == {"payment_id": "abc"}
    assert broker.published[0]["persist"] is True
    assert message.published_at is not None


async def test_run_logs_and_keeps_polling_after_a_failed_poll(
    caplog: pytest.LogCaptureFixture,
) -> None:
    message = FakeOutboxMessage({"payment_id": "abc"}, "payments.new")
    repository = FlakyOutboxRepository([message])
    session = FakeSession(repository)
    broker = FakeBroker()

    relay = OutboxRelay(
        session_factory=lambda: session,
        broker=broker,
        settings=FakeSettings(),
        outbox_repository_factory=lambda _session: repository,
    )
    with caplog.at_level("ERROR"):
        await relay.start()
        for _ in range(200):
            if message.published_at is not None:
                break
            await asyncio.sleep(0.01)
        await relay.stop()

    assert repository.calls >= 2
    assert message.published_at is not None
    assert len(broker.published) == 1
    assert "outbox relay poll failed" in caplog.text
