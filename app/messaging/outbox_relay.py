from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Protocol

from aio_pika.exceptions import AMQPError, ChannelInvalidStateError, PublishError
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.messaging.broker import PAYMENTS_EXCHANGE
from app.repositories.payment_repository import OutboxRepository

logger = logging.getLogger("payment_processing_service.outbox_relay")

RECOVERABLE_POLL_ERRORS = (SQLAlchemyError, AMQPError, ChannelInvalidStateError, OSError)


class _OutboxMessageLike(Protocol):
    id: object
    payload: dict[str, object]
    routing_key: str
    published_at: datetime | None


class _OutboxRepositoryLike(Protocol):
    async def fetch_unpublished(self, limit: int) -> list[_OutboxMessageLike]: ...


class _BrokerLike(Protocol):
    async def publish(self, payload: dict[str, object], **kwargs: object) -> None: ...


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession] | Callable[[], AsyncSession],
        broker: _BrokerLike,
        settings: Settings,
        declare_topology: Callable[[], Awaitable[None]],
        outbox_repository_factory: Callable[[AsyncSession], _OutboxRepositoryLike] = (
            OutboxRepository
        ),
    ) -> None:
        self._session_factory = session_factory
        self._broker = broker
        self._settings = settings
        self._declare_topology = declare_topology
        self._outbox_repository_factory = outbox_repository_factory
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        await self._declare_topology()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            await self._task

    async def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                await self._publish_pending_batch()
            except RECOVERABLE_POLL_ERRORS:
                logger.exception("outbox relay poll failed, retrying after poll interval")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self._settings.outbox_poll_interval_seconds
                )

    async def _publish_pending_batch(self) -> None:
        async with self._session_factory() as session, session.begin():
            repository = self._outbox_repository_factory(session)
            messages = await repository.fetch_unpublished(self._settings.outbox_batch_size)
            for message in messages:
                try:
                    await self._broker.publish(
                        message.payload,
                        exchange=PAYMENTS_EXCHANGE,
                        routing_key=message.routing_key,
                        mandatory=True,
                        persist=True,
                    )
                except PublishError:
                    logger.error(
                        "outbox message %s for routing key %s was not routed to any queue, "
                        "re-declaring payments topology and leaving it unpublished",
                        message.id,
                        message.routing_key,
                    )
                    await self._declare_topology()
                    continue
                message.published_at = datetime.now(UTC)
