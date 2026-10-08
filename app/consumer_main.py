from __future__ import annotations

import asyncio

import httpx
from faststream import FastStream
from faststream.rabbit.annotations import RabbitMessage

from app.config import Settings
from app.db.session import create_engine, create_session_factory
from app.messaging.broker import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_QUEUE,
    create_broker,
    declare_retry_topology,
)
from app.messaging.consumer import BrokerRetryPublisher, handle_payment_message
from app.messaging.webhook_client import WebhookClient
from app.repositories.payment_repository import PaymentRepository


def main() -> None:
    settings = Settings.from_env()
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    broker = create_broker(settings.rabbitmq_url)
    retry_publisher = BrokerRetryPublisher(broker)
    http_client = httpx.AsyncClient()
    webhook_client = WebhookClient(settings, http_client)

    @broker.subscriber(PAYMENTS_QUEUE, PAYMENTS_EXCHANGE)
    async def on_payment_new(payload: dict[str, object], message: RabbitMessage) -> None:
        async with session_factory() as session:
            repository = PaymentRepository(session)
            await handle_payment_message(
                payload,
                dict(message.headers),
                repository,
                webhook_client,
                retry_publisher,
                settings,
            )

    async def _declare_retry_topology() -> None:
        await declare_retry_topology(broker)

    app = FastStream(broker)
    app.after_startup(_declare_retry_topology)
    app.after_shutdown(http_client.aclose)
    app.after_shutdown(engine.dispose)

    asyncio.run(app.run())


if __name__ == "__main__":
    main()
