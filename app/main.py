from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.health import router as health_router
from app.api.payments import router as payments_router
from app.config import Settings, configure_logging
from app.db.session import create_engine, create_session_factory
from app.messaging.broker import create_broker, declare_payments_topology
from app.messaging.outbox_relay import OutboxRelay


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = Settings.from_env()
    configure_logging(settings.log_level)
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    broker = create_broker(settings.rabbitmq_url)

    app.state.settings = settings
    app.state.session_factory = session_factory

    await broker.start()
    relay = OutboxRelay(
        session_factory,
        broker,
        settings,
        declare_topology=lambda: declare_payments_topology(broker),
    )
    await relay.start()
    try:
        yield
    finally:
        await relay.stop()
        await broker.stop()
        await engine.dispose()


def create_app() -> FastAPI:
    app = FastAPI(title="Payment Processing Service", lifespan=lifespan)
    app.include_router(health_router)
    app.include_router(payments_router)
    return app


app = create_app()
