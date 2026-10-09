from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.db.session import create_engine, create_session_factory


def integration_settings() -> Settings:
    env = dict(os.environ)
    env.setdefault(
        "DATABASE_URL",
        "postgresql+asyncpg://payments:payments@localhost:5433/payments",
    )
    env.setdefault("RABBITMQ_URL", "amqp://payments:payments@localhost:5673/")
    env.setdefault("API_KEY", "local-dev-api-key")
    env.setdefault("PROCESSING_MIN_SECONDS", "0")
    env.setdefault("PROCESSING_MAX_SECONDS", "0")
    return Settings.from_env(env)


@pytest.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    settings = integration_settings()
    engine = create_engine(settings.database_url)
    factory = create_session_factory(engine)
    yield factory
    async with engine.begin() as connection:
        await connection.execute(
            text("TRUNCATE outbox_messages, payments RESTART IDENTITY CASCADE")
        )
    await engine.dispose()
