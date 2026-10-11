from __future__ import annotations

import os
from collections.abc import AsyncIterator

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.config import Settings
from app.db.session import create_engine, create_session_factory
from tests.integration.environment import (
    DEFAULT_TEST_DATABASE_URL,
    DEFAULT_TEST_RABBITMQ_MANAGEMENT_URL,
    DEFAULT_TEST_RABBITMQ_URL,
    apply_database_migrations,
    assert_test_database,
    assert_test_rabbitmq_vhost,
    ensure_database_exists,
    ensure_rabbitmq_vhost_exists,
)


def integration_env() -> dict[str, str]:
    env = dict(os.environ)
    env["DATABASE_URL"] = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    env["RABBITMQ_URL"] = os.environ.get("TEST_RABBITMQ_URL", DEFAULT_TEST_RABBITMQ_URL)
    env.setdefault("API_KEY", "local-dev-api-key")
    env.setdefault("PROCESSING_MIN_SECONDS", "0")
    env.setdefault("PROCESSING_MAX_SECONDS", "0")
    return env


def integration_settings() -> Settings:
    return Settings.from_env(integration_env())


@pytest.fixture(autouse=True)
async def test_environment() -> None:
    settings = integration_settings()
    assert_test_database(settings.database_url)
    assert_test_rabbitmq_vhost(settings.rabbitmq_url)
    await ensure_database_exists(settings.database_url)
    await apply_database_migrations(settings.database_url, settings.rabbitmq_url, settings.api_key)
    management_url = os.environ.get(
        "TEST_RABBITMQ_MANAGEMENT_URL", DEFAULT_TEST_RABBITMQ_MANAGEMENT_URL
    )
    await ensure_rabbitmq_vhost_exists(settings.rabbitmq_url, management_url)


@pytest.fixture
async def session_factory() -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    settings = integration_settings()
    assert_test_database(settings.database_url)
    engine = create_engine(settings.database_url)
    factory = create_session_factory(engine)
    yield factory
    assert_test_database(settings.database_url)
    async with engine.begin() as connection:
        await connection.execute(
            text("TRUNCATE outbox_messages, payments RESTART IDENTITY CASCADE")
        )
    await engine.dispose()
