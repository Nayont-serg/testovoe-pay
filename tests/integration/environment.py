from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path
from urllib.parse import urlsplit

import asyncpg
import httpx
from yarl import URL

TEST_DATABASE_SUFFIX = "_test"
TEST_RABBITMQ_VHOST = "test"

DEFAULT_TEST_DATABASE_URL = "postgresql+asyncpg://payments:payments@localhost:5433/payments_test"
DEFAULT_TEST_RABBITMQ_URL = f"amqp://payments:payments@localhost:5673/{TEST_RABBITMQ_VHOST}"
DEFAULT_TEST_RABBITMQ_MANAGEMENT_URL = "http://localhost:15673"

PROJECT_ROOT = Path(__file__).resolve().parents[2]


class UnsafeTestEnvironmentError(RuntimeError):
    pass


def resolve_database_name(database_url: str) -> str:
    return urlsplit(database_url).path.lstrip("/")


def resolve_rabbitmq_vhost(rabbitmq_url: str) -> str:
    path = URL(rabbitmq_url).path
    return "/" if path in ("", "/") else path[1:]


def assert_test_database(database_url: str) -> None:
    database_name = resolve_database_name(database_url)
    if not database_name.endswith(TEST_DATABASE_SUFFIX):
        raise UnsafeTestEnvironmentError(
            f"refusing destructive integration test setup: database {database_name!r} "
            f"does not end with {TEST_DATABASE_SUFFIX!r}; point TEST_DATABASE_URL at a "
            "dedicated *_test database"
        )


def assert_test_rabbitmq_vhost(rabbitmq_url: str) -> None:
    vhost = resolve_rabbitmq_vhost(rabbitmq_url)
    if vhost != TEST_RABBITMQ_VHOST:
        raise UnsafeTestEnvironmentError(
            f"refusing destructive integration test setup: RabbitMQ vhost {vhost!r} is not "
            f"the dedicated test vhost {TEST_RABBITMQ_VHOST!r}; point TEST_RABBITMQ_URL at it"
        )


def _as_asyncpg_dsn(database_url: str) -> str:
    return database_url.replace("postgresql+asyncpg://", "postgresql://", 1)


async def ensure_database_exists(database_url: str) -> None:
    assert_test_database(database_url)
    database_name = resolve_database_name(database_url)
    admin_dsn = _as_asyncpg_dsn(database_url).replace(f"/{database_name}", "/postgres", 1)
    connection = await asyncpg.connect(admin_dsn)
    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1", database_name
        )
        if not exists:
            await connection.execute(f'CREATE DATABASE "{database_name}"')
    finally:
        await connection.close()


async def apply_database_migrations(database_url: str, rabbitmq_url: str, api_key: str) -> None:
    assert_test_database(database_url)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "upgrade",
        "head",
        cwd=PROJECT_ROOT,
        env=os.environ
        | {"DATABASE_URL": database_url, "RABBITMQ_URL": rabbitmq_url, "API_KEY": api_key},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await process.communicate()
    if process.returncode != 0:
        raise RuntimeError(
            f"alembic upgrade head against {database_url!r} failed: {stderr.decode()}"
        )


async def ensure_rabbitmq_vhost_exists(rabbitmq_url: str, management_url: str) -> None:
    assert_test_rabbitmq_vhost(rabbitmq_url)
    vhost = resolve_rabbitmq_vhost(rabbitmq_url)
    parsed = urlsplit(rabbitmq_url)
    async with httpx.AsyncClient(
        auth=(parsed.username or "", parsed.password or ""), timeout=10.0
    ) as client:
        vhost_response = await client.put(f"{management_url}/api/vhosts/{vhost}")
        vhost_response.raise_for_status()
        permission_response = await client.put(
            f"{management_url}/api/permissions/{vhost}/{parsed.username}",
            json={"configure": ".*", "write": ".*", "read": ".*"},
        )
        permission_response.raise_for_status()
