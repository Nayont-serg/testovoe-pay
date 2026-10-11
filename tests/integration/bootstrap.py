from __future__ import annotations

import asyncio
import os

from tests.integration.environment import (
    DEFAULT_TEST_DATABASE_URL,
    DEFAULT_TEST_RABBITMQ_MANAGEMENT_URL,
    DEFAULT_TEST_RABBITMQ_URL,
    apply_database_migrations,
    ensure_database_exists,
    ensure_rabbitmq_vhost_exists,
)


async def main() -> None:
    database_url = os.environ.get("TEST_DATABASE_URL", DEFAULT_TEST_DATABASE_URL)
    rabbitmq_url = os.environ.get("TEST_RABBITMQ_URL", DEFAULT_TEST_RABBITMQ_URL)
    management_url = os.environ.get(
        "TEST_RABBITMQ_MANAGEMENT_URL", DEFAULT_TEST_RABBITMQ_MANAGEMENT_URL
    )
    api_key = os.environ.get("API_KEY", "local-dev-api-key")
    await ensure_database_exists(database_url)
    await apply_database_migrations(database_url, rabbitmq_url, api_key)
    await ensure_rabbitmq_vhost_exists(rabbitmq_url, management_url)


if __name__ == "__main__":
    asyncio.run(main())
