from __future__ import annotations

import httpx
import pytest

from app.config import Settings
from app.messaging.webhook_client import WebhookClient, WebhookDeliveryError


def make_settings(**overrides: object) -> Settings:
    base = {
        "DATABASE_URL": "postgresql+asyncpg://u:p@localhost/db",
        "RABBITMQ_URL": "amqp://u:p@localhost/",
        "API_KEY": "key",
        "WEBHOOK_MAX_ATTEMPTS": "3",
        "WEBHOOK_RETRY_BASE_DELAY_SECONDS": "0",
    }
    return Settings.from_env({**base, **{k: str(v) for k, v in overrides.items()}})


async def test_send_succeeds_on_first_try() -> None:
    calls = []

    async def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = WebhookClient(make_settings(), http_client)
        await client.send("https://example.com/hook", {"payment_id": "abc"})

    assert len(calls) == 1


async def test_send_retries_then_succeeds() -> None:
    attempts = {"count": 0}

    async def handler(_request: httpx.Request) -> httpx.Response:
        attempts["count"] += 1
        if attempts["count"] < 2:
            return httpx.Response(500)
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = WebhookClient(make_settings(), http_client)
        await client.send("https://example.com/hook", {"payment_id": "abc"})

    assert attempts["count"] == 2


async def test_send_raises_after_exhausting_attempts() -> None:
    async def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as http_client:
        client = WebhookClient(make_settings(WEBHOOK_MAX_ATTEMPTS=2), http_client)
        with pytest.raises(WebhookDeliveryError):
            await client.send("https://example.com/hook", {"payment_id": "abc"})
