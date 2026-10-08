from __future__ import annotations

import asyncio

import httpx

from app.config import Settings


class WebhookDeliveryError(Exception):
    pass


class WebhookClient:
    def __init__(self, settings: Settings, client: httpx.AsyncClient) -> None:
        self._settings = settings
        self._client = client

    async def send(self, webhook_url: str, payload: dict[str, object]) -> None:
        attempt = 0
        while True:
            attempt += 1
            try:
                response = await self._client.post(webhook_url, json=payload, timeout=10)
                response.raise_for_status()
                return
            except (httpx.HTTPStatusError, httpx.TransportError) as error:
                if attempt >= self._settings.webhook_max_attempts:
                    raise WebhookDeliveryError(
                        f"webhook delivery to {webhook_url} failed after {attempt} attempts"
                    ) from error
                delay = self._settings.webhook_retry_base_delay_seconds * (2 ** (attempt - 1))
                await asyncio.sleep(delay)
