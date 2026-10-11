from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.payments import router as payments_router
from app.config import Settings
from app.models.outbox import OutboxMessage
from app.models.payment import Payment
from tests.integration.conftest import integration_settings

pytestmark = pytest.mark.integration

API_KEY_HEADER = "X-API-Key"


def build_app(session_factory: async_sessionmaker[AsyncSession], settings: Settings) -> FastAPI:
    app = FastAPI()
    app.include_router(payments_router)
    app.state.settings = settings
    app.state.session_factory = session_factory
    return app


@pytest.fixture
async def client(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncIterator[tuple[AsyncClient, Settings]]:
    settings = integration_settings()
    app = build_app(session_factory, settings)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as http_client:
        yield http_client, settings


def valid_payload(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "amount": "10.00",
        "currency": "USD",
        "description": "order",
        "metadata": {},
        "webhook_url": "https://example.com/hook",
    }
    payload.update(overrides)
    return payload


async def row_counts(session_factory: async_sessionmaker[AsyncSession]) -> tuple[int, int]:
    async with session_factory() as session:
        payment_count = await session.scalar(select(func.count()).select_from(Payment))
        outbox_count = await session.scalar(select(func.count()).select_from(OutboxMessage))
    return payment_count or 0, outbox_count or 0


class _RecordingHTTPServer(ThreadingHTTPServer):
    def __init__(
        self, server_address: tuple[str, int], handler_class: type[BaseHTTPRequestHandler]
    ) -> None:
        super().__init__(server_address, handler_class)
        self.received_paths: list[str] = []


class _RecordingWebhookHandler(BaseHTTPRequestHandler):
    server: _RecordingHTTPServer

    def do_POST(self) -> None:
        self.server.received_paths.append(self.path)
        self.send_response(200)
        self.end_headers()

    def log_message(self, *_args: object) -> None:
        return


@pytest.fixture
def local_webhook_receiver() -> AsyncIterator[tuple[str, _RecordingHTTPServer]]:
    server = _RecordingHTTPServer(("127.0.0.1", 0), _RecordingWebhookHandler)
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/hook", server
    finally:
        server.shutdown()
        thread.join()


async def test_subcent_amount_is_rejected_without_silent_rounding(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    before_payments, before_outbox = await row_counts(session_factory)

    response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": str(uuid.uuid4())},
        json=valid_payload(amount="0.001"),
    )

    assert response.status_code == 422
    assert await row_counts(session_factory) == (before_payments, before_outbox)


async def test_excess_decimal_amount_is_rejected_without_silent_rounding(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    before_payments, before_outbox = await row_counts(session_factory)

    response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": str(uuid.uuid4())},
        json=valid_payload(amount="10.999"),
    )

    assert response.status_code == 422
    assert await row_counts(session_factory) == (before_payments, before_outbox)


async def test_overflowing_amount_is_rejected_instead_of_crashing(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    before_payments, before_outbox = await row_counts(session_factory)

    response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": str(uuid.uuid4())},
        json=valid_payload(amount="10000000000000000"),
    )

    assert response.status_code == 422
    assert await row_counts(session_factory) == (before_payments, before_outbox)


async def test_private_webhook_destination_is_rejected_and_never_contacted(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
    local_webhook_receiver: tuple[str, _RecordingHTTPServer],
) -> None:
    http_client, settings = client
    webhook_url, receiver = local_webhook_receiver
    before_payments, before_outbox = await row_counts(session_factory)

    response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": str(uuid.uuid4())},
        json=valid_payload(webhook_url=webhook_url),
    )

    assert response.status_code == 422
    assert await row_counts(session_factory) == (before_payments, before_outbox)
    assert receiver.received_paths == []


async def test_empty_idempotency_key_is_rejected(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    before_payments, before_outbox = await row_counts(session_factory)

    response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": ""},
        json=valid_payload(),
    )

    assert response.status_code == 422
    assert await row_counts(session_factory) == (before_payments, before_outbox)


async def test_oversized_incompressible_idempotency_key_is_rejected_instead_of_crashing(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    before_payments, before_outbox = await row_counts(session_factory)
    incompressible_key = os.urandom(2048).hex()
    assert len(incompressible_key) == 4096

    response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": incompressible_key},
        json=valid_payload(),
    )

    assert response.status_code == 422
    assert await row_counts(session_factory) == (before_payments, before_outbox)


async def test_nul_byte_in_description_is_rejected_instead_of_crashing(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    before_payments, before_outbox = await row_counts(session_factory)

    response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": str(uuid.uuid4())},
        json=valid_payload(description="bad\x00description"),
    )

    assert response.status_code == 422
    assert await row_counts(session_factory) == (before_payments, before_outbox)


async def test_nul_byte_nested_in_metadata_value_is_rejected_instead_of_crashing(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    before_payments, before_outbox = await row_counts(session_factory)

    response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": str(uuid.uuid4())},
        json=valid_payload(
            metadata={"level1": {"level2": ["safe", "bad\x00deep"]}},
        ),
    )

    assert response.status_code == 422
    assert await row_counts(session_factory) == (before_payments, before_outbox)


async def test_nul_byte_in_metadata_key_is_rejected_instead_of_crashing(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    before_payments, before_outbox = await row_counts(session_factory)

    response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": str(uuid.uuid4())},
        json=valid_payload(metadata={"bad\x00key": "value"}),
    )

    assert response.status_code == 422
    assert await row_counts(session_factory) == (before_payments, before_outbox)


async def test_replay_with_different_body_returns_409_and_keeps_original_payment(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    idempotency_key = str(uuid.uuid4())

    first_response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": idempotency_key},
        json=valid_payload(amount="10.00", description="original"),
    )
    assert first_response.status_code == 202
    payment_id = first_response.json()["payment_id"]

    second_response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": idempotency_key},
        json=valid_payload(amount="999.00", description="different"),
    )
    assert second_response.status_code == 409

    async with session_factory() as session:
        stored = await session.get(Payment, uuid.UUID(payment_id))
    assert stored is not None
    assert str(stored.amount) == "10.00"
    assert stored.description == "original"

    payment_count, _ = await row_counts(session_factory)
    assert payment_count == 1


async def test_replay_with_identical_body_still_returns_202_with_original_payment(
    client: tuple[AsyncClient, Settings],
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    http_client, settings = client
    idempotency_key = str(uuid.uuid4())

    first_response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": idempotency_key},
        json=valid_payload(amount="10.00", description="original"),
    )
    assert first_response.status_code == 202

    second_response = await http_client.post(
        "/api/v1/payments",
        headers={API_KEY_HEADER: settings.api_key, "Idempotency-Key": idempotency_key},
        json=valid_payload(amount="10.00", description="original"),
    )

    assert second_response.status_code == 202
    assert second_response.json()["payment_id"] == first_response.json()["payment_id"]

    payment_count, outbox_count = await row_counts(session_factory)
    assert payment_count == 1
    assert outbox_count == 1
