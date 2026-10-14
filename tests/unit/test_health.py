from __future__ import annotations

from collections.abc import AsyncIterator

from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.exc import OperationalError

from app.api.deps import get_session
from app.api.health import router


class FakeSession:
    def __init__(self, error_to_raise: Exception | None = None) -> None:
        self._error_to_raise = error_to_raise

    async def execute(self, *_args: object, **_kwargs: object) -> None:
        if self._error_to_raise is not None:
            raise self._error_to_raise


async def make_client(error_to_raise: Exception | None = None) -> AsyncClient:
    async def fake_get_session() -> AsyncIterator[FakeSession]:
        yield FakeSession(error_to_raise)

    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_session] = fake_get_session
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_health_returns_ok_when_database_reachable() -> None:
    async with await make_client() as client:
        response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}


async def test_health_returns_503_when_database_raises_sqlalchemy_error() -> None:
    error = OperationalError("SELECT 1", (), Exception("connection lost"))
    async with await make_client(error_to_raise=error) as client:
        response = await client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "error", "database": "error"}


async def test_health_returns_503_when_database_connection_refused() -> None:
    error = ConnectionRefusedError("connection refused")
    async with await make_client(error_to_raise=error) as client:
        response = await client.get("/health")

    assert response.status_code == 503
    assert response.json() == {"status": "error", "database": "error"}
