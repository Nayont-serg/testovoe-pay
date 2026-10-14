from __future__ import annotations

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.api.health import router
from app.db.session import create_engine, create_session_factory

pytestmark = pytest.mark.integration

UNREACHABLE_DATABASE_URL = "postgresql+asyncpg://payments:payments@localhost:59999/payments_test"


def make_health_app(session_factory: async_sessionmaker[AsyncSession]) -> FastAPI:
    app = FastAPI()
    app.include_router(router)
    app.state.session_factory = session_factory
    return app


async def test_health_returns_200_when_database_reachable(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    app = make_health_app(session_factory)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        response = await client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "ok"}


async def test_health_returns_503_when_database_unreachable() -> None:
    engine = create_engine(UNREACHABLE_DATABASE_URL)
    session_factory = create_session_factory(engine)
    app = make_health_app(session_factory)
    try:
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/health")
    finally:
        await engine.dispose()

    assert response.status_code == 503
    assert response.json() == {"status": "error", "database": "error"}
