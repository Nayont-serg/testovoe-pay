from __future__ import annotations

from fastapi import APIRouter, Depends, Response, status
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_session

router = APIRouter(tags=["health"])

DATABASE_UNREACHABLE_ERRORS = (SQLAlchemyError, OSError)


@router.get("/health")
async def health(
    response: Response,
    session: AsyncSession = Depends(get_session),
) -> dict[str, str]:
    try:
        await session.execute(text("SELECT 1"))
    except DATABASE_UNREACHABLE_ERRORS:
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return {"status": "error", "database": "error"}
    return {"status": "ok", "database": "ok"}
