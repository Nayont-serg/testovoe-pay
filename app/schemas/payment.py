from __future__ import annotations

import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, HttpUrl, field_validator

from app.models.payment import Currency, PaymentStatus

_NUL_CHARACTER = "\x00"


def _reject_nul_characters(value: Any, *, field_path: str) -> None:
    if isinstance(value, str):
        if _NUL_CHARACTER in value:
            raise ValueError(f"{field_path} must not contain NUL characters")
        return
    if isinstance(value, dict):
        for key, nested_value in value.items():
            _reject_nul_characters(key, field_path=f"{field_path} key {key!r}")
            _reject_nul_characters(nested_value, field_path=f"{field_path}.{key}")
        return
    if isinstance(value, list):
        for index, item in enumerate(value):
            _reject_nul_characters(item, field_path=f"{field_path}[{index}]")


class PaymentCreateRequest(BaseModel):
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: Currency
    description: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl

    @field_validator("description")
    @classmethod
    def _validate_description(cls, value: str) -> str:
        _reject_nul_characters(value, field_path="description")
        return value

    @field_validator("metadata")
    @classmethod
    def _validate_metadata(cls, value: dict[str, Any]) -> dict[str, Any]:
        _reject_nul_characters(value, field_path="metadata")
        return value


class PaymentCreateResponse(BaseModel):
    payment_id: uuid.UUID
    status: PaymentStatus
    created_at: datetime


class PaymentDetailResponse(BaseModel):
    payment_id: uuid.UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None
