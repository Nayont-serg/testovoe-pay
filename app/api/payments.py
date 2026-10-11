from __future__ import annotations

import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, status

from app.api.deps import get_payment_service, verify_api_key
from app.messaging.webhook_destination import (
    UnsafeWebhookDestinationError,
    WebhookDestinationUnavailableError,
)
from app.schemas.payment import (
    PaymentCreateRequest,
    PaymentCreateResponse,
    PaymentDetailResponse,
)
from app.services.payment_service import IdempotencyKeyConflictError, PaymentService

router = APIRouter(
    prefix="/api/v1/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
)


@router.post("", status_code=status.HTTP_202_ACCEPTED, response_model=PaymentCreateResponse)
async def create_payment(
    request: PaymentCreateRequest,
    idempotency_key: str = Header(alias="Idempotency-Key", min_length=1, max_length=200),
    service: PaymentService = Depends(get_payment_service),
) -> PaymentCreateResponse:
    try:
        payment = await service.create_payment(request, idempotency_key)
    except IdempotencyKeyConflictError as error:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(error)) from error
    except UnsafeWebhookDestinationError as error:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_CONTENT, detail=str(error)
        ) from error
    except WebhookDestinationUnavailableError as error:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(error)
        ) from error
    return PaymentCreateResponse(
        payment_id=payment.id, status=payment.status, created_at=payment.created_at
    )


@router.get("/{payment_id}", response_model=PaymentDetailResponse)
async def get_payment(
    payment_id: uuid.UUID,
    service: PaymentService = Depends(get_payment_service),
) -> PaymentDetailResponse:
    payment = await service.get_payment(payment_id)
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="payment not found")
    return PaymentDetailResponse(
        payment_id=payment.id,
        amount=payment.amount,
        currency=payment.currency,
        description=payment.description,
        metadata=payment.metadata_,
        status=payment.status,
        webhook_url=payment.webhook_url,
        created_at=payment.created_at,
        processed_at=payment.processed_at,
    )
