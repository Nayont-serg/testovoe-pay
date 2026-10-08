import uuid
from datetime import UTC, datetime

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.api.deps import get_payment_service, verify_api_key
from app.api.payments import router
from app.models.payment import Payment, PaymentStatus


class FakePaymentService:
    def __init__(self) -> None:
        self.payments: dict[uuid.UUID, Payment] = {}

    async def create_payment(self, request: object, idempotency_key: str) -> Payment:
        payment = Payment(
            id=uuid.uuid4(),
            amount=request.amount,
            currency=request.currency,
            description=request.description,
            metadata_=request.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=idempotency_key,
            webhook_url=str(request.webhook_url),
            created_at=datetime.now(UTC),
        )
        self.payments[payment.id] = payment
        return payment

    async def get_payment(self, payment_id: uuid.UUID) -> Payment | None:
        return self.payments.get(payment_id)


@pytest.fixture
def fake_service() -> FakePaymentService:
    return FakePaymentService()


@pytest.fixture
def client(fake_service: FakePaymentService) -> AsyncClient:
    app = FastAPI()
    app.include_router(router)
    app.dependency_overrides[get_payment_service] = lambda: fake_service
    app.dependency_overrides[verify_api_key] = lambda: None
    return AsyncClient(transport=ASGITransport(app=app), base_url="http://test")


async def test_create_payment_returns_202_with_payment_id(client: AsyncClient) -> None:
    async with client:
        response = await client.post(
            "/api/v1/payments",
            headers={"Idempotency-Key": "idem-1"},
            json={
                "amount": "10.00",
                "currency": "USD",
                "description": "order",
                "metadata": {},
                "webhook_url": "https://example.com/hook",
            },
        )
    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert "payment_id" in body


async def test_create_payment_requires_idempotency_key_header(client: AsyncClient) -> None:
    async with client:
        response = await client.post(
            "/api/v1/payments",
            json={
                "amount": "10.00",
                "currency": "USD",
                "description": "order",
                "metadata": {},
                "webhook_url": "https://example.com/hook",
            },
        )
    assert response.status_code == 422


async def test_get_payment_returns_404_for_unknown_id(client: AsyncClient) -> None:
    async with client:
        response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert response.status_code == 404


async def test_get_payment_returns_full_detail(client: AsyncClient) -> None:
    async with client:
        create_response = await client.post(
            "/api/v1/payments",
            headers={"Idempotency-Key": "idem-2"},
            json={
                "amount": "20.00",
                "currency": "EUR",
                "description": "order 2",
                "metadata": {"order_id": "xyz"},
                "webhook_url": "https://example.com/hook",
            },
        )
        payment_id = create_response.json()["payment_id"]
        response = await client.get(f"/api/v1/payments/{payment_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["amount"] == "20.00"
    assert body["metadata"] == {"order_id": "xyz"}


async def test_missing_api_key_returns_401() -> None:
    app = FastAPI()
    app.include_router(router)
    app.state.settings = type("S", (), {"api_key": "expected-key"})()
    app.state.session_factory = None
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert response.status_code == 401
