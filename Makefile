.PHONY: run run-consumer test test-integration lint migrate infra-up docker-up docker-down docker-reset prepare-test-environment docker-up-test

STACK_TEST_DATABASE_URL := postgresql+asyncpg://payments:payments@postgres:5432/payments_test
STACK_TEST_RABBITMQ_URL := amqp://payments:payments@rabbitmq:5672/test

run:
	uv run --env-file .env uvicorn app.main:app --host 0.0.0.0 --port 8000 --reload

run-consumer:
	uv run --env-file .env python -m app.consumer_main

test:
	uv run pytest tests/unit

test-integration:
	uv run pytest -m integration

lint:
	uv run ruff check .
	uv run ruff format --check .

migrate:
	uv run --env-file .env alembic upgrade head

infra-up:
	docker compose up -d --wait postgres rabbitmq

docker-up:
	docker compose up -d --build --wait

prepare-test-environment:
	docker compose up -d --wait postgres rabbitmq
	uv run python -m tests.integration.bootstrap

docker-up-test: prepare-test-environment
	STACK_DATABASE_URL=$(STACK_TEST_DATABASE_URL) STACK_RABBITMQ_URL=$(STACK_TEST_RABBITMQ_URL) \
	docker compose up -d --build --wait

docker-down:
	docker compose down

docker-reset:
	docker compose down -v
