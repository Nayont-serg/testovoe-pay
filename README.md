# Payment Processing Service

Асинхронный сервис процессинга платежей: принимает запрос на оплату, публикует
событие через outbox pattern, обрабатывает его consumer'ом с эмуляцией
платёжного шлюза и уведомляет клиента вебхуком.

## Настройка

    cp .env.example .env
    uv sync
    make docker-up
    make migrate

## Запуск

    make run             # api на http://localhost:8000
    make run-consumer    # consumer отдельным процессом

Либо всё целиком в Docker:

    docker compose up --build

RabbitMQ management UI будет доступен на http://localhost:15673
(логин/пароль — `payments`/`payments`).

## API

Все запросы требуют заголовок `X-API-Key` (значение — `API_KEY` из `.env`).

### Создать платёж

    curl -X POST http://localhost:8000/api/v1/payments \
      -H "X-API-Key: local-dev-api-key" \
      -H "Idempotency-Key: $(uuidgen)" \
      -H "Content-Type: application/json" \
      -d '{
        "amount": "100.00",
        "currency": "USD",
        "description": "order #1",
        "metadata": {"order_id": "abc"},
        "webhook_url": "https://example.com/webhook"
      }'

Ответ `202 Accepted`:

    {"payment_id": "...", "status": "pending", "created_at": "..."}

### Получить платёж

    curl http://localhost:8000/api/v1/payments/<payment_id> \
      -H "X-API-Key: local-dev-api-key"

## Тесты

    make test              # юнит-тесты, без сети и БД
    make test-integration   # требует `make docker-up`

## Архитектура

- `api` — FastAPI, принимает запросы, пишет `payments` + `outbox` в одной
  транзакции, в фоне (lifespan) гоняет outbox-relay в RabbitMQ.
- `consumer` — отдельный процесс, читает `payments.new`, эмулирует шлюз
  (2-5с, 90% успех / 10% failed-статус — это бизнес-исход, не ошибка),
  обновляет статус, шлёт webhook с retry.
- RabbitMQ: `payments.exchange` → `payments.new`; при инфраструктурной ошибке
  обработки — retry с экспоненциальным backoff через `payments.retry.exchange`
  (TTL+DLX), после 3 попыток — `payments.dlx` → `payments.dlq`.

## Известные ограничения

Outbox-relay публикует и коммитит батчами; при сбое после публикации
части сообщений в батче транзакция откатится и эти сообщения будут
опубликованы повторно при следующем опросе — RabbitMQ consumer должен
(и в данном случае может, благодаря `payment_id` в payload) считаться
идемпотентным по отношению к повторной публикации одного и того же события.
