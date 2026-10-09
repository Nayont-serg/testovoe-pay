# Payment Processing Service

Асинхронный сервис процессинга платежей: принимает запрос на оплату, публикует
событие через outbox pattern, обрабатывает его consumer'ом с эмуляцией
платёжного шлюза и уведомляет клиента вебхуком.

## Настройка

    cp .env.example .env
    uv sync

## Запуск

Есть два **альтернативных** (не последовательных!) способа поднять сервис —
выбрать нужно один. Оба используют один и тот же docker-compose файл, но
первый поднимает из него только инфраструктуру.

### Вариант A: инфраструктура в Docker, приложение локально

    make infra-up        # только postgres + rabbitmq
    make migrate
    make run             # api на http://localhost:8000
    make run-consumer    # consumer отдельным процессом (во втором терминале)

### Вариант B: всё целиком в Docker

    make docker-up       # postgres + rabbitmq + api + consumer

Контейнер `api` сам применяет миграции при старте, `make migrate` в этом
варианте не нужен. Порт `8000` занимает контейнер `api`, поэтому `make run`
одновременно с `make docker-up` запускать нельзя — будет конфликт порта и два
конкурирующих consumer'а.

Остановить: `make docker-down` (данные сохраняются) или `make docker-reset`
(`docker compose down -v`, удаляет тома postgres и rabbitmq).

RabbitMQ management UI будет доступен на http://localhost:15673
(логин/пароль — `payments`/`payments`).

Health-probe API: `GET /health` (без `X-API-Key`), её же использует
healthcheck контейнера `api`. Healthcheck контейнера `consumer` проверяет, что
его часть топологии RabbitMQ уже объявлена, поэтому `make docker-up` завершается
только когда consumer реально готов принимать сообщения.

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
    make test-integration  # требует полного стека: `make docker-up`

Интеграционные тесты публикуют сообщения в реальный RabbitMQ и ждут, что
статус платежа станет терминальным и что сообщение дойдёт до `payments.dlq`.
Обрабатывает их **контейнер `consumer`**, поэтому `make infra-up` для них
недостаточно — нужен именно `make docker-up` (или локально запущенный
`make run-consumer`).

## Архитектура

- `api` — FastAPI, принимает запросы, пишет `payments` + `outbox` в одной
  транзакции, в фоне (lifespan) гоняет outbox-relay в RabbitMQ.
- `consumer` — отдельный процесс, читает `payments.new`, эмулирует шлюз
  (2-5с, 90% успех / 10% failed-статус — это бизнес-исход, не ошибка),
  обновляет статус, шлёт webhook с retry.
- RabbitMQ: `payments.exchange` → `payments.new`; при инфраструктурной ошибке
  обработки — retry с экспоненциальным backoff через `payments.retry.exchange`
  (TTL+DLX), после 3 попыток — `payments.dlx` → `payments.dlq`. Очередь
  `payments.new` тоже привязана к `payments.dlx`, поэтому необработанное
  исключение в хендлере не теряет сообщение, а отправляет его в `payments.dlq`.
- Оба процесса активно (не passive) объявляют свою часть топологии при старте,
  поэтому полная потеря топологии RabbitMQ не ломает публикацию навсегда.
  Все сообщения публикуются персистентными.

## Известные ограничения

Outbox-relay публикует и коммитит батчами; при сбое после публикации
части сообщений в батче транзакция откатится и эти сообщения будут
опубликованы повторно при следующем опросе. Consumer защищён от такой
повторной доставки: `mark_processed` переводит статус только из `pending`,
а при повторе уже обработанного платежа webhook не отправляется повторно.
Защиты от двух consumer'ов, одновременно обрабатывающих одно и то же
сообщение, помимо `SELECT ... FOR UPDATE` на строке платежа, нет.

Очередь `payments.new` объявляется с аргументами `x-dead-letter-*`. Если в
RabbitMQ уже существует очередь `payments.new` без этих аргументов (например,
осталась от более ранней версии сервиса в томе `rabbitmqdata`), объявление
упадёт с `PRECONDITION_FAILED`. Лечится удалением очереди через management UI
либо чистым стартом: `make docker-reset && make docker-up`.
