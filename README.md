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

RabbitMQ management UI будет доступен на http://localhost:15673
(логин/пароль — `payments`/`payments`).

Health-probe API: `GET /health` (без `X-API-Key`) выполняет `SELECT 1` через
собственный пул соединений к PostgreSQL и возвращает `200 {"status": "ok",
"database": "ok"}`, если БД доступна, либо `503 {"status": "error",
"database": "error"}`, если нет — это и использует healthcheck контейнера
`api`. Недоступность RabbitMQ на этот ответ намеренно не влияет: outbox
накапливает неотправленные события и не теряет работу, пока брокер
восстанавливается, поэтому `/health` проверяет только то, от чего API
действительно не может продолжать работать — свою БД.

Healthcheck контейнера `consumer` проверяет два независимых сигнала: что его
часть топологии RabbitMQ объявлена (полезно, но само по себе не доказывает,
что СЕЙЧАС работающий процесс жив — очереди остаются в брокере и после того,
как consumer завис или был остановлен), и что heartbeat-файл процесса
(`/tmp/payment_consumer_heartbeat`, обновляется фоновой задачей раз в
`CONSUMER_HEARTBEAT_INTERVAL_SECONDS`) не старше трёхкратного интервала
heartbeat. Если event loop consumer'а завис или сам процесс встал колом (но
контейнер не упал), heartbeat-файл перестаёт обновляться и healthcheck
корректно сообщает unhealthy, даже если очереди в RabbitMQ по-прежнему
существуют.

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

### Валидация запроса на создание платежа

- `amount` должен точно укладываться в `NUMERIC(18, 2)`: строго больше нуля,
  не более 2 знаков после запятой (без неявного округления) и не более 16
  цифр до запятой. Нарушение — `422`, платёж не создаётся.
- `description` и любая строка внутри `metadata` (включая ключи, на любой
  глубине вложенности) не могут содержать NUL-байт (`\u0000`) — такая строка
  не сохранится в Postgres. Нарушение — `422`.
- `Idempotency-Key` — непустой, 1-200 символов, проверяется до обращения к
  БД. Повтор того же ключа с точно таким же телом возвращает `202` с тем же
  `payment_id` (платёж не создаётся повторно). Повтор того же ключа с
  **другим** телом (другие `amount`/`currency`/`description`/`metadata`/
  `webhook_url`) — `409 Conflict`, исходный платёж не изменяется.
- `webhook_url`: назначение резолвится (DNS) на этапе создания платежа, и
  запрос с резолвящимся в loopback/private (RFC1918)/link-local (включая
  `169.254.169.254`) адрес — IPv4 или IPv6 (`::1`, `fc00::/7`, `fe80::/10`)
  — отклоняется с `422` **до** записи платежа; таким образом consumer
  никогда не делает HTTP-запрос на запрещённый адрес. Отключить проверку
  (только для локальной разработки, не для прод-подобных окружений) можно
  переменной `WEBHOOK_ALLOW_PRIVATE_DESTINATIONS=true` в `.env`. Если DNS
  временно недоступен (таймаут резолва/ошибка резолвера на обычно валидном
  имени) — ответ `503`, а не `422`: отличаем "мы проверили и это запрещённый
  адрес" от "не смогли проверить прямо сейчас". **Известное ограничение:**
  это не защищает от DNS rebinding между моментом проверки и моментом, когда
  consumer реально отправляет вебхук — для полного закрытия нужен
  resolve-and-pin на уровне HTTP-транспорта, что в этой итерации не сделано.

### Получить платёж

    curl http://localhost:8000/api/v1/payments/<payment_id> \
      -H "X-API-Key: local-dev-api-key"

## Тесты

    make test              # юнит-тесты, без сети и БД
    make docker-up-test    # стек с api/consumer, указывающими на тестовые БД/vhost
    make test-integration  # требует стек, поднятый через docker-up-test


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

То же самое касается retry-очередей `payments.retry.1`/`payments.retry.2`:
TTL (`x-message-ttl`) теперь объявлен на уровне очереди и берётся из
`MESSAGE_RETRY_BASE_DELAY_MS`/`MESSAGE_MAX_ATTEMPTS` в момент старта
consumer'а. Если изменить `MESSAGE_RETRY_BASE_DELAY_MS` (или число tier'ов
через `MESSAGE_MAX_ATTEMPTS`) для уже существующего тома RabbitMQ, где эти
очереди объявлены со старым TTL, `declare_retry_topology` упадёт с тем же
`PRECONDITION_FAILED` при старте consumer'а — лечится так же, как и выше
(`make docker-reset && make docker-up`), либо вручную удалить
`payments.retry.1`/`payments.retry.2` через management UI перед перезапуском.
Если в томе остались очередь/exchange от версии сервиса до этого фикса
(`payments.retry`, `payments.retry.exchange` — общая очередь с per-message
TTL вместо отдельных очередей на tier) — они больше не используются кодом и
осиротели; их можно безопасно удалить вручную через management UI.
