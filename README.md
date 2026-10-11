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

Интеграционные тесты публикуют сообщения в реальный RabbitMQ и ждут, что
статус платежа станет терминальным и что сообщение дойдёт до `payments.dlq`.
Обрабатывает их **контейнер `consumer`**, поэтому `make infra-up` для них
недостаточно — нужен именно `make docker-up-test` (поднимает тот же стек, но
`api`/`consumer` указывают на `payments_test`/vhost `test`, а не на dev
ресурсы) либо локально запущенные `make run-consumer`/`make run` с
`DATABASE_URL`/`RABBITMQ_URL`, переопределёнными на `TEST_DATABASE_URL`/
`TEST_RABBITMQ_URL`.

### Изоляция тестового окружения (R5)

`tests/integration/environment.py` перед любым `TRUNCATE`/`purge` проверяет,
что имя базы оканчивается на `_test`, а vhost RabbitMQ равен `test` — иначе
бросает `UnsafeTestEnvironmentError` до любого деструктивного запроса, не
наследуя `DATABASE_URL`/`RABBITMQ_URL` обычного окружения как допустимый
fallback. `TEST_DATABASE_URL`/`TEST_RABBITMQ_URL`/`TEST_RABBITMQ_MANAGEMENT_URL`
(см. `.env.example`) — единственный способ указать тестам, куда идти; по
умолчанию это `payments_test` и vhost `test` на том же контейнере
postgres/rabbitmq, что и у dev-стека. `make docker-up-test` сначала
создаёт эту базу/vhost (`tests/integration/bootstrap.py`, использует те же
функции, что и фикстуры), а затем поднимает `api`/`consumer`, указав им на
эти тестовые ресурсы через `STACK_DATABASE_URL`/`STACK_RABBITMQ_URL` —
обычный `make docker-up` их не трогает и продолжает поднимать `api`/
`consumer` на dev `payments`/vhost `/`, который интеграционные тесты теперь
никогда не затрагивают.

## Архитектура

- `api` — FastAPI, принимает запросы, пишет `payments` + `outbox` в одной
  транзакции, в фоне (lifespan) гоняет outbox-relay в RabbitMQ.
- `consumer` — отдельный процесс, читает `payments.new`, эмулирует шлюз
  (2-5с, 90% успех / 10% failed-статус — это бизнес-исход, не ошибка),
  обновляет статус, шлёт webhook с retry.
- RabbitMQ: `payments.exchange` → `payments.new`; при инфраструктурной ошибке
  обработки — retry с экспоненциальным backoff через отдельную очередь на
  каждый retry-уровень (`payments.retry.1`, `payments.retry.2`, ...; ровно
  `message_max_attempts - 1` очередей), у каждой свой TTL на уровне очереди
  (не per-message `expiration`) и свой DLX обратно на `payments.exchange`/
  `payments.new` — так доставка короткого retry не ждёт истечения TTL более
  длинного retry, стоящего в очереди перед ним. После исчерпания попыток —
  `payments.dlx` → `payments.dlq`. Очередь `payments.new` тоже привязана
  к `payments.dlx`, поэтому необработанное исключение в хендлере (включая
  ситуацию, когда сам retry/dead-letter publish не смог
  промаршрутизироваться) не теряет сообщение, а отправляет его в
  `payments.dlq`.
- Оба процесса активно (не passive) объявляют свою часть топологии при старте,
  поэтому полная потеря топологии RabbitMQ не ломает публикацию навсегда.
  Все сообщения публикуются персистентными с `publisher_confirms` и
  `on_return_raises`: если сообщение не промаршрутизировалось ни в одну
  очередь (например, потерян binding), outbox-relay не помечает его
  опубликованным, логирует и переобъявляет топологию (self-heal), прежде
  чем перейти к следующему сообщению батча.

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
