from __future__ import annotations

from faststream.rabbit import Channel, ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

from app.config import Settings

PAYMENTS_EXCHANGE = RabbitExchange("payments.exchange", type=ExchangeType.DIRECT, durable=True)
PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.new",
    },
)

DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True, routing_key="payments.new")


def create_broker(rabbitmq_url: str) -> RabbitBroker:
    return RabbitBroker(
        rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )


def retry_tier_queue_name(tier: int) -> str:
    return f"payments.retry.{tier}"


def retry_tier_exchange(tier: int) -> RabbitExchange:
    return RabbitExchange(
        f"{retry_tier_queue_name(tier)}.exchange", type=ExchangeType.DIRECT, durable=True
    )


def retry_tier_delay_ms(tier: int, base_delay_ms: int) -> int:
    return base_delay_ms * (2 ** (tier - 1))


def retry_tier_queue(tier: int, base_delay_ms: int) -> RabbitQueue:
    return RabbitQueue(
        retry_tier_queue_name(tier),
        durable=True,
        routing_key="payments.new",
        arguments={
            "x-message-ttl": retry_tier_delay_ms(tier, base_delay_ms),
            "x-dead-letter-exchange": "payments.exchange",
            "x-dead-letter-routing-key": "payments.new",
        },
    )


def retry_tiers(message_max_attempts: int) -> list[int]:
    return list(range(1, message_max_attempts))


async def _declare_bound_pair(
    broker: RabbitBroker, exchange: RabbitExchange, queue: RabbitQueue
) -> None:
    exchange_obj = await broker.declare_exchange(exchange)
    queue_obj = await broker.declare_queue(queue)
    await queue_obj.bind(exchange_obj, routing_key=queue.routing())


async def declare_payments_topology(broker: RabbitBroker) -> None:
    await _declare_bound_pair(broker, PAYMENTS_EXCHANGE, PAYMENTS_QUEUE)


async def declare_retry_topology(broker: RabbitBroker, settings: Settings) -> None:
    for tier in retry_tiers(settings.message_max_attempts):
        await _declare_bound_pair(
            broker,
            retry_tier_exchange(tier),
            retry_tier_queue(tier, settings.message_retry_base_delay_ms),
        )
    await _declare_bound_pair(broker, DLX_EXCHANGE, DLQ_QUEUE)
