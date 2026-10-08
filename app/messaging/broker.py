from __future__ import annotations

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments.exchange", type=ExchangeType.DIRECT, durable=True)
PAYMENTS_QUEUE = RabbitQueue("payments.new", durable=True, routing_key="payments.new")

RETRY_EXCHANGE = RabbitExchange("payments.retry.exchange", type=ExchangeType.DIRECT, durable=True)
RETRY_QUEUE = RabbitQueue(
    "payments.retry",
    durable=True,
    routing_key="payments.new",
    arguments={
        "x-dead-letter-exchange": "payments.exchange",
        "x-dead-letter-routing-key": "payments.new",
    },
)

DLX_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)
DLQ_QUEUE = RabbitQueue("payments.dlq", durable=True, routing_key="payments.new")


def create_broker(rabbitmq_url: str) -> RabbitBroker:
    return RabbitBroker(rabbitmq_url)


async def declare_retry_topology(broker: RabbitBroker) -> None:
    for exchange, queue in ((RETRY_EXCHANGE, RETRY_QUEUE), (DLX_EXCHANGE, DLQ_QUEUE)):
        exchange_obj = await broker.declare_exchange(exchange)
        queue_obj = await broker.declare_queue(queue)
        await queue_obj.bind(exchange_obj, routing_key=queue.routing())
