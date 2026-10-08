from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    rabbitmq_url: str
    api_key: str
    outbox_poll_interval_seconds: float
    outbox_batch_size: int
    webhook_max_attempts: int
    webhook_retry_base_delay_seconds: float
    message_max_attempts: int
    message_retry_base_delay_ms: int
    processing_min_seconds: float
    processing_max_seconds: float
    processing_failure_rate: float

    @classmethod
    def from_env(cls, env: dict[str, str] | None = None) -> Settings:
        source = env if env is not None else os.environ
        return cls(
            database_url=source["DATABASE_URL"],
            rabbitmq_url=source["RABBITMQ_URL"],
            api_key=source["API_KEY"],
            outbox_poll_interval_seconds=float(source.get("OUTBOX_POLL_INTERVAL_SECONDS", "0.5")),
            outbox_batch_size=int(source.get("OUTBOX_BATCH_SIZE", "20")),
            webhook_max_attempts=int(source.get("WEBHOOK_MAX_ATTEMPTS", "3")),
            webhook_retry_base_delay_seconds=float(
                source.get("WEBHOOK_RETRY_BASE_DELAY_SECONDS", "1")
            ),
            message_max_attempts=int(source.get("MESSAGE_MAX_ATTEMPTS", "3")),
            message_retry_base_delay_ms=int(source.get("MESSAGE_RETRY_BASE_DELAY_MS", "2000")),
            processing_min_seconds=float(source.get("PROCESSING_MIN_SECONDS", "2")),
            processing_max_seconds=float(source.get("PROCESSING_MAX_SECONDS", "5")),
            processing_failure_rate=float(source.get("PROCESSING_FAILURE_RATE", "0.1")),
        )
