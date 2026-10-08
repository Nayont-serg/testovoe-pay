from __future__ import annotations

from app.config import Settings


def test_settings_from_env_reads_required_and_defaulted_fields() -> None:
    env = {
        "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db",
        "RABBITMQ_URL": "amqp://u:p@localhost:5672/",
        "API_KEY": "secret",
    }
    settings = Settings.from_env(env)
    assert settings.database_url == env["DATABASE_URL"]
    assert settings.rabbitmq_url == env["RABBITMQ_URL"]
    assert settings.api_key == "secret"
    assert settings.outbox_poll_interval_seconds == 0.5
    assert settings.outbox_batch_size == 20
    assert settings.webhook_max_attempts == 3
    assert settings.message_max_attempts == 3
    assert settings.processing_failure_rate == 0.1


def test_settings_from_env_honors_overrides() -> None:
    env = {
        "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db",
        "RABBITMQ_URL": "amqp://u:p@localhost:5672/",
        "API_KEY": "secret",
        "OUTBOX_BATCH_SIZE": "50",
        "PROCESSING_FAILURE_RATE": "0.25",
    }
    settings = Settings.from_env(env)
    assert settings.outbox_batch_size == 50
    assert settings.processing_failure_rate == 0.25
