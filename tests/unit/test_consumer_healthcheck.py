from __future__ import annotations

import os
import time
from pathlib import Path

import pytest

from app.config import Settings
from app.consumer_healthcheck import ConsumerHeartbeatStaleError, check_heartbeat_fresh


def make_settings(heartbeat_interval_seconds: str = "1") -> Settings:
    return Settings.from_env(
        {
            "DATABASE_URL": "postgresql+asyncpg://u:p@localhost:5432/db",
            "RABBITMQ_URL": "amqp://u:p@localhost:5672/",
            "API_KEY": "secret",
            "CONSUMER_HEARTBEAT_INTERVAL_SECONDS": heartbeat_interval_seconds,
        }
    )


def test_check_heartbeat_fresh_passes_for_recently_written_file(tmp_path: Path) -> None:
    heartbeat_path = tmp_path / "heartbeat"
    heartbeat_path.write_text("now")

    check_heartbeat_fresh(make_settings(), heartbeat_path=heartbeat_path)


def test_check_heartbeat_fresh_raises_for_missing_file(tmp_path: Path) -> None:
    heartbeat_path = tmp_path / "missing-heartbeat"

    with pytest.raises(ConsumerHeartbeatStaleError):
        check_heartbeat_fresh(make_settings(), heartbeat_path=heartbeat_path)


def test_check_heartbeat_fresh_raises_for_stale_file(tmp_path: Path) -> None:
    heartbeat_path = tmp_path / "heartbeat"
    heartbeat_path.write_text("old")
    stale_time = time.time() - 60
    os.utime(heartbeat_path, (stale_time, stale_time))

    with pytest.raises(ConsumerHeartbeatStaleError):
        check_heartbeat_fresh(make_settings(), heartbeat_path=heartbeat_path)
