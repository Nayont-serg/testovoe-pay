from __future__ import annotations

import asyncio
import os
import time
from pathlib import Path

from app.consumer_heartbeat import HeartbeatWriter, is_heartbeat_fresh


async def test_heartbeat_writer_writes_file_immediately_on_start(tmp_path: Path) -> None:
    heartbeat_path = tmp_path / "heartbeat"
    writer = HeartbeatWriter(interval_seconds=0.05, heartbeat_path=heartbeat_path)

    await writer.start()
    try:
        assert heartbeat_path.exists()
    finally:
        await writer.stop()


async def test_heartbeat_writer_updates_mtime_while_running(tmp_path: Path) -> None:
    heartbeat_path = tmp_path / "heartbeat"
    writer = HeartbeatWriter(interval_seconds=0.02, heartbeat_path=heartbeat_path)

    await writer.start()
    try:
        first_mtime = heartbeat_path.stat().st_mtime
        await asyncio.sleep(0.1)
        second_mtime = heartbeat_path.stat().st_mtime
    finally:
        await writer.stop()

    assert second_mtime > first_mtime


async def test_heartbeat_writer_stops_updating_file_after_stop(tmp_path: Path) -> None:
    heartbeat_path = tmp_path / "heartbeat"
    writer = HeartbeatWriter(interval_seconds=0.02, heartbeat_path=heartbeat_path)

    await writer.start()
    await writer.stop()
    mtime_after_stop = heartbeat_path.stat().st_mtime
    await asyncio.sleep(0.1)

    assert heartbeat_path.stat().st_mtime == mtime_after_stop


def test_is_heartbeat_fresh_accepts_recent_mtime(tmp_path: Path) -> None:
    heartbeat_path = tmp_path / "heartbeat"
    heartbeat_path.write_text("now")

    assert is_heartbeat_fresh(heartbeat_path, max_age_seconds=5.0) is True


def test_is_heartbeat_fresh_rejects_stale_mtime(tmp_path: Path) -> None:
    heartbeat_path = tmp_path / "heartbeat"
    heartbeat_path.write_text("old")
    stale_time = time.time() - 60
    os.utime(heartbeat_path, (stale_time, stale_time))

    assert is_heartbeat_fresh(heartbeat_path, max_age_seconds=5.0) is False


def test_is_heartbeat_fresh_rejects_missing_file(tmp_path: Path) -> None:
    heartbeat_path = tmp_path / "missing-heartbeat"

    assert is_heartbeat_fresh(heartbeat_path, max_age_seconds=5.0) is False
