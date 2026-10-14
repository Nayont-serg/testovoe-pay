from __future__ import annotations

import asyncio
import contextlib
import time
from pathlib import Path

DEFAULT_HEARTBEAT_PATH = Path("/tmp/payment_consumer_heartbeat")


def is_heartbeat_fresh(heartbeat_path: Path, max_age_seconds: float) -> bool:
    try:
        mtime = heartbeat_path.stat().st_mtime
    except FileNotFoundError:
        return False
    return (time.time() - mtime) <= max_age_seconds


class HeartbeatWriter:
    def __init__(
        self,
        interval_seconds: float,
        heartbeat_path: Path = DEFAULT_HEARTBEAT_PATH,
    ) -> None:
        self._interval_seconds = interval_seconds
        self._heartbeat_path = heartbeat_path
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    async def start(self) -> None:
        await self._write_heartbeat()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stopped.set()
        if self._task is not None:
            await self._task

    async def _run(self) -> None:
        while not self._stopped.is_set():
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stopped.wait(), timeout=self._interval_seconds)
            if not self._stopped.is_set():
                await self._write_heartbeat()

    async def _write_heartbeat(self) -> None:
        await asyncio.to_thread(self._heartbeat_path.write_text, str(time.time()))
