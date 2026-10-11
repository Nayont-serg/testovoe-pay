from __future__ import annotations

import asyncio
import sys

import pytest

from tests.integration.conftest import integration_env
from tests.integration.environment import PROJECT_ROOT

pytestmark = pytest.mark.integration


async def test_alembic_check_detects_no_drift_against_freshly_migrated_database() -> None:
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "check",
        cwd=PROJECT_ROOT,
        env=integration_env(),
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await process.communicate()
    assert process.returncode == 0, (
        f"alembic check reported schema drift: {stdout.decode()}\n{stderr.decode()}"
    )
