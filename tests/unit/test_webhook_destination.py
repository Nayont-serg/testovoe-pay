from __future__ import annotations

import asyncio
import socket
from contextlib import AbstractContextManager
from unittest.mock import patch

import pytest

from app.messaging.webhook_destination import (
    UnsafeWebhookDestinationError,
    WebhookDestinationUnavailableError,
    validate_webhook_destination,
)


class _FakeLoop:
    def __init__(
        self,
        addrinfos: list[tuple[object, object, int, str, tuple[str, int]]] | None = None,
        error: OSError | None = None,
        delay_seconds: float = 0.0,
    ) -> None:
        self._addrinfos = addrinfos
        self._error = error
        self._delay_seconds = delay_seconds

    async def getaddrinfo(self, _hostname: str, _port: int | None) -> list[tuple]:
        if self._delay_seconds:
            await asyncio.sleep(self._delay_seconds)
        if self._error is not None:
            raise self._error
        assert self._addrinfos is not None
        return self._addrinfos


def _patched_loop(fake_loop: _FakeLoop) -> AbstractContextManager[object]:
    return patch(
        "app.messaging.webhook_destination.asyncio.get_running_loop", return_value=fake_loop
    )


async def test_public_hostname_resolving_to_a_public_address_is_allowed() -> None:
    fake_loop = _FakeLoop(
        addrinfos=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("93.184.216.34", 0))]
    )
    with _patched_loop(fake_loop):
        await validate_webhook_destination(
            "https://example.com/hook", allow_private_destinations=False
        )


async def test_hostname_resolving_to_a_private_address_is_rejected() -> None:
    fake_loop = _FakeLoop(addrinfos=[(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("10.0.0.5", 0))])
    with _patched_loop(fake_loop), pytest.raises(UnsafeWebhookDestinationError):
        await validate_webhook_destination(
            "https://looks-public.example.com/hook", allow_private_destinations=False
        )


async def test_gaierror_resolution_failure_is_treated_as_unavailable_not_rejected() -> None:
    fake_loop = _FakeLoop(error=socket.gaierror(-2, "Name or service not known"))
    with _patched_loop(fake_loop), pytest.raises(WebhookDestinationUnavailableError):
        await validate_webhook_destination(
            "https://this-domain-should-not-exist.invalid/hook",
            allow_private_destinations=False,
        )


async def test_dns_resolution_timeout_is_treated_as_unavailable_not_rejected(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr("app.messaging.webhook_destination._DNS_RESOLUTION_TIMEOUT_SECONDS", 0.05)
    fake_loop = _FakeLoop(delay_seconds=1.0)
    with _patched_loop(fake_loop), pytest.raises(WebhookDestinationUnavailableError):
        await validate_webhook_destination(
            "https://slow-resolver.example.com/hook", allow_private_destinations=False
        )


async def test_overlong_dns_label_is_rejected_instead_of_crashing() -> None:
    overlong_label = "a" * 70
    with pytest.raises(UnsafeWebhookDestinationError):
        await validate_webhook_destination(
            f"https://{overlong_label}.example.com/hook", allow_private_destinations=False
        )


async def test_loopback_literal_ip_is_rejected() -> None:
    with pytest.raises(UnsafeWebhookDestinationError):
        await validate_webhook_destination(
            "http://127.0.0.1:9999/hook", allow_private_destinations=False
        )


async def test_rfc1918_private_literal_ip_is_rejected() -> None:
    with pytest.raises(UnsafeWebhookDestinationError):
        await validate_webhook_destination(
            "http://10.89.0.1:18091/ok", allow_private_destinations=False
        )


async def test_link_local_cloud_metadata_address_is_rejected() -> None:
    with pytest.raises(UnsafeWebhookDestinationError):
        await validate_webhook_destination(
            "http://169.254.169.254/latest/meta-data/", allow_private_destinations=False
        )


async def test_ipv6_loopback_is_rejected() -> None:
    with pytest.raises(UnsafeWebhookDestinationError):
        await validate_webhook_destination(
            "http://[::1]:9999/hook", allow_private_destinations=False
        )


async def test_ipv6_unique_local_is_rejected() -> None:
    with pytest.raises(UnsafeWebhookDestinationError):
        await validate_webhook_destination(
            "http://[fc00::1]:9999/hook", allow_private_destinations=False
        )


async def test_ipv6_link_local_is_rejected() -> None:
    with pytest.raises(UnsafeWebhookDestinationError):
        await validate_webhook_destination(
            "http://[fe80::1]:9999/hook", allow_private_destinations=False
        )


async def test_allow_private_destinations_bypasses_the_check() -> None:
    await validate_webhook_destination("http://10.89.0.1:18091/ok", allow_private_destinations=True)
