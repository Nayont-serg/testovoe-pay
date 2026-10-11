from __future__ import annotations

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit

_DNS_RESOLUTION_TIMEOUT_SECONDS = 5.0


class UnsafeWebhookDestinationError(Exception):
    def __init__(self, webhook_url: str, hostname: str) -> None:
        super().__init__(
            f"webhook destination {webhook_url!r} is disallowed for hostname {hostname!r}"
        )
        self.webhook_url = webhook_url
        self.hostname = hostname


class WebhookDestinationUnavailableError(Exception):
    def __init__(self, webhook_url: str, hostname: str) -> None:
        super().__init__(
            f"webhook destination {webhook_url!r} hostname {hostname!r} could not be resolved "
            "right now"
        )
        self.webhook_url = webhook_url
        self.hostname = hostname


def _is_disallowed_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return address.is_loopback or address.is_private or address.is_link_local


async def _resolve_addresses(
    webhook_url: str, hostname: str
) -> list[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    loop = asyncio.get_running_loop()
    try:
        address_infos = await asyncio.wait_for(
            loop.getaddrinfo(hostname, None), timeout=_DNS_RESOLUTION_TIMEOUT_SECONDS
        )
    except UnicodeError as error:
        raise UnsafeWebhookDestinationError(webhook_url, hostname) from error
    except (socket.gaierror, TimeoutError) as error:
        raise WebhookDestinationUnavailableError(webhook_url, hostname) from error
    return [ipaddress.ip_address(address_info[4][0]) for address_info in address_infos]


async def validate_webhook_destination(
    webhook_url: str, *, allow_private_destinations: bool
) -> None:
    if allow_private_destinations:
        return

    hostname = urlsplit(webhook_url).hostname
    if hostname is None:
        raise UnsafeWebhookDestinationError(webhook_url, "")

    resolved_addresses = await _resolve_addresses(webhook_url, hostname)
    for address in resolved_addresses:
        if _is_disallowed_address(address):
            raise UnsafeWebhookDestinationError(webhook_url, hostname)
