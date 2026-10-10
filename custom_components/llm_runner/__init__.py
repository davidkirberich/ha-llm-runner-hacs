"""Home Assistant integration for the HA LLM Runner add-on."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
import logging
from typing import Any

from .const import (
    DEFAULT_TIMEOUT,
    DOMAIN,
    SERVICE_EXECUTE,
    SETUP_TIMEOUT,
)
from .protocol import RunnerClient, RunnerError

_LOGGER = logging.getLogger(__name__)
_ENTRY_CLIENTS: dict[str, RunnerClient] = {}


def _service_schema(data: dict[str, Any]) -> dict[str, Any]:
    import voluptuous as vol

    timeout = data.get("timeout", DEFAULT_TIMEOUT)
    files = data.get("files")
    variables = data.get("variables")
    if files is not None and not isinstance(files, dict):
        raise vol.Invalid("files must be a mapping from aliases to paths")
    if variables is not None and not isinstance(variables, dict):
        raise vol.Invalid("variables must be a mapping from aliases to strings")
    return {
        **data,
        "timeout": timeout,
        "files": files,
        "variables": variables,
    }


def _build_service_schema() -> Any:
    import voluptuous as vol

    return vol.Schema(
        vol.All(
            {
                vol.Required("task_id"): str,
                vol.Optional("timeout", default=DEFAULT_TIMEOUT): vol.Any(int, float),
                vol.Optional("files"): dict,
                vol.Optional("variables"): dict,
                vol.Optional("request_id"): str,
            },
            _service_schema,
        ),
        extra=vol.PREVENT_EXTRA,
    )


async def async_setup(hass: Any, config: dict[str, Any]) -> bool:
    """Register the response-only task action even before an entry is configured."""
    from homeassistant.core import SupportsResponse
    from homeassistant.exceptions import HomeAssistantError

    async def async_execute(call: Any) -> dict[str, Any]:
        clients = tuple(_ENTRY_CLIENTS.values())
        if len(clients) != 1:
            raise HomeAssistantError(
                "Exactly one HA LLM Runner integration entry must be loaded to execute a task"
            )
        try:
            return await clients[0].async_execute(
                call.data["task_id"],
                call.data["timeout"],
                call.data.get("files"),
                call.data.get("variables"),
                call.data.get("request_id"),
            )
        except RunnerError as err:
            raise HomeAssistantError(str(err)) from err

    hass.services.async_register(
        DOMAIN,
        SERVICE_EXECUTE,
        async_execute,
        schema=_build_service_schema(),
        supports_response=SupportsResponse.ONLY,
    )
    return True


async def async_setup_entry(hass: Any, entry: Any) -> bool:
    """Set up subscriptions and require a live runner catalog before loading."""
    from homeassistant.components import mqtt
    from homeassistant.exceptions import ConfigEntryNotReady

    if not mqtt.is_connected(hass):
        raise ConfigEntryNotReady("The Home Assistant MQTT integration is not connected")

    loop = asyncio.get_running_loop()
    client: RunnerClient

    def subscribe(topic: str, callback: Callable[[str, bool], None]) -> Callable[[], None]:
        def message_received(message: Any) -> None:
            callback(message.payload, message.retain)

        return awaitable_subscribe(hass, topic, message_received)

    async def publish(topic: str, payload: str, qos: int, retain: bool) -> None:
        await mqtt.async_publish(hass, topic, payload, qos=qos, retain=retain)

    client = RunnerClient(subscribe, publish, loop=loop)
    client.start(initially_connected=True)
    client.add_unsubscriber(
        mqtt.async_subscribe_connection_status(hass, client.on_connection_change)
    )
    client.on_connection_change(mqtt.is_connected(hass))
    entry_id = entry.entry_id
    _ENTRY_CLIENTS[entry_id] = client
    try:
        await client.async_wait_ready(SETUP_TIMEOUT)
    except RunnerError as err:
        client.close()
        _ENTRY_CLIENTS.pop(entry_id, None)
        _LOGGER.warning("Runner setup unavailable: %s", err)
        raise ConfigEntryNotReady(str(err)) from err
    except Exception:
        client.close()
        _ENTRY_CLIENTS.pop(entry_id, None)
        raise
    return True


def awaitable_subscribe(
    hass: Any, topic: str, callback: Callable[[Any], None]
) -> Callable[[], None]:
    """Subscribe at QoS 1 using Home Assistant's MQTT integration API."""
    from homeassistant.components import mqtt

    return mqtt.async_subscribe(hass, topic, callback, qos=1)


async def async_unload_entry(hass: Any, entry: Any) -> bool:
    """Clean up MQTT subscriptions and abort active requests on unload."""
    client = _ENTRY_CLIENTS.pop(entry.entry_id, None)
    if client is not None:
        client.close()
    return True
