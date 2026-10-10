"""Home Assistant integration for the HA LLM Runner add-on."""

from __future__ import annotations

import asyncio
import copy
from collections.abc import Callable
import logging
from typing import Any
from pathlib import Path

from .const import (
    DEFAULT_TIMEOUT,
    DOMAIN,
    SERVICE_EXECUTE,
    SETUP_TIMEOUT,
)
from .protocol import RunnerClient, RunnerError

_LOGGER = logging.getLogger(__name__)
_ENTRY_CLIENTS: dict[str, RunnerClient] = {}
_DESCRIPTION_KEY = f"{DOMAIN}_execute_description"


def _load_action_description() -> dict[str, Any]:
    import yaml

    with Path(__file__).with_name("services.yaml").open(encoding="utf-8") as stream:
        return yaml.safe_load(stream)[SERVICE_EXECUTE]


def _update_task_selector(hass: Any, tasks: frozenset[str]) -> None:
    from homeassistant.helpers.service import async_set_service_schema

    description = copy.deepcopy(hass.data[_DESCRIPTION_KEY])
    description["fields"]["task_id"]["selector"] = {
        "select": {
            "options": sorted(tasks),
            "mode": "dropdown",
            "custom_value": False,
        }
    }
    async_set_service_schema(hass, DOMAIN, SERVICE_EXECUTE, description)


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
    hass.data[_DESCRIPTION_KEY] = await hass.async_add_executor_job(_load_action_description)
    _update_task_selector(hass, {})
    return True


async def async_setup_entry(hass: Any, entry: Any) -> bool:
    """Set up subscriptions and require a live runner catalog before loading."""
    from homeassistant.components import mqtt
    from homeassistant.core import callback
    from homeassistant.exceptions import ConfigEntryNotReady

    if not mqtt.is_connected(hass):
        raise ConfigEntryNotReady("The Home Assistant MQTT integration is not connected")

    loop = asyncio.get_running_loop()
    client: RunnerClient

    async def subscribe(topic: str, handler: Callable[[str, bool], None]) -> Callable[[], None]:
        @callback
        def message_received(message: Any) -> None:
            handler(message.payload, message.retain)

        return await mqtt.async_subscribe(hass, topic, message_received, qos=1)

    async def publish(topic: str, payload: str, qos: int, retain: bool) -> None:
        await mqtt.async_publish(hass, topic, payload, qos=qos, retain=retain)

    @callback
    def tasks_changed() -> None:
        _update_task_selector(hass, client.tasks)

    client = RunnerClient(subscribe, publish, loop=loop, on_tasks_changed=tasks_changed)
    @callback
    def connection_changed(connected: bool) -> None:
        client.on_connection_change(connected)

    entry_id = entry.entry_id
    try:
        await client.async_start(initially_connected=True)
        client.add_unsubscriber(
            mqtt.async_subscribe_connection_status(hass, connection_changed)
        )
        client.on_connection_change(mqtt.is_connected(hass))
        await client.async_wait_ready(SETUP_TIMEOUT)
    except RunnerError as err:
        client.close()
        _ENTRY_CLIENTS.pop(entry_id, None)
        _LOGGER.warning("Runner setup unavailable: %s", err)
        raise ConfigEntryNotReady(str(err)) from err
    except (Exception, asyncio.CancelledError):
        client.close()
        _ENTRY_CLIENTS.pop(entry_id, None)
        raise
    _ENTRY_CLIENTS[entry_id] = client
    return True


async def async_unload_entry(hass: Any, entry: Any) -> bool:
    """Clean up MQTT subscriptions and abort active requests on unload."""
    client = _ENTRY_CLIENTS.pop(entry.entry_id, None)
    if client is not None:
        client.close()
    _update_task_selector(hass, {})
    return True
