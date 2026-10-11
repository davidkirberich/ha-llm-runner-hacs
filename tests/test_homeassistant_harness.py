"""Fake-HA MQTT tests for integration entry setup, action response, and unload."""

from __future__ import annotations

import asyncio
import importlib
import json
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch
import uuid


class FakeBroker:
    def __init__(self) -> None:
        self.subscriptions: dict[str, list[object]] = {}
        self.published: list[tuple[str, str, int, bool]] = []
        self.publish_handler = None

    def subscribe(self, topic: str, callback: object):
        self.subscriptions.setdefault(topic, []).append(callback)

        def unsubscribe() -> None:
            self.subscriptions[topic].remove(callback)

        return unsubscribe

    async def publish(self, topic: str, payload: str, qos: int, retain: bool) -> None:
        self.published.append((topic, payload, qos, retain))
        if self.publish_handler is not None:
            await self.publish_handler(topic, payload, qos, retain)

    def emit(self, topic: str, payload: dict, *, retained: bool = False) -> None:
        message = SimpleNamespace(payload=json.dumps(payload), retain=retained)
        for callback in tuple(self.subscriptions.get(topic, [])):
            callback(message)


class FakeHomeAssistant:
    def __init__(self, broker: FakeBroker) -> None:
        self.broker = broker
        self.connected = True
        self.connection_callbacks = []
        self.services = SimpleNamespace(async_register=self._register_service)
        self.registered_services = {}
        self.data = {}
        self.descriptions = {}

    async def async_add_executor_job(self, function, *args):
        return function(*args)

    def _register_service(self, domain, name, handler, **options) -> None:
        self.registered_services[(domain, name)] = (handler, options)


def _fake_voluptuous() -> ModuleType:
    module = ModuleType("voluptuous")
    module.PREVENT_EXTRA = object()
    module.Invalid = ValueError
    module.Required = lambda name: name
    module.Optional = lambda name, **kwargs: name
    module.Any = lambda *values: values
    module.All = lambda *values: values
    module.Schema = lambda schema, **kwargs: schema
    return module


class IntegrationHarnessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.broker = FakeBroker()
        self.hass = FakeHomeAssistant(self.broker)
        self.entry = SimpleNamespace(entry_id=f"test-{uuid.uuid4().hex}")
        self.session_id = uuid.uuid4().hex
        self.mqtt = ModuleType("homeassistant.components.mqtt")
        self.mqtt.is_connected = lambda hass: hass.connected

        async def async_subscribe(hass, topic, callback, *, qos=0):
            self.assertEqual(qos, 1)
            self.assertTrue(getattr(callback, "_hass_callback", False))
            return hass.broker.subscribe(topic, callback)

        def async_subscribe_connection_status(hass, callback):
            hass.connection_callbacks.append(callback)

            def unsubscribe() -> None:
                hass.connection_callbacks.remove(callback)

            return unsubscribe

        async def async_publish(hass, topic, payload, *, qos, retain):
            await hass.broker.publish(topic, payload, qos, retain)

        self.mqtt.async_subscribe = async_subscribe
        self.mqtt.async_subscribe_connection_status = async_subscribe_connection_status
        self.mqtt.async_publish = async_publish

        homeassistant = ModuleType("homeassistant")
        components = ModuleType("homeassistant.components")
        components.mqtt = self.mqtt
        exceptions = ModuleType("homeassistant.exceptions")

        class HomeAssistantError(Exception):
            pass

        class ConfigEntryNotReady(HomeAssistantError):
            pass

        exceptions.HomeAssistantError = HomeAssistantError
        exceptions.ConfigEntryNotReady = ConfigEntryNotReady
        core = ModuleType("homeassistant.core")

        class SupportsResponse:
            ONLY = "only"
            OPTIONAL = "optional"

        core.SupportsResponse = SupportsResponse
        def callback(handler):
            handler._hass_callback = True
            return handler
        core.callback = callback
        helpers = ModuleType("homeassistant.helpers")
        service = ModuleType("homeassistant.helpers.service")
        def async_set_service_schema(hass, domain, name, description):
            hass.descriptions[(domain, name)] = description
        service.async_set_service_schema = async_set_service_schema
        modules = {
            "homeassistant": homeassistant,
            "homeassistant.components": components,
            "homeassistant.components.mqtt": self.mqtt,
            "homeassistant.exceptions": exceptions,
            "homeassistant.core": core,
            "homeassistant.helpers": helpers,
            "homeassistant.helpers.service": service,
            "voluptuous": _fake_voluptuous(),
        }
        self.module_patch = patch.dict("sys.modules", modules)
        self.module_patch.start()
        self.addCleanup(self.module_patch.stop)
        self.integration = importlib.import_module("custom_components.llm_runner")
        await self.integration.async_setup(self.hass, {})

        self.setup_task = asyncio.create_task(
            self.integration.async_setup_entry(self.hass, self.entry)
        )
        for _ in range(30):
            if len(self.broker.subscriptions) == 3:
                break
            await asyncio.sleep(0)
        self.broker.emit(
            "ha_llm_runner/tasks",
            {
                "protocol": 1,
                "session_id": self.session_id,
                "tasks": [{"task_id": "weather"}],
            },
            retained=True,
        )
        self.broker.emit(
            "ha_llm_runner/availability",
            {"protocol": 1, "session_id": self.session_id, "state": "online"},
            retained=True,
        )
        await self.setup_task

    async def asyncTearDown(self) -> None:
        if self.entry.entry_id in self.integration._ENTRY_CLIENTS:
            await self.integration.async_unload_entry(self.hass, self.entry)
        self.module_patch.stop()

    async def test_service_allows_optional_response_and_returns_result(self) -> None:
        handler, options = self.hass.registered_services[("llm_runner", "execute")]
        self.assertEqual(options["supports_response"], "optional")

        async def runner(topic, payload, qos, retain):
            request = json.loads(payload)
            self.broker.emit(
                "ha_llm_runner/events",
                {
                    "protocol": 1,
                    "session_id": self.session_id,
                    "task_id": "weather",
                    "run_id": request["run_id"],
                    "state": "ok",
                    "queued_at": "2026-10-10T10:00:00+00:00",
                    "finished_at": "2026-10-10T10:00:01+00:00",
                    "duration": 1.0,
                    "result": {"summary": "Cloudy"},
                },
            )

        self.broker.publish_handler = runner
        response = await handler(
            SimpleNamespace(
                data={
                    "task_id": "weather",
                    "timeout": 10,
                    "files": None,
                    "variables": None,
                    "request_id": None,
                }
            )
        )
        self.assertEqual(response["result"], {"summary": "Cloudy"})
        self.assertEqual(self.broker.published[-1][2:], (1, False))

    async def test_unload_unsubscribes_every_mqtt_listener(self) -> None:
        await self.integration.async_unload_entry(self.hass, self.entry)
        self.assertTrue(all(not callbacks for callbacks in self.broker.subscriptions.values()))
        self.assertEqual(self.hass.connection_callbacks, [])
        self.assertEqual(self.task_options(), [])

    def task_options(self):
        return self.hass.descriptions[("llm_runner", "execute")]["fields"]["task_id"]["selector"]["select"]["options"]

    async def test_task_dropdown_tracks_catalog_and_preserves_other_fields(self) -> None:
        self.assertEqual(self.task_options(), ["weather"])
        self.broker.emit("ha_llm_runner/tasks", {
            "protocol": 1, "session_id": self.session_id,
            "tasks": [{"task_id": "z"}, {"task_id": "weather"}],
        }, retained=True)
        self.assertEqual(self.task_options(), ["weather", "z"])
        fields = self.hass.descriptions[("llm_runner", "execute")]["fields"]
        self.assertEqual(set(fields), {"task_id", "timeout", "files", "variables", "request_id"})
        self.assertEqual(fields["timeout"]["default"], 180)
        self.broker.emit("ha_llm_runner/tasks", {
            "protocol": 1, "session_id": self.session_id, "tasks": [],
        }, retained=True)
        self.assertEqual(self.task_options(), [])

    async def test_task_dropdown_clears_on_disconnect_and_recovers(self) -> None:
        self.broker.emit("ha_llm_runner/availability", {
            "protocol": 1, "session_id": self.session_id, "state": "offline",
        }, retained=True)
        self.assertEqual(self.task_options(), [])
        self.broker.emit("ha_llm_runner/availability", {
            "protocol": 1, "session_id": self.session_id, "state": "online",
        }, retained=True)
        self.assertEqual(len(self.task_options()), 1)
        for callback in self.hass.connection_callbacks:
            callback(False)
        self.assertEqual(self.task_options(), [])

    async def test_setup_timeout_cleans_real_async_subscription_callbacks(self) -> None:
        await self.integration.async_unload_entry(self.hass, self.entry)
        with patch.object(self.integration, "SETUP_TIMEOUT", 0.01):
            with self.assertRaisesRegex(Exception, "matching online availability"):
                await self.integration.async_setup_entry(self.hass, self.entry)
        self.assertNotIn(self.entry.entry_id, self.integration._ENTRY_CLIENTS)
        self.assertTrue(all(not callbacks for callbacks in self.broker.subscriptions.values()))
        self.assertEqual(self.hass.connection_callbacks, [])

    async def test_partial_subscription_failure_cleans_previous_subscriptions(self) -> None:
        await self.integration.async_unload_entry(self.hass, self.entry)
        original = self.mqtt.async_subscribe
        count = 0
        async def failing_subscribe(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                raise RuntimeError("subscription failed")
            return await original(*args, **kwargs)
        with patch.object(self.mqtt, "async_subscribe", failing_subscribe):
            with self.assertRaisesRegex(RuntimeError, "subscription failed"):
                await self.integration.async_setup_entry(self.hass, self.entry)
        self.assertTrue(all(not callbacks for callbacks in self.broker.subscriptions.values()))
        self.assertNotIn(self.entry.entry_id, self.integration._ENTRY_CLIENTS)
