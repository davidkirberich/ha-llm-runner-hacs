"""Optional in-memory end-to-end check against the adjacent add-on's real run.py."""

from __future__ import annotations

import asyncio
import importlib
import json
import os
from pathlib import Path
import sys
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from custom_components.llm_runner.const import TOPIC_EVENTS
from custom_components.llm_runner.protocol import RunnerClient

_repo_default = Path(__file__).resolve().parents[2] / "ha-llm-runner-addon" / "ha-llm-runner"
_addon_dir = Path(os.environ.get("HA_LLM_RUNNER_ADDON_PATH", _repo_default))
_addon = None
_import_error: Exception | None = None
if (_addon_dir / "run.py").is_file():
    sys.path.insert(0, str(_addon_dir))
    try:
        _addon = importlib.import_module("run")
    except Exception as err:  # Optional cross-repository development dependency.
        _import_error = err


@unittest.skipUnless(_addon is not None, "Set HA_LLM_RUNNER_ADDON_PATH to a usable add-on source tree")
class AddonEndToEndTests(unittest.IsolatedAsyncioTestCase):
    async def test_weather_data_task_runs_through_real_addon_callback(self) -> None:
        """Send an integration request into run.on_message and await its real lifecycle reply."""
        run = _addon
        session_id = run.SESSION_ID
        subscribers: dict[str, list] = {}
        addon_messages: list[tuple[str, dict, dict]] = []
        loop = asyncio.get_running_loop()

        class Broker:
            def publish(self, topic, payload, **options):
                message = json.loads(payload) if isinstance(payload, str) else None
                addon_messages.append((topic, message, options))
                if topic == run.EVENT_TOPIC:
                    for callback in tuple(subscribers.get(TOPIC_EVENTS, [])):
                        loop.call_soon_threadsafe(callback, payload, options.get("retain", False))
                return SimpleNamespace(rc=0)

        broker = Broker()

        def subscribe(topic, callback):
            subscribers.setdefault(topic, []).append(callback)

            def unsubscribe():
                subscribers[topic].remove(callback)

            return unsubscribe

        captured_threads = []
        original_run_task_async = run.run_task_async

        def capture_thread(*args, **kwargs):
            thread = original_run_task_async(*args, **kwargs)
            if thread is not None:
                captured_threads.append(thread)
            return thread

        async def publish(topic, payload, qos, retain):
            if qos != 1 or retain:
                raise AssertionError("The integration must publish requests at QoS 1 without retain")
            run.on_message(
                broker,
                None,
                SimpleNamespace(
                    topic=topic,
                    payload=payload.encode("utf-8"),
                    retain=False,
                ),
            )

        client = RunnerClient(subscribe, publish, loop=loop)
        client.start(initially_connected=True)
        client._on_availability(
            json.dumps({"protocol": 1, "session_id": session_id, "state": "online"}),
            True,
        )
        client._on_catalog(
            json.dumps(
                {
                    "protocol": 1,
                    "session_id": session_id,
                    "tasks": [{"task_id": "weather_data", "name": "Weather data"}],
                }
            ),
            True,
        )

        patched = (
            patch.object(run, "load_options", return_value={}),
            patch.object(run, "load_tasks", return_value={"weather_data": {"hours": 0}}),
            patch.object(run, "load_memory", return_value=[]),
            patch.object(run, "run_task_async", side_effect=capture_thread),
            patch.object(run, "TASK_STATUS", {}),
            patch.object(run, "_active_invocations", {}),
            patch.object(run, "_completed_invocations", run.OrderedDict()),
            patch.object(run, "_task_lifecycle", {}),
            patch.object(run, "_ha_config", {}),
        )
        try:
            for patcher in patched:
                patcher.start()
            response = await client.async_execute(
                "weather_data",
                5,
                request_id="weather-e2e",
            )
            for thread in captured_threads:
                thread.join(3)
                self.assertFalse(thread.is_alive())
        finally:
            client.close()
            for patcher in reversed(patched):
                patcher.stop()

        self.assertEqual(response["task_id"], "weather_data")
        self.assertEqual(response["request_id"], "weather-e2e")
        self.assertEqual(response["result"]["data"], None)
        self.assertEqual(
            [message[1]["state"] for message in addon_messages if message[0] == run.EVENT_TOPIC],
            ["triggered", "running", "ok"],
        )
        self.assertTrue(all(message[2] == {"qos": 1, "retain": False}
                            for message in addon_messages if message[0] == run.EVENT_TOPIC))


if _addon is None and _import_error is not None:
    AddonEndToEndTests.__doc__ = f"Skipped because add-on imports failed: {_import_error}"
