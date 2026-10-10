"""Focused tests for the MQTT wire protocol and in-flight request handling."""

from __future__ import annotations

import asyncio
import json
from typing import Any
import unittest
import uuid

from custom_components.llm_runner.const import TOPIC_EVENTS
from custom_components.llm_runner.protocol import (
    RunnerClient,
    RunnerDisconnected,
    RunnerProtocolError,
    RunnerRestarted,
    RunnerTaskError,
    RunnerTimeout,
    RunnerUnavailable,
    validate_catalog,
)

SESSION = uuid.uuid4().hex
NEW_SESSION = uuid.uuid4().hex
STAMP = "2026-10-10T10:00:00+00:00"


class MemoryBroker:
    """Minimal injectable broker boundary used by the production client."""

    def __init__(self) -> None:
        self.subscriptions: dict[str, list[Any]] = {}
        self.published: list[tuple[str, str, int, bool]] = []
        self.publish_handler: Any = None

    async def subscribe(self, topic: str, callback: Any) -> Any:
        callbacks = self.subscriptions.setdefault(topic, [])
        callbacks.append(callback)

        def unsubscribe() -> None:
            callbacks.remove(callback)

        return unsubscribe

    async def publish(self, topic: str, payload: str, qos: int, retain: bool) -> None:
        self.published.append((topic, payload, qos, retain))
        if self.publish_handler is not None:
            await self.publish_handler(topic, payload, qos, retain)

    def emit(self, topic: str, payload: dict[str, Any], *, retained: bool = False) -> None:
        message = json.dumps(payload, separators=(",", ":"))
        for callback in tuple(self.subscriptions.get(topic, [])):
            callback(message, retained)


class RunnerClientTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.broker = MemoryBroker()
        self.client = RunnerClient(
            self.broker.subscribe,
            self.broker.publish,
            loop=asyncio.get_running_loop(),
        )
        await self.client.async_start(initially_connected=True)
        self.broker.emit(
            "ha_llm_runner/availability",
            {"protocol": 1, "session_id": SESSION, "state": "online"},
            retained=True,
        )
        self.broker.emit(
            "ha_llm_runner/tasks",
            {
                "protocol": 1,
                "session_id": SESSION,
                "tasks": [{"task_id": "weather"}],
            },
            retained=True,
        )

    async def asyncTearDown(self) -> None:
        self.client.close()

    @staticmethod
    def event(request: dict[str, Any], **updates: Any) -> dict[str, Any]:
        return {
            "protocol": 1,
            "session_id": SESSION,
            "task_id": "weather",
            "run_id": request["run_id"],
            "state": "ok",
            "queued_at": STAMP,
            "started_at": STAMP,
            "finished_at": STAMP,
            "duration": 0.25,
            "result": {"summary": "Cloudy"},
            **({"request_id": request["request_id"]} if "request_id" in request else {}),
            **updates,
        }

    def respond(self, request: dict[str, Any], *, retained: bool = False, **updates: Any) -> None:
        event = self.event(request, **updates)
        if updates.get("result", object()) is None:
            event.pop("result", None)
        self.broker.emit(TOPIC_EVENTS, event, retained=retained)

    async def request_started(self, *, request_id: str | None = None) -> tuple[asyncio.Task, dict[str, Any]]:
        captured: list[dict[str, Any]] = []

        async def capture(topic: str, payload: str, qos: int, retain: bool) -> None:
            captured.append(json.loads(payload))

        self.broker.publish_handler = capture
        service = asyncio.create_task(
            self.client.async_execute("weather", 2, request_id=request_id)
        )
        for _ in range(30):
            if captured:
                return service, captured[0]
            await asyncio.sleep(0)
        self.fail("The invocation was not published")

    async def test_immediate_reply_is_caught_and_metadata_is_returned(self) -> None:
        async def immediate(topic: str, payload: str, qos: int, retain: bool) -> None:
            request = json.loads(payload)
            self.respond(request)

        self.broker.publish_handler = immediate
        response = await self.client.async_execute("weather", 2, request_id="automation-7")

        topic, payload, qos, retain = self.broker.published[-1]
        request = json.loads(payload)
        self.assertEqual(topic, "ha_llm_runner/run/weather")
        self.assertEqual(qos, 1)
        self.assertFalse(retain)
        self.assertEqual(request["request_id"], "automation-7")
        self.assertRegex(request["run_id"], r"^[0-9a-f]{32}$")
        self.assertEqual(
            response,
            {
                "task_id": "weather",
                "run_id": request["run_id"],
                "request_id": "automation-7",
                "result": {"summary": "Cloudy"},
                "queued_at": STAMP,
                "started_at": STAMP,
                "finished_at": STAMP,
                "duration": 0.25,
            },
        )

    async def test_same_task_concurrent_requests_match_by_run_id(self) -> None:
        first = asyncio.create_task(self.client.async_execute("weather", 2))
        second = asyncio.create_task(self.client.async_execute("weather", 2))
        for _ in range(30):
            if len(self.broker.published) == 2:
                break
            await asyncio.sleep(0)
        requests = [json.loads(item[1]) for item in self.broker.published]
        self.assertEqual(len(requests), 2)
        self.respond(requests[1], result={"summary": "second"})
        self.respond(requests[0], result={"summary": "first"})
        responses = await asyncio.gather(first, second)
        self.assertEqual(
            {response["run_id"]: response["result"]["summary"] for response in responses},
            {
                requests[0]["run_id"]: "first",
                requests[1]["run_id"]: "second",
            },
        )

    async def test_retained_completion_is_never_used_as_a_reply(self) -> None:
        service, request = await self.request_started()
        self.respond(request, retained=True)
        self.assertFalse(service.done())
        service.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await service

    async def test_task_error_is_returned_as_a_service_failure(self) -> None:
        service, request = await self.request_started()
        self.respond(
            request,
            state="error",
            result=None,
            error="Task input was rejected",
        )
        with self.assertRaisesRegex(RunnerTaskError, "input was rejected"):
            await service

    async def test_invalid_result_or_protocol_fails_only_its_waiter(self) -> None:
        service, request = await self.request_started()
        self.respond(request, protocol=99)
        with self.assertRaises(RunnerProtocolError):
            await service

        service, request = await self.request_started()
        self.respond(request, result=["not", "an", "object"])
        with self.assertRaises(RunnerProtocolError):
            await service

    async def test_disconnect_and_runner_restart_fail_pending_calls(self) -> None:
        service, _ = await self.request_started()
        self.client.on_connection_change(False)
        with self.assertRaises(RunnerDisconnected):
            await service

        self.client.on_connection_change(True)
        self.broker.emit(
            "ha_llm_runner/availability",
            {"protocol": 1, "session_id": SESSION, "state": "online"},
            retained=True,
        )
        self.broker.emit(
            "ha_llm_runner/tasks",
            {
                "protocol": 1,
                "session_id": SESSION,
                "tasks": [{"task_id": "weather"}],
            },
            retained=True,
        )
        service, _ = await self.request_started()
        self.broker.emit(
            "ha_llm_runner/availability",
            {"protocol": 1, "session_id": NEW_SESSION, "state": "online"},
            retained=True,
        )
        with self.assertRaises(RunnerRestarted):
            await service

    async def test_availability_and_catalog_can_arrive_in_either_order(self) -> None:
        other = RunnerClient(
            self.broker.subscribe,
            self.broker.publish,
            loop=asyncio.get_running_loop(),
        )
        await other.async_start(initially_connected=True)
        other._on_catalog(
            json.dumps(
                {
                    "protocol": 1,
                    "session_id": NEW_SESSION,
                    "tasks": [{"task_id": "weather"}],
                }
            ),
            True,
        )
        other._on_availability(
            json.dumps({"protocol": 1, "session_id": NEW_SESSION, "state": "online"}),
            True,
        )
        await other.async_wait_ready(0.1)
        self.assertIn("weather", other.tasks)
        other.close()

    async def test_stale_catalog_is_not_paired_with_a_different_session(self) -> None:
        other = RunnerClient(
            self.broker.subscribe,
            self.broker.publish,
            loop=asyncio.get_running_loop(),
        )
        await other.async_start(initially_connected=True)
        other._on_availability(
            json.dumps({"protocol": 1, "session_id": NEW_SESSION, "state": "online"}),
            True,
        )
        other._on_catalog(
            json.dumps(
                {
                    "protocol": 1,
                    "session_id": SESSION,
                    "tasks": [{"task_id": "weather"}],
                }
            ),
            True,
        )
        with self.assertRaises(RunnerUnavailable):
            await other.async_wait_ready(0.01)
        other.close()

    async def test_invalid_catalog_is_reported_and_duplicates_are_rejected(self) -> None:
        with self.assertRaisesRegex(RunnerProtocolError, "only task_id"):
            validate_catalog(json.dumps({
                "protocol": 1, "session_id": SESSION,
                "tasks": [{"task_id": "weather", "name": "Weather"}],
            }))
        with self.assertRaises(RunnerProtocolError):
            validate_catalog(
                json.dumps(
                    {
                        "protocol": 1,
                        "session_id": SESSION,
                        "tasks": [
                            {"task_id": "weather"},
                            {"task_id": "weather"},
                        ],
                    }
                )
            )
        other = RunnerClient(
            self.broker.subscribe,
            self.broker.publish,
            loop=asyncio.get_running_loop(),
        )
        await other.async_start(initially_connected=True)
        other._on_availability(
            json.dumps({"protocol": 1, "session_id": NEW_SESSION, "state": "online"}),
            True,
        )
        other._on_catalog(
            json.dumps(
                {
                    "protocol": 1,
                    "session_id": NEW_SESSION,
                    "tasks": [{"task_id": "bad/id"}],
                }
            ),
            True,
        )
        with self.assertRaisesRegex(RunnerProtocolError, "catalog is invalid"):
            await other.async_wait_ready(0.1)
        other.close()

    async def test_unload_unsubscribes_and_fails_pending_waiters(self) -> None:
        service, _ = await self.request_started()
        self.client.close()
        with self.assertRaises(RunnerUnavailable):
            await service
        self.assertTrue(all(not callbacks for callbacks in self.broker.subscriptions.values()))
