"""Validated MQTT protocol and correlated task execution."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
import json
import math
import re
import uuid
from typing import Any

from .const import (
    MAX_ALIAS_LENGTH,
    MAX_CATALOG_BYTES,
    MAX_ERROR_LENGTH,
    MAX_EVENT_BYTES,
    MAX_INPUTS,
    MAX_PATH_LENGTH,
    MAX_REQUEST_ID_LENGTH,
    MAX_RESULT_DEPTH,
    MAX_RESULT_NODES,
    MAX_TASKS,
    MAX_TASK_ID_LENGTH,
    MAX_VARIABLE_LENGTH,
    PROTOCOL_VERSION,
    RUN_ID_PATTERN,
    TASK_ID_PATTERN,
    TOPIC_AVAILABILITY,
    TOPIC_EVENTS,
    TOPIC_RUN_PREFIX,
    TOPIC_TASKS,
    UUID_HEX_PATTERN,
)

_RUN_ID_RE = re.compile(RUN_ID_PATTERN)
_TASK_ID_RE = re.compile(TASK_ID_PATTERN)
_UUID_HEX_RE = re.compile(UUID_HEX_PATTERN)
_ALIAS_RE = re.compile(r"[A-Za-z_][A-Za-z0-9_-]{0,63}")
_SAFE_JSON_SCALARS = (str, int, float, bool, type(None))


class RunnerError(Exception):
    """Base error for service failures."""


class RunnerUnavailable(RunnerError):
    """The runner or its catalog is unavailable."""


class RunnerProtocolError(RunnerError):
    """The runner sent data outside the supported protocol."""


class RunnerTaskError(RunnerError):
    """The requested task completed with an error."""


class RunnerDisconnected(RunnerUnavailable):
    """MQTT disconnected while a request was pending."""


class RunnerRestarted(RunnerUnavailable):
    """The runner restarted while a request was pending."""


class RunnerTimeout(RunnerError):
    """The request did not complete before its deadline."""


@dataclass(frozen=True, slots=True)
class TaskResult:
    """Fields returned by a successful task execution."""

    task_id: str
    run_id: str
    request_id: str | None
    result: dict[str, Any]
    queued_at: str
    started_at: str | None
    finished_at: str
    duration: float

    def as_dict(self) -> dict[str, Any]:
        """Return the public service response without absent optional values."""
        response: dict[str, Any] = {
            "task_id": self.task_id,
            "run_id": self.run_id,
            "result": self.result,
            "queued_at": self.queued_at,
            "finished_at": self.finished_at,
            "duration": self.duration,
        }
        if self.request_id is not None:
            response["request_id"] = self.request_id
        if self.started_at is not None:
            response["started_at"] = self.started_at
        return response


@dataclass(slots=True)
class _Waiter:
    task_id: str
    run_id: str
    request_id: str | None
    session_id: str
    future: asyncio.Future[dict[str, Any]]


Subscribe = Callable[[str, Callable[[str, bool], None]], Awaitable[Callable[[], None]]]
Publish = Callable[[str, str, int, bool], Awaitable[None]]


def _is_bounded_text(value: object, limit: int, *, allow_empty: bool = False) -> bool:
    return (
        isinstance(value, str)
        and (allow_empty or bool(value))
        and len(value) <= limit
    )


def _is_uuid_hex(value: object) -> bool:
    return isinstance(value, str) and _UUID_HEX_RE.fullmatch(value) is not None


def _is_task_id(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) <= MAX_TASK_ID_LENGTH
        and _TASK_ID_RE.fullmatch(value) is not None
    )


def _parse_json(payload: str, maximum: int, label: str) -> dict[str, Any]:
    if len(payload.encode("utf-8")) > maximum:
        raise RunnerProtocolError(f"{label} exceeds the {maximum}-byte limit")
    try:
        value = json.loads(payload)
    except (json.JSONDecodeError, UnicodeError) as err:
        raise RunnerProtocolError(f"{label} is not valid JSON") from err
    if not isinstance(value, dict):
        raise RunnerProtocolError(f"{label} must be a JSON object")
    return value


def _validate_json_value(value: Any, *, depth: int = 0, count: list[int] | None = None) -> None:
    if count is None:
        count = [0]
    count[0] += 1
    if count[0] > MAX_RESULT_NODES or depth > MAX_RESULT_DEPTH:
        raise RunnerProtocolError("Task result exceeds the supported nesting limits")
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str) or len(key) > MAX_VARIABLE_LENGTH:
                raise RunnerProtocolError("Task result contains an invalid object key")
            _validate_json_value(child, depth=depth + 1, count=count)
    elif isinstance(value, list):
        for child in value:
            _validate_json_value(child, depth=depth + 1, count=count)
    elif isinstance(value, str):
        if len(value) > MAX_EVENT_BYTES:
            raise RunnerProtocolError("Task result contains an oversized string")
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise RunnerProtocolError("Task result contains a non-finite number")
    elif not isinstance(value, _SAFE_JSON_SCALARS):
        raise RunnerProtocolError("Task result contains a non-JSON value")


def _validate_timestamp(value: object, field: str) -> str:
    if not _is_bounded_text(value, 64):
        raise RunnerProtocolError(f"{field} must be a non-empty ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as err:
        raise RunnerProtocolError(f"{field} must be an ISO timestamp") from err
    if parsed.tzinfo is None:
        raise RunnerProtocolError(f"{field} must include a timezone")
    return value


def _validate_duration(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise RunnerProtocolError("duration must be a non-negative number")
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise RunnerProtocolError("duration must be a finite non-negative number")
    return number


def validate_availability(payload: str) -> tuple[str, str]:
    """Validate and return a v1 availability session and state."""
    message = _parse_json(payload, MAX_CATALOG_BYTES, "Availability")
    if set(message) != {"protocol", "session_id", "state"}:
        raise RunnerProtocolError("Availability has unexpected or missing fields")
    if message["protocol"] != PROTOCOL_VERSION or isinstance(message["protocol"], bool):
        raise RunnerProtocolError("Unsupported availability protocol")
    if not _is_uuid_hex(message["session_id"]):
        raise RunnerProtocolError("Availability session_id must be a UUID hex string")
    if not isinstance(message["state"], str) or message["state"] not in {"online", "offline"}:
        raise RunnerProtocolError("Availability state must be online or offline")
    return message["session_id"], message["state"]


def validate_catalog(payload: str) -> tuple[str, frozenset[str]]:
    """Validate and return a v1 catalog session and its task IDs."""
    message = _parse_json(payload, MAX_CATALOG_BYTES, "Task catalog")
    if set(message) != {"protocol", "session_id", "tasks"}:
        raise RunnerProtocolError("Task catalog has unexpected or missing fields")
    if message["protocol"] != PROTOCOL_VERSION or isinstance(message["protocol"], bool):
        raise RunnerProtocolError("Unsupported task catalog protocol")
    session_id = message["session_id"]
    if not _is_uuid_hex(session_id):
        raise RunnerProtocolError("Task catalog session_id must be a UUID hex string")
    tasks = message["tasks"]
    if not isinstance(tasks, list) or len(tasks) > MAX_TASKS:
        raise RunnerProtocolError("Task catalog tasks must be a bounded list")
    task_ids: set[str] = set()
    for task in tasks:
        if not isinstance(task, dict) or set(task) != {"task_id"}:
            raise RunnerProtocolError("Each task must contain only task_id")
        task_id = task["task_id"]
        if not _is_task_id(task_id):
            raise RunnerProtocolError("Task catalog contains an invalid task_id")
        if task_id in task_ids:
            raise RunnerProtocolError("Task catalog contains duplicate task_ids")
        task_ids.add(task_id)
    return session_id, frozenset(task_ids)


def _correlation_fields(message: Mapping[str, Any]) -> tuple[str, str, str] | None:
    task_id, run_id, session_id = (
        message.get("task_id"),
        message.get("run_id"),
        message.get("session_id"),
    )
    if (
        _is_task_id(task_id)
        and isinstance(run_id, str)
        and _RUN_ID_RE.fullmatch(run_id) is not None
        and _is_uuid_hex(session_id)
    ):
        return task_id, run_id, session_id
    return None


def _validate_event(message: dict[str, Any], waiter: _Waiter) -> dict[str, Any]:
    allowed = {
        "protocol",
        "session_id",
        "task_id",
        "run_id",
        "state",
        "request_id",
        "queued_at",
        "started_at",
        "finished_at",
        "duration",
        "result",
        "error",
    }
    if set(message) - allowed:
        raise RunnerProtocolError("Runner event has unexpected fields")
    if message.get("protocol") != PROTOCOL_VERSION or isinstance(message.get("protocol"), bool):
        raise RunnerProtocolError("Unsupported runner event protocol")
    if message.get("task_id") != waiter.task_id or message.get("run_id") != waiter.run_id:
        raise RunnerProtocolError("Runner event correlation fields do not match")
    if message.get("session_id") != waiter.session_id:
        raise RunnerProtocolError("Runner event belongs to a different runner session")
    request_id = message.get("request_id")
    if request_id is not None and not _is_bounded_text(request_id, MAX_REQUEST_ID_LENGTH):
        raise RunnerProtocolError("Runner event contains an invalid request_id")
    if waiter.request_id is not None and request_id != waiter.request_id:
        raise RunnerProtocolError("Runner event request_id does not match")
    if "queued_at" not in message:
        raise RunnerProtocolError("Runner event is missing queued_at")
    _validate_timestamp(message["queued_at"], "queued_at")
    if "started_at" in message:
        _validate_timestamp(message["started_at"], "started_at")
    state = message.get("state")
    if not isinstance(state, str) or state not in {"triggered", "running", "ok", "error"}:
        raise RunnerProtocolError("Runner event has an invalid lifecycle state")
    if state in {"triggered", "running"}:
        if "finished_at" in message or "duration" in message or "result" in message or "error" in message:
            raise RunnerProtocolError(f"{state} event contains terminal-only fields")
    else:
        if "finished_at" not in message or "duration" not in message:
            raise RunnerProtocolError("Terminal runner event is missing completion fields")
        _validate_timestamp(message["finished_at"], "finished_at")
        _validate_duration(message["duration"])
        if state == "ok":
            result = message.get("result")
            if not isinstance(result, dict) or "error" in message:
                raise RunnerProtocolError("Successful runner event must contain a result object")
            _validate_json_value(result)
        else:
            if "result" in message or not _is_bounded_text(message.get("error"), MAX_ERROR_LENGTH):
                raise RunnerProtocolError("Error runner event must contain only a redacted error string")
    return message


class RunnerClient:
    """Own MQTT subscriptions, readiness, and all in-flight task waiters."""

    def __init__(self, subscribe: Subscribe, publish: Publish, *, loop: asyncio.AbstractEventLoop,
                 on_tasks_changed: Callable[[], None] | None = None):
        self._subscribe = subscribe
        self._publish = publish
        self._loop = loop
        self._unsubscribers: list[Callable[[], None]] = []
        self._waiters: dict[str, _Waiter] = {}
        self._catalogs: dict[str, frozenset[str]] = {}
        self._catalog_errors: dict[str, str] = {}
        self._session_id: str | None = None
        self._connected = False
        self._ready = asyncio.Event()
        self._closed = False
        self._on_tasks_changed = on_tasks_changed

    def _notify_tasks_changed(self) -> None:
        if self._on_tasks_changed is not None:
            self._on_tasks_changed()

    @property
    def session_id(self) -> str | None:
        """Current runner session when availability and catalog agree."""
        return self._session_id if self._ready.is_set() else None

    @property
    def tasks(self) -> frozenset[str]:
        """Task IDs of the current validated catalog."""
        if not self._ready.is_set() or self._session_id is None:
            return frozenset()
        return self._catalogs.get(self._session_id, frozenset())

    async def async_start(self, initially_connected: bool) -> None:
        """Subscribe before any request can be published."""
        if self._closed:
            raise RunnerUnavailable("The integration client has been unloaded")
        self._connected = initially_connected
        for topic, handler in (
            (TOPIC_AVAILABILITY, self._on_availability),
            (TOPIC_TASKS, self._on_catalog),
            (TOPIC_EVENTS, self._on_event),
        ):
            self._unsubscribers.append(await self._subscribe(topic, handler))

    def add_unsubscriber(self, unsubscribe: Callable[[], None]) -> None:
        """Track an additional MQTT status subscription for unload cleanup."""
        self._unsubscribers.append(unsubscribe)

    def on_connection_change(self, connected: bool) -> None:
        """Handle MQTT integration connection-state notifications."""
        if self._closed or connected == self._connected:
            return
        self._connected = connected
        if not connected:
            self._session_id = None
            self._ready.clear()
            self._fail_waiters(RunnerDisconnected("MQTT disconnected during task execution"))
            self._notify_tasks_changed()

    def _refresh_ready(self) -> None:
        if (
            not self._closed
            and self._connected
            and self._session_id is not None
            and (
                self._session_id in self._catalogs
                or self._session_id in self._catalog_errors
            )
        ):
            self._ready.set()
        else:
            self._ready.clear()
        self._notify_tasks_changed()

    def _on_availability(self, payload: str, retained: bool) -> None:
        try:
            session_id, state = validate_availability(payload)
        except RunnerProtocolError:
            return
        if state == "offline":
            if self._session_id is not None and self._session_id != session_id:
                return
            self._session_id = None
            self._ready.clear()
            self._fail_waiters(RunnerUnavailable("The LLM Runner add-on is offline"))
            self._notify_tasks_changed()
            return
        if self._session_id is not None and self._session_id != session_id:
            self._fail_waiters(RunnerRestarted("The LLM Runner add-on restarted during task execution"))
            self._ready.clear()
        self._session_id = session_id
        self._refresh_ready()

    def _on_catalog(self, payload: str, retained: bool) -> None:
        try:
            session_id, tasks = validate_catalog(payload)
        except RunnerProtocolError as err:
            try:
                envelope = _parse_json(payload, MAX_CATALOG_BYTES, "Task catalog")
            except RunnerProtocolError:
                return
            candidate = envelope.get("session_id")
            if _is_uuid_hex(candidate) and candidate == self._session_id:
                self._catalog_errors[candidate] = str(err)
                self._catalogs.pop(candidate, None)
                self._refresh_ready()
            elif _is_uuid_hex(candidate):
                self._catalog_errors[candidate] = str(err)
                while len(self._catalog_errors) > 4:
                    self._catalog_errors.pop(next(iter(self._catalog_errors)))
            return
        self._catalog_errors.pop(session_id, None)
        self._catalogs[session_id] = tasks
        while len(self._catalogs) > 4:
            old_session = next(iter(self._catalogs))
            self._catalogs.pop(old_session, None)
            self._catalog_errors.pop(old_session, None)
        self._refresh_ready()

    def _on_event(self, payload: str, retained: bool) -> None:
        if retained or self._closed:
            return
        try:
            message = _parse_json(payload, MAX_EVENT_BYTES, "Runner event")
        except RunnerProtocolError:
            return
        correlation = _correlation_fields(message)
        if correlation is None:
            return
        task_id, run_id, session_id = correlation
        waiter = self._waiters.get(run_id)
        if (
            waiter is None
            or waiter.task_id != task_id
            or waiter.session_id != session_id
            or waiter.future.done()
        ):
            return
        try:
            event = _validate_event(message, waiter)
        except RunnerProtocolError as err:
            waiter.future.set_exception(err)
            return
        if event["state"] in {"triggered", "running"}:
            return
        waiter.future.set_result(event)

    def _fail_waiters(self, error: Exception) -> None:
        for waiter in tuple(self._waiters.values()):
            if not waiter.future.done():
                waiter.future.set_exception(type(error)(str(error)))

    async def async_wait_ready(self, timeout: float) -> None:
        """Wait until online availability and a same-session catalog are present."""
        if self._closed:
            raise RunnerUnavailable("The integration client has been unloaded")
        try:
            await asyncio.wait_for(self._ready.wait(), timeout)
        except TimeoutError as err:
            raise RunnerUnavailable(
                "The runner did not publish matching online availability and task catalog "
                f"within {timeout:g} seconds"
            ) from err
        if self._session_id in self._catalog_errors:
            raise RunnerProtocolError(
                f"The runner task catalog is invalid: {self._catalog_errors[self._session_id]}"
            )

    async def async_execute(
        self,
        task_id: str,
        timeout: float,
        files: Mapping[str, str] | None = None,
        variables: Mapping[str, str] | None = None,
        request_id: str | None = None,
    ) -> dict[str, Any]:
        """Publish one invocation and await only its matching non-retained reply."""
        validate_invocation(task_id, timeout, files, variables, request_id)
        await self.async_wait_ready(timeout)
        if task_id not in self.tasks:
            raise RunnerUnavailable(f"Task '{task_id}' is not present in the runner catalog")
        if not self._connected or self._session_id is None:
            raise RunnerDisconnected("MQTT or the runner is disconnected")

        run_id = uuid.uuid4().hex
        future: asyncio.Future[dict[str, Any]] = self._loop.create_future()
        waiter = _Waiter(task_id, run_id, request_id, self._session_id, future)
        self._waiters[run_id] = waiter
        request: dict[str, Any] = {"run_id": run_id}
        if request_id is not None:
            request["request_id"] = request_id
        if files:
            request["files"] = dict(files)
        if variables:
            request["variables"] = dict(variables)
        try:
            async with asyncio.timeout(timeout):
                await self._publish(
                    f"{TOPIC_RUN_PREFIX}/{task_id}",
                    json.dumps(request, separators=(",", ":"), ensure_ascii=False),
                    1,
                    False,
                )
                event = await future
        except TimeoutError as err:
            raise RunnerTimeout(f"Task '{task_id}' did not finish within {timeout:g} seconds") from err
        finally:
            self._waiters.pop(run_id, None)
            if not future.done():
                future.cancel()

        if event["state"] == "error":
            raise RunnerTaskError(event["error"])
        return TaskResult(
            task_id=task_id,
            run_id=run_id,
            request_id=request_id,
            result=event["result"],
            queued_at=event["queued_at"],
            started_at=event.get("started_at"),
            finished_at=event["finished_at"],
            duration=float(event["duration"]),
        ).as_dict()

    def close(self) -> None:
        """Unsubscribe and explicitly fail pending service calls."""
        if self._closed:
            return
        self._closed = True
        self._ready.clear()
        self._session_id = None
        self._fail_waiters(RunnerUnavailable("The LLM Runner integration was unloaded"))
        for unsubscribe in self._unsubscribers:
            unsubscribe()
        self._unsubscribers.clear()
        self._notify_tasks_changed()


def validate_invocation(
    task_id: object,
    timeout: object,
    files: Mapping[str, str] | None,
    variables: Mapping[str, str] | None,
    request_id: object,
) -> None:
    """Validate all service data before publishing any part of a request."""
    if not _is_task_id(task_id):
        raise RunnerProtocolError("task_id must contain 1-128 letters, digits, '_' or '-'")
    if (
        isinstance(timeout, bool)
        or not isinstance(timeout, (int, float))
        or not math.isfinite(float(timeout))
        or float(timeout) <= 0
    ):
        raise RunnerProtocolError("timeout must be a positive finite number of seconds")
    if request_id is not None and not _is_bounded_text(request_id, MAX_REQUEST_ID_LENGTH):
        raise RunnerProtocolError("request_id must be a non-empty string up to 4096 characters")
    for label, values in (("files", files), ("variables", variables)):
        if values is None:
            continue
        if not isinstance(values, Mapping) or len(values) > MAX_INPUTS:
            raise RunnerProtocolError(f"{label} must be an object with at most {MAX_INPUTS} entries")
        for alias, value in values.items():
            if not isinstance(alias, str) or _ALIAS_RE.fullmatch(alias) is None or len(alias) > MAX_ALIAS_LENGTH:
                raise RunnerProtocolError(f"{label} contains an invalid alias")
            if label == "files":
                if not _is_bounded_text(value, MAX_PATH_LENGTH):
                    raise RunnerProtocolError("File paths must be non-empty strings up to 4096 characters")
            elif not _is_bounded_text(value, MAX_VARIABLE_LENGTH):
                raise RunnerProtocolError("Variable values must be non-empty strings up to 4096 characters")
