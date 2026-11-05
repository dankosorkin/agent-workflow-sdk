# Copyright 2026 Daniel Sorkin
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Kiro agent backend — async ACP client.

Drives a ``kiro-cli acp`` subprocess over JSON-RPC 2.0 (one JSON object per
line on stdin/stdout), turning the session into a structured event stream.
This is the async port of the original synchronous ``src/acp.py``.

The engine only ever calls :meth:`start`, :meth:`invoke` / :meth:`prompt`,
and :meth:`close`. Kiro-specific configuration (engine v2/v3, auth method,
model, agent mode) is internal to this class.

Requires ``kiro-cli`` on PATH; no third-party Python dependency.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any, Literal

from agentflow.backends.base import BaseAgentBackend, PermissionPolicy
from agentflow.errors import BackendError, BackendTransportError
from agentflow.events import (
    Allow,
    BackendEvent,
    BackendRequest,
    ErrorEvent,
    PermissionDecision,
    PermissionOption,
    PermissionRequest,
    TextChunk,
    TextRequest,
    ToolCall,
    ToolResult,
    TurnEnd,
)

__all__ = ["KiroBackend"]

_DEFAULT_CWD = Path.cwd()


class KiroBackend(BaseAgentBackend):
    """Async ACP client for a Kiro agent session."""

    def __init__(
        self,
        agent: str,
        *,
        model: str | None = None,
        engine: str = "v3",
        cwd: Path | str = _DEFAULT_CWD,
        permission: PermissionPolicy,
        close_grace: float = 0.5,
    ) -> None:
        super().__init__(permission=permission)
        self.agent = agent
        self.model = model
        self.engine = engine
        self.cwd = Path(cwd)
        # Per-phase grace on close(): wait this long for a clean exit, then
        # terminate, then (after another grace) kill. Kept short because the
        # v3 engine typically will not exit on stdin close, and we should not
        # block a caller's shutdown for seconds waiting on it.
        self._close_grace = close_grace

        self._proc: asyncio.subprocess.Process | None = None
        self._session_id: str | None = None
        self._request_id = 0

        # Correlates outstanding JSON-RPC requests to their response futures.
        self._pending: dict[int, asyncio.Future[dict[str, Any]]] = {}
        # The active turn's event queue, or None when no turn is in flight.
        self._turn_queue: asyncio.Queue[BackendEvent | _Sentinel] | None = None
        self._turn_text = ""
        self._reader_task: asyncio.Task[None] | None = None
        self._closing = False
        # Permission decisions awaited by the reader, resolved by the generator
        # frame (where the policy — and any engine interrupt — must run).
        self._perm_answers: dict[str, asyncio.Future[PermissionDecision]] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        cmd = ["kiro-cli", "acp"]
        if self.engine:
            cmd += ["--agent-engine", self.engine]
        if self.engine == "v3":
            # v3 selects the agent as a session mode and resolves auth from the
            # CLI credential store; it takes neither --agent nor --model.
            cmd += ["--auth-method", "cli"]
        else:
            cmd += ["--agent", self.agent]

        try:
            self._proc = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=str(self.cwd),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=None,  # inherit parent's stderr
                # ACP responses (e.g. session/new listing every available mode)
                # can exceed asyncio's 64KiB default readline buffer; raise it.
                limit=8 * 1024 * 1024,
                # Put kiro-cli in its own process group so close() can signal
                # the whole tree. The v3 CLI spawns a child (kiro-cli-chat)
                # that outlives a kill of the parent alone, stranding the
                # terminal; killing the group reaps it too. POSIX-only —
                # ignored on platforms without it.
                start_new_session=True,
            )
        except OSError as exc:
            raise BackendTransportError(f"could not start kiro-cli: {exc}") from exc

        self._reader_task = asyncio.create_task(self._read_loop())

        await self._initialize()
        await self._create_session()
        if self.model and self.engine != "v3":
            await self._set_model(self.model)

    async def close(self) -> None:
        self._closing = True
        proc = self._proc
        if proc is None:
            return
        try:
            if proc.stdin and not proc.stdin.is_closing():
                proc.stdin.close()
            # The v3 engine often does not exit on stdin close (it waits on its
            # own idle timeout), so don't block for seconds hoping it will.
            # Give it a short grace to exit cleanly, then escalate by intent:
            # ask it to terminate, then force-kill. On POSIX both operations
            # target the whole process group so the CLI's child process
            # (kiro-cli-chat) is reaped too, not just the parent. Each phase
            # has a brief window so close() returns promptly.
            try:
                await asyncio.wait_for(proc.wait(), timeout=self._close_grace)
            except TimeoutError:
                self._terminate(proc)
                try:
                    await asyncio.wait_for(proc.wait(), timeout=self._close_grace)
                except TimeoutError:
                    self._kill(proc)
        finally:
            if self._reader_task:
                self._reader_task.cancel()
                with contextlib.suppress(asyncio.CancelledError, Exception):
                    await self._reader_task
            self._proc = None
            self._fail_pending(BackendTransportError("backend closed"))

    async def reset_session(self, *, delete_previous: bool = False) -> None:
        """Start a fresh ACP session on the same process; the next turn will use it.

        Re-runs the session setup done by :meth:`start` (``session/new``, then the
        agent mode on engine v3 or the model on v2), so the agent has no memory of
        earlier turns. The process is reused, which is cheaper than a new backend.

        By default the previous session is left alone: no ``session/close`` or
        ``session/delete`` is sent. Against kiro-cli 2.26 that costs the process no
        memory or file descriptors, but every session is a persisted record on disk
        (under ``~/.kiro/sessions``) that survives ``close()``, so a long pipeline
        that resets per step accumulates them. Pass ``delete_previous=True`` to
        delete the previous session with ``session/delete`` once the new one is
        ready; its history is then gone, so keep the default if you want to inspect
        a finished step's conversation afterwards. The new session is created
        FIRST, so a failure to create it leaves the old session intact and in use.
        If the delete itself fails (for example an older kiro-cli without
        ``session/delete``) the error propagates, but the new session is already
        active and the old one is simply left behind.

        Raises :class:`BackendError` before ``start()`` or while a turn is in
        progress.
        """
        if self._proc is None or self._session_id is None:
            raise BackendError("KiroBackend.start() was not called")
        if self._turn_queue is not None:
            raise BackendError("cannot reset the session while a turn is in progress")
        previous = self._session_id
        await self._create_session()
        if self.model and self.engine != "v3":
            await self._set_model(self.model)
        if delete_previous:
            await self._request("session/delete", {"sessionId": previous})

    @classmethod
    def _terminate(cls, proc: asyncio.subprocess.Process) -> None:
        """Ask the child (and, on POSIX, its whole group) to terminate. Never raises.

        POSIX: ``SIGTERM`` to the process group. Windows: ``proc.terminate()``,
        which is already a hard ``TerminateProcess`` there — there is no softer
        step, so the following :meth:`_kill` is a harmless repeat on Windows.
        """
        cls._stop(proc, hard=False)

    @classmethod
    def _kill(cls, proc: asyncio.subprocess.Process) -> None:
        """Force-kill the child (and, on POSIX, its whole group). Never raises.

        POSIX: ``SIGKILL`` to the process group. Windows: ``proc.kill()``.

        Windows: ``taskkill /F /T`` first, so a grandchild such as
        ``kiro-cli-chat`` is reaped with its parent; ``proc.kill()`` follows as a
        harmless repeat (and the fallback if ``taskkill`` is unavailable).
        """
        cls._stop(proc, hard=True)

    @staticmethod
    def _stop(proc: asyncio.subprocess.Process, *, hard: bool) -> None:
        """Stop ``proc`` by intent, never reading a platform-specific name unguarded.

        The child was started with ``start_new_session=True`` so on POSIX it
        leads its own process group and signalling the group reaches its
        descendants. Where process groups or the signal don't exist (Windows),
        or the group has already gone, fall back to signalling just the process.
        """
        killpg = getattr(os, "killpg", None)
        getpgid = getattr(os, "getpgid", None)
        sig = getattr(signal, "SIGKILL" if hard else "SIGTERM", None)
        if killpg is not None and getpgid is not None and sig is not None:
            with contextlib.suppress(ProcessLookupError, PermissionError, OSError, AttributeError):
                killpg(getpgid(proc.pid), sig)
                return
        if hard and sys.platform == "win32":
            # No process groups on Windows: kill the whole tree by pid.
            with contextlib.suppress(OSError, subprocess.SubprocessError):
                subprocess.run(
                    ["taskkill", "/F", "/T", "/PID", str(proc.pid)],
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                    timeout=5,
                    check=False,
                )
        with contextlib.suppress(ProcessLookupError, OSError):
            if hard:
                proc.kill()
            else:
                proc.terminate()

    # ------------------------------------------------------------------
    # Public API — one turn as an event stream
    # ------------------------------------------------------------------

    async def invoke(
        self, request: BackendRequest, *, session: str | None = None
    ) -> AsyncIterator[BackendEvent]:
        if not isinstance(request, TextRequest):
            raise BackendError(f"KiroBackend accepts TextRequest, got {type(request).__name__}")
        if self._session_id is None:
            raise BackendError("KiroBackend.start() was not called")
        if self._turn_queue is not None:
            raise BackendError("a turn is already in progress on this backend")

        queue: asyncio.Queue[BackendEvent | _Sentinel] = asyncio.Queue()
        self._turn_text = ""
        self._turn_queue = queue
        turn_session = self._session_id

        # Fire the prompt request; its response resolves the turn.
        response_future = self._send_request(
            "session/prompt",
            {
                "sessionId": self._session_id,
                "prompt": [{"type": "text", "text": request.text}],
            },
        )

        async def _finish() -> None:
            # Await the RPC response, then enqueue the terminal TurnEnd.
            try:
                result = await response_future
                stop = result.get("stopReason") if isinstance(result, dict) else None
                await queue.put(TurnEnd(text=self._turn_text, stop_reason=stop))
            except Exception as exc:  # transport / RPC failure ends the turn
                await queue.put(ErrorEvent(message=str(exc)))
                await queue.put(TurnEnd(text=self._turn_text, stop_reason="error"))
            finally:
                await queue.put(_SENTINEL)

        finisher = asyncio.create_task(_finish())

        try:
            while True:
                item = await queue.get()
                if isinstance(item, _Sentinel):
                    break
                if isinstance(item, PermissionRequest):
                    # Resolve here, in the node's frame, so an Interactive
                    # policy's ctx.interrupt (and its InterruptError) propagate
                    # out through this generator to the runtime.
                    await self._resolve_permission(item)
                if isinstance(item, TurnEnd):
                    # The turn is over the moment its terminal event exists. Free
                    # the backend NOW, not when this generator is finalised: a
                    # consumer that stops reading at TurnEnd (``return`` or ``break``
                    # inside ``async for``) leaves the generator suspended, and the
                    # next prompt() or reset_session() would otherwise fail with
                    # "a turn is already in progress".
                    self._release_turn(queue)
                yield item
        finally:
            # If we exit the loop early (e.g. an interrupt raised out of the
            # generator), unblock the reader so it doesn't hang on an answer.
            # _release_turn is a no-op when the turn was already released at
            # TurnEnd, so a generator finalised late (garbage collection) cannot
            # disturb a LATER turn that is now using the backend.
            if self._turn_queue is queue:
                # Closed BEFORE TurnEnd: the agent is still working on a turn
                # nobody reads any more (it keeps editing files and spending
                # tokens). Tell it to stop. Best-effort: the process may be gone.
                self._cancel_turn(turn_session)
            self._release_turn(queue)
            await self._drain_finisher(finisher)

    def _cancel_turn(self, session_id: str | None) -> None:
        """Send the ACP ``session/cancel`` notification (no response is expected)."""
        if session_id is None:
            return
        with contextlib.suppress(BackendTransportError, OSError, RuntimeError):
            self._write(
                {"jsonrpc": "2.0", "method": "session/cancel", "params": {"sessionId": session_id}}
            )

    def _release_turn(self, queue: asyncio.Queue[BackendEvent | _Sentinel]) -> None:
        """Mark the turn that owns ``queue`` as finished. Only the owner may do this."""
        if self._turn_queue is queue:
            self._turn_queue = None
            self._cancel_pending_permissions()

    async def _resolve_permission(self, req: PermissionRequest) -> None:
        future = self._perm_answers.get(req.id)
        if future is None or future.done():
            return
        decision = await self.permission.decide(req)
        if not future.done():
            future.set_result(decision)

    def _cancel_pending_permissions(self) -> None:
        for future in self._perm_answers.values():
            if not future.done():
                future.cancel()

    @staticmethod
    async def _drain_finisher(finisher: asyncio.Task[None]) -> None:
        if finisher.done():
            # Surface any exception the finisher captured.
            finisher.result()
            return
        finisher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await finisher

    # ------------------------------------------------------------------
    # Session setup
    # ------------------------------------------------------------------

    async def _initialize(self) -> None:
        await self._request(
            "initialize",
            {
                "protocolVersion": 1,
                "clientCapabilities": {},
                "clientInfo": {"name": "agentflow", "version": "0.1.0"},
            },
        )

    async def _create_session(self) -> None:
        result = await self._request("session/new", {"cwd": str(self.cwd), "mcpServers": []})
        session_id = result.get("sessionId")
        if not session_id:
            raise BackendTransportError(f"session/new returned no sessionId: {result}")
        self._session_id = session_id
        if self.engine == "v3":
            await self._select_agent_mode(result)

    async def _select_agent_mode(self, session_result: dict[str, Any]) -> None:
        modes = session_result.get("modes", {})
        if modes.get("currentModeId") == self.agent:
            return
        available = {m.get("id") for m in modes.get("availableModes", [])}
        if self.agent not in available:
            raise BackendError(f"agent {self.agent!r} not in available modes: {sorted(available)}")
        await self._request(
            "session/set_mode", {"sessionId": self._session_id, "modeId": self.agent}
        )

    async def _set_model(self, model: str) -> None:
        await self._request("session/set_model", {"sessionId": self._session_id, "modelId": model})

    # ------------------------------------------------------------------
    # JSON-RPC transport (async)
    # ------------------------------------------------------------------

    def _next_request_id(self) -> int:
        self._request_id += 1
        return self._request_id

    def _send_request(self, method: str, params: dict[str, Any]) -> asyncio.Future[dict[str, Any]]:
        """Send a request and return a future for its response."""
        request_id = self._next_request_id()
        future: asyncio.Future[dict[str, Any]] = asyncio.get_running_loop().create_future()
        self._pending[request_id] = future
        self._write({"jsonrpc": "2.0", "id": request_id, "method": method, "params": params})
        return future

    async def _request(self, method: str, params: dict[str, Any]) -> dict[str, Any]:
        """Send a request and await its response (used for setup RPCs)."""
        return await self._send_request(method, params)

    def _write(self, message: dict[str, Any]) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise BackendTransportError("kiro-cli process is not running")
        data = json.dumps(message, ensure_ascii=False, separators=(",", ":")) + "\n"
        proc.stdin.write(data.encode("utf-8"))
        # Apply write backpressure: schedule a drain so a slow child stdin
        # cannot let our write buffer grow unbounded. Best-effort — a closing
        # pipe during shutdown must not raise here.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return
        loop.create_task(self._drain_stdin())

    async def _drain_stdin(self) -> None:
        proc = self._proc
        if proc is None or proc.stdin is None:
            return
        with contextlib.suppress(ConnectionResetError, BrokenPipeError, RuntimeError):
            await proc.stdin.drain()

    async def _read_loop(self) -> None:
        """Continuously read newline-delimited JSON and dispatch it."""
        proc = self._proc
        assert proc is not None and proc.stdout is not None
        try:
            while True:
                line = await proc.stdout.readline()
                if line == b"":
                    if not self._closing:
                        self._fail_pending(
                            BackendTransportError(
                                f"kiro-cli terminated unexpectedly (exit {proc.returncode})"
                            )
                        )
                    return
                text = line.strip()
                if not text:
                    continue
                try:
                    message = json.loads(text)
                except json.JSONDecodeError:
                    # A malformed line is fatal to the protocol.
                    self._fail_pending(
                        BackendTransportError(f"invalid JSON from kiro-cli: {text!r}")
                    )
                    return
                await self._dispatch(message)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # unexpected reader failure
            self._fail_pending(BackendTransportError(f"reader loop failed: {exc}"))

    async def _dispatch(self, message: dict[str, Any]) -> None:
        # Agent -> client request (has both method and id): e.g. permission.
        if "method" in message and "id" in message:
            await self._handle_agent_request(message)
            return
        # Notification / streaming (method, no id).
        if "method" in message and "id" not in message:
            self._handle_notification(message)
            return
        # Response to one of our requests.
        request_id = message.get("id")
        if not isinstance(request_id, int):
            return
        future = self._pending.pop(request_id, None)
        if future is None or future.done():
            return
        if "error" in message:
            future.set_exception(
                BackendTransportError(
                    f"RPC error: {json.dumps(message['error'], ensure_ascii=False)}"
                )
            )
            return
        result = message.get("result")
        if result is None:
            future.set_result({})
        elif isinstance(result, dict):
            future.set_result(result)
        else:
            future.set_result({"value": result})

    def _fail_pending(self, exc: Exception) -> None:
        for future in self._pending.values():
            if not future.done():
                future.set_exception(exc)
        self._pending.clear()

    # ------------------------------------------------------------------
    # Agent -> client requests (permission)
    # ------------------------------------------------------------------

    async def _handle_agent_request(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        request_id = message.get("id")
        params = message.get("params", {})

        if method == "session/request_permission":
            await self._handle_permission(request_id, params)
            return

        self._write(
            {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {"code": -32601, "message": f"Unsupported client method: {method}"},
            }
        )

    async def _handle_permission(self, request_id: Any, params: dict[str, Any]) -> None:
        raw_options = params.get("options", [])
        options = tuple(
            PermissionOption(
                option_id=o.get("optionId", ""),
                name=o.get("name", ""),
                kind=o.get("kind", ""),
            )
            for o in raw_options
        )
        req = PermissionRequest(
            id=str(request_id),
            tool=_extract_tool_name(params),
            options=options,
            detail=params,
        )

        # The policy must run in the node's frame (so an Interactive policy can
        # drive an engine interrupt), not here in the reader task. Publish the
        # request to the turn stream and wait for the generator to resolve it.
        loop = asyncio.get_running_loop()
        answer: asyncio.Future[PermissionDecision] = loop.create_future()
        self._perm_answers[req.id] = answer
        self._emit(req)

        try:
            decision = await answer
        except asyncio.CancelledError:
            # The generator tore down before answering (e.g. the turn was
            # suspended by an engine interrupt). Leave the RPC unanswered; the
            # agent process is abandoned with the turn. Do not kill the reader.
            self._perm_answers.pop(req.id, None)
            return
        finally:
            self._perm_answers.pop(req.id, None)

        if isinstance(decision, Allow):
            option_id = decision.option_id or self._default_allow_option(raw_options)
            if option_id is None:
                self._answer_permission(request_id, {"outcome": {"outcome": "cancelled"}})
                return
            self._answer_permission(
                request_id,
                {"outcome": {"outcome": "selected", "optionId": option_id}},
            )
        else:  # Deny
            self._answer_permission(request_id, {"outcome": {"outcome": "cancelled"}})

    @staticmethod
    def _default_allow_option(raw_options: list[dict[str, Any]]) -> str | None:
        for kind in ("allow_always", "allow_once"):
            for o in raw_options:
                if o.get("kind") == kind:
                    return o.get("optionId")
        return None

    def _answer_permission(self, request_id: Any, result: dict[str, Any]) -> None:
        self._write({"jsonrpc": "2.0", "id": request_id, "result": result})

    # ------------------------------------------------------------------
    # Notifications / streaming -> events on the active turn queue
    # ------------------------------------------------------------------

    def _handle_notification(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        if method not in ("session/update", "session/notification"):
            return
        params = message.get("params", {})
        update = params.get("update") or params.get("notification") or params
        if not isinstance(update, dict):
            return

        update_type = update.get("sessionUpdate") or update.get("type") or update.get("kind")
        event = self._notification_to_event(update_type, update)
        if event is not None:
            self._emit(event)

    def _notification_to_event(
        self, update_type: Any, update: dict[str, Any]
    ) -> BackendEvent | None:
        if update_type in ("agent_message_chunk", "AgentMessageChunk"):
            content = update.get("content") or update.get("chunk")
            text = content.get("text") if isinstance(content, dict) else content
            if text:
                self._turn_text += text
                return TextChunk(text=text)
            return None
        if update_type in ("tool_call", "ToolCall"):
            return ToolCall(
                id=str(update.get("toolCallId") or update.get("id") or ""),
                name=update.get("name") or update.get("toolName") or "tool",
                args=update.get("args") or update.get("input") or {},
                title=update.get("title"),
            )
        if update_type in ("tool_call_update", "ToolCallUpdate"):
            return ToolResult(
                id=str(update.get("toolCallId") or update.get("id") or ""),
                status=_map_status(update.get("status")),
                content=update.get("content"),
            )
        return None

    def _emit(self, event: BackendEvent) -> None:
        queue = self._turn_queue
        if queue is not None:
            queue.put_nowait(event)


def _extract_tool_name(params: dict[str, Any]) -> str:
    """Pull the tool name out of a session/request_permission payload.

    Kiro v3 does not use ``toolName``/``tool``; it puts the machine name under
    ``_meta.kiro.toolId`` (and ``_meta.kiro.consent.capability``) and a
    human-readable label under ``toolCall.title``. Fall back through those,
    then to the legacy flat keys, so older shapes still work.
    """
    meta = params.get("_meta")
    if isinstance(meta, dict):
        kiro_meta = meta.get("kiro")
        if isinstance(kiro_meta, dict):
            tool_id = kiro_meta.get("toolId")
            if tool_id:
                return str(tool_id)
            consent = kiro_meta.get("consent")
            if isinstance(consent, dict) and consent.get("capability"):
                return str(consent["capability"])
    tool_call = params.get("toolCall")
    if isinstance(tool_call, dict) and tool_call.get("title"):
        return str(tool_call["title"])
    return params.get("toolName") or params.get("tool") or ""


def _map_status(status: Any) -> Literal["ok", "error", "pending"]:
    if status in ("completed", "success", "ok"):
        return "ok"
    if status in ("failed", "error"):
        return "error"
    return "pending"


class _Sentinel:
    """Marks the end of a turn's event queue."""


_SENTINEL = _Sentinel()
