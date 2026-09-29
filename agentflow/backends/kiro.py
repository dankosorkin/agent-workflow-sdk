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
import json
from pathlib import Path
from typing import Any, AsyncIterator

from agentflow.backends.base import BaseAgentBackend, PermissionPolicy
from agentflow.errors import BackendError, BackendTransportError
from agentflow.events import (
    Allow,
    BackendEvent,
    BackendRequest,
    ErrorEvent,
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
        permission: PermissionPolicy | None = None,
    ) -> None:
        super().__init__(permission=permission)
        self.agent = agent
        self.model = model
        self.engine = engine
        self.cwd = Path(cwd)

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
            try:
                await asyncio.wait_for(proc.wait(), timeout=3)
            except asyncio.TimeoutError:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2)
                except asyncio.TimeoutError:
                    proc.kill()
        finally:
            if self._reader_task:
                self._reader_task.cancel()
                try:
                    await self._reader_task
                except (asyncio.CancelledError, Exception):
                    pass
            self._proc = None
            self._fail_pending(BackendTransportError("backend closed"))

    # ------------------------------------------------------------------
    # Public API — one turn as an event stream
    # ------------------------------------------------------------------

    async def invoke(
        self, request: BackendRequest, *, session: str | None = None
    ) -> AsyncIterator[BackendEvent]:
        if not isinstance(request, TextRequest):
            raise BackendError(
                f"KiroBackend accepts TextRequest, got {type(request).__name__}"
            )
        if self._session_id is None:
            raise BackendError("KiroBackend.start() was not called")
        if self._turn_queue is not None:
            raise BackendError("a turn is already in progress on this backend")

        queue: asyncio.Queue[BackendEvent | _Sentinel] = asyncio.Queue()
        self._turn_text = ""
        self._turn_queue = queue

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
                if item is _SENTINEL:
                    break
                if isinstance(item, PermissionRequest):
                    # Resolve here, in the node's frame, so an Interactive
                    # policy's ctx.interrupt (and its InterruptError) propagate
                    # out through this generator to the runtime.
                    await self._resolve_permission(item)
                yield item
        finally:
            self._turn_queue = None
            # If we exit the loop early (e.g. an interrupt raised out of the
            # generator), unblock the reader so it doesn't hang on an answer.
            self._cancel_pending_permissions()
            await self._drain_finisher(finisher)

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
    async def _drain_finisher(finisher: "asyncio.Task[None]") -> None:
        if finisher.done():
            # Surface any exception the finisher captured.
            finisher.result()
            return
        finisher.cancel()
        try:
            await finisher
        except asyncio.CancelledError:
            pass

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
        result = await self._request(
            "session/new", {"cwd": str(self.cwd), "mcpServers": []}
        )
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
            raise BackendError(
                f"agent {self.agent!r} not in available modes: {sorted(available)}"
            )
        await self._request(
            "session/set_mode", {"sessionId": self._session_id, "modeId": self.agent}
        )

    async def _set_model(self, model: str) -> None:
        await self._request(
            "session/set_model", {"sessionId": self._session_id, "modelId": model}
        )

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
        # drain is fire-and-forget here; the reader loop owns backpressure

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

        self._write({
            "jsonrpc": "2.0",
            "id": request_id,
            "error": {"code": -32601, "message": f"Unsupported client method: {method}"},
        })

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
            tool=params.get("toolName") or params.get("tool") or "",
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

        update_type = (
            update.get("sessionUpdate") or update.get("type") or update.get("kind")
        )
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


def _map_status(status: Any) -> str:
    if status in ("completed", "success", "ok"):
        return "ok"
    if status in ("failed", "error"):
        return "error"
    return "pending"


class _Sentinel:
    """Marks the end of a turn's event queue."""


_SENTINEL = _Sentinel()
